package server

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/eug48/fhir/utils"
	"github.com/pkg/errors"

	"gopkg.in/mgo.v2/bson"

	"github.com/eug48/fhir/models"
	"github.com/eug48/fhir/models2"
	"github.com/eug48/fhir/search"
	"github.com/gin-gonic/gin"
	"github.com/golang/glog"
	"go.opencensus.io/trace"
)

// BatchController handles FHIR batch operations via input bundles
type BatchController struct {
	DAL    DataAccessLayer
	Config Config
}

// NewBatchController creates a new BatchController based on the passed in DAL
func NewBatchController(dal DataAccessLayer, config Config) *BatchController {
	return &BatchController{
		DAL:    dal,
		Config: config,
	}
}

type response struct {
	httpStatus int
	err        error
	errOutcome *models.OperationOutcome
	reply      interface{}
}

func newFailureResponse(httpStatus int, err error, outcome *models.OperationOutcome) *response {
	return &response{httpStatus: httpStatus, errOutcome: outcome, err: err}
}

func sendReply(httpStatus int, reply interface{}) *response {
	return &response{httpStatus: httpStatus, reply: reply}
}

func badStructure(err error) *response {
	outcome := models.CreateOpOutcome("fatal", "structure", "", err.Error())
	return newFailureResponse(http.StatusBadRequest, err, outcome)
}
func badValue(err error) *response {
	outcome := models.CreateOpOutcome("fatal", "value", "", err.Error())
	return newFailureResponse(http.StatusBadRequest, err, outcome)
}
func brokenInvariant(err error) *response {
	outcome := models.CreateOpOutcome("fatal", "invariant", "", err.Error())
	return newFailureResponse(http.StatusBadRequest, err, outcome)
}
func multipleMatches(err error) *response {
	outcome := models.CreateOpOutcome("fatal", "multiple-matches", "", err.Error())
	return newFailureResponse(http.StatusBadRequest, err, outcome)
}
func notFound(err error) *response {
	outcome := models.CreateOpOutcome("fatal", "not-found", "", err.Error())
	return newFailureResponse(http.StatusBadRequest, err, outcome)
}
func internalError(err error) *response {
	outcome := models.CreateOpOutcome("fatal", "exception", "", err.Error())
	return newFailureResponse(http.StatusInternalServerError, err, outcome)
}
func internalErrorWithStatus(httpStatus int, err error) *response {
	outcome := models.CreateOpOutcome("fatal", "exception", "", err.Error())
	return newFailureResponse(httpStatus, err, outcome)
}

// Handles batch and transaction requests
func (b *BatchController) Post(c *gin.Context) {

	req := c.Request

	// Start trace span
	ctx, span := trace.StartSpan(req.Context(), "FHIR POST")
	defer span.End()

	// Get HTTP headers
	customDbName := c.GetHeader("Db")
	provenanceHeader := strings.TrimSpace(c.GetHeader("X-Provenance"))

	// Load FHIR request resource (should be a Bundle)
	bundleResource, err := FHIRBind(c, b.Config.ValidatorURL)
	if err != nil {
		response := badStructure(err)
		c.AbortWithStatusJSON(response.httpStatus, response.errOutcome)
		return
	}

	bundle, err := bundleResource.AsShallowBundle(b.Config.FailedRequestsDir)
	if err != nil {
		response := badStructure(err)
		c.JSON(response.httpStatus, response.reply)
		return
	}

	// retry if transaction
	attemptsLeft := 1
	if bundle.Type == "transaction" {
		attemptsLeft = 3
	}

	var response *response
	for attemptsLeft > 0 {
		glog.Infof("FHIR POST: attempts left: %d", attemptsLeft)
		attemptsLeft -= 1

		response = b.postInner(ctx, span, c, bundle, customDbName, provenanceHeader)

		if response.reply != nil {
			// success
			if c.GetBool("SendXML") {
				converterInt := c.MustGet("FhirFormatConverter")
				converter := converterInt.(*FhirFormatConverter)
				converter.SendXML(response.httpStatus, response.reply, c)
			} else {
				c.JSON(response.httpStatus, response.reply)
			}
			return
		}

		if response.err != nil && strings.Contains(response.err.Error(), "WriteConflict") {
			// retry

			// must reload bundle since it gets modified in placed (e.g. entry.Request = nil)
			bundle, err = bundleResource.AsShallowBundle(b.Config.FailedRequestsDir)
			if err != nil {
				response := badStructure(errors.Wrap(err, "subsequent AsShallowBundle failed"))
				c.JSON(response.httpStatus, response.reply)
				return
			}

			continue
		} else {
			break
		}
	}

	if response.err != nil {
		c.AbortWithStatusJSON(response.httpStatus, response.errOutcome)
	}

}

// Handles batch and transaction requests
func (b *BatchController) postInner(ctx context.Context, span *trace.Span, c *gin.Context, bundle *models2.ShallowBundle, customDbName string, provenanceHeader string) *response {

	req := c.Request

	// Sort & validate bundle entries
	entries, response := sortBundleEntries(bundle)
	if response != nil {
		return response
	}

	// start DB session +- transaction
	session := b.DAL.StartSession(ctx, customDbName)
	defer session.Finish()

	var transaction bool
	switch bundle.Type {
	case "transaction":
		glog.V(2).Info("starting transaction")
		transaction = true
		err := session.StartTransaction()
		if err != nil {
			return internalError(errors.Wrap(err, "error starting MongoDB transaction"))
		}
	case "batch":
		glog.V(2).Info("starting batch")
		transaction = false

		if provenanceHeader != "" {
			return brokenInvariant(errors.Errorf("X-Provenance header is only supported from transactions"))
		}

		// TODO: If type is batch, ensure there are no interdependent resources

	default:
		return badValue(fmt.Errorf("Bundle type is neither 'batch' nor 'transaction'"))
	}

	span.AddAttributes(trace.BoolAttribute("transaction", transaction))

	// Now loop through the entries, assigning new IDs to those that are POST or Conditional PUT and fixing any
	// references to reference the new ID.
	_, spanForResolvingIDs := trace.StartSpan(ctx, "resolving IDs")
	defer spanForResolvingIDs.End()
	refMap := make(map[string]string)
	newIDs := make([]string, len(entries))
	createStatus := make([]string, len(entries))
	for i, entry := range entries {
		if entry.Request.Method == "POST" {

			id := ""

			if len(entry.Request.IfNoneExist) > 0 {
				// Conditional Create
				query := search.Query{Resource: entry.Request.Url, Query: entry.Request.IfNoneExist}
				existingIds, err := session.FindIDs(query)
				if err != nil {
					return internalError(err)
				}
				glog.V(3).Infof("  conditional create (%s?%s): existing: %v", entry.Request.Url, entry.Request.IfNoneExist, existingIds)

				if len(existingIds) == 0 {
					createStatus[i] = "201"
				} else if len(existingIds) == 1 {
					createStatus[i] = "200"
					id = existingIds[0]
				} else if len(existingIds) > 1 {
					createStatus[i] = "412" // HTTP 412 - Precondition Failed
				}
			} else {
				// Unconditional create
				createStatus[i] = "201"
			}

			if createStatus[i] == "201" {
				// Create a new ID
				id = bson.NewObjectId().Hex()
				glog.V(3).Infof("    create (%s): new id: %s", entry.Request.Url, id)
				newIDs[i] = id
			}

			if len(id) > 0 {
				// Add id to the reference map
				refMap[entry.FullUrl] = entry.Request.Url + "/" + id
				glog.V(3).Infof("    need to rewrite %s --> %s", entry.FullUrl, entry.Request.Url+"/"+id)
				// Rewrite the FullUrl using the new ID
				entry.FullUrl = b.Config.responseURL(req, entry.Request.Url, id).String()
			}

		} else if entry.Request.Method == "PUT" && isConditional(entry) {

			glog.V(3).Infof("  conditional PUT: %s", entry.Request.Url)

			// We need to process conditionals referencing temp IDs in a second pass, so skip them here
			if hasTempID(entry.Request.Url) {
				glog.V(3).Info("    hasTempID")
				continue
			}

			if err := b.resolveConditionalPut(req, session, i, entry, newIDs, refMap); err != nil {
				return internalError(err)
			}
			glog.V(3).Infof("    resolved to: %s", entry.Request.Url)
		}
	}
	spanForResolvingIDs.End()
	spanForResolvingIDs = nil // gracefully handled by deferred End()

	// Second pass to take care of conditionals referencing temporary IDs.  Known limitation: if a conditional
	// references a temp ID also defined by a conditional, we error out if it hasn't been resolved yet -- too many
	// rabbit holes.
	_, spanForConditionalTemporaryIDs := trace.StartSpan(ctx, "resolving conditional temporary IDs")
	defer spanForConditionalTemporaryIDs.End()
	for i, entry := range entries {
		if entry.Request.Method == "PUT" && isConditional(entry) {
			// Use a regex to swap out the temp IDs with the new IDs
			for oldID, ref := range refMap {
				re := regexp.MustCompile("([=,])(" + oldID + "|" + url.QueryEscape(oldID) + ")(&|,|$)")
				origUrl := entry.Request.Url
				entry.Request.Url = re.ReplaceAllString(origUrl, "${1}"+ref+"${3}")
				glog.V(3).Infof("  replaced %s --> %s", origUrl, entry.Request.Url)
			}

			if hasTempID(entry.Request.Url) {
				return internalErrorWithStatus(http.StatusNotImplemented, errors.New("Cannot resolve conditionals referencing other conditionals"))
			}

			if err := b.resolveConditionalPut(req, session, i, entry, newIDs, refMap); err != nil {
				return internalError(err)
			}
			glog.V(3).Infof("    resolved to %s", entry.Request.Url)
		}
	}
	spanForConditionalTemporaryIDs.End()
	spanForConditionalTemporaryIDs = nil // gracefully handled by deferred End()

	// Process references
	_, spanForResolvingReferences := trace.StartSpan(ctx, "resolving references")
	defer spanForResolvingReferences.End()
	references, err := bundle.GetAllReferences()
	if err != nil {
		return badStructure(err)
	}
	for _, reference := range references {

		if _, alreadyMapped := refMap[reference]; alreadyMapped {
			glog.V(3).Infof("  reference already mapped: %s", reference)
			continue
		}

		// Conditional references
		// TODO: as per spec this needs to include in-bundle resources..
		queryPos := strings.Index(reference, "?")
		if queryPos >= 0 {

			if bundle.Type != "transaction" {
				return brokenInvariant(errors.New("conditional references are only allowed in transactions, not batches"))
			}
			glog.V(3).Infof("  conditional reference: %s", reference)

			resourceType := reference[0:queryPos]
			queryString := reference[queryPos+1:]
			searchQuery := search.Query{Resource: resourceType, Query: queryString}
			ids, err := session.FindIDs(searchQuery)
			if err != nil {
				return internalError(errors.Wrapf(err, "lookup of conditional reference failed (%s)", reference))
			}
			glog.V(3).Infof("    ids: %v", ids)

			if len(ids) == 1 {
				refMap[reference] = resourceType + "/" + ids[0]
			} else if len(ids) == 0 {
				return notFound(errors.Errorf("no matches for conditional reference (%s)", reference))
			} else {
				return multipleMatches(errors.Errorf("multiple matches for conditional reference (%s)", reference))
			}
		}
	}

	// When being converted to BSON references will be updated to reflect newly assigned or conditional IDs
	bundle.SetTransformReferencesMap(refMap)
	spanForResolvingReferences.End()
	spanForResolvingReferences = nil // gracefully handled by deferred End()

	// Handle If-Match
	var spanForIfMatch *trace.Span
	for _, entry := range entries {
		switch entry.Request.Method {
		case "PUT":
			if entry.Request.IfMatch != "" {
				glog.V(3).Infof(" PUT %s, If-Match: %s", entry.Request.Url, entry.Request.IfMatch)

				if spanForIfMatch == nil {
					_, spanForIfMatch = trace.StartSpan(ctx, "handling If-Match")
					defer spanForIfMatch.End()
				}

				parts := strings.SplitN(entry.Request.Url, "/", 2)
				if len(parts) != 2 { // TODO: refactor
					return badStructure(fmt.Errorf("Couldn't identify resource and id to put from %s", entry.Request.Url))
				}
				id := parts[1]

				conditionalVersionId, err := utils.ETagToVersionId(entry.Request.IfMatch)
				if err != nil {
					return badValue(fmt.Errorf("Couldn't parse If-Match: %s", entry.Request.IfMatch))
				}

				currentResource, err := session.Get(id, entry.Resource.ResourceType())
				if err == ErrNotFound {
					glog.V(3).Infof("   current resource not found")
					entry.Response = &models.BundleEntryResponseComponent{
						Status:  "404",
						Outcome: models.CreateOpOutcome("error", "not-found", "", "Existing resource not found when handling If-Match"),
					}
					entry.Resource = nil
				} else if err != nil {
					err = errors.Wrapf(err, "failed to get current resource while processing If-Match for %s", entry.Request.Url)
					return internalError(err)
				} else if conditionalVersionId != currentResource.VersionId() {
					glog.V(3).Infof("   conflict with current resource")
					entry.Response = &models.BundleEntryResponseComponent{
						Status:  "409",
						Outcome: models.CreateOpOutcome("error", "conflict", "", fmt.Sprintf("Version mismatch when handling If-Match (current=%s wanted=%s)", currentResource.VersionId(), conditionalVersionId)),
					}
					entry.Resource = nil
				}
			}
		}
	}
	spanForIfMatch.End()
	spanForIfMatch = nil // gracefully handled by deferred End()

	// If have an error for a transaction, do not proceeed
	proceed := true
	if transaction {
		for _, entry := range entries {
			if entry.Response != nil && entry.Response.Outcome != nil {

				// FIXME: ensure it is a "failed" outcome

				glog.V(3).Infof("  transaction aborting due to %s %s: %v", entry.Request.Method, entry.Request.Url, entry.Response.Outcome)

				proceed = false
				break
			}
		}
	}

	// Make the changes in the database and update the entry responses
	concurrency := 1
	if !transaction {
		concurrency = b.Config.BatchConcurrency
	}
	if len(entries) <= 1 {
		concurrency = 1
	}
	if proceed {
		if concurrency == 1 {
			glog.V(4).Info(" executing serially")
			// transactions or concurrency disabled
			for i, entry := range entries {
				response = b.doRequest(req, transaction, session, i, entry, createStatus, newIDs)
				if response != nil {
					return response
				}
			}
		} else {
			glog.V(4).Infof(" executing with concurrency capped to %d", concurrency)

			// batches - try to do in parallel with capped concurrency (as in https://pocketgophers.com/limit-concurrent-use/)
			var wg sync.WaitGroup
			semaphore := make(chan bool, concurrency)

			for i, _ := range entries {
				wg.Add(1)

				go func(i int) {
					defer wg.Done()

					semaphore <- true // "acquire" by writing to channel with capped capacty
					defer func() {
						<-semaphore // "release" by reading from channel
					}()

					// have to start a new session as mongo-driver warns that they aren't goroutine-safe
					// (sessions do come from a pool)
					newSession := b.DAL.StartSession(ctx, customDbName)

					entry := entries[i]
					response = b.doRequest(req, transaction, newSession, i, entry, createStatus, newIDs)
					newSession.Finish()
					if response != nil {
						panic("doRequest should always return nil error in batches")
					}
				}(i)
			}
			wg.Wait()
		}
	}

	if transaction {
		for _, entry := range entries {
			// For failing transactions return a single operation-outcome
			if entry.Response != nil && entry.Response.Outcome != nil {

				glog.V(3).Infof("  transaction failing due to: %v", entry.Response)

				status, err := strconv.Atoi(entry.Response.Status)
				if err != nil {
					panic(fmt.Errorf("bad Response.Status (%s)", entry.Response.Status))
				}

				return sendReply(status, entry.Response.Outcome)
			}
		}

		response = b.processProvenanceHeader(provenanceHeader, c, entries, session)
		if response != nil {
			return response
		}

		var start time.Time
		if glog.V(4) {
			start = time.Now()
			glog.V(4).Infof("    starting transaction commit")
		}
		session.CommmitIfTransaction()
		if glog.V(4) {
			glog.V(4).Infof("    finished transaction commit in %v", time.Since(start))
		}
	}

	if proceed {
		total := uint32(len(entries))
		bundle.Total = &total
		bundle.Type = fmt.Sprintf("%s-response", bundle.Type)

		return sendReply(http.StatusOK, bundle)
	} else {
		return internalError(errors.New("invalid state (proceed is false)"))
	}

}

func (b *BatchController) doRequest(req *http.Request, transaction bool, session DataAccessSession, i int, entry *models2.ShallowBundleEntryComponent, createStatus []string, newIDs []string) *response {
	err := b.doRequestInner(req, session, i, entry, createStatus, newIDs)

	if err != nil {
		glog.V(4).Infof("  --> ERROR %+v", err)
	}
	if entry.Response != nil {
		glog.V(11).Infof("  --> %s", entry.Response.DebugString())
	} else {
		glog.V(4).Infof("  --> nil Response")
	}
	if entry.Resource != nil {
		glog.V(11).Infof("  --> %s", entry.Resource.JsonBytes())
	} else {
		glog.V(4).Infof("  --> nil Resource")
	}

	if err != nil {
		statusCode, outcome := ErrorToOpOutcome(err)
		if transaction {
			glog.V(2).Infof("  transaction failed for %s %s: %d %v", entry.Request.Method, entry.Request.Url, statusCode, outcome)
			return newFailureResponse(statusCode, err, outcome)
		} else {
			glog.V(2).Infof("  batch entry failed for %s %s: %d %v", entry.Request.Method, entry.Request.Url, statusCode, outcome)
			entry.Resource = nil
			entry.Request = nil
			entry.Response = &models.BundleEntryResponseComponent{
				Status:  strconv.Itoa(statusCode),
				Outcome: outcome,
			}
			return nil // continue onwards
		}
	}
	return nil
}

func (b *BatchController) doRequestInner(req *http.Request, session DataAccessSession, i int, entry *models2.ShallowBundleEntryComponent, createStatus []string, newIDs []string) error {
	glog.V(3).Infof("  doRequest %s %s", entry.Request.Method, entry.Request.Url)
	if entry.Response != nil {
		// already handled (e.g. conditional update returned 409)
		glog.V(3).Infof("  already handled (%s)", entry.Response.DebugString())
		return nil
	}

	switch entry.Request.Method {
	case "DELETE":
		if !isConditional(entry) {
			// It's a normal DELETE
			parts := strings.SplitN(entry.Request.Url, "/", 2)
			if len(parts) != 2 {
				return fmt.Errorf("Couldn't identify resource and id to delete from %s", entry.Request.Url)
			}
			glog.V(3).Infof("    normal delete")
			if _, err := session.Delete(parts[1], parts[0]); err != nil && err != ErrNotFound {
				return errors.Wrapf(err, "failed to delete %s", entry.Request.Url)
			}
		} else {
			// It's a conditional (query-based) delete
			parts := strings.SplitN(entry.Request.Url, "?", 2)
			query := search.Query{Resource: parts[0], Query: parts[1]}
			glog.V(3).Infof("    conditional delete")
			if _, err := session.ConditionalDelete(query); err != nil {
				return errors.Wrapf(err, "failed to conditional-delete %s", entry.Request.Url)
			}
		}

		entry.Request = nil
		entry.Response = &models.BundleEntryResponseComponent{
			Status: "204",
		}
	case "POST":

		entry.Response = &models.BundleEntryResponseComponent{
			Status:   createStatus[i],
			Location: entry.FullUrl,
		}

		if createStatus[i] == "201" {
			// creating
			err := session.PostWithID(newIDs[i], entry.Resource)
			if err != nil {
				return errors.Wrapf(err, "failed to create %s", entry.Request.Url)
			}
			updateEntryMeta(entry)
		} else if createStatus[i] == "200" {
			// have one existing resource
			components := strings.Split(entry.FullUrl, "/")
			existingId := components[len(components)-1]

			existingResource, err := session.Get(existingId, entry.Request.Url)
			if err != nil {
				return errors.Wrapf(err, "failed to get existing resource during conditional create of %s", entry.Request.Url)
			}
			entry.Resource = existingResource
			updateEntryMeta(entry)
		} else if createStatus[i] == "412" {
			entry.Response.Outcome = models.CreateOpOutcome("error", "duplicate", "", "search criteria were not selective enough")
			entry.Resource = nil
		}
		entry.Request = nil

	case "PUT":
		// Because we pre-process conditional PUTs, we know this is always a normal PUT operation
		entry.FullUrl = b.Config.responseURL(req, entry.Request.Url).String()
		parts := strings.SplitN(entry.Request.Url, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("Couldn't identify resource and id to put from %s", entry.Request.Url)
		}

		// Write
		createdNew, err := session.Put(parts[1], "", entry.Resource)
		if err != nil {
			return errors.Wrapf(err, "failed to update %s", entry.Request.Url)
		}

		// Response
		entry.Request = nil
		entry.Response = new(models.BundleEntryResponseComponent)
		entry.Response.Location = entry.FullUrl
		if createdNew {
			entry.Response.Status = "201"
		} else {
			entry.Response.Status = "200"
		}
		updateEntryMeta(entry)
	case "GET":
		/*
			examples
				1 /Patient
				2 /Patient/_search
				2 /Patient/12345
				3 /Patient/12345/_history
				4 /Patient/12345/_history/55
		*/

		pathAndQuery := strings.SplitN(entry.Request.Url, "?", 2)
		path := pathAndQuery[0]
		var queryString string
		var err error
		if len(pathAndQuery) == 2 {
			queryString = pathAndQuery[1]
			if err != nil {
				return errors.Wrapf(err, "failed to parse query string: %s", entry.Request.Url)
			}
		}

		// remove leading /
		path = strings.TrimPrefix(path, "/")
		segments := strings.Split(path, "/")
		var id, vid string
		var historyRequest bool
		resourceType := segments[0]
		glog.V(3).Infof("  segments: %q (%d)", segments, len(segments))
		if len(segments) >= 2 {
			id = segments[1]
			if id == "_search" {
				id = ""
			}
			if id == "_history" {
				return errors.Errorf("resource-level history not supported in request: %s", entry.Request.Url)
			}

			if len(segments) >= 3 {
				op := segments[2]
				glog.V(3).Infof("  op = %s", op)
				if op != "_history" {
					return errors.Errorf("operation not supported in request: %s", entry.Request.Url)
				}

				if len(segments) == 3 {
					historyRequest = true
				} else if len(segments) == 4 {
					vid = segments[3]
				} else {
					return errors.Errorf("failed to parse request path: %s", entry.Request.Url)
				}
			}
		}

		if historyRequest {
			baseURL := b.Config.responseURL(req, resourceType)
			bundle, err := session.History(*baseURL, resourceType, id)
			glog.V(3).Infof("  history request (%s/%s) --> err %+v", resourceType, id, err)
			if err != nil && err != ErrNotFound {
				return errors.Wrapf(err, "History request failed: %s", entry.Request.Url)
			}

			if err == ErrNotFound {
				entry.Response = &models.BundleEntryResponseComponent{
					Status: "404",
				}
			} else {
				entry.Response = &models.BundleEntryResponseComponent{
					Status: "200",
				}
				entry.Resource, err = bundle.ToResource()
				if err != nil {
					return errors.Wrapf(err, "bundle.ToResource failed for request: %s", entry.Request.Url)
				}
			}
		} else if id != "" {
			// /Patient/12345
			// /Patient/12345/_history/55

			entry.Response = &models.BundleEntryResponseComponent{}
			if vid == "" {
				entry.Resource, err = session.Get(id, resourceType)
			} else {
				entry.Resource, err = session.GetVersion(id, vid, resourceType)
			}
			glog.V(3).Infof("  get resource request (%s id=%s vid=%s) --> err %+v", resourceType, id, vid, err)

			switch err {
			case nil:
				lastUpdated := entry.Resource.LastUpdated()
				if lastUpdated != "" {
					// entry.Response.LastModified = entry.Resource.LastUpdatedTime().UTC().Format(http.TimeFormat)
					entry.Response.LastModified = &models.FHIRDateTime{
						Time:      entry.Resource.LastUpdatedTime(),
						Precision: models.Timestamp,
					}
				}
				versionId := entry.Resource.VersionId()
				if versionId != "" {
					entry.Response.Etag = "W/\"" + versionId + "\""
				}
			case ErrNotFound:
				entry.Response.Status = "404"
			case ErrDeleted:
				entry.Response.Status = "410"
			default:
				return errors.Wrapf(err, "Get/GetVersion failed for %s", entry.Request.Url)
			}

		} else {
			// Search:
			// /Patient
			// /Patient/_search
			searchQuery := search.Query{Resource: resourceType, Query: queryString}
			baseURL := b.Config.responseURL(req, resourceType)
			bundle, err := session.Search(*baseURL, searchQuery)
			glog.V(3).Infof("  search request (%s %s) --> err %#v", resourceType, queryString, err)
			if err != nil {
				return errors.Wrapf(err, "Search failed for %s", entry.Request.Url)
			}
			entry.Response = &models.BundleEntryResponseComponent{
				Status: "200",
			}
			entry.Resource, err = bundle.ToResource()
			if err != nil {
				return errors.Wrapf(err, "bundle.ToResource failed for request: %s", entry.Request.Url)
			}
		}
		entry.Request = nil
	}

	glog.V(3).Infof("    done")
	return nil
}

func updateEntryMeta(entry *models2.ShallowBundleEntryComponent) {

	// TODO: keep LastModified as a string
	lastUpdated := entry.Resource.LastUpdated()
	if lastUpdated != "" {
		t := time.Time{}
		err := t.UnmarshalJSON([]byte("\"" + lastUpdated + "\""))
		if err != nil {
			panic(fmt.Errorf("failed to parse LastUpdated String: %s", lastUpdated))
		}
		entry.Response.LastModified = &models.FHIRDateTime{Time: t, Precision: "timestamp"}
	}

	versionId := entry.Resource.VersionId()
	if versionId != "" {
		entry.Response.Etag = "W/\"" + versionId + "\""
	}
}

func (b *BatchController) resolveConditionalPut(request *http.Request, session DataAccessSession, entryIndex int, entry *models2.ShallowBundleEntryComponent, newIDs []string, refMap map[string]string) error {
	// Do a preflight to either get the existing ID, get a new ID, or detect multiple matches (not allowed)
	parts := strings.SplitN(entry.Request.Url, "?", 2)
	query := search.Query{Resource: parts[0], Query: parts[1]}

	var id string
	if IDs, err := session.FindIDs(query); err == nil {
		switch len(IDs) {
		case 0:
			id = bson.NewObjectId().Hex()
		case 1:
			id = IDs[0]
		default:
			return &ErrMultipleMatches{msg: fmt.Sprintf("Multiple matches for %s (%v)", entry.Request.Url, IDs)}
		}
	} else {
		return err
	}

	// Rewrite the PUT as a normal (non-conditional) PUT
	entry.Request.Url = query.Resource + "/" + id

	// Add the new ID to the reference map
	newIDs[entryIndex] = id
	refMap[entry.FullUrl] = entry.Request.Url

	// Rewrite the FullUrl using the new ID
	entry.FullUrl = b.Config.responseURL(request, query.Resource, id).String()

	return nil
}

func (b *BatchController) processProvenanceHeader(provenanceHeader string, c *gin.Context, entries []*models2.ShallowBundleEntryComponent, session DataAccessSession) *response {
	// spec: http://www.hl7.org/fhir/provenance.html#header

	if provenanceHeader != "" {
		headerBytes := []byte(provenanceHeader)

		// check resourceType
		resourceType, dataType, _, err := jsonparser.Get(headerBytes, "resourceType")
		if err != nil {
			err = errors.Wrap(err, "error parsing X-Provenance header resourceType")
			return badValue(err)
		}
		if string(resourceType) != "Provenance" {
			err = errors.Errorf("error parsing X-Provenance header: invalid resourceType")
			return badValue(err)
		}

		// make sure "target" is not set
		_, dataType, _, err = jsonparser.Get(headerBytes, "target")
		if dataType == jsonparser.NotExist {
		} else if err != nil {
			err = errors.Wrap(err, "error parsing X-Provenance header")
			return badValue(err)
		} else {
			err = errors.Errorf("error parsing X-Provenance header: target should not be set")
			return badValue(err)
		}

		if headerBytes[len(headerBytes)-1] != '}' {
			err = errors.Errorf("error parsing X-Provenance header: doesn't end with }")
			return badValue(err)
		}

		// generate targets field
		var sb bytes.Buffer
		sb.Write(headerBytes[:len(headerBytes)-1]) // remove final '{'}
		sb.WriteString(", \"target\": [")
		addComma := false
		for _, entry := range entries {
			if entry.Resource == nil {
				continue
			}
			if addComma {
				sb.WriteString(", ")
			}
			addComma = true

			if entry.Resource.ResourceType() == "" {
				err = errors.Errorf("processProvenanceHeader: missing resourceType for %s", entry.FullUrl)
				return internalError(err)
			}
			if entry.Resource.Id() == "" {
				err = errors.Errorf("processProvenanceHeader: missing id for %s", entry.FullUrl)
				return internalError(err)
			}
			if b.Config.EnableHistory && entry.Resource.VersionId() == "" {
				err = errors.Errorf("processProvenanceHeader: missing versionId for %s", entry.FullUrl)
				return internalError(err)
			}

			sb.WriteString("{ \"reference\": \"")
			sb.WriteString(entry.Resource.ResourceType())
			sb.WriteString("/")
			sb.WriteString(entry.Resource.Id())
			if b.Config.EnableHistory {
				sb.WriteString("/_history/")
				sb.WriteString(entry.Resource.VersionId())
			}
			sb.WriteString("\" }")
		}

		sb.WriteString("] }")

		if glog.V(8) {
			glog.V(8).Info("  saving X-Provenance ", sb.String())
		}

		// load resource with target
		provenanceResource, err := models2.NewResourceFromJsonBytes(sb.Bytes())
		if err != nil {
			err = errors.Wrap(err, "error loading X-Provenance header")
			return badValue(err)
		}

		// save
		newId := bson.NewObjectId().Hex()
		err = session.PostWithID(newId, provenanceResource)
		if err != nil {
			err = errors.Wrapf(err, "failed to create provenanceResource")
			return internalError(err)
		}

		c.Header("X-GoFHIR-Provenance-Location", "Provenance/"+newId)
	}
	return nil
}

func isConditional(entry *models2.ShallowBundleEntryComponent) bool {
	if entry.Request == nil {
		return false
	} else if entry.Request.Method != "PUT" && entry.Request.Method != "DELETE" {
		return false
	}
	return !strings.Contains(entry.Request.Url, "/") || strings.Contains(entry.Request.Url, "?")
}

func hasTempID(str string) bool {

	// do not match URLs like Patient?identifier=urn:oid:0.1.2.3.4.5.6.7|urn:uuid:6002c2ab-9571-4db7-9a79-87163475b071
	tempIdRegexp := regexp.MustCompile("([=,])(urn:uuid:|urn%3Auuid%3A)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(&|,|$)")
	matches := tempIdRegexp.MatchString(str)

	// hasPrefix := strings.HasPrefix(str, "urn:uuid:") || strings.HasPrefix(str, "urn%3Auuid%3A")
	// contains := strings.Contains(str, "urn:uuid:") || strings.Contains(str, "urn%3Auuid%3A")
	// if matches != contains {
	// fmt.Printf("re != contains (re = %t): %s\n", matches, str)
	// }

	return matches
}

func sortBundleEntries(bundle *models2.ShallowBundle) ([]*models2.ShallowBundleEntryComponent, *response) {
	// Validate bundle entries, ensuring they have a request and that we support the method,
	// while also creating a new entries array that can be sorted by method.
	entries := make([]*models2.ShallowBundleEntryComponent, len(bundle.Entry))
	for i := range bundle.Entry {
		if bundle.Entry[i].Request == nil {
			return nil, brokenInvariant(errors.New("Entries in a batch operation require a request"))
		}

		switch bundle.Entry[i].Request.Method {
		default:
			return nil, badValue(errors.New("Operation currently unsupported in batch requests: " + bundle.Entry[i].Request.Method))
		case "DELETE":
			if bundle.Entry[i].Request.Url == "" {
				return nil, brokenInvariant(errors.New("Batch DELETE must have a URL"))
			}
		case "POST":
			if bundle.Entry[i].Resource == nil {
				return nil, brokenInvariant(errors.New("Batch POST must have a resource body"))
			}
		case "PUT":
			if bundle.Entry[i].Resource == nil {
				return nil, brokenInvariant(errors.New("Batch PUT must have a resource body"))
			}
			if !strings.Contains(bundle.Entry[i].Request.Url, "/") && !strings.Contains(bundle.Entry[i].Request.Url, "?") {
				return nil, brokenInvariant(errors.New("Batch PUT URL must have an id or a condition"))
			}
		case "GET":
			if bundle.Entry[i].Request.Url == "" {
				return nil, brokenInvariant(errors.New("Batch GET must have a URL"))
			}
		}
		entries[i] = &bundle.Entry[i]
	}

	// sort entries by request method as per FHIR spec
	sort.Sort(byRequestMethod(entries))

	return entries, nil
}

// Support sorting by request method, as defined in the spec
type byRequestMethod []*models2.ShallowBundleEntryComponent

func (e byRequestMethod) Len() int {
	return len(e)
}
func (e byRequestMethod) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}
func (e byRequestMethod) Less(i, j int) bool {
	methodMap := map[string]int{"DELETE": 0, "POST": 1, "PUT": 2, "GET": 3}
	return methodMap[e[i].Request.Method] < methodMap[e[j].Request.Method]
}
