package server

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/eug48/fhir/models"
	"github.com/eug48/fhir/models2"
	"github.com/eug48/fhir/search"
	"github.com/golang/glog"
	"github.com/pkg/errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"

	mongowrapper "github.com/opencensus-integrations/gomongowrapper"
)

type mongoDataAccessLayer struct {
	client                       *mongowrapper.WrappedClient
	defaultDbName                string
	enableMultiDB                bool
	dbSuffix                     string
	Interceptors                 map[string]InterceptorList
	countTotalResults            bool
	enableCISearches             bool
	tokenParametersCaseSensitive bool
	enableHistory                bool
	readonly                     bool
}

type mongoSession struct {
	session       mongo.Session
	context       mongo.SessionContext
	db            *mongowrapper.WrappedDatabase
	dal           *mongoDataAccessLayer
	inTransaction bool
}

func (dal *mongoDataAccessLayer) StartSession(ctx context.Context, customDbName string) DataAccessSession {
	opts := options.Session()
	opts.SetCausalConsistency(true)
	opts.SetDefaultReadConcern(readconcern.Majority())
	opts.SetDefaultWriteConcern(writeconcern.New(writeconcern.WMajority(), writeconcern.J(true)))

	session, err := dal.client.StartSession(opts)
	if err != nil {
		panic(errors.Wrap(err, "StartSession failed"))
	}

	var dbName string
	if dal.enableMultiDB && customDbName != "" {
		if dal.dbSuffix != "" && !strings.HasSuffix(customDbName, dal.dbSuffix) {
			panic(errors.Wrapf(err, "database name (%s) doesn't end with suffix (%s)", customDbName, dal.dbSuffix))
		}
		dbName = customDbName
	} else {
		dbName = dal.defaultDbName
	}

	db := dal.client.Database(dbName)
	if db == nil {
		panic(errors.Wrap(err, "client.Database failed"))
	}

	var contextWithSession mongo.SessionContext
	wrappedSession := session.(*mongowrapper.WrappedSession) // unwrap - mongo's sessionFromContext wants its own session impl
	mongo.WithSession(ctx, wrappedSession.Session, func(sc mongo.SessionContext) error {
		// hack to work around this closure-based API
		// WithSession just calls the callback and does nothing else.. (at least in MongoDB Go Driver 1.0.3)
		contextWithSession = sc
		return nil
	})

	return &mongoSession{
		session:       session,
		context:       contextWithSession,
		db:            db,
		inTransaction: false,
		dal:           dal,
	}
}

func (ms *mongoSession) CurrentVersionCollection(resourceType string) *mongowrapper.WrappedCollection {
	return ms.db.Collection(models.PluralizeLowerResourceName(resourceType))
}
func (ms *mongoSession) PreviousVersionsCollection(resourceType string) *mongowrapper.WrappedCollection {
	return ms.db.Collection(models.PluralizeLowerResourceName(resourceType) + "_prev")
}

func (ms *mongoSession) StartTransaction() error {
	if ms.inTransaction {
		// sucess if already in a transaction
		return nil
	}

	err := ms.session.StartTransaction()
	glog.V(3).Infof("StartTransaction")
	if err == nil {
		ms.inTransaction = true
	}
	return errors.Wrap(err, "mongoSession.StartTransaction")
}
func (ms *mongoSession) CommmitIfTransaction() error {
	if ms.inTransaction {
		glog.V(3).Infof("CommmitTransaction")
		err := ms.session.CommitTransaction(ms.context)
		ms.inTransaction = false
		return errors.Wrap(err, "mongoSession.CommmitIfTransaction")
	} else {
		return nil
	}
}
func (ms *mongoSession) Finish() {
	var err error
	if ms.inTransaction {
		err = ms.session.AbortTransaction(ms.context)
		if err == nil {
			glog.Warningf("AbortTransaction called from mongoSession.Finish")
			ms.inTransaction = false
		} else {
			commandErr, ok := err.(mongo.CommandError)
			if ok && commandErr.Name == "OperationNotSupportedInTransaction" {
				// can ignore - occurs if we fail before issuing a command. msg is "Command is not supported as the first command in a transaction"
				glog.V(5).Infof("ignoring failed AbortTransaction in mongoSession.Finish (%s %s)", commandErr.Name, commandErr.Message)
			} else if ok && commandErr.Name == "NoSuchTransaction" {
				/*
					after any error in a transaction
					AbortTransaction will fail with NoSuchTransaction
					since the transaction gets aborted by the error
				*/
				glog.V(5).Infof("AbortTransaction in mongoSession.Finish failed: %T %v", err, err)
			} else {
				panic(fmt.Sprintf("AbortTransaction in mongoSession.Finish failed: %T %v", err, err))
			}
		}
	}
	ms.session.EndSession(ms.context)
}

// NewMongoDataAccessLayer returns an implementation of DataAccessLayer that is backed by a Mongo database
func NewMongoDataAccessLayer(client *mongowrapper.WrappedClient, defaultDbName string, enableMultiDB bool, dbSuffix string, interceptors map[string]InterceptorList, config Config) DataAccessLayer {
	return &mongoDataAccessLayer{
		client:                       client,
		defaultDbName:                defaultDbName,
		enableMultiDB:                enableMultiDB,
		dbSuffix:                     dbSuffix,
		Interceptors:                 interceptors,
		countTotalResults:            config.CountTotalResults,
		enableCISearches:             config.EnableCISearches,
		tokenParametersCaseSensitive: config.TokenParametersCaseSensitive,
		enableHistory:                config.EnableHistory,
		readonly:                     config.ReadOnly,
	}
}

// InterceptorList is a list of interceptors registered for a given database operation
type InterceptorList []Interceptor

// Interceptor optionally executes functions on a specified resource type before and after
// a database operation involving that resource. To register an interceptor for ALL resource
// types use a "*" as the resourceType.
type Interceptor struct {
	ResourceType string
	Handler      InterceptorHandler
}

// InterceptorHandler is an interface that defines three methods that are executed on a resource
// before the database operation, after the database operation SUCCEEDS, and after the database
// operation FAILS.
type InterceptorHandler interface {
	Before(resource interface{})
	After(resource interface{})
	OnError(err error, resource interface{})
}

// invokeInterceptorsBefore invokes the interceptor list for the given resource type before a database
// operation occurs.
func (ms *mongoSession) invokeInterceptorsBefore(op, resourceType string, resource interface{}) {

	for _, interceptor := range ms.dal.Interceptors[op] {
		if interceptor.ResourceType == resourceType || interceptor.ResourceType == "*" {
			interceptor.Handler.Before(resource)
		}
	}
}

// invokeInterceptorsAfter invokes the interceptor list for the given resource type after a database
// operation occurs and succeeds.
func (ms *mongoSession) invokeInterceptorsAfter(op, resourceType string, resource interface{}) {

	for _, interceptor := range ms.dal.Interceptors[op] {
		if interceptor.ResourceType == resourceType || interceptor.ResourceType == "*" {
			interceptor.Handler.After(resource)
		}
	}
}

// invokeInterceptorsOnError invokes the interceptor list for the given resource type after a database
// operation occurs and fails.
func (ms *mongoSession) invokeInterceptorsOnError(op, resourceType string, err error, resource interface{}) {

	for _, interceptor := range ms.dal.Interceptors[op] {
		if interceptor.ResourceType == resourceType || interceptor.ResourceType == "*" {
			interceptor.Handler.OnError(err, resource)
		}
	}
}

// hasInterceptorsForOpAndType checks if any interceptors are registered for a particular database operation AND resource type
func (ms *mongoSession) hasInterceptorsForOpAndType(op, resourceType string) bool {

	if len(ms.dal.Interceptors[op]) > 0 {
		for _, interceptor := range ms.dal.Interceptors[op] {
			if interceptor.ResourceType == resourceType || interceptor.ResourceType == "*" {
				// At least 1 interceptor is registered for this database operation and resource type
				return true
			}
		}
	}
	return false
}

func (ms *mongoSession) Get(id, resourceType string) (resource *models2.Resource, err error) {
	bsonID, err := convertIDToBsonID(id)
	if err != nil {
		return nil, ErrNotFound
	}

	collection := ms.CurrentVersionCollection(resourceType)
	filter := bson.D{{"_id", bsonID.Hex()}}
	var doc bson.D
	err = collection.FindOne(ms.context, filter).Decode(&doc)
	glog.V(3).Infof("Get %s/%s --> %s (err %+v)", resourceType, id, doc, err)
	if err == mongo.ErrNoDocuments && ms.dal.enableHistory {
		// check whether this is a deleted record
		prevCollection := ms.PreviousVersionsCollection(resourceType)
		prevQuery := bson.D{
			{"_id._id", bsonID.Hex()},
			{"_id._deleted", 1},
		}
		idOnly := bson.D{{"_id", 1}}

		cursor, err := prevCollection.Find(ms.context, prevQuery, options.Find().SetLimit(1).SetProjection(idOnly))
		if err != nil {
			return nil, errors.Wrap(err, "Get --> prevCollection.Find")
		}

		deleted := cursor.Next(ms.context)
		err = cursor.Err()
		glog.V(3).Infof("   deleted version: %t (err %+v)", deleted, err)
		if err != nil {
			return nil, errors.Wrap(err, "Get --> prevCollection.Find --> cursor error")
		}

		if deleted {
			return nil, ErrDeleted
		}
		return nil, ErrNotFound
	}

	if err != nil {
		return nil, convertMongoErr(err)
	}

	resource, err = models2.NewResourceFromBSON(doc)
	return
}

func (ms *mongoSession) GetVersion(id, versionIdStr, resourceType string) (resource *models2.Resource, err error) {
	bsonID, err := convertIDToBsonID(id)
	if err != nil {
		return nil, ErrNotFound
	}

	versionIdInt, err := strconv.Atoi(versionIdStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to convert versionId to an integer (%s)", versionIdStr)
	}

	// First assume versionId is for the current version
	curQuery := bson.D{
		{"_id", bsonID.Hex()},
		{"meta.versionId", versionIdStr},
	}
	curCollection := ms.CurrentVersionCollection(resourceType)
	var result bson.D
	err = curCollection.FindOne(ms.context, curQuery).Decode(&result)
	// glog.Debugf("GetVersion: curQuery=%+v; err=%+v\n", curQuery, err)

	if err == mongo.ErrNoDocuments {
		// try to search for previous versions
		prevQuery := bson.D{
			{"_id._id", bsonID.Hex()},
			{"_id._version", int32(versionIdInt)},
		}
		prevCollection := ms.PreviousVersionsCollection(resourceType)
		cur, err := prevCollection.Find(ms.context, prevQuery, options.Find().SetLimit(1))
		if err != nil {
			return nil, errors.Wrap(err, "GetVersion --> prevCollection.Find")
		}

		if cur.Next(ms.context) {

			var prevDoc bson.Raw
			err = cur.Decode(&prevDoc)
			if err != nil {
				return nil, errors.Wrap(err, "GetVersion --> prevCollection.Find --> Decode")
			}

			var deleted bool
			deleted, resource, err = unmarshalPreviousVersion(&prevDoc)
			if err != nil {
				return nil, errors.Wrap(err, "failed to unmarshal previous version")
			}
			if deleted {
				return nil, ErrDeleted
			} else {
				return resource, nil
			}
		}
		if err := cur.Err(); err != nil {
			return nil, errors.Wrap(err, "GetVersion --> prevCollection.Find --> cursor error")
		}

		return nil, ErrNotFound

	} else if err != nil {
		return nil, errors.Wrap(convertMongoErr(err), "failed to search for current version")
	} else {
		resource, err = models2.NewResourceFromBSON(result)
	}

	return
}

// Convert document stored in one of the _prev collections into a resource
func unmarshalPreviousVersion(rawDoc *bson.Raw) (deleted bool, resource *models2.Resource, err error) {
	// glog.Debugf("[unmarshalPreviousVersion] %+v\n", rawDoc)
	// first we have to parse the vermongo-style id
	idItem, err := rawDoc.IndexErr(0)
	if err != nil {
		return false, nil, fmt.Errorf("unmarshalPreviousVersion: input empty: %s", err)
	}
	if idItem.Key() != "_id" {
		return false, nil, fmt.Errorf("unmarshalPreviousVersion: first element not an _id")
	}

	idValue, ok := idItem.Value().DocumentOK()
	if !ok {
		return false, nil, fmt.Errorf("unmarshalPreviousVersion: _id not a bson dictionary")
	}

	actualIdVal, err := idValue.LookupErr("_id")
	if err != nil {
		return false, nil, fmt.Errorf("unmarshalPreviousVersion: _id._id missing")
	}

	actualId, ok := actualIdVal.StringValueOK()
	if !ok {
		return false, nil, fmt.Errorf("unmarshalPreviousVersion: _id._id not a string")
	}

	// check if actually deleted
	deletedVal, err := idValue.LookupErr("_deleted")
	if err == nil {
		deleted, ok := deletedVal.Int32OK()
		if ok && deleted > 0 {
			return true, nil, nil
		}
	}

	// convert to a bson.D
	var doc bson.D
	err = bson.Unmarshal(*rawDoc, &doc)
	if err != nil {
		return false, nil, errors.Wrapf(err, "unmarshalPreviousVersion: unmarshal failed")
	}

	// replace first element with a string id
	doc[0] = bson.E{"_id", actualId}

	// convert to JSON
	resource, err = models2.NewResourceFromBSON(doc)
	if err != nil {
		return false, nil, errors.Wrap(err, "unmarshalPreviousVersion: NewResourceFromBSON failed")
	}

	return false, resource, nil
}

func (ms *mongoSession) Post(resource *models2.Resource) (id string, err error) {
	id = primitive.NewObjectID().Hex()
	err = convertMongoErr(ms.PostWithID(id, resource))
	return
}

func (ms *mongoSession) ConditionalPost(query search.Query, resource *models2.Resource) (httpStatus int, id string, outputResource *models2.Resource, err error) {
	existingIds, err := ms.FindIDs(query)
	if err != nil {
		return
	}

	if len(existingIds) == 0 {
		httpStatus = 201
		id = primitive.NewObjectID().Hex()
		err = convertMongoErr(ms.PostWithID(id, resource))
		if err == nil {
			outputResource = resource
		}

	} else if len(existingIds) == 1 {
		httpStatus = 200
		id = existingIds[0]
		outputResource, err = ms.Get(id, query.Resource)

	} else if len(existingIds) > 1 {
		httpStatus = 412
	}

	return
}

func (ms *mongoSession) PostWithID(id string, resource *models2.Resource) error {
	bsonID, err := convertIDToBsonID(id)
	if err != nil {
		return convertMongoErr(err)
	}

	resource.SetId(bsonID.Hex())
	updateResourceMeta(resource, 1)
	resourceType := resource.ResourceType()
	curCollection := ms.CurrentVersionCollection(resourceType)

	ms.invokeInterceptorsBefore("Create", resourceType, resource)

	glog.V(3).Infof("PostWithID: inserting %s/%s", resourceType, id)
	_, err = curCollection.InsertOne(ms.context, resource)

	if err == nil {
		ms.invokeInterceptorsAfter("Create", resourceType, resource)
	} else {
		ms.invokeInterceptorsOnError("Create", resourceType, err, resource)
	}

	return convertMongoErr(err)
}

func (ms *mongoSession) Put(id string, conditionalVersionId string, resource *models2.Resource) (createdNew bool, err error) {
	bsonID, err := convertIDToBsonID(id)
	if err != nil {
		return false, convertMongoErr(err)
	}

	resourceType := resource.ResourceType()
	curCollection := ms.CurrentVersionCollection(resourceType)
	resource.SetId(bsonID.Hex())
	if conditionalVersionId != "" {
		glog.V(3).Infof("PUT %s/%s (If-Match %s)", resourceType, resource.Id(), conditionalVersionId)
	} else {
		glog.V(3).Infof("PUT %s/%s", resourceType, resource.Id())
	}

	var curVersionId *int = nil
	var newVersionId = 1
	var start time.Time

	if ms.dal.enableHistory == false {
		if conditionalVersionId != "" {
			return false, errors.Errorf("If-Match specified for a conditional put, but version histories are disabled")
		}
		glog.V(3).Infof("  versionIds: history disabled; new %d", newVersionId)
	} else {

		// get current version of this document
		if glog.V(5) {
			start = time.Now()
		}
		var currentDoc bson.D
		var currentDocRaw bson.Raw
		currentDocQuery := bson.D{{"_id", bsonID.Hex()}}
		if err = curCollection.FindOne(ms.context, currentDocQuery).Decode(&currentDocRaw); err != nil && err != mongo.ErrNoDocuments {
			return false, errors.Wrap(convertMongoErr(err), "Put handler: error retrieving current version")
		}
		if glog.V(5) {
			glog.V(5).Infof("get_current_version took %v", time.Since(start))
		}

		if err == mongo.ErrNoDocuments {
			if conditionalVersionId != "" {
				return false, ErrConflict{msg: "If-Match specified for a resource that doesn't exist"}
			}
			glog.V(3).Infof("  versionIds: no current; new %d", newVersionId)
		} else {
			// unmarshal fully
			err = bson.Unmarshal(currentDocRaw, &currentDoc)
			if err != nil {
				return false, errors.Wrap(convertMongoErr(err), "Put: error unmarshalling current version")
			}

			hasVersionId, curVersionIdTemp, curVersionIdStr := getVersionIdFromResource(&currentDocRaw)
			if hasVersionId {
				newVersionId = curVersionIdTemp + 1
			} else {
				// for documents created by previous versions not supporting versioning or if it was disabled
				newVersionId = 1
				curVersionIdTemp = 0
			}
			curVersionId = &curVersionIdTemp
			glog.V(3).Infof("  versionIds: current %d; new %d", *curVersionId, newVersionId)

			if conditionalVersionId != "" && conditionalVersionId != curVersionIdStr {
				return false, ErrConflict{msg: "If-Match doesn't match current versionId"}
			}

			// store current document in the previous version collection, adding its versionId to
			// its mongo _id like in vermongo (https://github.com/thiloplanz/v7files/wiki/Vermongo)
			//   i.e. { "_id" : { "_id" : ObjectId("4c78da..."), "_version" : "2" }
			setVermongoId(&currentDoc, curVersionIdTemp)
			// NOTE: currentDoc._id modified in-place

			prevCollection := ms.PreviousVersionsCollection(resourceType)

			vermongoIdField := bson.D{currentDoc[0]}

			// TODO: figure out why ReplaceOne isn't working (FindOneAndReplace returns a doc even though we don't need it)
			res := prevCollection.FindOneAndReplace(ms.context, &vermongoIdField, &currentDoc, options.FindOneAndReplace().SetUpsert(true).SetReturnDocument(options.Before))
			err = res.Err()
			// _, err := prevCollection.ReplaceOne(ms.context, &vermongoIdField, &currentDoc, options.Replace().SetUpsert(true))
			// if err != nil {
			if err != nil && strings.Contains(err.Error(), "duplicate key") {
				return false, ErrConflict{msg: fmt.Sprintf("duplicate key storing previous version for %s/%s", resourceType, id)}
			}
			if err != nil && err != mongo.ErrNoDocuments {
				return false, errors.Wrap(convertMongoErr(err), "failed to store previous version")
			}

		}
	}

	updateResourceMeta(resource, newVersionId)

	if ms.hasInterceptorsForOpAndType("Update", resourceType) {
		oldResource, getError := ms.Get(id, resourceType)
		if getError == nil {
			ms.invokeInterceptorsBefore("Update", resourceType, oldResource)
		}
	}

	var updated int64
	if curVersionId == nil {
		var info *mongo.UpdateResult
		selector := bson.D{{"_id", bsonID.Hex()}}
		if glog.V(5) {
			start = time.Now()
		}
		info, err = curCollection.ReplaceOne(ms.context, selector, resource, options.Replace().SetUpsert(true))
		if glog.V(5) {
			glog.V(3).Infof("   upsert %#v took %v", selector, time.Since(start))
		}
		if err != nil {
			bson, err2 := resource.GetBSON()
			if err2 != nil {
				panic(err2)
			}
			err = errors.Wrapf(err, "PUT handler: failed to upsert new document: %#v --> %s %#v", selector, resource.JsonBytes(), bson)
		} else {
			updated = info.ModifiedCount
		}
	} else {
		// atomic check-then-update
		selector := bson.D{
			{"_id", bsonID.Hex()},
			{"meta.versionId", strconv.Itoa(*curVersionId)},
		}
		if *curVersionId == 0 {
			// cur doc won't actually have a versionId field
			selector[1] = bson.E{"meta.versionId", bson.D{{"$exists", false}}}
		}
		var updateOneInfo *mongo.UpdateResult
		if glog.V(5) {
			start = time.Now()
		}
		updateOneInfo, err = curCollection.ReplaceOne(ms.context, selector, resource)
		if glog.V(5) {
			glog.V(3).Infof("   update %#v --> %#v (err %#v) took %v", selector, updateOneInfo, err, time.Since(start))
		}
		if err != nil {
			err = errors.Wrap(err, "PUT handler: failed to update current document")
		} else if updateOneInfo.ModifiedCount == 0 {
			return false, ErrConflict{msg: fmt.Sprintf("conflicting update for %+v", selector)}
		}
		updated = 1
	}
	if updated == 0 {
		glog.V(3).Infof("      created new")
	} else {
		glog.V(3).Infof("      updated %d", updated)
	}

	if err == nil {
		createdNew = (updated == 0)
		if createdNew {
			ms.invokeInterceptorsAfter("Create", resourceType, resource)
		} else {
			ms.invokeInterceptorsAfter("Update", resourceType, resource)
		}
	} else {
		ms.invokeInterceptorsOnError("Update", resourceType, err, resource)
	}

	return createdNew, convertMongoErr(err)
}

func getVersionIdFromResource(doc *bson.Raw) (hasVersionId bool, versionIdInt int, versionIdStr string) {
	versionId, err := doc.LookupErr("meta", "versionId")
	if err == bsoncore.ErrElementNotFound {
		return false, -1, ""
	} else if err != nil {
		panic(errors.Wrap(err, "getVersionIdFromResource LookupErr failed"))
	}

	hasVersionId = true
	var isString bool
	versionIdStr, isString = versionId.StringValueOK()
	if !isString {
		panic(errors.Errorf("meta.versionId is not a BSON string"))
	}
	versionIdInt, err = strconv.Atoi(versionIdStr)
	if err == nil {
		return
	} else {
		panic(errors.Errorf("meta.versionId BSON string is not an integer: %s", versionIdStr))
	}
}

// Updates the doc to use a vermongo-like _id (_id: current_id, _version: versionId)
func setVermongoId(doc *bson.D, versionIdInt int) {
	idItem := &((*doc)[0])
	if idItem == nil || idItem.Key != "_id" {
		panic("_id field not first in bson document")
	}

	newId := bson.D{
		{"_id", idItem.Value},
		{"_version", int32(versionIdInt)},
	}

	(*doc)[0] = bson.E{"_id", newId}
}

func (ms *mongoSession) ConditionalPut(query search.Query, conditionalVersionId string, resource *models2.Resource) (id string, createdNew bool, err error) {
	if IDs, err := ms.FindIDs(query); err == nil {
		switch len(IDs) {
		case 0:
			id = primitive.NewObjectID().Hex()
		case 1:
			id = IDs[0]
		default:
			return "", false, &ErrMultipleMatches{msg: fmt.Sprintf("Multiple matches for %s?%s", query.Resource, query.Query)}
		}
	} else {
		return "", false, err
	}

	createdNew, err = ms.Put(id, conditionalVersionId, resource)
	return id, createdNew, err
}

func (ms *mongoSession) Delete(id, resourceType string) (newVersionId string, err error) {
	bsonID, err := convertIDToBsonID(id)
	if err != nil {
		return "", ErrNotFound
	}

	curCollection := ms.CurrentVersionCollection(resourceType)
	prevCollection := ms.PreviousVersionsCollection(resourceType)

	if ms.dal.enableHistory {
		newVersionId, err = saveDeletionIntoHistory(resourceType, bsonID.Hex(), curCollection, prevCollection, ms)
		if err == mongo.ErrNoDocuments {
			return "", ErrNotFound
		} else if err != nil {
			return "", errors.Wrap(err, "failed to save deletion into history")
		}
	}

	var resource interface{}
	var getError error
	hasInterceptor := ms.hasInterceptorsForOpAndType("Delete", resourceType)
	if hasInterceptor {
		// Although this is a delete operation we need to get the resource first so we can
		// run any interceptors on the resource before it's deleted.
		resource, getError = ms.Get(id, resourceType)
		ms.invokeInterceptorsBefore("Delete", resourceType, resource)
	}

	filter := bson.D{{"_id", bsonID.Hex()}}
	deleteInfo, err := curCollection.DeleteOne(ms.context, filter)
	glog.V(3).Infof("   deleteInfo: %+v (err %+v)", deleteInfo, err)
	if deleteInfo.DeletedCount == 0 && err == nil {
		err = mongo.ErrNoDocuments
	}

	if hasInterceptor {
		if err == nil && getError == nil {
			ms.invokeInterceptorsAfter("Delete", resourceType, resource)
		} else {
			ms.invokeInterceptorsOnError("Delete", resourceType, err, resource)
		}
	}

	err = convertMongoErr(err)
	return
}

func saveDeletionIntoHistory(resourceType string, id string, curCollection *mongowrapper.WrappedCollection, prevCollection *mongowrapper.WrappedCollection, ms *mongoSession) (newVersionIdStr string, err error) {
	// get current version of this document
	var currentDoc bson.D
	var currentDocRaw bson.Raw
	currentDocQuery := bson.D{{"_id", id}}
	err = curCollection.FindOne(ms.context, currentDocQuery).Decode(&currentDocRaw)

	if err == mongo.ErrNoDocuments {
		return "", err
	} else if err != nil {
		return "", errors.Wrap(convertMongoErr(err), "saveDeletionIntoHistory: error retrieving current version")
	} else {

		// unmarshal fully
		err = bson.Unmarshal(currentDocRaw, &currentDoc)
		if err != nil {
			return "", errors.Wrap(convertMongoErr(err), "saveDeletionIntoHistory: error unmarshalling current version")
		}

		// extract current version
		hasVersionId, curVersionId, _ := getVersionIdFromResource(&currentDocRaw)
		var newVersionId int
		if hasVersionId {
			newVersionId = curVersionId + 1
		} else {
			// document created by previous versions not supporting versioning or if it was disabled
			newVersionId = 1
			curVersionId = 0
		}
		newVersionIdStr = strconv.Itoa(newVersionId)

		// store current document in the previous version collection, adding its versionId to
		// its mongo _id like in vermongo (https://github.com/thiloplanz/v7files/wiki/Vermongo)
		//   i.e. { "_id" : { "_id" : ObjectId("4c78da..."), "_version" : "2" }
		setVermongoId(&currentDoc, curVersionId)
		// NOTE: currentDoc._id modified in-place

		vermongoIdField := bson.D{currentDoc[0]}

		// TODO: figure out why ReplaceOne isn't working (FindOneAndReplace returns a doc even though we don't need it)
		res := prevCollection.FindOneAndReplace(ms.context, &vermongoIdField, &currentDoc, options.FindOneAndReplace().SetUpsert(true).SetReturnDocument(options.Before))
		err = res.Err()
		if err == mongo.ErrNoDocuments {
			err = nil
		}
		if err != nil && strings.Contains(err.Error(), "duplicate key") {
			return "", ErrConflict{msg: fmt.Sprintf("Delete handler: duplicate key storing previous version for %s/%s", resourceType, id)}
		}
		if err != nil {
			return "", errors.Wrap(convertMongoErr(err), "Delete handler: failed to store previous version")
		}

		// create a deletion record
		now := time.Now()
		deletionRecord := bson.D{
			{"_id", bson.D{
				{"_id", id},
				{"_version", int32(newVersionId)},
				{"_deleted", 1},
			}},
			{"meta", bson.D{
				{"versionId", newVersionIdStr},
				{"lastUpdated", now},
			}},
		}

		vermongoIdField = bson.D{deletionRecord[0]}

		// TODO: figure out why ReplaceOne isn't working (FindOneAndReplace returns a doc even though we don't need it)
		res = prevCollection.FindOneAndReplace(ms.context, &vermongoIdField, &deletionRecord, options.FindOneAndReplace().SetUpsert(true).SetReturnDocument(options.Before))
		err = res.Err()
		if err == mongo.ErrNoDocuments {
			err = nil
		}
		if err != nil && strings.Contains(err.Error(), "duplicate key") {
			return "", ErrConflict{msg: fmt.Sprintf("Delete handler: duplicate key storing deletion marker for %s/%s", resourceType, id)}
		}
		if err != nil {
			return "", errors.Wrap(convertMongoErr(err), "Delete handler: failed to store deletion marker")
		}
	}
	return
}

func (ms *mongoSession) ConditionalDelete(query search.Query) (count int64, err error) {

	IDsToDelete, err := ms.FindIDs(query)
	if err != nil {
		return 0, err
	}
	// There is the potential here for the delete to fail if the slice of IDs
	// is too large (exceeding Mongo's 16MB document size limit).
	deleteQuery := bson.D{
		{"_id", bson.D{
			{"$in", IDsToDelete},
		},
		},
	}
	resourceType := query.Resource
	curCollection := ms.CurrentVersionCollection(resourceType)
	prevCollection := ms.PreviousVersionsCollection(resourceType)

	hasInterceptors := ms.hasInterceptorsForOpAndType("Delete", resourceType)

	if hasInterceptors || ms.dal.enableHistory {
		/* Interceptors for a conditional delete are tricky since an interceptor is only run
		   AFTER the database operation and only on resources that were SUCCESSFULLY deleted. We use
		   the following approach:
		   1. Bulk delete those resources by ID
		   2. Search again using the SAME query, to verify that those resources were in fact deleted
		   3. Run the interceptor(s) on all resources that ARE NOT in the second search (since they were truly deleted)
		*/

		// get the resources that are about to be deleted
		bundle, err := ms.Search(url.URL{}, query) // the baseURL argument here does not matter

		if err == nil {
			for _, elem := range bundle.Entry {
				if hasInterceptors {
					ms.invokeInterceptorsBefore("Delete", resourceType, elem.Resource)
				}
			}

			for _, elem := range bundle.Entry {
				if ms.dal.enableHistory {
					id := elem.Resource.Id()
					_, err = saveDeletionIntoHistory(resourceType, id, curCollection, prevCollection, ms)
					if err != nil {
						return count, errors.Wrapf(err, "failed to save deletion into history (%s/%s)", resourceType, id)
					}
				}
			}

			// Do the bulk delete by ID.
			info, err := curCollection.DeleteMany(ms.context, deleteQuery)
			deletedIds := make([]string, len(IDsToDelete))
			if info != nil {
				count = info.DeletedCount
			}

			if err != nil {
				if hasInterceptors {
					for _, elem := range bundle.Entry {
						ms.invokeInterceptorsOnError("Delete", resourceType, err, elem.Resource)
					}
				}
				return count, convertMongoErr(err)
			} else if hasInterceptors == false {
				return count, nil
			}

			var searchErr error

			if count < int64(len(IDsToDelete)) {
				// Some but not all resources were removed, so use the original search query
				// to see which resources are left.
				var failBundle *models2.ShallowBundle
				failBundle, searchErr = ms.Search(url.URL{}, query)
				deletedIds = setDiff(IDsToDelete, getResourceIdsFromBundle(failBundle))
			} else {
				// All resources were successfully removed
				deletedIds = IDsToDelete
			}

			if searchErr == nil {
				for _, elem := range bundle.Entry {
					id := elem.Resource.Id()

					if elementInSlice(id, deletedIds) {
						// This resource was confirmed deleted
						ms.invokeInterceptorsAfter("Delete", resourceType, elem.Resource)
					} else {
						// This resource was not confirmed deleted, which is an error
						resourceErr := fmt.Errorf("ConditionalDelete: failed to delete resource %s with ID %s", resourceType, id)
						ms.invokeInterceptorsOnError("Delete", resourceType, resourceErr, elem.Resource)
					}
				}
			}
		}
		return count, convertMongoErr(err)
	} else {
		// do the bulk delete the usual way
		info, err := curCollection.DeleteMany(ms.context, deleteQuery)
		if info != nil {
			count = info.DeletedCount
		}
		return count, convertMongoErr(err)
	}
}

func (ms *mongoSession) History(baseURL url.URL, resourceType string, id string) (bundle *models2.ShallowBundle, err error) {

	// check id
	_, err = convertIDToBsonID(id)
	if err != nil {
		return nil, ErrNotFound
	}

	baseURLstr := baseURL.String()
	if !strings.HasSuffix(baseURLstr, "/") {
		baseURLstr = baseURLstr + "/"
	}
	fullUrl := baseURLstr + id

	curCollection := ms.CurrentVersionCollection(resourceType)
	prevCollection := ms.PreviousVersionsCollection(resourceType)

	var entryList []models2.ShallowBundleEntryComponent
	makeEntryRequest := func(method string) *models.BundleEntryRequestComponent {
		return &models.BundleEntryRequestComponent{
			Url:    resourceType + "/" + id,
			Method: method,
		}
	}

	// add current version
	var curDoc bson.D
	curDocQuery := bson.D{{"_id", id}}
	err = curCollection.FindOne(ms.context, curDocQuery).Decode(&curDoc)
	if err == nil {
		var entry models2.ShallowBundleEntryComponent
		entry.FullUrl = fullUrl
		entry.Resource, err = models2.NewResourceFromBSON(curDoc)
		if err != nil {
			return nil, errors.Wrap(err, "History: NewResourceFromBSON failed")
		}
		entry.Request = makeEntryRequest("PUT")
		entryList = append(entryList, entry)
	} else if err != mongo.ErrNoDocuments {
		return nil, err
	}

	// sort - oldest versions last
	prevDocsQuery := bson.D{{"_id._id", id}}
	prevDocsSort := options.Find().SetSort(bson.D{{"_id._version", -1}})
	cursor, err := prevCollection.Find(ms.context, prevDocsQuery, prevDocsSort)
	if err != nil {
		return nil, errors.Wrap(err, "History: prevCollection.Find failed")
	}

	for cursor.Next(ms.context) {

		var prevDocBson bson.Raw
		err = cursor.Decode(&prevDocBson)
		glog.V(8).Infof("History: decoded prev document: %s", prevDocBson.String())
		if err != nil {
			return nil, errors.Wrap(err, "History: cursor.Decode failed")
		}

		var entry models2.ShallowBundleEntryComponent
		entry.FullUrl = fullUrl

		deleted, resource, err := unmarshalPreviousVersion(&prevDocBson)
		if err != nil {
			return nil, errors.Wrap(err, "History: unmarshalPreviousVersion failed")
		}
		if deleted {
			entry.Request = makeEntryRequest("DELETE")
		} else {
			entry.Resource = resource
			entry.Request = makeEntryRequest("PUT")
		}

		entryList = append(entryList, entry)
	}
	if err := cursor.Err(); err != nil {
		return nil, errors.Wrap(err, "History: MongoDB query for previous versions failed")
	}

	totalDocs := uint32(len(entryList))
	if totalDocs == 0 {
		return nil, ErrNotFound
	}

	// last entry should be a POST
	entryList[len(entryList)-1].Request.Method = "POST"
	entryList[len(entryList)-1].Request.Url = resourceType

	// output a Bundle
	bundle = &models2.ShallowBundle{
		Id:    primitive.NewObjectID().Hex(),
		Type:  "history",
		Entry: entryList,
		Total: &totalDocs,
	}

	// TODO: use paging
	// bundle.Link = dal.generatePagingLinks(baseURL, searchQuery, total, uint32(numResults))

	return bundle, nil
}

func (ms *mongoSession) Search(baseURL url.URL, searchQuery search.Query) (*models2.ShallowBundle, error) {

	searcher := search.NewMongoSearcher(ms.db, ms.context, ms.dal.countTotalResults, ms.dal.enableCISearches, ms.dal.tokenParametersCaseSensitive, ms.dal.readonly)

	resources, total, err := searcher.Search(searchQuery)
	if err != nil {
		return nil, convertMongoErr(err)
	}

	includesMap := make(map[string]*models2.Resource)
	var entryList []models2.ShallowBundleEntryComponent
	numResults := len(resources)
	baseURLstr := baseURL.String()
	if !strings.HasSuffix(baseURLstr, "/") {
		baseURLstr = baseURLstr + "/"
	}

	for i := 0; i < numResults; i++ {
		var entry models2.ShallowBundleEntryComponent
		entry.Resource = resources[i]
		entry.FullUrl = baseURLstr + resources[i].Id()
		entry.Search = &models.BundleEntrySearchComponent{Mode: "match"}
		entryList = append(entryList, entry)

		if searchQuery.UsesIncludes() || searchQuery.UsesRevIncludes() {

			for _, included := range entry.Resource.SearchIncludes() {
				includesMap[included.ResourceType()+"/"+included.Id()] = included
			}

		}
	}

	for _, v := range includesMap {
		if glog.V(4) {
			glog.V(4).Infof("includesMap: %s/%s/_history/%s\n", v.ResourceType(), v.Id(), v.VersionId())
		}
		var entry models2.ShallowBundleEntryComponent
		entry.Resource = v
		entry.Search = &models.BundleEntrySearchComponent{Mode: "include"}
		entryList = append(entryList, entry)
	}

	bundle := models2.ShallowBundle{
		Id:    primitive.NewObjectID().Hex(),
		Type:  "searchset",
		Entry: entryList,
	}

	// Only include the total if counts are enabled, or if _summary=count was applied.
	if ms.dal.countTotalResults || searchQuery.Options().Summary == "count" {
		bundle.Total = &total
	}

	bundle.Link = ms.generatePagingLinks(baseURL, searchQuery, total, uint32(numResults))

	return &bundle, nil
}

func (ms *mongoSession) FindIDs(searchQuery search.Query) (IDs []string, err error) {

	// First create a new query with the unsupported query options filtered out
	oldParams := searchQuery.URLQueryParameters(false)
	newParams := search.URLQueryParameters{}
	for _, param := range oldParams.All() {
		switch param.Key {
		case search.ContainedParam, search.ContainedTypeParam, search.ElementsParam, search.IncludeParam,
			search.RevIncludeParam, search.SummaryParam:
			continue
		default:
			newParams.Add(param.Key, param.Value)
		}
	}
	newQuery := search.Query{Resource: searchQuery.Resource, Query: newParams.Encode()}

	// Now search on that query, unmarshaling to a temporary struct and converting results to []string
	searcher := search.NewMongoSearcher(ms.db, ms.context, ms.dal.countTotalResults, ms.dal.enableCISearches, ms.dal.tokenParametersCaseSensitive, ms.dal.readonly)
	results, _, err := searcher.Search(newQuery)
	if err != nil {
		return nil, convertMongoErr(err)
	}

	IDs = make([]string, len(results))
	for i, result := range results {
		IDs[i] = result.Id()
	}

	return IDs, nil
}

func (ms *mongoSession) generatePagingLinks(baseURL url.URL, query search.Query, total uint32, numResults uint32) []models.BundleLinkComponent {

	links := make([]models.BundleLinkComponent, 0, 5)
	params := query.URLQueryParameters(true)
	offset := 0
	if pOffset := params.Get(search.OffsetParam); pOffset != "" {
		offset, _ = strconv.Atoi(pOffset)
		if offset < 0 {
			offset = 0
		}
	}
	count := search.NewQueryOptions().Count
	if pCount := params.Get(search.CountParam); pCount != "" {
		count, _ = strconv.Atoi(pCount)
		if count < 1 {
			count = search.NewQueryOptions().Count
		}
	}

	// For queries that don't support paging, only return the "self" link created directly from the original query.
	if !query.SupportsPaging() {
		links = append(links, newRawSelfLink(baseURL, query))
		return links
	}

	// Self link
	links = append(links, newLink("self", baseURL, params, offset, count))

	// First link
	links = append(links, newLink("first", baseURL, params, 0, count))

	// Previous link
	if offset > 0 {
		prevOffset := offset - count
		// Handle case where paging is uneven (e.g., count=10&offset=5)
		if prevOffset < 0 {
			prevOffset = 0
		}
		prevCount := offset - prevOffset
		links = append(links, newLink("previous", baseURL, params, prevOffset, prevCount))
	}

	// If counts are enabled, the total is accurate and can be used to compute the links.
	if ms.dal.countTotalResults {
		// Next Link
		if total > uint32(offset+count) {
			nextOffset := offset + count
			links = append(links, newLink("next", baseURL, params, nextOffset, count))
		}

		// Last Link
		remainder := (int(total) - offset) % count
		if int(total) < offset {
			remainder = 0
		}
		newOffset := int(total) - remainder
		if remainder == 0 && int(total) > count {
			newOffset = int(total) - count
		}
		links = append(links, newLink("last", baseURL, params, newOffset, count))

	} else {
		// Otherwise, we can only use the number of results returned by the search, and compare
		// it to the expected paging count to determine if we've exhaused the search results or not.

		// Next Link
		if int(numResults) == count {
			nextOffset := offset + count
			links = append(links, newLink("next", baseURL, params, nextOffset, count))
		}

		// Last Link
		// Without a total there is no way to compute the Last link. However, this still conforms
		// to RFC 5005 (https://tools.ietf.org/html/rfc5005).
	}

	return links
}

func newRawSelfLink(baseURL url.URL, query search.Query) models.BundleLinkComponent {
	queryString := ""
	if len(query.Query) > 0 {
		queryString = "?" + query.Query
	}

	return models.BundleLinkComponent{
		Relation: "self",
		Url:      baseURL.String() + queryString,
	}
}

func newLink(relation string, baseURL url.URL, params search.URLQueryParameters, offset int, count int) models.BundleLinkComponent {
	params.Set(search.OffsetParam, strconv.Itoa(offset))
	params.Set(search.CountParam, strconv.Itoa(count))
	baseURL.RawQuery = params.Encode()
	return models.BundleLinkComponent{Relation: relation, Url: baseURL.String()}
}

func convertIDToBsonID(id string) (primitive.ObjectID, error) {
	objId, err := primitive.ObjectIDFromHex(id)
	if err == nil {
		return objId, nil
	}
	return primitive.NilObjectID, models.NewOperationOutcome("fatal", "exception", "Id must be a valid BSON ObjectId")
}

func updateResourceMeta(resource *models2.Resource, versionId int) {
	now := time.Now()
	resource.SetLastUpdatedTime(now)
	resource.SetVersionId(versionId)
}

func convertMongoErr(err error) error {
	if err == nil {
		return nil
	}
	switch err {
	case mongo.ErrNoDocuments:
		return ErrNotFound
	default:
		_, filename, lineno, _ := runtime.Caller(1)
		return errors.Wrapf(err, "MongoDB operation error (%s:%d)", filename, lineno)
	}
}

// getResourceIdsFromBundle parses a slice of BSON resource IDs from a valid
// bundle of resources (typically returned from a search operation). Order is
// preserved.
func getResourceIdsFromBundle(bundle *models2.ShallowBundle) []string {
	resourceIds := make([]string, int(*bundle.Total))
	for i, elem := range bundle.Entry {
		resourceIds[i] = elem.Resource.Id()
	}
	return resourceIds
}

// setDiff returns all the elements in slice X that are not in slice Y
func setDiff(X, Y []string) []string {
	m := make(map[string]int)

	for _, y := range Y {
		m[y]++
	}

	var ret []string
	for _, x := range X {
		if m[x] > 0 {
			m[x]--
			continue
		}
		ret = append(ret, x)
	}

	return ret
}

// elementInSlice tests if a string element is in a larger slice of strings
func elementInSlice(element string, slice []string) bool {
	for _, el := range slice {
		if element == el {
			return true
		}
	}
	return false
}
