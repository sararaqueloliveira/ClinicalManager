package server

import (
	"context"
	"errors"
	"net/url"

	"github.com/eug48/fhir/models2"
	"github.com/eug48/fhir/search"
)

type DataAccessLayer interface {
	StartSession(ctx context.Context, dbname string) DataAccessSession
}

// DataAccessLayer is an interface for the various interactions that can occur on a FHIR data store.
type DataAccessSession interface {
	// Starts a transaction
	StartTransaction() error
	// Commits a transaction
	CommmitIfTransaction() error
	// Ends the session (aborts any running transactions) - for use in defer statements after StartSession
	Finish()

	// Get retrieves a single resource instance identified by its resource type and ID
	Get(id, resourceType string) (resource *models2.Resource, err error)
	// GetVersion retrieves a single resource instance identified by its resource type, ID and versionId
	GetVersion(id, versionId, resourceType string) (resource *models2.Resource, err error)
	// Post creates a resource instance, returning its new ID.
	Post(resource *models2.Resource) (id string, err error)
	// ConditionalPost creates a resource if the query finds no matches
	ConditionalPost(query search.Query, resource *models2.Resource) (httpStatus int, id string, outputResource *models2.Resource, err error)
	// PostWithID creates a resource instance with the given ID.
	PostWithID(id string, resource *models2.Resource) error
	// Put creates or updates a resource instance with the given ID.
	Put(id string, conditionalVersionId string, resource *models2.Resource) (createdNew bool, err error)
	// ConditionalPut creates or updates a resource based on search criteria.  If the criteria results in zero matches,
	// the resource is created.  If the criteria results in one match, it is updated.  Otherwise, a ErrMultipleMatches
	// error is returned.
	ConditionalPut(query search.Query, conditionalVersionId string, resource *models2.Resource) (id string, createdNew bool, err error)
	// Delete removes the resource instance with the given ID.  This operation cannot be undone.
	Delete(id, resourceType string) (newVersionId string, err error)
	// ConditionalDelete removes zero or more resources matching the passed in search criteria.  This operation cannot
	// be undone.
	ConditionalDelete(query search.Query) (count int64, err error)
	// Search executes a search given the baseURL and searchQuery.
	Search(baseURL url.URL, searchQuery search.Query) (bundle *models2.ShallowBundle, err error)
	// FindIDs executes a search given the searchQuery and returns only the matching IDs.  This function ignores
	// search options that don't make sense in this context: _include, _revinclude, _summary, _elements, _contained,
	// and _containedType.  It honors search options such as _count, _sort, and _offset.
	FindIDs(searchQuery search.Query) (result []string, err error)
	// History executes the history operation (partial support)
	History(baseURL url.URL, resoureType string, id string) (bundle *models2.ShallowBundle, err error)
}

// ErrNotFound indicates that the resource was not found (HTTP 404)
var ErrNotFound = errors.New("Resource Not Found")

// ErrDeleted indicates that the resource has been deleted (HTTP 410)
var ErrDeleted = errors.New("Resource deleted")

// ErrMultipleMatches indicates that the conditional update query returned multiple matches
type ErrMultipleMatches struct {
	msg string
}

func (e ErrMultipleMatches) Error() string {
	return e.msg
}

// ErrOpInterrupted indicates that the query was interrupted by a killOp() operation
var ErrOpInterrupted = errors.New("Operation Interrupted")

type ErrConflict struct {
	msg string
}

func (e ErrConflict) Error() string {
	return e.msg
}
