package server

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/eug48/fhir/auth"
)

// Config is used to hold information about the configuration of the FHIR server.
type Config struct {
	// ServerURL is the full URL for the root of the server. This may be used
	// by other middleware to compute redirect URLs
	ServerURL string

	// Auth determines what, if any authentication and authorization will be used
	// by the FHIR server
	Auth auth.Config

	// Whether to create indexes on startup
	CreateIndexes bool

	// IndexConfigPath is the path to an indexes.conf configuration file, specifying
	// what mongo indexes the server should create (or verify) on startup
	IndexConfigPath string

	// DatabaseURI is the url of the mongo replica set to use for the FHIR database.
	// A replica set is required for transactions support
	// e.g. mongodb://db1:27017,db2:27017/?replicaSet=rs1
	DatabaseURI string

	// DatabaseName is the name of the mongo database used for the fhir database by default.
	// Typically this will be the "fhir".
	DefaultDatabaseName string

	// EnableMultiDB allows requests to specify a specific Mongo database instead of the default
	// e.g. to use test4_fhir http://fhir-server/db/test4_fhir/Patient?name=alex
	EnableMultiDB bool

	// All custom database names should end with this suffix (default is "_fhir")
	DatabaseSuffix string

	// DatabaseSocketTimeout is the amount of time the mgo driver will wait for a response
	// from mongo before timing out.
	DatabaseSocketTimeout time.Duration

	// DatabaseOpTimeout is the amount of time GoFHIR will wait before killing a long-running
	// database process. This defaults to a reasonable upper bound for slow, pipelined queries: 30s.
	DatabaseOpTimeout time.Duration

	// DatabaseKillOpPeriod is the length of time between scans of the database to kill long-running ops.
	DatabaseKillOpPeriod time.Duration

	// CountTotalResults toggles whether the searcher should also get a total
	// count of the total results of a search. In practice this is a performance hit
	// for large datasets.
	CountTotalResults bool

	// EnableCISearches toggles whether the mongo searches uses regexes to maintain
	// case-insesitivity when performing searches on string fields, codes, etc.
	EnableCISearches bool

	// Whether to use case-sensitive search for token-type search parameters
	// Slower but default off for backwards compatibility and strict STU3 support
	// R4 leans towards case-sensitive, whereas STU3 text suggests case-insensitive (https://github.com/HL7/fhir/commit/13fb1c1f102caf7de7266d6e78ab261efac06a1f)
	TokenParametersCaseSensitive bool

	// Whether to support storing previous versions of each resource
	EnableHistory bool

	// Number of concurrent operations to do during batch bundle processing
	BatchConcurrency int

	// Whether to allow retrieving resources with no meta component,
	// meaning Last-Modified & ETag headers can't be generated (breaking spec compliance)
	// May be needed to support previous databases
	AllowResourcesWithoutMeta bool

	// ValidatorURL is an endpoint to which validation requests will be sent
	ValidatorURL string

	// ReadOnly toggles whether the server is in read-only mode. In read-only
	// mode any HTTP verb other than GET, HEAD or OPTIONS is rejected.
	ReadOnly bool

	// Enables requests and responses using FHIR XML MIME-types
	EnableXML bool

	// Debug toggles debug-level logging.
	Debug bool

	// Where to dump failed requests for debugging
	FailedRequestsDir string
}

// DefaultConfig is the default server configuration
var DefaultConfig = Config{
	ServerURL:                    "",
	IndexConfigPath:              "config/indexes.conf",
	DatabaseURI:                  "mongodb://localhost:27017/?replicaSet=rs0",
	DatabaseSuffix:               "_fhir",
	DatabaseSocketTimeout:        2 * time.Minute,
	DatabaseOpTimeout:            90 * time.Second,
	DatabaseKillOpPeriod:         10 * time.Second,
	Auth:                         auth.None(),
	EnableCISearches:             true,
	TokenParametersCaseSensitive: false,
	EnableHistory:                true,
	BatchConcurrency:             1,
	EnableXML:                    true,
	CountTotalResults:            true,
	ReadOnly:                     false,
	Debug:                        false,
}

func (config *Config) responseURL(r *http.Request, paths ...string) *url.URL {

	dbPrefix := r.Header.Get("db")
	if dbPrefix != "" {
		dbPrefix = "/db/" + dbPrefix
	}

	if config.ServerURL != "" {
		theURL := fmt.Sprintf("%s%s/%s", strings.TrimSuffix(config.ServerURL, "/"), dbPrefix, strings.Join(paths, "/"))
		responseURL, err := url.Parse(theURL)

		if err == nil {
			return responseURL
		}
	}

	responseURL := url.URL{}

	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		responseURL.Scheme = "https"
	} else {
		responseURL.Scheme = "http"
	}
	responseURL.Host = r.Host
	responseURL.Path = fmt.Sprintf("%s/%s", dbPrefix, strings.Join(paths, "/"))

	return &responseURL
}
