package server

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/eug48/fhir/models2"
	"github.com/gin-gonic/gin"
	cors "github.com/itsjamie/gin-cors"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	mongowrapper "github.com/opencensus-integrations/gomongowrapper"
)

type AfterRoutes func(*gin.Engine)

type FHIRServer struct {
	Config           Config
	Engine           *gin.Engine
	MiddlewareConfig map[string][]gin.HandlerFunc
	AfterRoutes      []AfterRoutes
	Interceptors     map[string]InterceptorList
}

func (f *FHIRServer) AddMiddleware(key string, middleware gin.HandlerFunc) {
	f.MiddlewareConfig[key] = append(f.MiddlewareConfig[key], middleware)
}

// AddInterceptor adds a new interceptor for a particular database operation and FHIR resource.
// For example:
// AddInterceptor("Create", "Patient", patientInterceptorHandler) would register the
// patientInterceptorHandler methods to be run against a Patient resource when it is created.
//
// To run a handler against ALL resources pass "*" as the resourceType.
//
// Supported database operations are: "Create", "Update", "Delete"
func (f *FHIRServer) AddInterceptor(op, resourceType string, handler InterceptorHandler) error {

	if op == "Create" || op == "Update" || op == "Delete" {
		f.Interceptors[op] = append(f.Interceptors[op], Interceptor{ResourceType: resourceType, Handler: handler})
		return nil
	}
	return fmt.Errorf("AddInterceptor: unsupported database operation %s", op)
}

func NewServer(config Config) *FHIRServer {
	server := &FHIRServer{
		Config:           config,
		MiddlewareConfig: make(map[string][]gin.HandlerFunc),
		Interceptors:     make(map[string]InterceptorList),
	}
	server.Engine = gin.Default()

	if config.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	gin.DisableConsoleColor()

	server.Engine.Use(cors.Middleware(cors.Config{
		Origins:         "*",
		Methods:         "GET, PUT, POST, DELETE",
		RequestHeaders:  "Origin, Authorization, Content-Type, If-Match, If-None-Exist",
		ExposedHeaders:  "Location, ETag, Last-Modified",
		MaxAge:          86400 * time.Second, // Preflight expires after 1 day
		Credentials:     true,
		ValidateHeaders: false,
	}))

	if config.EnableXML {
		server.Engine.Use(EnableXmlToJsonConversionMiddleware())
		server.Engine.Use(AbortNonFhirXMLorJSONRequestsMiddleware)
	} else {
		server.Engine.Use(AbortNonJSONRequestsMiddleware)
	}

	if config.ReadOnly {
		server.Engine.Use(ReadOnlyMiddleware)
	}

	return server
}

func (f *FHIRServer) InitEngine() {
	var err error

	// TODO: Register OpenCensus metrics
	// TODO: StackDriver currently throwing up errors like  InvalidArgument desc = Field timeSeries[0].points[0].distributionValue had an invalid value: Distribution value has 34 |bucket_counts| fields, which is more than the 33 buckets allowed by the bucketing options.
	// if err := mongowrapper.RegisterAllViews(); err != nil {
	// log.Fatalf("Failed to register all OpenCensus views: %v\n", err)
	// }

	// Establish initial connection to mongo
	client, err := mongowrapper.Connect(context.Background(), options.Client().ApplyURI(f.Config.DatabaseURI))
	if err != nil {
		panic(errors.Wrap(err, "connecting to MongoDB"))
	}

	getFCV := bson.D{
		{"getParameter", 1},
		{"featureCompatibilityVersion", 1},
	}
	fcvResult := client.Database("admin").RunCommand(context.TODO(), getFCV)
	if fcvResult.Err() != nil {
		fmt.Printf("MongoDB: unable to read featureCompatibilityVersion: %s\n", fcvResult.Err().Error())
	} else {
		var fcvDoc bson.Raw
		err = fcvResult.Decode(&fcvDoc)
		if err != nil {
			panic(errors.Wrap(err, "decoding featureCompatibilityVersion"))
		}

		fcvVal := fcvDoc.Lookup("featureCompatibilityVersion", "version")
		fcv, ok := fcvVal.StringValueOK()
		if !ok {
			panic(errors.Wrap(err, "loading featureCompatibilityVersion as a string"))
		}
		fmt.Printf("MongoDB: featureCompatibilityVersion %s\n", fcv)
	}

	log.Printf("MongoDB: Connected (default database %s)\n", f.Config.DefaultDatabaseName)

	// Pre-create collections for transactions
	db := client.Database(f.Config.DefaultDatabaseName)
	CreateCollections(db)

	// Ensure all indexes
	if f.Config.CreateIndexes {
		NewIndexer(f.Config.DefaultDatabaseName, f.Config).ConfigureIndexes(db)
	}

	// Kick off the database op monitoring routine. This periodically checks db.currentOp() and
	// kills client-initiated operations exceeding the configurable timeout. Do this AFTER the index
	// build to ensure no index build processes are killed unintentionally.

	// ticker := time.NewTicker(f.Config.DatabaseKillOpPeriod)
	// TODO: disabled as requires high-grade permissions. Remove completely?
	// go killLongRunningOps(ticker, client.ConnectionString(), "admin", f.Config)

	// Register all API routes
	RegisterRoutes(f.Engine, f.MiddlewareConfig, NewMongoDataAccessLayer(client, f.Config.DefaultDatabaseName, f.Config.EnableMultiDB, f.Config.DatabaseSuffix, f.Interceptors, f.Config), f.Config)

	for _, ar := range f.AfterRoutes {
		ar(f.Engine)
	}

	// If not in -readonly mode, clear the count cache
	if !f.Config.ReadOnly {
		dbNames, err := client.ListDatabaseNames(context.TODO(), bson.D{})
		if err != nil {
			panic(fmt.Sprint("Server: Failed to call ListDatabaseNames: ", err))
		}
		dbNames = append(dbNames, f.Config.DefaultDatabaseName)

		for _, databaseName := range dbNames {
			if strings.HasSuffix(databaseName, f.Config.DatabaseSuffix) {
				db := client.Database(databaseName)
				count, err := db.Collection("countcache").CountDocuments(context.Background(), nil)
				if count > 0 || err != nil {
					err = db.Collection("countcache").Drop(context.Background())
					if err != nil {
						panic(fmt.Sprintf("Server: Failed to clear count cache (%+v)", err))
					}
				}
			}
		}
	} else {
		log.Println("Server: Running in read-only mode")
	}
}

func (f *FHIRServer) Run(port int, localhostOnly bool) {
	f.InitEngine()

	if localhostOnly {
		f.Engine.Run(fmt.Sprintf("localhost:%d", port))
	} else {
		f.Engine.Run(fmt.Sprintf(":%d", port))
	}
}

func (f *FHIRServer) InitDB(databaseName string) {
	// Connect
	client, err := mongowrapper.Connect(context.Background(), options.Client().ApplyURI(f.Config.DatabaseURI))
	if err != nil {
		panic(errors.Wrap(err, "connecting to MongoDB"))
	}

	// Pre-create collections for transactions
	db := client.Database(databaseName)
	CreateCollections(db)

	// Ensure all indexes
	if f.Config.CreateIndexes {
		NewIndexer(databaseName, f.Config).ConfigureIndexes(db)
	}
}

func CreateCollections(db *mongowrapper.WrappedDatabase) {
	// MongoDB transactions require that collections be pre-created
	for _, name := range models2.AllFhirResourceCollectionNames() {
		// fmt.Printf("pre-creating collection %s, %s\n", name, name+"_prev")
		res := db.RunCommand(context.Background(), bson.D{{"create", name + "_prev"}})
		if res.Err() != nil && !strings.Contains(res.Err().Error(), "already exists") {
			panic(res.Err())
		}
		res = db.RunCommand(context.Background(), bson.D{{"create", name}})
		if res.Err() != nil && !strings.Contains(res.Err().Error(), "already exists") {
			panic(res.Err())
		}
	}
}
