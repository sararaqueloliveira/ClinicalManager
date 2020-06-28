package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"contrib.go.opencensus.io/exporter/jaeger"
	"contrib.go.opencensus.io/exporter/stackdriver"
	stackdriverPropagation "contrib.go.opencensus.io/exporter/stackdriver/propagation"
	"go.opencensus.io/plugin/ochttp"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"

	"github.com/eug48/fhir/auth"
	"github.com/eug48/fhir/fhir-server/middleware"
	"github.com/eug48/fhir/server"
	"github.com/golang/glog"
)

var gitCommit string

func main() {
	port := flag.Int("port", 3001, "Port to listen on")
	reqLog := flag.Bool("reqlog", false, "Enables request logging -- use with caution in production")
	mongodbURI := flag.String("mongodbURI", "mongodb://localhost:27017/fhir?replicaSet=rs0", "MongoDB connection URI - a replica set is required for transactions support")
	databaseName := flag.String("databaseName", "fhir", "MongoDB database name to use by default")
	enableMultiDB := flag.Bool("enableMultiDB", false, "Allow request to specify a specific Mongo database instead of the default, e.g. http://fhir-server/db/test4_fhir/Patient?name=alex")
	enableHistory := flag.Bool("enableHistory", true, "Keep previous versions of every resource")
	tokenParametersCaseSensitive := flag.Bool("tokenParametersCaseSensitive", false, "Whether token-type search parameters should be case sensitive (faster and R4 leans towards case-sensitive, whereas STU3 text suggests case-insensitive)")
	batchConcurrency := flag.Int("batchConcurrency", 1, "Number of concurrent database operations to do during batch bundle processing (1 to disable)")
	databaseSuffix := flag.String("databaseSuffix", "", "Request-specific MongoDB database name has to end with this (optional, e.g. '_fhir')")
	dontCreateIndexes := flag.Bool("dontCreateIndexes", false, "Don't create indexes for the 'fhr' database on startup")
	disableSearchTotals := flag.Bool("disableSearchTotals", false, "Don't query for all results of a search to return Bundle.total, only do paging")
	enableXML := flag.Bool("enableXML", false, "Enable support for the FHIR XML encoding")
	validatorURL := flag.String("validatorURL", "", "A FHIR validation endpoint to proxy validation requests to")
	failedRequestsDir := flag.String("failedRequestsDir", "", "Directory where to dump failed requests (e.g. with malformed json)")
	requestsDumpDir := flag.String("requestsDumpDir", "", "Directory where to dump all requests and responses")
	requestsDumpGET := flag.Bool("requestsDumpGET", true, "Whether to dump HTTP GET requests")
	enableStackdriverTracing := flag.Bool("enableStackdriverTracing", false, "Enable OpenCensus tracing to StackDriver")
	enableJaegerTracing := flag.Bool("enableJaegerTracing", false, "Enable OpenCensus tracing to Jaeger")
	startMongod := flag.Bool("startMongod", false, "Run mongod (for 'getting started' docker images - development only)")

	onlyInitDB := false
	if os.Args[1] == "initdb" {
		// collections are now created automatically using PrecreateCollectionsMiddleware
		// but this also creates indices and allows for cases when PrecreateCollectionsMiddleware
		// doesn't have permissions to create collections
		onlyInitDB = true
		flag.CommandLine.Parse(os.Args[2:])
	} else {
		flag.CommandLine.Parse(os.Args[1:])
	}

	if *startMongod {
		startMongoDB()
	}

	if *enableXML == false {
		fmt.Println("XML support is disabled (use --enableXML to enable)")
	}

	if gitCommit != "" {
		fmt.Printf("GoFHIR version %s\n", gitCommit)
	}
	glog.Infof("MongoDB URI is %s\n", *mongodbURI)

	if *enableStackdriverTracing {
		gcloudProject := os.Getenv("GCLOUD_PROJECT")
		if gcloudProject == "" {
			log.Fatal("GCLOUD_PROJECT not specified")
		}
		sde, err := stackdriver.NewExporter(stackdriver.Options{
			ProjectID:    gcloudProject,
			MetricPrefix: "gofhir",
		})
		if err != nil {
			log.Fatalf("Failed to create Stackdriver exporter: %v", err)
		}
		view.RegisterExporter(sde)
		trace.RegisterExporter(sde)
	}
	if *enableJaegerTracing {
		jaegerAgentEndpointURI := os.Getenv("JAEGER_AGENT_ENDPOINT_URI")
		jaegerCollectorEndpointURI := os.Getenv("JAEGER_COLLECTOR_ENDPOINT_URI")

		je, err := jaeger.NewExporter(jaeger.Options{
			AgentEndpoint:     jaegerAgentEndpointURI,
			CollectorEndpoint: jaegerCollectorEndpointURI,
			ServiceName:       "gofhir",
		})
		if err != nil {
			log.Fatalf("Failed to create the Jaeger exporter: %v", err)
		}
		trace.RegisterExporter(je)
	}
	tracingEnabled := *enableJaegerTracing || *enableStackdriverTracing
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})

	var MyConfig = server.Config{
		CreateIndexes:                !*dontCreateIndexes,
		IndexConfigPath:              "config/indexes.conf",
		DatabaseURI:                  *mongodbURI,
		DefaultDatabaseName:          *databaseName,
		EnableMultiDB:                *enableMultiDB,
		DatabaseSuffix:               *databaseSuffix,
		DatabaseSocketTimeout:        2 * time.Minute,
		DatabaseOpTimeout:            90 * time.Second,
		DatabaseKillOpPeriod:         10 * time.Second,
		Auth:                         auth.None(),
		EnableCISearches:             true,
		TokenParametersCaseSensitive: *tokenParametersCaseSensitive,
		CountTotalResults:            *disableSearchTotals == false,
		ReadOnly:                     false,
		EnableXML:                    *enableXML,
		EnableHistory:                *enableHistory,
		BatchConcurrency:             *batchConcurrency,
		Debug:                        true,
		ValidatorURL:                 *validatorURL,
		FailedRequestsDir:            *failedRequestsDir,
	}
	s := server.NewServer(MyConfig)
	if *reqLog {
		s.Engine.Use(server.RequestLoggerHandler)
	}

	if onlyInitDB {
		fmt.Printf("Initialising MongoDB database %s\n", *databaseName)
		s.InitDB(*databaseName)
		return
	}

	// Mutex middleware to work around the lack of proper transactions in MongoDB
	// (unless using a MongoDB >= 4.0 replica set)
	s.Engine.Use(middleware.ClientSpecifiedMutexesMiddleware())

	// Pre-create collections as required by MongoDB transactions
	s.Engine.Use(middleware.PrecreateCollectionsMiddleware(*mongodbURI))

	s.InitEngine()

	var handler http.Handler
	handler = s.Engine

	if *requestsDumpDir != "" {
		handler = middleware.FileLoggerMiddleware(*requestsDumpDir, *requestsDumpGET, handler)
	}
	if tracingEnabled {
		// receives and propagates distributed trace context
		handler = &ochttp.Handler{
			Handler:     handler,
			Propagation: &stackdriverPropagation.HTTPFormat{},
		}
	}

	address := fmt.Sprintf(":%d", *port)
	err := http.ListenAndServe(address, handler)
	if err != nil {
		panic("ListenAndServe failed: " + err.Error())
	}
}

func startMongoDB() {
	// this is for the fhir-server-with-mongo docker image
	mongod := exec.Command("mongod", "--replSet", "rs0")
	err := mongod.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[server.go] ERROR: failed to start mongod: %#v\n", err)
		os.Exit(1)
	}
	go func() {
		err := mongod.Wait()
		fmt.Fprintf(os.Stdout, "[server.go] mongod has exited with %#v, also exiting\n", err)
		if err == nil {
			os.Exit(0)
		} else {
			os.Exit(1)
		}
	}()

	// wait for MongoDB
	fmt.Println("Waiting for MongoDB")
	for {
		conn, err := net.Dial("tcp", "127.0.0.1:27017")
		if err == nil {
			conn.Close()
			break
		} else {
			time.Sleep(1 * time.Second)
			fmt.Print(".")
		}
	}
	fmt.Println()

	// initiate the replica set
	time.Sleep(2 * time.Second)
	mongoShell := exec.Command("mongo", "--eval", "rs.initiate()")
	err = mongoShell.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[server.go] ERROR: failed to start mongo shell: %#v", err)
		os.Exit(1)
	}
	time.Sleep(2 * time.Second)
}
