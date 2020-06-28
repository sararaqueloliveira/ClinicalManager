package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"gopkg.in/mgo.v2/dbtest"

	"github.com/gin-gonic/gin"
	mongowrapper "github.com/opencensus-integrations/gomongowrapper"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MiddlewareTestSuite struct {
	suite.Suite
	DBServer *dbtest.DBServer
	client   *mongowrapper.WrappedClient
	dbname   string
}

func TestMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(MiddlewareTestSuite))
}

func (m *MiddlewareTestSuite) SetupSuite() {
	// Create a temporary directory for the test database
	testDbDir := mongoTestDbDir()
	var err error
	err = os.Mkdir(testDbDir, 0775)

	if err != nil {
		panic(err)
	}

	// setup the mongo database
	m.DBServer = &dbtest.DBServer{}
	m.DBServer.SetPath(testDbDir)
	mgoSession := m.DBServer.Session()
	defer mgoSession.Close()
	serverUri := mgoSession.LiveServers()[0]
	m.client, err = mongowrapper.Connect(context.TODO(), options.Client().ApplyURI("mongodb://"+serverUri))
	m.dbname = "fhir-test"
	if err != nil {
		panic(err)
	}

	// Set gin to release mode (less verbose output)
	gin.SetMode(gin.ReleaseMode)
}

func (m *MiddlewareTestSuite) TearDownSuite() {
	m.client.Disconnect(context.TODO())
	m.DBServer.Stop()
	m.DBServer.Wipe()

	// remove the temporary database directory
	testDbDir := mongoTestDbDir()
	var err error
	err = removeContents(testDbDir)

	if err != nil {
		panic(err)
	}

	err = os.Remove(testDbDir)

	if err != nil {
		panic(err)
	}
}

func (m *MiddlewareTestSuite) TestRejectXML() {
	e := gin.New()
	e.Use(AbortNonJSONRequestsMiddleware)
	RegisterRoutes(e, nil, NewMongoDataAccessLayer(m.client, m.dbname, true, "", nil, DefaultConfig), DefaultConfig)
	server := httptest.NewServer(e)

	req, err := http.NewRequest("GET", server.URL+"/Patient", nil)
	m.NoError(err)
	req.Header.Add("Accept", "application/xml")
	resp, err := http.DefaultClient.Do(req)
	m.Equal(http.StatusNotAcceptable, resp.StatusCode)
}

func (m *MiddlewareTestSuite) TestReadOnlyMode() {
	e := gin.New()
	e.Use(ReadOnlyMiddleware)
	config := DefaultConfig
	config.ReadOnly = true
	RegisterRoutes(e, nil, NewMongoDataAccessLayer(m.client, m.dbname, true, "", nil, config), config)
	server := httptest.NewServer(e)

	req, err := http.NewRequest("POST", server.URL+"/Patient", nil)
	m.NoError(err)
	resp, err := http.DefaultClient.Do(req)
	m.Equal(http.StatusMethodNotAllowed, resp.StatusCode)
}
