package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/eug48/fhir/models"
	"github.com/eug48/fhir/search"
	"github.com/gin-gonic/gin"
	mongowrapper "github.com/opencensus-integrations/gomongowrapper"
	"github.com/pebbe/util"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo/options"
	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type ServerSuite struct {
	initialSession *mgo.Session
	client         *mongowrapper.WrappedClient
	dbname         string
	Engine         *gin.Engine
	Server         *httptest.Server
	Interceptors   map[string]InterceptorList
	FixtureID      string
}

func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&ServerSuite{})

func (s *ServerSuite) SetUpSuite(c *C) {
	// Server configuration
	config := DefaultConfig
	s.dbname = "fhir-test"
	config.DatabaseSuffix = "-test"
	config.IndexConfigPath = "../fixtures/test_indexes.conf"
	config.AllowResourcesWithoutMeta = true

	// Set up the database
	var err error
	s.initialSession, err = mgo.Dial("localhost")
	util.CheckErr(err)
	s.client, err = mongowrapper.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost"))
	util.CheckErr(err)

	// Set gin to release mode (less verbose output)
	gin.SetMode(gin.ReleaseMode)

	// Build routes for testing
	s.Engine = gin.New()
	s.Engine.Use(gin.Logger())
	s.Engine.Use(gin.ErrorLogger())
	RegisterRoutes(s.Engine, make(map[string][]gin.HandlerFunc), NewMongoDataAccessLayer(s.client, s.dbname, true, "_fhir", s.Interceptors, config), config)

	// Create httptest server
	s.Server = httptest.NewServer(s.Engine)
}

func (s *ServerSuite) DB() *mgo.Database {
	return s.initialSession.DB("fhir-test")
}

func (s *ServerSuite) SetUpTest(c *C) {
	// Add patient fixture
	p := s.insertPatientFromFixture("../fixtures/patient-example-a.json")
	s.FixtureID = p.Id

	db := s.client.Database(s.dbname)
	CreateCollections(db)
}

func (s *ServerSuite) TearDownTest(c *C) {
	s.DB().C("patients").DropCollection()
}

func (s *ServerSuite) TearDownSuite(c *C) {
	s.DB().DropDatabase()
	s.initialSession.Close()
	s.Server.Close()
}

func (s *ServerSuite) TestGetPatients(c *C) {
	// Add 4 more patients
	for i := 0; i < 4; i++ {
		s.insertPatientFromFixture("../fixtures/patient-example-a.json")
	}
	assertBundleCount(c, s.Server.URL+"/Patient", 5, 5)
}

func (s *ServerSuite) TestGetPatientsWithOptions(c *C) {
	// Add 4 more patients
	for i := 0; i < 4; i++ {
		s.insertPatientFromFixture("../fixtures/patient-example-a.json")
	}
	assertBundleCount(c, s.Server.URL+"/Patient?_count=2", 2, 5)
	assertBundleCount(c, s.Server.URL+"/Patient?_offset=2", 3, 5)
	assertBundleCount(c, s.Server.URL+"/Patient?_count=2&_offset=1", 2, 5)
	assertBundleCount(c, s.Server.URL+"/Patient?_count=2&_offset=4", 1, 5)
	assertBundleCount(c, s.Server.URL+"/Patient?_offset=100", 0, 5)
}

func (s *ServerSuite) TestGetPatientsDefaultLimitIs100(c *C) {
	// Add 100 more patients
	for i := 0; i < 100; i++ {
		s.insertPatientFromFixture("../fixtures/patient-example-a.json")
	}
	assertBundleCount(c, s.Server.URL+"/Patient", 100, 101)
}

func (s *ServerSuite) TestGetPatientsPaging(c *C) {
	// Add 39 more patients
	for i := 0; i < 39; i++ {
		s.insertPatientFromFixture("../fixtures/patient-example-a.json")
	}

	// Default counts and less results than count
	bundle := performSearch(c, s.Server.URL+"/Patient")
	c.Assert(bundle.Link, HasLen, 3)
	assertPagingLink(c, bundle.Link[0], "self", 100, 0)
	assertPagingLink(c, bundle.Link[1], "first", 100, 0)
	assertPagingLink(c, bundle.Link[2], "last", 100, 0)

	// More results than count, first page
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10")
	c.Assert(bundle.Link, HasLen, 4)
	assertPagingLink(c, bundle.Link[0], "self", 10, 0)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "next", 10, 10)
	assertPagingLink(c, bundle.Link[3], "last", 10, 30)

	// More results than count, middle page
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&_offset=20")
	c.Assert(bundle.Link, HasLen, 5)
	assertPagingLink(c, bundle.Link[0], "self", 10, 20)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "previous", 10, 10)
	assertPagingLink(c, bundle.Link[3], "next", 10, 30)
	assertPagingLink(c, bundle.Link[4], "last", 10, 30)

	// More results than count, last page
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&_offset=30")
	c.Assert(bundle.Link, HasLen, 4)
	assertPagingLink(c, bundle.Link[0], "self", 10, 30)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "previous", 10, 20)
	assertPagingLink(c, bundle.Link[3], "last", 10, 30)

	// More results than count, uneven last page
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&_offset=25")
	c.Assert(bundle.Link, HasLen, 5)
	assertPagingLink(c, bundle.Link[0], "self", 10, 25)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "previous", 10, 15)
	assertPagingLink(c, bundle.Link[3], "next", 10, 35)
	assertPagingLink(c, bundle.Link[4], "last", 10, 35)

	// More results than count, uneven previous page and last page
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&_offset=5")
	c.Assert(bundle.Link, HasLen, 5)
	assertPagingLink(c, bundle.Link[0], "self", 10, 5)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "previous", 5, 0)
	assertPagingLink(c, bundle.Link[3], "next", 10, 15)
	assertPagingLink(c, bundle.Link[4], "last", 10, 35)

	// Search with other search criteria and results
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&gender=male")
	c.Assert(bundle.Link, HasLen, 4)
	assertPagingLink(c, bundle.Link[0], "self", 10, 0)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "next", 10, 10)
	assertPagingLink(c, bundle.Link[3], "last", 10, 30)

	// Search with no results
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&gender=FOO")
	c.Assert(bundle.Link, HasLen, 3)
	assertPagingLink(c, bundle.Link[0], "self", 10, 0)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "last", 10, 0)

	// Search with out of bounds offset
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=10&_offset=1000")
	c.Assert(bundle.Link, HasLen, 4)
	assertPagingLink(c, bundle.Link[0], "self", 10, 1000)
	assertPagingLink(c, bundle.Link[1], "first", 10, 0)
	assertPagingLink(c, bundle.Link[2], "previous", 10, 990)
	assertPagingLink(c, bundle.Link[3], "last", 10, 30)

	// Search with negative offset
	bundle = performSearch(c, s.Server.URL+"/Patient?_offset=-10")
	c.Assert(bundle.Link, HasLen, 3)
	assertPagingLink(c, bundle.Link[0], "self", 100, 0)
	assertPagingLink(c, bundle.Link[1], "first", 100, 0)
	assertPagingLink(c, bundle.Link[2], "last", 100, 0)

	// Search with negative count
	bundle = performSearch(c, s.Server.URL+"/Patient?_count=-10")
	c.Assert(bundle.Link, HasLen, 3)
	assertPagingLink(c, bundle.Link[0], "self", 100, 0)
	assertPagingLink(c, bundle.Link[1], "first", 100, 0)
	assertPagingLink(c, bundle.Link[2], "last", 100, 0)
}

func (s *ServerSuite) TestPatientPagingWithCountsDisabled(c *C) {
	config := DefaultConfig
	config.CountTotalResults = false
	dal, ok := NewMongoDataAccessLayer(s.client, s.dbname, true, "_fhir", nil, config).(*mongoDataAccessLayer)
	c.Assert(ok, Equals, true)

	// numResults is equal to the default query count of 100, so we should get a next link here
	u := url.URL{
		Scheme: "https",
		Host:   "fhir.example.com",
		Path:   "fhir/Patient",
	}
	session := dal.StartSession(context.TODO(), s.dbname).(*mongoSession)
	defer session.Finish()
	links := session.generatePagingLinks(u, search.Query{Resource: "Patient"}, 0, 100)
	c.Assert(len(links), Equals, 3)
	c.Assert(links[0].Relation, Equals, "self")
	c.Assert(links[1].Relation, Equals, "first")

	// The next link:
	next := links[2]
	c.Assert(next.Relation, Equals, "next")
	c.Assert(next.Url, Equals, "https://fhir.example.com/fhir/Patient?_offset=100&_count=100")

	// There should be no next link if numResults < count
	links = session.generatePagingLinks(u, search.Query{Resource: "Patient"}, 0, 75)
	c.Assert(len(links), Equals, 2)
	c.Assert(links[0].Relation, Equals, "self")
	c.Assert(links[1].Relation, Equals, "first")
}

func (s *ServerSuite) TestGetPatientSearchPagingPreservesSearchParams(c *C) {
	// Add 39 more patients
	for i := 0; i < 39; i++ {
		s.insertPatientFromFixture("../fixtures/patient-example-a.json")
	}

	// Default counts and less results than count
	bundle := performSearch(c, s.Server.URL+"/Patient?gender=male&name=Donald&name=Duck")
	v := url.Values{}
	v.Set("gender", "male")
	v.Add("name", "Donald")
	v.Add("name", "Duck")
	c.Assert(bundle.Link, HasLen, 3)
	assertPagingLinkWithParams(c, bundle.Link[0], "self", v, 100, 0)
	assertPagingLinkWithParams(c, bundle.Link[1], "first", v, 100, 0)
	assertPagingLinkWithParams(c, bundle.Link[2], "last", v, 100, 0)

	// More results than count, first page
	bundle = performSearch(c, s.Server.URL+"/Patient?gender=male&name=Donald&name=Duck&_count=10")
	c.Assert(bundle.Link, HasLen, 4)
	assertPagingLinkWithParams(c, bundle.Link[0], "self", v, 10, 0)
	assertPagingLinkWithParams(c, bundle.Link[1], "first", v, 10, 0)
	assertPagingLinkWithParams(c, bundle.Link[2], "next", v, 10, 10)
	assertPagingLinkWithParams(c, bundle.Link[3], "last", v, 10, 30)

	// More results than count, middle page
	bundle = performSearch(c, s.Server.URL+"/Patient?gender=male&name=Donald&name=Duck&_count=10&_offset=20")
	c.Assert(bundle.Link, HasLen, 5)
	assertPagingLinkWithParams(c, bundle.Link[0], "self", v, 10, 20)
	assertPagingLinkWithParams(c, bundle.Link[1], "first", v, 10, 0)
	assertPagingLinkWithParams(c, bundle.Link[2], "previous", v, 10, 10)
	assertPagingLinkWithParams(c, bundle.Link[3], "next", v, 10, 30)
	assertPagingLinkWithParams(c, bundle.Link[4], "last", v, 10, 30)
}

func (s *ServerSuite) TestGetPatient(c *C) {
	url := s.Server.URL + "/Patient/" + s.FixtureID
	res, err := http.Get(url)
	util.CheckErr(err)

	decoder := json.NewDecoder(res.Body)
	patient := &models.Patient{}
	err = decoder.Decode(patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donald")
}

func (s *ServerSuite) TestGetNonExistingPatient(c *C) {
	res, err := http.Get(s.Server.URL + "/Patient/" + bson.NewObjectId().Hex())
	util.CheckErr(err)
	logBody(res)
	c.Assert(res.StatusCode, Equals, 404)
}

func (s *ServerSuite) TestShowPatient(c *C) {

	res, err := http.Get(s.Server.URL + "/Patient")
	util.CheckErr(err)

	decoder := json.NewDecoder(res.Body)
	patientBundle := &models.Bundle{}
	err = decoder.Decode(patientBundle)
	util.CheckErr(err)

	var result []models.Patient
	collection := s.DB().C("patients")
	iter := collection.Find(nil).Iter()
	err = iter.All(&result)
	util.CheckErr(err)

	c.Assert(int(*patientBundle.Total), Equals, len(result))
}

func (s *ServerSuite) TestCreatePatient(c *C) {
	data, err := os.Open("../fixtures/patient-example-b.json")
	util.CheckErr(err)
	defer data.Close()

	res, err := http.Post(s.Server.URL+"/Patient", "application/json", data)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 201)
	createdPatientID := resourceIdFromLocation(res)
	s.checkCreatedPatient(createdPatientID, c)
}

func resourceIdFromLocation(res *http.Response) string {
	return resourceIdFromLocationStr(res.Header["Location"][0])
}
func resourceIdFromLocationStr(location string) string {
	splitLocation := strings.Split(location, "/")
	for i, str := range splitLocation {
		if str == "_history" {
			return splitLocation[i-1] // id comes before _history
		}
	}
	return splitLocation[len(splitLocation)-1] // should be last
}

func (s *ServerSuite) TestCreatePatient987(c *C) {
	data, err := os.Open("../fixtures/patient-example-MRN-987.json")
	util.CheckErr(err)
	defer data.Close()

	res, err := http.Post(s.Server.URL+"/Patient", "application/json", data)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 201)
	createdPatientID := resourceIdFromLocation(res)
	s.checkCreatedPatient(createdPatientID, c)
}

func (s *ServerSuite) TestCreatePatientConditionalCreated(c *C) {
	data, err := os.Open("../fixtures/patient-example-b.json")
	util.CheckErr(err)
	defer data.Close()

	client := &http.Client{}
	req, err := http.NewRequest("POST", s.Server.URL+"/Patient", data)
	util.CheckErr(err)
	req.Header.Add("If-None-Exist", "identifier=urn:oid:0.1.2.3.4.5.6.7|123")
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 201)
	createdPatientID := resourceIdFromLocation(res)
	s.checkCreatedPatient(createdPatientID, c)
	s.checkPatientCount(2, c) // 1st patient from SetUpTest
}

func (s *ServerSuite) TestCreatePatientConditionalCreated2(c *C) {
	s.TestCreatePatient(c)
	s.TestCreatePatient987(c)

	data, err := os.Open("../fixtures/patient-example-b.json")
	util.CheckErr(err)
	defer data.Close()

	client := &http.Client{}
	req, err := http.NewRequest("POST", s.Server.URL+"/Patient", data)
	util.CheckErr(err)
	req.Header.Add("If-None-Exist", "identifier=urn:oid:0.1.2.3.4.5.6.7|123")
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 201)
	createdPatientID := resourceIdFromLocation(res)
	s.checkCreatedPatient(createdPatientID, c)
	s.checkPatientCount(4, c) // 1st patient from SetUpTest
}

func (s *ServerSuite) TestCreatePatientConditionalExists(c *C) {
	s.checkPatientCount(1, c) // 1st patient from SetUpTest
	s.TestCreatePatient(c)
	s.checkPatientCount(2, c)
	s.TestCreatePatient987(c)
	s.checkPatientCount(3, c)

	data, err := os.Open("../fixtures/patient-example-b.json")
	util.CheckErr(err)
	defer data.Close()

	client := &http.Client{}
	req, err := http.NewRequest("POST", s.Server.URL+"/Patient", data)
	util.CheckErr(err)
	req.Header.Add("If-None-Exist", "identifier=urn:oid:0.1.2.3.4.5.6.7|987")
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 200)
	createdPatientID := resourceIdFromLocation(res)
	s.checkCreatedPatient(createdPatientID, c)

	s.checkPatientCount(3, c)
}

func (s *ServerSuite) TestCreatePatientConditionalMultiple(c *C) {
	s.TestCreatePatient987(c)
	s.TestCreatePatient987(c)

	data, err := os.Open("../fixtures/patient-example-b.json")
	util.CheckErr(err)
	defer data.Close()

	client := &http.Client{}
	req, err := http.NewRequest("POST", s.Server.URL+"/Patient", data)
	util.CheckErr(err)
	req.Header.Add("If-None-Exist", "identifier=urn:oid:0.1.2.3.4.5.6.7|987")
	req.Header.Add("Content-Type", "application/json")
	res, err := client.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 412)
	c.Assert(res.Header["Location"], IsNil)
	s.checkPatientCount(3, c)
}

func (s *ServerSuite) TestCreatePatientByPut(c *C) {
	data, err := os.Open("../fixtures/patient-example-b.json")
	util.CheckErr(err)
	defer data.Close()

	createdPatientID := bson.NewObjectId().Hex()
	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient/"+createdPatientID, data)
	util.CheckErr(err)

	req.Header.Add("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 201)
	s.checkCreatedPatient(createdPatientID, c)
}

func (s *ServerSuite) checkCreatedPatient(createdPatientID string, c *C) {
	if false {
		_, file, line, _ := runtime.Caller(1)
		c.Logf("checkCreatedPatient: called from %s:%d", path.Base(file), line)
	}

	patientCollection := s.DB().C("patients")
	patient := models.Patient{}
	err := patientCollection.Find(bson.M{"_id": createdPatientID}).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Don")
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}
func (s *ServerSuite) checkPatientCount(expected int, c *C) {
	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, expected)
}

func (s *ServerSuite) TestGetConditionsWithIncludes(c *C) {

	// Add 1 more patient
	patient := s.insertPatientFromFixture("../fixtures/patient-example-a.json")

	// Add condition
	data, err := os.Open("../fixtures/condition.json")
	util.CheckErr(err)
	defer data.Close()
	decoder := json.NewDecoder(data)
	condition := &models.Condition{}
	err = decoder.Decode(condition)
	util.CheckErr(err)
	// Set condition patient
	condition.Subject = &models.Reference{
		Reference:    "Patient/" + patient.Id,
		Type:         "Patient",
		ReferencedID: patient.Id,
		External:     new(bool),
	}
	condition.Id = bson.NewObjectId().Hex()
	err = s.DB().C("conditions").Insert(condition)
	util.CheckErr(err)

	assertBundleCount(c, s.Server.URL+"/Condition", 1, 1)
	b := assertBundleCount(c, s.Server.URL+"/Condition?_include=Condition:patient", 2, 1)
	c.Assert(b.Entry[0].Resource, FitsTypeOf, &models.Condition{})
	c.Assert(b.Entry[0].Search.Mode, Equals, "match")
	c.Assert(b.Entry[1].Resource, FitsTypeOf, &models.Patient{})
	c.Assert(b.Entry[1].Search.Mode, Equals, "include")
}

func (s *ServerSuite) TestWrongResource(c *C) {
	data, err := os.Open("../fixtures/patient-wrong-type.json")
	util.CheckErr(err)
	defer data.Close()

	res, err := http.Post(s.Server.URL+"/Patient", "application/json", data)
	logBody(res)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, http.StatusBadRequest)
}

func (s *ServerSuite) TestUpdatePatient(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient/"+s.FixtureID, data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)

	c.Assert(res.StatusCode, Equals, 200)
	patientCollection := s.DB().C("patients")
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donny")
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}

func (s *ServerSuite) TestConditionalUpdatePatientNoMatch(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient?name=Donny", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)

	c.Assert(res.StatusCode, Equals, 201)
	createdPatientID := resourceIdFromLocation(res)

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 2)

	// Check new patient
	patient := models.Patient{}
	err = patientCollection.FindId(createdPatientID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donny")
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)

	// Check existing (unmatched) patient
	patient2 := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient2)
	util.CheckErr(err)
	c.Assert(patient2.Name[0].Given[0], Equals, "Donald")
}

func (s *ServerSuite) TestConditionalUpdatePatientOneMatch(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient?name=Donald", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 200)
	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 1)
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donny")
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}

func (s *ServerSuite) TestVersionedConditionalUpdatePatientOneMatch200(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient?name=Donald", data)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("If-Match", "W/\"1\"")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	c.Assert(res.StatusCode, Equals, 200)

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 1)
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donny")
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}

func (s *ServerSuite) TestVersionedConditionalUpdatePatientOneMatch409(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient?name=Donald", data)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("If-Match", "W/\"5\"")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	logBody(res)
	c.Assert(res.StatusCode, Equals, 409)

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 1)
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donald") // unchanged
	c.Assert(patient.Meta, NotNil)
}

func (s *ServerSuite) TestVersionedUpdatePatientOneMatch200(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient/"+s.FixtureID, data)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("If-Match", "W/\"1\"")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	c.Assert(res.StatusCode, Equals, 200)

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 1)
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donny")
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}

func (s *ServerSuite) TestVersionedUpdatePatientOneMatch409(c *C) {

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient/"+s.FixtureID, data)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("If-Match", "W/\"5\"")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	logBody(res)
	c.Assert(res.StatusCode, Equals, 409)

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 1)
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donald") // unchanged
	c.Assert(patient.Meta, NotNil)
}

func (s *ServerSuite) TestBatchConditionalUpdatePatientUUIDIdentifier(c *C) {

	testPatient := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")

	data, err := os.Open("../fixtures/patient-example-uuid-identifier-update.json")
	util.CheckErr(err)
	defer data.Close()
	req, err := http.NewRequest("POST", s.Server.URL+"/", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 200)
	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 2) // update should not have created a new patient (2nd patient is from SetUpTest)
	patient := models.Patient{}
	err = patientCollection.FindId(testPatient.Id).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donny") // patient should have been modified
	c.Assert(patient.Id, Equals, testPatient.Id)
	c.Assert(patient.Meta, NotNil)
	c.Assert(patient.Meta.LastUpdated, NotNil)
	c.Assert(patient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(patient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)

	// check prev version stored with versionId of 0 (as fixture didn't have an initial versionId of 1)
	prevCollection := s.DB().C("patients_prev")
	count, err = prevCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 1)
	prevQuery := bson.M{
		"_id._id":      testPatient.Id,
		"_id._version": 0,
	}
	patient = models.Patient{}
	err = prevCollection.Find(prevQuery).One(&patient)
	if err != nil {
		err = errors.Wrapf(err, "failed to find prev version (%#v)", prevQuery)
	}
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donald") // should have prev version
	c.Assert(patient.Meta, IsNil)
}

func (s *ServerSuite) TestBatchCreate(c *C) {

	testPatient := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")

	data, err := os.Open("../fixtures/patient-example-uuid-identifier-create.json")
	util.CheckErr(err)
	defer data.Close()
	req, err := http.NewRequest("POST", s.Server.URL+"/", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	c.Assert(res.StatusCode, Equals, 200)
	resBody, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	resBundle := &models.Bundle{}
	err = json.Unmarshal(resBody, resBundle)
	util.CheckErr(err)
	c.Assert(*resBundle.Total, Equals, uint32(1))
	c.Assert(resBundle.Entry[0].Response.Status, Equals, "201")

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 3) // update should have created a new patient (1nd patient is from SetUpTest)
	existingPatient := models.Patient{}
	err = patientCollection.FindId(testPatient.Id).One(&existingPatient)
	util.CheckErr(err)
	c.Assert(existingPatient.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient.Meta, IsNil)

	newPatientId := resourceIdFromLocationStr(resBundle.Entry[0].Response.Location)
	newPatient := models.Patient{}
	err = patientCollection.FindId(newPatientId).One(&newPatient)
	util.CheckErr(err)
	c.Assert(newPatient.Name[0].Given[0], Equals, "Donny")
	c.Assert(newPatient.Meta, NotNil)
	c.Assert(newPatient.Meta.LastUpdated, NotNil)
	c.Assert(newPatient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(newPatient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}

func (s *ServerSuite) TestBatchCreateConditional200(c *C) {

	testPatient := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")

	// should do nothing
	data, err := os.Open("../fixtures/patient-example-uuid-identifier-create-conditional-200-batch.json")
	util.CheckErr(err)
	defer data.Close()
	req, err := http.NewRequest("POST", s.Server.URL+"/", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	resBody, err := ioutil.ReadAll(logBody(res))
	c.Assert(res.StatusCode, Equals, 200)
	util.CheckErr(err)
	resBundle := &models.Bundle{}
	err = json.Unmarshal(resBody, resBundle)
	util.CheckErr(err)
	c.Assert(*resBundle.Total, Equals, uint32(1))
	c.Assert(resBundle.Entry[0].Response.Status, Equals, "200")

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 2) // update should not have created a new patient (1nd patient is from SetUpTest)
	existingPatient := models.Patient{}
	err = patientCollection.FindId(testPatient.Id).One(&existingPatient)
	util.CheckErr(err)
	c.Assert(existingPatient.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient.Meta, IsNil)
}

func (s *ServerSuite) TestBatchCreateConditional201(c *C) {

	testPatient := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")

	data, err := os.Open("../fixtures/patient-example-uuid-identifier-create-conditional-201.json")
	util.CheckErr(err)
	defer data.Close()
	req, err := http.NewRequest("POST", s.Server.URL+"/", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	c.Assert(res.StatusCode, Equals, 200)
	resBody, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	resBundle := &models.Bundle{}
	err = json.Unmarshal(resBody, resBundle)
	util.CheckErr(err)
	c.Assert(*resBundle.Total, Equals, uint32(1))
	c.Assert(resBundle.Entry[0].Response.Status, Equals, "201")

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 3) // update should have created a new patient (1nd patient is from SetUpTest)
	existingPatient := models.Patient{}
	err = patientCollection.FindId(testPatient.Id).One(&existingPatient)
	util.CheckErr(err)
	c.Assert(existingPatient.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient.Meta, IsNil)

	newPatientId := resourceIdFromLocationStr(resBundle.Entry[0].Response.Location)
	newPatient := models.Patient{}
	err = patientCollection.FindId(newPatientId).One(&newPatient)
	util.CheckErr(err)
	c.Assert(newPatient.Name[0].Given[0], Equals, "Donny")
	c.Assert(newPatient.Meta, NotNil)
	c.Assert(newPatient.Meta.LastUpdated, NotNil)
	c.Assert(newPatient.Meta.LastUpdated.Precision, Equals, models.Precision(models.Timestamp))
	c.Assert(time.Since(newPatient.Meta.LastUpdated.Time).Minutes() < float64(1), Equals, true)
}

func (s *ServerSuite) TestBatchCreateConditional412(c *C) {

	testPatient1 := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")
	testPatient2 := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")

	// should do nothing
	data, err := os.Open("../fixtures/patient-example-uuid-identifier-create-conditional-200-batch.json")
	util.CheckErr(err)
	defer data.Close()
	req, err := http.NewRequest("POST", s.Server.URL+"/", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	c.Assert(res.StatusCode, Equals, 200)
	resBody, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	resBundle := &models.Bundle{}
	err = json.Unmarshal(resBody, resBundle)
	util.CheckErr(err)
	c.Assert(*resBundle.Total, Equals, uint32(1))
	c.Assert(resBundle.Entry[0].Response.Status, Equals, "412")

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 3) // update should not have created a new patient (1nd patient is from SetUpTest)
	existingPatient1 := models.Patient{}
	err = patientCollection.FindId(testPatient1.Id).One(&existingPatient1)
	util.CheckErr(err)
	c.Assert(existingPatient1.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient1.Meta, IsNil)

	existingPatient2 := models.Patient{}
	err = patientCollection.FindId(testPatient2.Id).One(&existingPatient2)
	util.CheckErr(err)
	c.Assert(existingPatient2.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient2.Meta, IsNil)
}

func (s *ServerSuite) TestTransactionCreateConditional412(c *C) {

	testPatient1 := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")
	testPatient2 := s.insertPatientFromFixture("../fixtures/patient-example-uuid-identifier.json")

	// should do nothing
	data, err := os.Open("../fixtures/patient-example-uuid-identifier-create-conditional-200-transaction.json")
	util.CheckErr(err)
	defer data.Close()
	req, err := http.NewRequest("POST", s.Server.URL+"/", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	c.Assert(res.StatusCode, Equals, 412)
	resBody, err := ioutil.ReadAll(logBody(res))
	util.CheckErr(err)
	opOutcome := &models.OperationOutcome{}
	err = json.Unmarshal(resBody, opOutcome)
	util.CheckErr(err)

	c.Assert(opOutcome.Issue[0].Severity, Equals, "error")
	c.Assert(opOutcome.Issue[0].Code, Equals, "duplicate")
	c.Assert(opOutcome.Issue[0].Details.Text, Equals, "search criteria were not selective enough")

	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 3) // update should not have created a new patient (1nd patient is from SetUpTest)
	existingPatient1 := models.Patient{}
	err = patientCollection.FindId(testPatient1.Id).One(&existingPatient1)
	util.CheckErr(err)
	c.Assert(existingPatient1.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient1.Meta, IsNil)

	existingPatient2 := models.Patient{}
	err = patientCollection.FindId(testPatient2.Id).One(&existingPatient2)
	util.CheckErr(err)
	c.Assert(existingPatient2.Name[0].Given[0], Equals, "Donald") // patient should not have been modified
	c.Assert(existingPatient2.Meta, IsNil)
}

func (s *ServerSuite) TestConditionalUpdateMultipleMatches(c *C) {

	// Add another duck to the database so we can have multiple results
	p2 := s.insertPatientFromFixture("../fixtures/patient-example-b.json")

	data, err := os.Open("../fixtures/patient-example-c.json")
	util.CheckErr(err)
	defer data.Close()

	req, err := http.NewRequest("PUT", s.Server.URL+"/Patient?name=Duck", data)
	req.Header.Add("Content-Type", "application/json")
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)

	// Should return an HTTP 412 Precondition Failed
	c.Assert(res.StatusCode, Equals, 412)

	// Ensure there are still only two
	patientCollection := s.DB().C("patients")
	count, err := patientCollection.Count()
	util.CheckErr(err)
	c.Assert(count, Equals, 2)

	// Ensure the two remaining have the right names
	patient := models.Patient{}
	err = patientCollection.FindId(s.FixtureID).One(&patient)
	util.CheckErr(err)
	c.Assert(patient.Name[0].Given[0], Equals, "Donald")
	patient2 := models.Patient{}
	err = patientCollection.FindId(p2.Id).One(&patient2)
	util.CheckErr(err)
	c.Assert(patient2.Name[0].Given[0], Equals, "Don")
}

func (s *ServerSuite) TestDeletePatient(c *C) {

	data, err := os.Open("../fixtures/patient-example-d.json")
	util.CheckErr(err)
	defer data.Close()

	res, err := http.Post(s.Server.URL+"/Patient", "application/json", data)
	util.CheckErr(err)

	createdPatientID := resourceIdFromLocation(res)

	req, err := http.NewRequest("DELETE", s.Server.URL+"/Patient/"+createdPatientID, nil)
	util.CheckErr(err)
	res, err = http.DefaultClient.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 204)
	patientCollection := s.DB().C("patients")
	count, err := patientCollection.FindId(createdPatientID).Count()
	c.Assert(count, Equals, 0)
}

func (s *ServerSuite) TestConditionalDelete(c *C) {

	// Add 39 more patients (with total 32 male and 8 female)
	patientCollection := s.DB().C("patients")
	for i := 0; i < 39; i++ {
		fix := loadFixture("Patient", "../fixtures/patient-example-a.json")
		patient := fix.(*models.Patient)
		patient.Id = bson.NewObjectId().Hex()
		if i%5 == 0 {
			patient.Gender = "female"
		}
		err := patientCollection.Insert(patient)
		util.CheckErr(err)
	}

	// First make sure there are really 40 patients
	count, err := patientCollection.Count()
	c.Assert(count, Equals, 40)

	req, err := http.NewRequest("DELETE", s.Server.URL+"/Patient?gender=male", nil)
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)

	c.Assert(res.StatusCode, Equals, 204)

	// Only the 8 females should be left
	count, err = patientCollection.Count()
	c.Assert(count, Equals, 8)
}

func (s *ServerSuite) TestUnescapedLinksInJSONResponse(c *C) {
	req, err := http.NewRequest("GET", s.Server.URL+"/Bundle", nil)
	util.CheckErr(err)
	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)

	body, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)

	// There should be none of these escape characters in the response
	c.Assert(bytes.Contains(body, []byte("\\u003c")), Equals, false)
	c.Assert(bytes.Contains(body, []byte("\\u003e")), Equals, false)
	c.Assert(bytes.Contains(body, []byte("\\u0026")), Equals, false)
}

func (s *ServerSuite) TestEmbbeddedResourceIDsGetRetrievedCorrectly(c *C) {
	res, err := postFixture(s.Server.URL, "Bundle", "../fixtures/clint_abbott_bundle.json")
	util.CheckErr(err)

	body, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	postedBundle := &models.Bundle{}
	err = json.Unmarshal(body, postedBundle)
	util.CheckErr(err)

	req, err := http.NewRequest("GET", s.Server.URL+"/Bundle/"+postedBundle.Resource.Id, nil)
	util.CheckErr(err)
	res, err = http.DefaultClient.Do(req)
	util.CheckErr(err)

	body, err = ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	var jsonBundle map[string]interface{}
	err = json.Unmarshal(body, &jsonBundle)
	util.CheckErr(err)

	// Check that you can get a patient's "id", not "_id"
	entry := jsonBundle["entry"].([]interface{})[0]
	entryMap := entry.(map[string]interface{})
	resource := entryMap["resource"].(map[string]interface{})
	c.Assert(len(resource["id"].(string)), Equals, 36)
	c.Assert(resource["_id"], IsNil)
}

func (s *ServerSuite) TestContainedResources(c *C) {
	res, err := postFixture(s.Server.URL, "Condition", "../fixtures/condition_with_contained_patient.json")
	util.CheckErr(err)

	body, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	postedCondition := &models.Condition{}
	err = json.Unmarshal(body, postedCondition)
	util.CheckErr(err)

	req, err := http.NewRequest("GET", s.Server.URL+"/Condition/"+postedCondition.Resource.Id, nil)
	util.CheckErr(err)
	res, err = http.DefaultClient.Do(req)
	util.CheckErr(err)

	body, err = ioutil.ReadAll(res.Body)
	util.CheckErr(err)
	var jsonCondition map[string]interface{}
	err = json.Unmarshal(body, &jsonCondition)
	util.CheckErr(err)

	// Check that the contained resources's ID is correct
	contained := jsonCondition["contained"].([]interface{})[0]
	containedMap := contained.(map[string]interface{})
	c.Assert(len(containedMap["id"].(string)), Equals, 19)
	c.Assert(containedMap["_id"], IsNil)

	// the extension should be without internal fields like @context
	extension := containedMap["extension"].([]interface{})[0]
	extensionMap := extension.(map[string]interface{})
	c.Assert(extensionMap["@context"], IsNil)
	c.Assert(extensionMap["url"], Equals, "http://hl7.org/fhir/StructureDefinition/us-core-race")

	// the managingOrganization reference should be without internal fields like reference__id
	managingOrganizationMap := containedMap["managingOrganization"].(map[string]interface{})
	c.Assert(managingOrganizationMap["reference"], Equals, "Organization/1")
	c.Assert(managingOrganizationMap["reference__id"], IsNil)
	c.Assert(managingOrganizationMap["reference__type"], IsNil)
	c.Assert(managingOrganizationMap["external"], IsNil)

	// Delete this entry
	err = s.DB().C("conditions").RemoveId(postedCondition.Resource.Id)
	util.CheckErr(err)
}

func (s *ServerSuite) TestSummaryCount(c *C) {
	req, err := http.NewRequest("GET", s.Server.URL+"/Patient?_summary=count", nil)
	util.CheckErr(err)

	res, err := http.DefaultClient.Do(req)
	util.CheckErr(err)
	defer res.Body.Close()

	// Response should be a bundle with a total, a raw self link, and no entries
	bundle := &models.Bundle{}
	body, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)

	err = json.Unmarshal(body, bundle)
	util.CheckErr(err)

	c.Assert(*bundle.Total, Equals, uint32(1))
	c.Assert(len(bundle.Entry), Equals, 0)

	c.Assert(len(bundle.Link), Equals, 1)
	self := bundle.Link[0]
	c.Assert(self.Relation, Equals, "self")
	c.Assert(self.Url, Equals, s.Server.URL+"/Patient?_summary=count")
}

func (s *ServerSuite) TestPatientEverything(c *C) {

	data, err := os.Open("../fixtures/patient-example-d.json")
	util.CheckErr(err)
	defer data.Close()

	res, err := http.Post(s.Server.URL+"/Patient", "application/json", data)
	util.CheckErr(err)

	createdPatientID := resourceIdFromLocation(res)

	// Do the $everything query
	res, err = http.Get(s.Server.URL + "/Patient/" + createdPatientID + "/$everything")
	util.CheckErr(err)

	// Response should be a bundle with a total of 1, a raw self link, and some entries
	bundle := &models.Bundle{}
	body, err := ioutil.ReadAll(res.Body)
	util.CheckErr(err)

	err = json.Unmarshal(body, bundle)
	util.CheckErr(err)

	c.Assert(*bundle.Total, Equals, uint32(1))
	// The only resource referring to this patient is the Patient resource itself, so we expect only 1 entry
	c.Assert(len(bundle.Entry), Equals, 1)

	c.Assert(len(bundle.Link), Equals, 1)
	self := bundle.Link[0]
	c.Assert(self.Relation, Equals, "self")
	// The self link should correctly show $everything as it's defined: _id=<id>&_include=*&_revinclude=*
	c.Assert(self.Url, Equals, s.Server.URL+"/Patient?_id="+createdPatientID+"&_include=*&_revinclude=*")
}

func performSearch(c *C, url string) *models.Bundle {
	res, err := http.Get(url)
	util.CheckErr(err)
	decoder := json.NewDecoder(logBody(res))
	bundle := &models.Bundle{}
	err = decoder.Decode(bundle)
	util.CheckErr(err)
	return bundle
}

func assertBundleCount(c *C, url string, expectedResults int, expectedTotal int) *models.Bundle {
	bundle := performSearch(c, url)
	c.Assert(len(bundle.Entry), Equals, expectedResults)
	c.Assert(*bundle.Total, Equals, uint32(expectedTotal))
	return bundle
}

func assertPagingLink(c *C, link models.BundleLinkComponent, relation string, count int, offset int) {
	c.Assert(link.Relation, Equals, relation)

	urlStr := link.Url
	urlURL, err := url.Parse(urlStr)
	util.CheckErr(err)
	v := urlURL.Query()

	c.Assert(v.Get(search.CountParam), Equals, fmt.Sprint(count))
	c.Assert(v.Get(search.OffsetParam), Equals, fmt.Sprint(offset))
}

func assertPagingLinkWithParams(c *C, link models.BundleLinkComponent, relation string, values url.Values, count int, offset int) {
	c.Assert(link.Relation, Equals, relation)

	urlStr := link.Url
	urlURL, err := url.Parse(urlStr)
	util.CheckErr(err)
	v := urlURL.Query()

	for key, val := range values {
		c.Assert(v[key], DeepEquals, val)
	}
	c.Assert(v.Get(search.CountParam), Equals, fmt.Sprint(count))
	c.Assert(v.Get(search.OffsetParam), Equals, fmt.Sprint(offset))
}

func postFixture(fhirHost, resourceName, fixturePath string) (res *http.Response, err error) {
	data, err := os.Open(fixturePath)
	if err != nil {
		return nil, err
	}
	defer data.Close()
	res, err = http.Post(fhirHost+"/"+resourceName, "application/json", data)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *ServerSuite) insertBundleFromFixture(filePath string) *models.Bundle {
	bundleCollection := s.DB().C("bundles")
	fix := loadFixture("Bundle", filePath)
	bundle := fix.(*models.Bundle)
	bundle.Id = bson.NewObjectId().Hex()
	err := bundleCollection.Insert(bundle)
	util.CheckErr(err)
	return bundle
}

func (s *ServerSuite) insertPatientFromFixture(filePath string) *models.Patient {
	patientCollection := s.DB().C("patients")
	fix := loadFixture("Patient", filePath)
	patient := fix.(*models.Patient)
	patient.Id = bson.NewObjectId().Hex()
	err := patientCollection.Insert(patient)
	util.CheckErr(err)
	return patient
}

func loadFixture(resourceName, fileName string) interface{} {
	data, err := os.Open(fileName)
	util.CheckErr(err)
	defer data.Close()

	decoder := json.NewDecoder(data)
	resource := models.NewStructForResourceName(resourceName)
	err = decoder.Decode(&resource)
	util.CheckErr(err)
	return resource
}

func logBody(res *http.Response) io.Reader {
	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("[logBody] %d bytes: %s\n", len(bodyBytes), string(bodyBytes))
	return bytes.NewReader(bodyBytes)
}
