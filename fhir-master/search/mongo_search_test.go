package search

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/eug48/fhir/models"
	"github.com/pebbe/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	. "gopkg.in/check.v1"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/dbtest"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type MongoSearchSuite struct {
	DBServer      *dbtest.DBServer
	Session       *mgo.Session
	MongoUri      string
	MongoSearcher *MongoSearcher
	EST           *time.Location
	Local         *time.Location
}

var _ = Suite(&MongoSearchSuite{})

func (m *MongoSearchSuite) SetUpSuite(c *C) {
	m.EST = time.FixedZone("EST", -5*60*60)
	m.Local, _ = time.LoadLocation("Local")

	models.DisableOperationOutcomeDiagnosticsFileLine()
	var err error

	//turnOnDebugLog()

	// Set up the database
	m.DBServer = &dbtest.DBServer{}
	m.DBServer.SetPath(c.MkDir())

	// m.Session = m.DBServer.Session()
	m.MongoUri = "mongodb://localhost"
	m.Session, err = mgo.Dial("localhost")
	util.CheckErr(err)
	m.Session.SetSafe(&mgo.Safe{})
	db := m.Session.DB("fhir-test")
	db.DropDatabase()
	m.MongoSearcher = NewMongoSearcherForUri("mongodb://localhost", "fhir-test", true, true, false, false) // enableCISearches = true, readonly = false

	// Read in the data in FHIR format
	data, err := ioutil.ReadFile("../fixtures/search_test_data.json")
	util.CheckErr(err)

	maps := make([]interface{}, 19)
	err = json.Unmarshal(data, &maps)
	util.CheckErr(err)

	for _, resourceMap := range maps {
		r, err := models.MapToResource(resourceMap, true)
		util.CheckErr(err)
		collection := models.PluralizeLowerResourceName(reflect.TypeOf(r).Elem().Name())
		util.CheckErr(db.C(collection).Insert(r))
	}
}

func (m *MongoSearchSuite) TearDownSuite(c *C) {
	// m.MongoSearcher.db.DropDatabase()
	m.MongoSearcher.Close()
	m.Session.Close()
	m.DBServer.Wipe()
	m.DBServer.Stop()
}

func turnOnDebugLog() {
	mgo.SetDebug(true)
	var aLogger *log.Logger
	aLogger = log.New(os.Stderr, "", log.LstdFlags)
	mgo.SetLogger(aLogger)
}

// Tests token searches on CodeableConcept

func (m *MongoSearchSuite) TestConditionCodeQueryObjectBySystemAndCode(c *C) {
	q := Query{"Condition", "code=http://snomed.info/sct|123641001"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"code.coding": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^http://snomed\\.info/sct$", Options: "i"},
				"code":   primitive.Regex{Pattern: "^123641001$", Options: "i"},
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionCodeQueryBySystemAndCode(c *C) {
	q := Query{"Condition", "code=http://snomed.info/sct|123641001"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var condition models.Condition
		util.CheckErr(results[i].Unmarshal(&condition))
		conditions[i] = condition
	}

	foundIvd, foundCad := false, false
	for _, cond := range conditions {
		if strings.Contains(cond.Code.Text, "Ischemic Vascular Disease") {
			foundIvd = true
		} else if strings.Contains(cond.Code.Text, "Coronary Artery Disease No MI") {
			foundCad = true
		}
	}
	c.Assert(foundIvd && foundCad, Equals, true)
}

func (m *MongoSearchSuite) TestConditionCodeQueryByWrongCodeSystem(c *C) {
	q := Query{"Condition", "code=http://hl7.org/fhir/sid/icd-9|123641001"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestConditionCodeQueryObjectByCode(c *C) {
	q := Query{"Condition", "code=123641001"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{"code.coding.code": primitive.Regex{Pattern: "^123641001$", Options: "i"}})
}

func (m *MongoSearchSuite) TestConditionCodeQueryByCode(c *C) {
	q := Query{"Condition", "code=123641001"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	// convert search results to conditions

	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var condition models.Condition
		util.CheckErr(results[i].Unmarshal(&condition))
		conditions[i] = condition
	}

	foundIvd, foundCad := false, false
	for _, cond := range conditions {
		if strings.Contains(cond.Code.Text, "Ischemic Vascular Disease") {
			foundIvd = true
		} else if strings.Contains(cond.Code.Text, "Coronary Artery Disease No MI") {
			foundCad = true
		}
	}
	c.Assert(foundIvd && foundCad, Equals, true)
}

func (m *MongoSearchSuite) TestConditionSortByCodeAscending(c *C) {
	q := Query{"Condition", "_sort=code"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var condition models.Condition
		util.CheckErr(results[i].Unmarshal(&condition))
		conditions[i] = condition
	}

	var lastVal string
	for _, cond := range conditions {
		thisVal := getCodeableConceptComparisonValue(cond.Code)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestConditionSortByCodeDescending(c *C) {
	q := Query{"Condition", "_sort:desc=code"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var condition models.Condition
		util.CheckErr(results[i].Unmarshal(&condition))
		conditions[i] = condition
	}

	lastVal := "~"
	for _, cond := range conditions {
		thisVal := getCodeableConceptComparisonValue(cond.Code)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Approximating MongoDB sort strategy
func getCodeableConceptComparisonValue(c *models.CodeableConcept) string {
	//return getCodingsComparisonValue(c.Coding) + c.Text
	if len(c.Coding) > 0 {
		c0 := c.Coding[0]
		var userSel string
		if c0.UserSelected != nil {
			userSel = fmt.Sprintf("%t", *c0.UserSelected)
		}
		return fmt.Sprintf("%s%s%s%s%s", c0.Code, c0.Display, c0.System, userSel, c0.Version) + c.Text
	}

	return c.Text
}

// Tests token searches on Coding

func (m *MongoSearchSuite) TestImagingStudyBodySiteQueryObjectBySystemAndCode(c *C) {
	q := Query{"ImagingStudy", "bodysite=http://snomed.info/sct|67734004"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"series": bson.M{
			"$elemMatch": bson.M{
				"bodySite.system": primitive.Regex{Pattern: "^http://snomed\\.info/sct$", Options: "i"},
				"bodySite.code":   primitive.Regex{Pattern: "^67734004$", Options: "i"},
			},
		},
	})
}

func (m *MongoSearchSuite) TestImagingStudyBodySiteQueryBySystemAndCode(c *C) {
	q := Query{"ImagingStudy", "bodysite=http://snomed.info/sct|67734004"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestImagingStudyBodySiteQueryByWrongCodeSystem(c *C) {
	q := Query{"ImagingStudy", "bodysite=http://hl7.org/fhir/sid/icd-9|67734004"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// Tests token searches on []Identifier

func (m *MongoSearchSuite) TestEncounterIdentifierQueryObjectBySystemAndValue(c *C) {
	q := Query{"Encounter", "identifier=http://acme.com|1"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"identifier": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^http://acme\\.com$", Options: "i"},
				"value":  primitive.Regex{Pattern: "^1$", Options: "i"},
			},
		},
	})
}

func (m *MongoSearchSuite) TestEncounterIdentifierQueryBySystemAndValue(c *C) {
	q := Query{"Encounter", "identifier=http://acme.com|1"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestEncounterIdentifierQueryByWrongSystem(c *C) {
	q := Query{"Encounter", "identifier=http://example.com|1"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestEncounterSortByIdentifierAscending(c *C) {
	q := Query{"Encounter", "_sort=identifier"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)

	// convert search results to encounters
	encounters := make([]models.Encounter, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Encounter
		util.CheckErr(results[i].Unmarshal(&resource))
		encounters[i] = resource
	}

	var lastVal string
	for _, enc := range encounters {
		thisVal := getIdentifiersComparisonValue(enc.Identifier, false)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestEncounterSortByIdentifierDescending(c *C) {
	q := Query{"Encounter", "_sort:desc=identifier"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)

	// convert search results to encounters
	encounters := make([]models.Encounter, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Encounter
		util.CheckErr(results[i].Unmarshal(&resource))
		encounters[i] = resource
	}

	lastVal := "~"
	for _, enc := range encounters {
		thisVal := getIdentifiersComparisonValue(enc.Identifier, true)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Approximating MongoDB sort strategy
func getIdentifiersComparisonValue(iSlice []models.Identifier, descending bool) string {
	if len(iSlice) == 0 {
		return ""
	}

	strs := make([]string, len(iSlice))
	for i := range iSlice {
		strs[i] = iSlice[i].System + iSlice[i].Use + iSlice[i].Value
	}
	sort.Strings(strs)
	if descending {
		return strs[len(strs)-1]
	}
	return strs[0]
}

// Tests token searches on boolean

func (m *MongoSearchSuite) TestImmunizationNotGivenQueryObject(c *C) {
	q := Query{"Immunization", "notgiven=false"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"notGiven": false,
	})
}

func (m *MongoSearchSuite) TestImmunizationNotGivenQuery(c *C) {
	q := Query{"Immunization", "notgiven=false"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Immunization", "notgiven=true"}

	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestInvalidBooleanValuePanics(c *C) {
	q := Query{"Immunization", "notgiven=maybe"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createInvalidSearchError("MSG_PARAM_INVALID", "Parameter \"notgiven\" content is invalid"))
}

// TODO: Test token searches on code, string, and ContactPoint

// Tests reference searches by reference id

func (m *MongoSearchSuite) TestConditionReferenceQueryObjectByPatientId(c *C) {
	q := Query{"Condition", "patient=4954037118555241963"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"subject.reference__id":   "4954037118555241963",
		"subject.reference__type": "Patient",
	})
}

func (m *MongoSearchSuite) TestConditionReferenceQueryByPatientId(c *C) {
	q := Query{"Condition", "patient=4954037118555241963"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)
}

func (m *MongoSearchSuite) TestConditionReferenceQueryObjectByPatientTypeAndId(c *C) {
	q := Query{"Condition", "patient=Patient/4954037118555241963"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{"subject.reference__id": "4954037118555241963", "subject.reference__type": "Patient"})
}

func (m *MongoSearchSuite) TestConditionPatientQueryByTypeAndId(c *C) {
	q := Query{"Condition", "patient=Patient/4954037118555241963"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)
}

func (m *MongoSearchSuite) TestConditionReferenceQueryObjectByPatientURL(c *C) {
	q := Query{"Condition", "patient=http://acme.com/Patient/123456789"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{"subject.reference": primitive.Regex{Pattern: "^http://acme\\.com/Patient/123456789$", Options: "i"}})
}

func (m *MongoSearchSuite) TestConditionSortByPatientAscending(c *C) {
	q := Query{"Condition", "_sort=patient"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	var lastVal string
	for _, cond := range conditions {
		thisVal := getReferenceComparisonValue(cond.Subject)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestConditionSortByPatientDescending(c *C) {
	q := Query{"Condition", "_sort:desc=patient"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	lastVal := "~"
	for _, cond := range conditions {
		thisVal := getReferenceComparisonValue(cond.Subject)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Approximating MongoDB sort strategy
func getReferenceComparisonValue(r *models.Reference) string {
	var ext string
	if r.External != nil {
		ext = fmt.Sprintf("%t", *r.External)
	}
	return fmt.Sprintf("%s%s%s%s%s", r.Display, ext, r.Reference, r.ReferencedID, r.Type)
}

// These next tests ensure that the indexer is properly converted to a mongo
// query, since the Bundle message param indicates only the first resource
// should be considered.  It also ensures inline resource search works with id.
func (m *MongoSearchSuite) TestBundleReferenceQueryObjectByMessageId(c *C) {
	q := Query{"Bundle", "message=4954037118555241963"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"entry.0.resource.resourceType": "MessageHeader",
		"entry.0.resource._id":          "4954037118555241963",
	})
}

func (m *MongoSearchSuite) TestBundleReferenceQueryByMessageId(c *C) {
	q := Query{"Bundle", "message=5542705384245559634"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Bundle", "message=5542705384245559635"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// TODO: Test execution of reference search on PatientURL (as above)

// These tests validate chained search using the mongo Pipeline
func (m *MongoSearchSuite) TestConditionChainedSearchPipelineObject(c *C) {
	q := Query{"Condition", "patient.gender=male"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "Condition")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "patients",
			"localField":   "subject.reference__id",
			"foreignField": "_id",
			"as":           "_lookup0",
		}},
		bson.M{"$match": bson.M{
			"_lookup0.gender": primitive.Regex{Pattern: "^male$", Options: "i"},
		}},
	})
}

func (m *MongoSearchSuite) TestChainedSearchPipelineObjectWithOr(c *C) {
	q := Query{"Condition", "patient.gender=foo,bar"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "Condition")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "patients",
			"localField":   "subject.reference__id",
			"foreignField": "_id",
			"as":           "_lookup0",
		}},
		bson.M{"$match": bson.M{
			"$or": []bson.M{
				bson.M{"_lookup0.gender": primitive.Regex{Pattern: "^foo$", Options: "i"}},
				bson.M{"_lookup0.gender": primitive.Regex{Pattern: "^bar$", Options: "i"}},
			},
		}},
	})
}

func (m *MongoSearchSuite) TestChainedSearchPipelineObjectWithMultipleReferencePaths(c *C) {
	q := Query{"AuditEvent", "patient.gender=male"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "AuditEvent")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "patients",
			"localField":   "agent.reference.reference__id",
			"foreignField": "_id",
			"as":           "_lookup0",
		}},
		bson.M{"$lookup": bson.M{
			"from":         "patients",
			"localField":   "entity.reference.reference__id",
			"foreignField": "_id",
			"as":           "_lookup1",
		}},
		bson.M{"$match": bson.M{
			"$or": []bson.M{
				bson.M{"_lookup0.gender": primitive.Regex{Pattern: "^male$", Options: "i"}},
				bson.M{"_lookup1.gender": primitive.Regex{Pattern: "^male$", Options: "i"}},
			},
		}},
	})
}

func (m *MongoSearchSuite) TestChainedSearchPipelineObjectWithMultipleReferencePathsAndOr(c *C) {
	q := Query{"AuditEvent", "patient.gender=foo,bar"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "AuditEvent")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "patients",
			"localField":   "agent.reference.reference__id",
			"foreignField": "_id",
			"as":           "_lookup0",
		}},
		bson.M{"$lookup": bson.M{
			"from":         "patients",
			"localField":   "entity.reference.reference__id",
			"foreignField": "_id",
			"as":           "_lookup1",
		}},
		bson.M{"$match": bson.M{
			"$or": []bson.M{
				bson.M{"$or": []bson.M{
					bson.M{"_lookup0.gender": primitive.Regex{Pattern: "^foo$", Options: "i"}},
					bson.M{"_lookup1.gender": primitive.Regex{Pattern: "^foo$", Options: "i"}},
				}},
				bson.M{"$or": []bson.M{
					bson.M{"_lookup0.gender": primitive.Regex{Pattern: "^bar$", Options: "i"}},
					bson.M{"_lookup1.gender": primitive.Regex{Pattern: "^bar$", Options: "i"}},
				}},
			},
		}},
	})
}

func (m *MongoSearchSuite) TestConditionReferenceQueryByPatientGender(c *C) {
	q := Query{"Condition", "patient.gender=male"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)

	q = Query{"Condition", "patient.gender=female"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestConditionReferenceQueryByPatientGenderOr(c *C) {
	q := Query{"Condition", "patient.gender=male,foo"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)

	q = Query{"Condition", "patient.gender=male,female"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)
}

// These tests validate reverse chained search using the mongo pipeline
func (m *MongoSearchSuite) TestPatientReverseChainedSearchPipelineObject(c *C) {
	q := Query{"Patient", "_has:Observation:subject:code=1234-5"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "Patient")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.Pipeline, HasLen, 3)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "observations",
			"localField":   "_id",
			"foreignField": "subject.reference__id",
			"as":           "_lookup0",
		}},
		bson.M{"$match": bson.M{"_lookup0.code.coding.code": primitive.Regex{Pattern: "^1234-5$", Options: "i"}}},
	})
}

func (m *MongoSearchSuite) TestPatientReverseChainedSearchPipelineObjectWithOr(c *C) {
	q := Query{"Patient", "_has:Observation:subject:code=1234-5,5678-9"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "Patient")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.Pipeline, HasLen, 3)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "observations",
			"localField":   "_id",
			"foreignField": "subject.reference__id",
			"as":           "_lookup0",
		}},
		bson.M{"$match": bson.M{
			"$or": []bson.M{
				bson.M{"_lookup0.code.coding.code": primitive.Regex{Pattern: "^1234-5$", Options: "i"}},
				bson.M{"_lookup0.code.coding.code": primitive.Regex{Pattern: "^5678-9$", Options: "i"}},
			}},
		},
	})
}

func (m *MongoSearchSuite) TestReverseChainedSearchPipelineObjectWithMultipleReferencePaths(c *C) {
	q := Query{"Patient", "_has:AuditEvent:patient:outcome=foo"}

	bsonQuery := m.MongoSearcher.convertToBSON(q)
	c.Assert(bsonQuery.Resource, Equals, "Patient")
	c.Assert(bsonQuery.Query, IsNil)
	c.Assert(bsonQuery.Pipeline, HasLen, 4)
	c.Assert(bsonQuery.usesPipeline(), Equals, true)

	c.Assert(bsonQuery.Pipeline, DeepEquals, []bson.M{
		bson.M{"$match": bson.M{}},
		bson.M{"$lookup": bson.M{
			"from":         "auditevents",
			"localField":   "_id",
			"foreignField": "agent.reference.reference__id",
			"as":           "_lookup0",
		}},
		bson.M{"$lookup": bson.M{
			"from":         "auditevents",
			"localField":   "_id",
			"foreignField": "entity.reference.reference__id",
			"as":           "_lookup1",
		}},
		bson.M{"$match": bson.M{
			"$or": []bson.M{
				bson.M{"_lookup0.outcome": primitive.Regex{Pattern: "^foo$", Options: "i"}},
				bson.M{"_lookup1.outcome": primitive.Regex{Pattern: "^foo$", Options: "i"}},
			},
		}},
	})
}

func (m *MongoSearchSuite) TestPatientReferenceQueryByObservationCode(c *C) {
	q := Query{"Patient", "_has:Observation:subject:code=1234-5"}
	results, total, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(total, Equals, uint32(1))
	c.Assert(len(results), Equals, 1)

	q = Query{"Patient", "_has:Observation:subject:code=0000-0"}
	results, total, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(total, Equals, uint32(0))
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestPatientReferenceQueryByObservationCodeOr(c *C) {
	q := Query{"Patient", "_has:Observation:subject:code=1234-5,5678-9"}
	results, total, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(total, Equals, uint32(2))
	c.Assert(len(results), Equals, 2)

	q = Query{"Patient", "_has:Observation:subject:code=1234-5,0000-0"}
	results, total, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(total, Equals, uint32(1))
	c.Assert(len(results), Equals, 1)
}

// These next tests ensure that the indexer is properly converted to a mongo
// query, since the Bundle message param indicates only the first resource should
// be considered.  It also ensures chained search works for inlined resources.
func (m *MongoSearchSuite) TestBundleReferenceQueryObjectByMessageDestination(c *C) {
	q := Query{"Bundle", "message.destination-uri=http://acme.com/ehr/fhir"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"entry.0.resource.resourceType":         "MessageHeader",
		"entry.0.resource.destination.endpoint": "http://acme.com/ehr/fhir",
	})
}

func (m *MongoSearchSuite) TestBundleReferenceQueryByMessageDestination(c *C) {
	q := Query{"Bundle", "message.destination-uri=http://acme.com/ehr/fhir"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Bundle", "message.destination-uri=http://acme.com/ehr/foo"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// These tests ensure that a modifier works with a chained search
func (m *MongoSearchSuite) TestBundleReferenceQueryObjectByMessageHeaderDestination(c *C) {
	q := Query{"Bundle", "message:MessageHeader.destination-uri=http://acme.com/ehr/fhir"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"entry.0.resource.resourceType":         "MessageHeader",
		"entry.0.resource.destination.endpoint": "http://acme.com/ehr/fhir",
	})
}

func (m *MongoSearchSuite) TestBundleReferenceQueryByMessageHeaderDestination(c *C) {
	q := Query{"Bundle", "message:MessageHeader.destination-uri=http://acme.com/ehr/fhir"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Bundle", "message:MessageHeader.destination-uri=http://acme.com/ehr/foo"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// Test date searches on DateTime / Period

func (m *MongoSearchSuite) TestConditionOnsetQueryObject(c *C) {
	q := Query{"Condition", "onset-date=2012-03-01T07:00-05:00"}
	o := m.MongoSearcher.createQueryObject(q)

	// 2012-03-01T07:00-05:00 <= onsetDateTime < 2012-03-01T07:01-05:00
	onsetDtFrom := o["$or"].([]bson.M)[0]["onsetDateTime.__from"].(bson.M)
	onsetDtTo := o["$or"].([]bson.M)[0]["onsetDateTime.__to"].(bson.M)
	c.Assert(onsetDtFrom, HasLen, 1)
	c.Assert(onsetDtTo, HasLen, 1)

	gte := onsetDtFrom["$gte"].(time.Time)
	lt := onsetDtTo["$lte"].(time.Time)
	c.Assert(gte.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 0, 0, 0, m.EST).UnixNano())
	c.Assert(lt.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 1, 0, 0, m.EST).UnixNano())

	// 2012-03-01T07:00:00-05:00 <= onsetPeriod.start <= onsetPeriod.end < 2012-03-01T07:01:00-05:00
	start := o["$or"].([]bson.M)[1]["onsetPeriod.start.__from"].(bson.M)["$gte"].(time.Time)
	c.Assert(start.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 0, 0, 0, m.EST).UnixNano())
	end := o["$or"].([]bson.M)[1]["onsetPeriod.end.__to"].(bson.M)["$lte"].(time.Time)
	c.Assert(end.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 1, 0, 0, m.EST).UnixNano())
}

func (m *MongoSearchSuite) TestConditionOnsetQueryToMinute(c *C) {
	q := Query{"Condition", "onset-date=2012-03-01T07:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	cond := &conditions[0]
	cond2 := &models.Condition{}
	err = m.Session.DB("fhir-test").C("conditions").FindId("8664777288161060797").One(cond2)

	c.Assert(cond, DeepEquals, cond2)
}

func (m *MongoSearchSuite) TestConditionOnsetQueryToDay(c *C) {
	q := Query{"Condition", "onset-date=2012-03-01"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)
}

func (m *MongoSearchSuite) TestConditionOnsetQueryWrongTime(c *C) {
	q := Query{"Condition", "onset-date=2012-03-01T08:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestConditionOnsetGTQueryObject(c *C) {
	q := Query{"Condition", "onset-date=gt2012-03-01T07:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"onsetDateTime.__to": bson.M{
					"$gt": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.end.__to": bson.M{
					"$gt": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod":     bson.M{"$ne": nil},
				"onsetPeriod.end": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionOnsetGTQuery(c *C) {
	q := Query{"Condition", "onset-date=gt2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestConditionOnsetSAQueryObject(c *C) {
	q := Query{"Condition", "onset-date=sa2012-03-01T07:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"onsetDateTime.__from": bson.M{
					"$gt": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.start.__from": bson.M{
					"$gt": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionOnsetSAQuery(c *C) {
	q := Query{"Condition", "onset-date=sa2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestConditionOnsetLTQueryObject(c *C) {
	q := Query{"Condition", "onset-date=lt2012-03-01T07:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"onsetDateTime.__from": bson.M{
					"$lt": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.start.__from": bson.M{
					"$lt": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod":       bson.M{"$ne": nil},
				"onsetPeriod.start": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionOnsetLTQuery(c *C) {
	q := Query{"Condition", "onset-date=lt2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestConditionOnsetEBQueryObject(c *C) {
	q := Query{"Condition", "onset-date=eb2012-03-01T07:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"onsetDateTime.__to": bson.M{
					"$lt": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.end.__to": bson.M{
					"$lt": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionOnsetEBQuery(c *C) {
	q := Query{"Condition", "onset-date=eb2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestConditionOnsetGEQueryObject(c *C) {
	q := Query{"Condition", "onset-date=ge2012-03-01T07:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"onsetDateTime.__to": bson.M{
					"$gte": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetDateTime.__from": bson.M{
					"$gte": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.end.__to": bson.M{
					"$gte": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.start.__from": bson.M{
					"$gte": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod":     bson.M{"$ne": nil},
				"onsetPeriod.end": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionOnsetGEQuery(c *C) {
	q := Query{"Condition", "onset-date=ge2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)
}

func (m *MongoSearchSuite) TestConditionOnsetLEQueryObject(c *C) {
	q := Query{"Condition", "onset-date=le2012-03-01T07:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"onsetDateTime.__from": bson.M{
					"$lte": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetDateTime.__to": bson.M{
					"$lte": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.start.__from": bson.M{
					"$lte": time.Date(2012, time.March, 1, 7, 0, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod.end.__to": bson.M{
					"$lte": time.Date(2012, time.March, 1, 7, 1, 0, 0, m.Local),
				},
			},
			bson.M{
				"onsetPeriod":       bson.M{"$ne": nil},
				"onsetPeriod.start": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionOnsetLEQuery(c *C) {
	q := Query{"Condition", "onset-date=le2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)
}

func (m *MongoSearchSuite) TestConditionSortByOnsetAscending(c *C) {
	q := Query{"Condition", "_sort=onset-date"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	var lastVal time.Time
	for _, cond := range conditions {
		thisVal := cond.OnsetDateTime.Time
		c.Assert(thisVal.Before(lastVal), Equals, false)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestConditionSortByOnsetDescending(c *C) {
	q := Query{"Condition", "_sort:desc=onset-date"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}
	lastVal := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, cond := range conditions {
		thisVal := cond.OnsetDateTime.Time
		c.Assert(thisVal.After(lastVal), Equals, false)
		lastVal = thisVal
	}
}

// Test date searches on Period

func (m *MongoSearchSuite) TestEncounterPeriodQueryObject(c *C) {
	q := Query{"Encounter", "date=2012-11-01T08:50-05:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 2)

	// 2012-11-01T08:50:00-05:00 <= period.start <= period.end < 2012-11-01T08:51:00-05:00

	c.Assert(o["period.start.__from"].(bson.M), HasLen, 1)
	start := o["period.start.__from"].(bson.M)["$gte"].(time.Time)
	c.Assert(start.UnixNano(), Equals, time.Date(2012, time.November, 1, 8, 50, 0, 0, m.EST).UnixNano())

	c.Assert(o["period.end.__to"].(bson.M), HasLen, 1)
	end := o["period.end.__to"].(bson.M)["$lte"].(time.Time)
	c.Assert(end.UnixNano(), Equals, time.Date(2012, time.November, 1, 8, 51, 0, 0, m.EST).UnixNano())
}

func (m *MongoSearchSuite) TestEncounterPeriodQuery(c *C) {
	q := Query{"Encounter", "date=2012-11-01T08:50-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestEncounterPeriodQueryWrongTime(c *C) {
	q := Query{"Encounter", "date=2012-11-01T07:50:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestEncounterPeriodGTQueryObject(c *C) {
	q := Query{"Encounter", "date=gt2012-11-01T08:30"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 1)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"period.end.__to": bson.M{
					"$gt": time.Date(2012, time.November, 1, 8, 31, 0, 0, m.Local),
				},
			},
			bson.M{
				"period":     bson.M{"$ne": nil},
				"period.end": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestEncounterPeriodGTQuery(c *C) {
	q := Query{"Encounter", "date=gt2012-11-01T08:50-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestEncounterPeriodSAQueryObject(c *C) {
	q := Query{"Encounter", "date=sa2012-11-01T08:45"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 1)
	c.Assert(o, DeepEquals, bson.M{
		"period.start.__from": bson.M{
			"$gt": time.Date(2012, time.November, 1, 8, 46, 0, 0, m.Local),
		},
	})
}

func (m *MongoSearchSuite) TestEncounterPeriodSAQuery(c *C) {
	q := Query{"Encounter", "date=sa2012-11-01T08:45-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestEncounterPeriodLTQueryObject(c *C) {
	q := Query{"Encounter", "date=lt2012-11-01T08:30"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 1)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"period.start.__from": bson.M{
					"$lt": time.Date(2012, time.November, 1, 8, 30, 0, 0, m.Local),
				},
			},
			bson.M{
				"period":       bson.M{"$ne": nil},
				"period.start": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestEncounterPeriodLTQuery(c *C) {
	q := Query{"Encounter", "date=lt2012-11-01T08:50-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 3)
}

func (m *MongoSearchSuite) TestEncounterPeriodEBQueryObject(c *C) {
	q := Query{"Encounter", "date=eb2012-11-01T09:00"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 1)
	c.Assert(o, DeepEquals, bson.M{
		"period.end.__to": bson.M{
			"$lt": time.Date(2012, time.November, 1, 9, 0, 0, 0, m.Local),
		},
	})
}

func (m *MongoSearchSuite) TestEncounterPeriodEBQuery(c *C) {
	q := Query{"Encounter", "date=eb2012-11-01T09:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 3)
}

func (m *MongoSearchSuite) TestEncounterPeriodGEQueryObject(c *C) {
	q := Query{"Encounter", "date=ge2012-11-01T08:30"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 1)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"period.end.__to": bson.M{
					"$gte": time.Date(2012, time.November, 1, 8, 31, 0, 0, m.Local),
				},
			},
			bson.M{
				"period.start.__from": bson.M{
					"$gte": time.Date(2012, time.November, 1, 8, 30, 0, 0, m.Local),
				},
			},
			bson.M{
				"period":     bson.M{"$ne": nil},
				"period.end": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestEncounterPeriodGEQuery(c *C) {
	q := Query{"Encounter", "date=ge2012-11-01T08:50-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestEncounterPeriodLEQueryObject(c *C) {
	q := Query{"Encounter", "date=le2012-11-01T08:30"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, HasLen, 1)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"period.start.__from": bson.M{
					"$lte": time.Date(2012, time.November, 1, 8, 30, 0, 0, m.Local),
				},
			},
			bson.M{
				"period.end.__to": bson.M{
					"$lte": time.Date(2012, time.November, 1, 8, 31, 0, 0, m.Local),
				},
			},
			bson.M{
				"period":       bson.M{"$ne": nil},
				"period.start": nil,
			},
		},
	})
}

func (m *MongoSearchSuite) TestEncounterPeriodLEQuery(c *C) {
	q := Query{"Encounter", "date=le2012-11-01T08:50-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)
}

func (m *MongoSearchSuite) TestEncounterSortByPeriodAscending(c *C) {
	q := Query{"Encounter", "_sort=date"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)

	// convert search results to encounters
	encounters := make([]models.Encounter, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Encounter
		util.CheckErr(results[i].Unmarshal(&resource))
		encounters[i] = resource
	}

	var lastVal time.Time
	for _, enc := range encounters {
		thisVal := enc.Period.Start.Time
		c.Assert(thisVal.Before(lastVal), Equals, false)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestEncounterSortByPeriodDescending(c *C) {
	q := Query{"Encounter", "_sort:desc=date"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)

	// convert search results to encounters
	encounters := make([]models.Encounter, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Encounter
		util.CheckErr(results[i].Unmarshal(&resource))
		encounters[i] = resource
	}

	lastVal := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, enc := range encounters {
		thisVal := enc.Period.Start.Time
		c.Assert(thisVal.After(lastVal), Equals, false)
		lastVal = thisVal
	}
}

// TODO: Test date searches on date, instant, and Timing

// Test number searches on positiveInt

func (m *MongoSearchSuite) TestImmunizationDoseSequenceNumberQueryObject(c *C) {
	q := Query{"Immunization", "dose-sequence=1"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"vaccinationProtocol": bson.M{
			"$elemMatch": bson.M{
				"doseSequence": bson.M{
					"$gte": float64(0.5),
					"$lt":  float64(1.5),
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestImmunizationDoseSequenceNumberQuery(c *C) {
	q := Query{"Immunization", "dose-sequence=1"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestImmunizationDoseSequenceWrongNumberQuery(c *C) {
	q := Query{"Immunization", "dose-sequence=0"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)

	q = Query{"Immunization", "dose-sequence=2"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestNumberQueryLT(c *C) {
	q := Query{"Immunization", "dose-sequence=lt2"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"vaccinationProtocol.doseSequence": bson.M{
			"$lt": float64(2),
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Immunization", "dose-sequence=lt1"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestNumberQueryGT(c *C) {
	q := Query{"Immunization", "dose-sequence=gt0"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"vaccinationProtocol.doseSequence": bson.M{
			"$gt": float64(0),
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Immunization", "dose-sequence=gt1"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestNumberQueryLE(c *C) {
	q := Query{"Immunization", "dose-sequence=le2"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"vaccinationProtocol.doseSequence": bson.M{
			"$lte": float64(2.5),
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Immunization", "dose-sequence=le1"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestNumberQueryGE(c *C) {
	q := Query{"Immunization", "dose-sequence=ge0"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"vaccinationProtocol.doseSequence": bson.M{
			"$gte": float64(-0.5),
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Immunization", "dose-sequence=ge1"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestNumberQueryNE(c *C) {
	q := Query{"Immunization", "dose-sequence=ne0"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{"vaccinationProtocol.doseSequence": bson.M{"$lt": float64(-0.5)}},
			bson.M{"vaccinationProtocol.doseSequence": bson.M{"$gte": float64(0.5)}},
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Immunization", "dose-sequence=ne1"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// TODO: Test number searches on decimal, integer, and unsignedInt

// Test string searches on string

func (m *MongoSearchSuite) TestDeviceStringQueryObject(c *C) {
	q := Query{"Device", "manufacturer=Acme"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{"manufacturer": primitive.Regex{Pattern: "^Acme$", Options: "i"}})
}

func (m *MongoSearchSuite) TestDeviceStringQuery(c *C) {
	q := Query{"Device", "manufacturer=Acme"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
	var res models.Device
	util.CheckErr(results[0].Unmarshal(&res))
	dev := &res

	dev2 := &models.Device{}
	err = m.Session.DB("fhir-test").C("devices").FindId("7045606679745526995").One(dev2)

	c.Assert(dev, DeepEquals, dev2)
}

func (m *MongoSearchSuite) TestNonMatchingDeviceStringQuery(c *C) {
	q := Query{"Device", "manufacturer=Zinc"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestPatientSortByGivenAscending(c *C) {
	q := Query{"Patient", "_sort=given"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	// convert search results to patients
	patients := make([]models.Patient, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Patient
		util.CheckErr(results[i].Unmarshal(&resource))
		patients[i] = resource
	}

	var lastVal string
	for _, p := range patients {
		thisVal := p.Name[0].Given[0]
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestPatientSortByGivenDescending(c *C) {
	q := Query{"Patient", "_sort:desc=given"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	// convert search results to patients
	patients := make([]models.Patient, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Patient
		util.CheckErr(results[i].Unmarshal(&resource))
		patients[i] = resource
	}

	lastVal := "~"
	for _, p := range patients {
		thisVal := p.Name[0].Given[0]
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Test string searches on HumanName

func (m *MongoSearchSuite) TestPatientNameStringQueryObject(c *C) {
	q := Query{"Patient", "name=Peters"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{"name.text": primitive.Regex{Pattern: "^Peters", Options: "i"}},
			bson.M{"name.family": primitive.Regex{Pattern: "^Peters", Options: "i"}},
			bson.M{"name.given": primitive.Regex{Pattern: "^Peters", Options: "i"}},
		},
	})
}

func (m *MongoSearchSuite) TestPatientNameStringQuery(c *C) {
	q := Query{"Patient", "name=Peters"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	q = Query{"Patient", "name=John"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestNonMatchingPatientNameStringQuery(c *C) {
	q := Query{"Patient", "name=Peterson"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestPatientSortByNameAscending(c *C) {
	q := Query{"Patient", "_sort=name"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	// convert search results to patients
	patients := make([]models.Patient, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Patient
		util.CheckErr(results[i].Unmarshal(&resource))
		patients[i] = resource
	}

	var lastVal string
	for _, p := range patients {
		thisVal := getHumanNamesComparisonValue(p.Name, false)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestPatientSortByNameDescending(c *C) {
	q := Query{"Patient", "_sort:desc=name"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	// convert search results to patients
	patients := make([]models.Patient, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Patient
		util.CheckErr(results[i].Unmarshal(&resource))
		patients[i] = resource
	}

	lastVal := "~"
	for _, p := range patients {
		thisVal := getHumanNamesComparisonValue(p.Name, true)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Approximating MongoDB sort strategy
func getHumanNamesComparisonValue(nSlice []models.HumanName, descending bool) string {
	if len(nSlice) == 0 {
		return ""
	}

	strs := make([]string, len(nSlice))
	for i := range nSlice {
		strs[i] = getHumanNameComparisonValue(nSlice[i])
	}
	sort.Strings(strs)
	if descending {
		return strs[len(strs)-1]
	}
	return strs[0]
}

// Approximating MongoDB sort strategy
func getHumanNameComparisonValue(n models.HumanName) string {
	var last string
	if len(n.Family) > 0 {
		last = n.Family
	}
	var first string
	if len(n.Given) > 0 {
		first = n.Given[0]
	}
	return last + first
}

// Test string searches on Address

func (m *MongoSearchSuite) TestPatientAddressStringQueryObject(c *C) {
	q := Query{"Patient", "address=AK"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{"address.text": primitive.Regex{Pattern: "^AK", Options: "i"}},
			bson.M{"address.line": primitive.Regex{Pattern: "^AK", Options: "i"}},
			bson.M{"address.city": primitive.Regex{Pattern: "^AK", Options: "i"}},
			bson.M{"address.state": primitive.Regex{Pattern: "^AK", Options: "i"}},
			bson.M{"address.postalCode": primitive.Regex{Pattern: "^AK", Options: "i"}},
			bson.M{"address.country": primitive.Regex{Pattern: "^AK", Options: "i"}},
		},
	})
}

func (m *MongoSearchSuite) TestPatientAddressStringQuery(c *C) {
	q := Query{"Patient", "address=AK"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestNonMatchingPatientAddressStringQuery(c *C) {
	q := Query{"Patient", "address=CA"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// Test quantity searches on Quantity

func (m *MongoSearchSuite) TestValueQuantityQueryObjectByValueAndUnit(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=185||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"valueQuantity.value.__from": bson.M{"$gte": 184.5},
		"valueQuantity.value.__to":   bson.M{"$lte": 185.5},
		"$or": []bson.M{
			bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
			bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
		},
	})
}

func (m *MongoSearchSuite) TestValueQuantityQueryByValueAndUnit(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=185||lbs"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByValueAndCode(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=185||[lb_av]"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByWrongValueAndUnit(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=186||lbs"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByValueAndWrongUnit(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=185||pounds"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestValueQuantityQueryObjectByValueAndSystemAndCode(c *C) {
	q := Query{"Observation", "value-quantity=185|http://unitsofmeasure.org|[lb_av]"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"valueQuantity.value.__from": bson.M{"$gte": 184.5},
		"valueQuantity.value.__to":   bson.M{"$lte": 185.5},
		"valueQuantity.code":         primitive.Regex{Pattern: "^\\[lb_av\\]$", Options: "i"},
		"valueQuantity.system":       primitive.Regex{Pattern: "^http://unitsofmeasure\\.org$", Options: "i"},
	})
}

func (m *MongoSearchSuite) TestValueQuantityQueryObjectByValueAndUnitLT(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=lt186||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"valueQuantity.value.__from": bson.M{"$lt": float64(186)},
		"$or": []bson.M{
			bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
			bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Observation", "value-quantity=lt184.5||lbs"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestValueQuantityQueryObjectByValueAndUnitGT(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=gt184||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"valueQuantity.value.__to": bson.M{"$gt": float64(184)},
		"$or": []bson.M{
			bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
			bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Observation", "value-quantity=gt185.5||lbs"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestValueQuantityQueryObjectByValueAndUnitLE(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=le186||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"valueQuantity": bson.M{
			"$and": []bson.M{
				bson.M{
					"$or": []bson.M{
						bson.M{"value.__from": bson.M{"$lte": float64(185.5)}},
						bson.M{"value.__to": bson.M{"$lte": float64(186.5)}},
					},
				},
				bson.M{
					"$or": []bson.M{
						bson.M{"code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
						bson.M{"unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
					},
				},
			},
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Observation", "value-quantity=le185||lbs"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestValueQuantityQueryObjectByValueAndUnitGE(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=ge184||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"valueQuantity.value.__from": bson.M{"$gte": float64(183.5)},
		"$or": []bson.M{
			bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
			bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
		},
	})

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	q = Query{"Observation", "value-quantity=ge185||lbs"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByValueAndSystemAndCode(c *C) {
	q := Query{"Observation", "value-quantity=185|http://unitsofmeasure.org|[lb_av]"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByWrongValueAndSystemAndCode(c *C) {
	q := Query{"Observation", "value-quantity=184|http://unitsofmeasure.org|[lb_av]"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByValueAndWrongSystemAndCode(c *C) {
	q := Query{"Observation", "value-quantity=185|http://loinc.org|[lb_av]"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestValueQuantityQueryByValueAndSystemAndWrongCode(c *C) {
	q := Query{"Observation", "value-quantity=185|http://unitsofmeasure.org|lbs"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestComponentValueQuantityQueryObjectByValueAndUnit(c *C) {
	// New in STU3 - Searches component.value ONLY. This didn't exist prior to STU3 3.0.0.
	c.Skip("Sorting by parameters that resolve to multiple paths is not supported")
	q := Query{"Observation", "component-value-quantity=185||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"component": bson.M{
			"$elemMatch": bson.M{
				"valueQuantity.value.__from": bson.M{"$gte": 184.5},
				"valueQuantity.value.__to":   bson.M{"$lte": 185.5},
				"$or": []bson.M{
					bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
					bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestComboValueQuantityQueryObjectByValueAndUnit(c *C) {
	// New in STU3 - Searches component.value and value. This was the previous default behavior
	// before STU3 (3.0.0) was released.
	c.Skip("Sorting by parameters that resolve to multiple paths is not supported")
	q := Query{"Observation", "combo-value-quantity=185||lbs"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"component": bson.M{
					"$elemMatch": bson.M{
						"valueQuantity.value.__from": bson.M{"$gte": 184.5},
						"valueQuantity.value.__to":   bson.M{"$lte": 185.5},
						"$or": []bson.M{
							bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
							bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
						},
					},
				},
			},
			bson.M{
				"valueQuantity.value.__from": bson.M{"$gte": 184.5},
				"valueQuantity.value.__to":   bson.M{"$lte": 185.5},
				"$or": []bson.M{
					bson.M{"valueQuantity.code": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
					bson.M{"valueQuantity.unit": primitive.Regex{Pattern: "^lbs$", Options: "i"}},
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestObservationSortByValueQuantityAscending(c *C) {
	c.Skip("Sorting by parameters that resolve to multiple paths is not supported")
	q := Query{"Observation", "_sort=value-quantity"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)

	// convert search results to observations
	observations := make([]models.Observation, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Observation
		util.CheckErr(results[i].Unmarshal(&resource))
		observations[i] = resource
	}

	var lastVal string
	for _, o := range observations {
		thisVal := getQuantityComparisonValue(o.ValueQuantity)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestObservationSortByValueQuantityDescending(c *C) {
	c.Skip("Sorting by parameters that resolve to multiple paths is not supported")
	q := Query{"Observation", "_sort:desc=value-quantity"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 5)

	// convert search results to observations
	observations := make([]models.Observation, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Observation
		util.CheckErr(results[i].Unmarshal(&resource))
		observations[i] = resource
	}

	lastVal := "~"
	for _, o := range observations {
		thisVal := getQuantityComparisonValue(o.ValueQuantity)
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Approximating MongoDB sort strategy
func getQuantityComparisonValue(q *models.Quantity) string {
	if q == nil {
		return ""
	}

	var value string
	if q.Value != nil {
		value = fmt.Sprintf("%f", q.Value.Num)
	}
	return fmt.Sprintf("%s%s%s%s%s", q.Code, q.Comparator, q.System, q.Unit, value)
}

// TODO: Test quantity searches on Money, SimpleQuantity, Duration, Count, Distance, and Age

// Test URI searches on URI

func (m *MongoSearchSuite) TestSubscriptionURLQueryObject(c *C) {
	q := Query{"Subscription", "url=https://biliwatch.com/customers/mount-auburn-miu/on-result"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"channel.endpoint": "https://biliwatch.com/customers/mount-auburn-miu/on-result",
	})
}

func (m *MongoSearchSuite) TestSubscriptionURLQuery(c *C) {
	q := Query{"Subscription", "url=https://biliwatch.com/customers/mount-auburn-miu/on-result"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

// TODO: Test composite searches

// Test custom search

type BroParam struct {
	info  SearchParamInfo
	IsBro bool
}

func (b *BroParam) getInfo() SearchParamInfo {
	return b.info
}

func (b *BroParam) setInfo(info SearchParamInfo) {
	b.info = info
}

func (b *BroParam) getQueryParamAndValue() (string, string) {
	return b.info.Name, fmt.Sprint(b.IsBro)
}

func BroParser(info SearchParamInfo, data SearchParamData) (SearchParam, error) {
	broParam := new(BroParam)
	broParam.info = info
	broParam.IsBro = (data.Value == "true")
	return broParam, nil
}

func BroBSONBuilder(p SearchParam, m *MongoSearcher) (bson.M, error) {
	bp, ok := p.(*BroParam)
	if !ok {
		return nil, errors.New("Expected BroParam")
	}
	if bp.IsBro {
		return bson.M{
			"gender": "male",
		}, nil
	}
	return bson.M{
		"gender": bson.M{
			"$not": "male",
		},
	}, nil
}

var broSearchParamInfo = SearchParamInfo{
	Resource: "Patient",
	Name:     "bro",
	Type:     "test.bro",
}

func (m *MongoSearchSuite) TestBroCustomQueryObject(c *C) {
	GlobalRegistry().RegisterParameterInfo(broSearchParamInfo)
	GlobalRegistry().RegisterParameterParser("test.bro", BroParser)
	GlobalMongoRegistry().RegisterBSONBuilder("test.bro", BroBSONBuilder)

	q := Query{"Patient", "bro=true"}
	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"gender": "male",
	})

	q = Query{"Patient", "bro=false"}
	o = m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"gender": bson.M{
			"$not": "male",
		},
	})
}

func (m *MongoSearchSuite) TestBroCustomQuery(c *C) {
	GlobalRegistry().RegisterParameterInfo(broSearchParamInfo)
	GlobalRegistry().RegisterParameterParser("test.bro", BroParser)
	GlobalMongoRegistry().RegisterBSONBuilder("test.bro", BroBSONBuilder)

	q := Query{"Patient", "bro=true"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

// Tests special searches on _id

func (m *MongoSearchSuite) TestConditionIdQueryObject(c *C) {
	q := Query{"Condition", "_id=123456789"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{"_id": "123456789"})
}

func (m *MongoSearchSuite) TestConditionIdQuery(c *C) {
	q := Query{"Condition", "_id=8664777288161060797"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
	c.Assert(results[0].Id(), Equals, "8664777288161060797")

	var res models.Condition
	util.CheckErr(results[0].Unmarshal(&res))
	cond := &res

	cond2 := &models.Condition{}
	err = m.Session.DB("fhir-test").C("conditions").FindId("8664777288161060797").One(cond2)

	c.Assert(cond, DeepEquals, cond2)
}

func (m *MongoSearchSuite) TestConditionSortByIdAscending(c *C) {
	q := Query{"Condition", "_sort=_id"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	var lastVal string
	for _, cond := range conditions {
		thisVal := cond.Id
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), 1)
		lastVal = thisVal
	}
}

func (m *MongoSearchSuite) TestConditionSortByIdDescending(c *C) {
	q := Query{"Condition", "_sort:desc=_id"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	lastVal := "~"
	for _, cond := range conditions {
		thisVal := cond.Id
		c.Assert(strings.Compare(lastVal, thisVal), Not(Equals), -1)
		lastVal = thisVal
	}
}

// Tests special searches on _tag

func (m *MongoSearchSuite) TestConditionTagQueryObject(c *C) {
	q := Query{"Condition", "_tag=foo|bar"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"meta.tag": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^foo$", Options: "i"},
				"code":   primitive.Regex{Pattern: "^bar$", Options: "i"},
			}},
	})
}

func (m *MongoSearchSuite) TestConditionTagQuery(c *C) {
	q := Query{"Condition", "_tag=foo|bar"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	var res models.Condition
	util.CheckErr(results[0].Unmarshal(&res))
	cond := &res

	cond2 := &models.Condition{}
	err = m.Session.DB("fhir-test").C("conditions").FindId("4072118967138896162").One(cond2)

	c.Assert(cond, DeepEquals, cond2)
}

// TODO: Test special searches: _content, _lastUpdated, _profile, _query, _security, _text

// Test searches with multiple values
func (m *MongoSearchSuite) TestConditionMultipleCodesQueryObject(c *C) {
	q := Query{"Condition", "code=http://hl7.org/fhir/sid/icd-9|428.0,http://snomed.info/sct|981000124106,http://hl7.org/fhir/sid/icd-10|I20.0"}

	o := m.MongoSearcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"code.coding": bson.M{
					"$elemMatch": bson.M{
						"system": primitive.Regex{Pattern: "^http://hl7\\.org/fhir/sid/icd-9$", Options: "i"},
						"code":   primitive.Regex{Pattern: "^428\\.0$", Options: "i"},
					}},
			},
			bson.M{
				"code.coding": bson.M{
					"$elemMatch": bson.M{
						"system": primitive.Regex{Pattern: "^http://snomed\\.info/sct$", Options: "i"},
						"code":   primitive.Regex{Pattern: "^981000124106$", Options: "i"},
					}},
			},
			bson.M{
				"code.coding": bson.M{
					"$elemMatch": bson.M{
						"system": primitive.Regex{Pattern: "^http://hl7\\.org/fhir/sid/icd-10$", Options: "i"},
						"code":   primitive.Regex{Pattern: "^I20\\.0$", Options: "i"},
					}},
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionMultipleCodesQuery(c *C) {
	q := Query{"Condition", "code=http://hl7.org/fhir/sid/icd-9|428.0,http://snomed.info/sct|981000124106,http://hl7.org/fhir/sid/icd-10|I20.0"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 4)
}

func (m *MongoSearchSuite) TestConditionMultipleCodesWrongICD10Query(c *C) {
	q := Query{"Condition", "code=http://hl7.org/fhir/sid/icd-9|428.0,http://snomed.info/sct|981000124106,http://hl7.org/fhir/sid/icd-10|I21.0"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

// Test searches with multiple parameters
func (m *MongoSearchSuite) TestConditionPatientAndCodeAndOnsetQueryObject(c *C) {
	q := Query{"Condition", "patient=4954037118555241963&code=http://hl7.org/fhir/sid/icd-9|428.0&onset-date=2012-03-01T07:00-05:00"}

	o := m.MongoSearcher.createQueryObject(q)
	// Make sure only the expected elements are there
	c.Assert(o, HasLen, 4)

	// Check the patient part of the query
	c.Assert(o["subject.reference__id"], DeepEquals, "4954037118555241963")
	c.Assert(o["subject.reference__type"], Equals, "Patient")

	// Check the code part of the query
	c.Assert(o["code.coding"], DeepEquals, bson.M{
		"$elemMatch": bson.M{
			"system": primitive.Regex{Pattern: "^http://hl7\\.org/fhir/sid/icd-9$", Options: "i"},
			"code":   primitive.Regex{Pattern: "^428\\.0$", Options: "i"},
		},
	})

	// Check the onset part of the query
	// 2012-03-01T07:00-05:00 <= onsetDateTime < 2012-03-01T07:01-05:00
	onsetDtFrom := o["$or"].([]bson.M)[0]["onsetDateTime.__from"].(bson.M)
	onsetDtTo := o["$or"].([]bson.M)[0]["onsetDateTime.__to"].(bson.M)
	c.Assert(onsetDtFrom, HasLen, 1)
	c.Assert(onsetDtTo, HasLen, 1)
	gte := onsetDtFrom["$gte"].(time.Time)
	lt := onsetDtTo["$lte"].(time.Time)
	c.Assert(gte.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 0, 0, 0, m.EST).UnixNano())
	c.Assert(lt.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 1, 0, 0, m.EST).UnixNano())

	// 2012-03-01T07:00:00-05:00 <= onsetPeriod.start <= onsetPeriod.end < 2012-03-01T07:01:00-05:00
	start := o["$or"].([]bson.M)[1]["onsetPeriod.start.__from"].(bson.M)["$gte"].(time.Time)
	c.Assert(start.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 0, 0, 0, m.EST).UnixNano())
	end := o["$or"].([]bson.M)[1]["onsetPeriod.end.__to"].(bson.M)["$lte"].(time.Time)
	c.Assert(end.UnixNano(), Equals, time.Date(2012, time.March, 1, 7, 1, 0, 0, m.EST).UnixNano())
}

func (m *MongoSearchSuite) TestConditionPatientAndCodeAndOnsetQuery(c *C) {
	q := Query{"Condition", "patient=4954037118555241963&code=http://hl7.org/fhir/sid/icd-9|428.0&onset-date=2012-03-01T07:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)
}

func (m *MongoSearchSuite) TestConditionWrongPatientAndCodeAndOnsetQuery(c *C) {
	q := Query{"Condition", "patient=123456789&code=http://hl7.org/fhir/sid/icd-9|428.0&onset-date=2012-03-01T07:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestConditionPatientAndWrongCodeAndOnsetQuery(c *C) {
	q := Query{"Condition", "patient=4954037118555241963&code=http://snomed.info/sct|981000124106&onset-date=2012-03-01T07:00-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

func (m *MongoSearchSuite) TestConditionPatientAndCodeAndWrongOnsetQuery(c *C) {
	q := Query{"Condition", "patient=4954037118555241963&code=http://hl7.org/fhir/sid/icd-9|428.0&onset-date=2012-03-01T07:05-05:00"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
}

// Test multiple parameters with multiple values
func (m *MongoSearchSuite) TestConditionPatientAndMultipleCodesQueryObject(c *C) {
	q := Query{"Condition", "patient=4954037118555241963&code=http://hl7.org/fhir/sid/icd-9|428.0,http://snomed.info/sct|981000124106"}

	o := m.MongoSearcher.createQueryObject(q)
	// Make sure only the expected elements are there
	c.Assert(o, HasLen, 3)

	// Check the patient part of the query
	c.Assert(o["subject.reference__id"], DeepEquals, "4954037118555241963")
	c.Assert(o["subject.reference__type"], Equals, "Patient")

	// Check the code part of the query
	c.Assert(o["$or"], DeepEquals, []bson.M{
		bson.M{
			"code.coding": bson.M{
				"$elemMatch": bson.M{
					"system": primitive.Regex{Pattern: "^http://hl7\\.org/fhir/sid/icd-9$", Options: "i"},
					"code":   primitive.Regex{Pattern: "^428\\.0$", Options: "i"},
				},
			},
		},
		bson.M{
			"code.coding": bson.M{
				"$elemMatch": bson.M{
					"system": primitive.Regex{Pattern: "^http://snomed\\.info/sct$", Options: "i"},
					"code":   primitive.Regex{Pattern: "^981000124106$", Options: "i"},
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestConditionMultiplePatientAndMultipleCodesQueryObject(c *C) {
	q := Query{"Condition", "patient=4954037118555241963,123456789,ABCDEFG&code=http://hl7.org/fhir/sid/icd-9|428.0,http://snomed.info/sct|981000124106"}

	o := m.MongoSearcher.createQueryObject(q)
	// Make sure only the expected elements are there
	c.Assert(o, HasLen, 2)

	expectedTopOr := []bson.M{
		{
			"subject.reference__id":   "4954037118555241963",
			"subject.reference__type": "Patient",
		},
		{
			"subject.reference__id":   "123456789",
			"subject.reference__type": "Patient",
		},
		{
			"subject.reference__id":   "ABCDEFG",
			"subject.reference__type": "Patient",
		},
	}

	expectedNestedOr := []bson.M{
		bson.M{
			"code.coding": bson.M{
				"$elemMatch": bson.M{
					"system": primitive.Regex{Pattern: "^http://hl7\\.org/fhir/sid/icd-9$", Options: "i"},
					"code":   primitive.Regex{Pattern: "^428\\.0$", Options: "i"},
				},
			},
		},
		bson.M{
			"code.coding": bson.M{
				"$elemMatch": bson.M{
					"system": primitive.Regex{Pattern: "^http://snomed\\.info/sct$", Options: "i"},
					"code":   primitive.Regex{Pattern: "^981000124106$", Options: "i"},
				},
			},
		},
	}

	// Can't guarantee what order they're processed in, so need to account for that in the test
	if len(o["$or"].([]bson.M)) == 2 {
		expectedTopOr, expectedNestedOr = expectedNestedOr, expectedTopOr
	}

	c.Assert(o["$or"], DeepEquals, expectedTopOr)
	c.Assert(o["$and"], DeepEquals, []bson.M{
		bson.M{"$or": expectedNestedOr},
	})
}

// Test Encounter query with _count
func (m *MongoSearchSuite) TestEncounterTypeQueryOptionsWithDefaultOptions(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201"}
	opt := q.Options()
	c.Assert(opt.Count, Equals, 100)
	c.Assert(opt.Offset, Equals, 0)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryWithDefaultOptions(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 3)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryOptionsWithCount(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_count=2"}

	// Make sure it doesn't somehow mess up the query object
	obj := m.MongoSearcher.createQueryObject(q)
	c.Assert(obj, DeepEquals, bson.M{
		"type.coding": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^http://www\\.ama-assn\\.org/go/cpt$", Options: "i"},
				"code":   primitive.Regex{Pattern: "^99201$", Options: "i"},
			},
		},
	})

	// Check that the options are parsed correctly
	opt := q.Options()
	c.Assert(opt.Count, Equals, 2)
	c.Assert(opt.Offset, Equals, 0)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryWithCount(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_count=2"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryOptionsForOffset(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_offset=2"}

	// Make sure it doesn't somehow mess up the query object
	obj := m.MongoSearcher.createQueryObject(q)
	c.Assert(obj, DeepEquals, bson.M{
		"type.coding": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^http://www\\.ama-assn\\.org/go/cpt$", Options: "i"},
				"code":   primitive.Regex{Pattern: "^99201$", Options: "i"},
			},
		},
	})

	// Check that the options are parsed correctly
	opt := q.Options()
	c.Assert(opt.Count, Equals, 100)
	c.Assert(opt.Offset, Equals, 2)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryWithOffset(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_offset=1"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryOptionsForCountAndOffset(c *C) {
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_count=2&_offset=1"}

	// Make sure it doesn't somehow mess up the query object
	obj := m.MongoSearcher.createQueryObject(q)
	c.Assert(obj, DeepEquals, bson.M{
		"type.coding": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^http://www\\.ama-assn\\.org/go/cpt$", Options: "i"},
				"code":   primitive.Regex{Pattern: "^99201$", Options: "i"},
			},
		},
	})

	// Check that the options are parsed correctly
	opt := q.Options()
	c.Assert(opt.Count, Equals, 2)
	c.Assert(opt.Offset, Equals, 1)
}

func (m *MongoSearchSuite) TestEncounterTypeQueryWithCountAndOffset(c *C) {
	// First do with an offset of 1
	q := Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_offset=1&_count=1"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	var res models.Encounter
	util.CheckErr(results[0].Unmarshal(&res))
	offset1 := &res

	// Now do an offset of 2
	q = Query{"Encounter", "type=http://www.ama-assn.org/go/cpt|99201&_offset=2&_count=1"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	var res2 models.Encounter
	util.CheckErr(results[0].Unmarshal(&res2))
	offset2 := &res2

	// Now make sure they are not the same
	c.Assert(offset1.Id, Not(Equals), offset2.Id)
}

func (m *MongoSearchSuite) TestConditionSortWithMultipleSortParams(c *C) {
	q := Query{"Condition", "_sort=patient&_sort=onset-date&_sort=code"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	var lastPatient string
	var lastOnset time.Time
	var lastCode string
	for _, cond := range conditions {
		thisPatient := getReferenceComparisonValue(cond.Subject)
		thisOnset := cond.OnsetDateTime.Time
		thisCode := getCodeableConceptComparisonValue(cond.Code)
		c.Assert(strings.Compare(lastPatient, thisPatient), Not(Equals), 1)
		if thisPatient == lastPatient {
			c.Assert(thisOnset.Before(lastOnset), Equals, false)
			if thisOnset.Equal(lastOnset) {
				c.Assert(strings.Compare(lastCode, thisCode), Not(Equals), 1)
			}
		}
		lastPatient = thisPatient
		lastOnset = thisOnset
		lastCode = thisCode
	}
}

func (m *MongoSearchSuite) TestConditionSortWithMultipleSortParamsDescending(c *C) {
	q := Query{"Condition", "_sort:desc=patient&_sort:desc=onset-date&_sort:desc=code"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 6)

	// convert search results to conditions
	conditions := make([]models.Condition, len(results))
	for i := 0; i < len(results); i++ {
		var resource models.Condition
		util.CheckErr(results[i].Unmarshal(&resource))
		conditions[i] = resource
	}

	lastPatient := "~"
	lastOnset := time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
	lastCode := "~"
	for _, cond := range conditions {
		thisPatient := getReferenceComparisonValue(cond.Subject)
		thisOnset := cond.OnsetDateTime.Time
		thisCode := getCodeableConceptComparisonValue(cond.Code)
		c.Assert(strings.Compare(lastPatient, thisPatient), Not(Equals), -1)
		if thisPatient == lastPatient {
			c.Assert(thisOnset.After(lastOnset), Equals, false)
			if thisOnset.Equal(lastOnset) {
				c.Assert(strings.Compare(lastCode, thisCode), Not(Equals), -1)
			}
		}
		lastPatient = thisPatient
		lastOnset = thisOnset
		lastCode = thisCode
	}
}

func (m *MongoSearchSuite) TestSortingOnParallelArrayPathsDoesntPanic(c *C) {
	// NOTE: Sorting on family and patient normally causes MongoDB to balk because they have "parallel arrays", but we
	// should just drop the second sort param instead of panicing
	q := Query{"Patient", "_sort=family&_sort=given"}
	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)
}

func (m *MongoSearchSuite) TestObservationCodeQueryOptionsForInclude(c *C) {
	q := Query{"Observation", "code=http://loinc.org|17856-6&_include=Observation:subject&_include=Observation:context"}

	// Make sure it doesn't somehow mess up the query object
	obj := m.MongoSearcher.createQueryObject(q)
	c.Assert(obj, DeepEquals, bson.M{
		"code.coding": bson.M{
			"$elemMatch": bson.M{
				"system": primitive.Regex{Pattern: "^http://loinc\\.org$", Options: "i"},
				"code":   primitive.Regex{Pattern: "^17856-6$", Options: "i"},
			},
		},
	})

	// Check that the options are parsed correctly
	opt := q.Options()
	c.Assert(opt.Include, HasLen, 2)
	c.Assert(opt.Include[0].Resource, Equals, "Observation")
	c.Assert(opt.Include[0].Parameter.Name, Equals, "subject")
	c.Assert(opt.Include[1].Resource, Equals, "Observation")
	c.Assert(opt.Include[1].Parameter.Name, Equals, "context")
}

func (m *MongoSearchSuite) TestConditionQueryForIncludeWithTargets(c *C) {
	q := Query{"Condition", "_id=8664777288161060797,4072118967138896162&_include=Condition:asserter"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	var cond models.Condition
	util.CheckErr(results[0].Unmarshal(&cond))
	incl := results[0].SearchIncludes()
	c.Assert(incl, HasLen, 1)
	patient := incl[0]
	util.CheckErr(err)
	c.Assert(patient.Id(), Equals, "4954037118555241963")

	util.CheckErr(results[1].Unmarshal(&cond))
	incl = results[1].SearchIncludes()
	c.Assert(incl, HasLen, 1)
	practitioner := incl[0]
	util.CheckErr(err)
	c.Assert(practitioner.Id(), Equals, "7045606679745586371")

	q = Query{"Condition", "_id=8664777288161060797,4072118967138896162&_include=Condition:asserter:Patient"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	util.CheckErr(results[0].Unmarshal(&cond))
	incl = results[0].SearchIncludes()
	c.Assert(incl, HasLen, 1)
	patient = incl[0]
	util.CheckErr(err)
	c.Assert(patient.Id(), Equals, "4954037118555241963")

	util.CheckErr(results[1].Unmarshal(&cond))
	incl = results[1].SearchIncludes()
	c.Assert(incl, HasLen, 0)

	q = Query{"Condition", "_id=8664777288161060797,4072118967138896162&_include=Condition:asserter:Practitioner"}
	results, _, err = m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 2)

	util.CheckErr(results[0].Unmarshal(&cond))
	incl = results[0].SearchIncludes()
	c.Assert(incl, HasLen, 0)

	util.CheckErr(results[1].Unmarshal(&cond))
	incl = results[1].SearchIncludes()
	c.Assert(incl, HasLen, 1)
	practitioner = incl[0]
	util.CheckErr(err)
	c.Assert(practitioner.Id(), Equals, "7045606679745586371")
}

func (m *MongoSearchSuite) TestPatientGenderQueryOptionsForRevInclude(c *C) {
	q := Query{"Patient", "gender=male&_revinclude=Condition:subject&_revinclude=Encounter:patient"}

	// Make sure it doesn't somehow mess up the query object
	obj := m.MongoSearcher.createQueryObject(q)
	c.Assert(obj, DeepEquals, bson.M{
		"gender": primitive.Regex{Pattern: "^male$", Options: "i"},
	})

	// Check that the options are parsed correctly
	opt := q.Options()
	c.Assert(opt.RevInclude, HasLen, 2)
	c.Assert(opt.RevInclude[0].Resource, Equals, "Condition")
	c.Assert(opt.RevInclude[0].Parameter.Name, Equals, "subject")
	c.Assert(opt.RevInclude[1].Resource, Equals, "Encounter")
	c.Assert(opt.RevInclude[1].Parameter.Name, Equals, "patient")
}

func (m *MongoSearchSuite) TestPatientGenderQueryForRevInclude(c *C) {
	q := Query{"Patient", "gender=male&_revinclude=Condition:patient&_revinclude=Encounter:patient"}

	results, _, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 1)

	var patient models.Patient
	util.CheckErr(results[0].Unmarshal(&patient))
	c.Assert(patient.Id, Equals, "4954037118555241963")
	c.Assert(patient.Name[0].Given[0], Equals, "John")
	c.Assert(patient.Name[0].Family, Equals, "Peters")

	inclRevIncl := results[0].SearchIncludes()
	c.Assert(inclRevIncl, HasLen, 9)

	conditions := results[0].SearchIncludesOfType("Condition")
	c.Assert(conditions, HasLen, 5)
	// Just ensure they are populated to some degree
	for _, conditionRes := range conditions {
		var condition models.Condition
		util.CheckErr(conditionRes.Unmarshal(&condition))
		c.Assert(condition.Id, NotNil)
		c.Assert(condition.Subject.ReferencedID, Equals, "4954037118555241963")
	}

	encounters := results[0].SearchIncludesOfType("Encounter")
	c.Assert(encounters, HasLen, 4)
	// Just ensure they are populated to some degree
	for _, encounterRes := range encounters {
		var encounter models.Encounter
		util.CheckErr(encounterRes.Unmarshal(&encounter))
		c.Assert(encounter.Id, NotNil)
		c.Assert(encounter.Subject.ReferencedID, Equals, "4954037118555241963")
	}
}

// Test that invalid search parameters PANIC (to ensure people know they are broken)
func (m *MongoSearchSuite) TestInvalidSearchParameterPanics(c *C) {
	q := Query{"Condition", "abatement=2012"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createInvalidSearchError("SEARCH_NONE", "Error: no processable search found for Condition search parameters \"abatement\""))
}

// Test that unimplemented features PANIC (to ensure people know they are broken)
func (m *MongoSearchSuite) TestCompositeSearchPanics(c *C) {
	q := Query{"Group", "characteristic-value=gender$male"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createInvalidSearchError("SEARCH_NONE", "Error: no processable search found for Group search parameters \"characteristic-value\""))
}

func (m *MongoSearchSuite) TestPrefixedDateSearchPanicsForUnsupportedPrefix(c *C) {
	q := Query{"Condition", "onset-date=ap2012"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_INVALID", "Parameter \"onset-date\" content is invalid"))
}

func (m *MongoSearchSuite) TestPrefixedNumberSearchPanicsForUnsupportedPrefix(c *C) {
	q := Query{"Immunization", "dose-sequence=sa1"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_INVALID", "Parameter \"dose-sequence\" content is invalid"))
}

func (m *MongoSearchSuite) TestPrefixedQuantitySearchPanicsForUnsupportedPrefix(c *C) {
	c.Skip("quantity search without system not supported yet - see createQuantityQueryObject")
	q := Query{"Observation", "value-quantity=sa1||mg"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_INVALID", "Parameter \"value-quantity\" content is invalid"))
	q = Query{"Observation", "value-quantity=ne1||mg"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_INVALID", "Parameter \"value-quantity\" content is invalid"))
}

func (m *MongoSearchSuite) TestModifierSearchPanics(c *C) {
	q := Query{"Condition", "code:text=headache"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_MODIFIER_INVALID", "Parameter \"code\" modifier is invalid"))
}

func (m *MongoSearchSuite) TestUnsupportedSearchResultParameterPanics(c *C) {
	q := Query{"Condition", "_contained=true"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_UNKNOWN", "Parameter \"_contained\" not understood"))
}

func (m *MongoSearchSuite) TestUsupportedGlobalSearchParameterPanics(c *C) {
	q := Query{"Condition", "_text=diabetes"}
	c.Assert(func() { m.MongoSearcher.Search(q) }, Panics, createUnsupportedSearchError("MSG_PARAM_UNKNOWN", "Parameter \"_text\" not understood"))
}

func (m *MongoSearchSuite) TestDisableTotalCount(c *C) {
	db := m.Session.DB("fhir-test")
	searcher := NewMongoSearcherForUri(m.MongoUri, db.Name, false, true, false, false) // countTotalResults = false, enableCISearches = true, readonly = false
	defer searcher.Close()
	q := Query{"Patient", ""}

	// Get the total we expect for this search.
	_, expectedTotal, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)

	results, total, err := searcher.Search(q)
	c.Assert(err, Equals, nil)
	c.Assert(total, Equals, uint32(0))
	c.Assert(uint32(len(results)), Equals, expectedTotal)

}

func (m *MongoSearchSuite) TestDisableCISearch(c *C) {
	db := m.Session.DB("fhir-test")
	searcher := NewMongoSearcherForUri(m.MongoUri, db.Name, true, false, false, false) // countTotalResults = true, enableCISearches = false, readonly = false
	defer searcher.Close()

	q := Query{"Condition", "code=http://hl7.org/fhir/sid/icd-9|428.0,http://snomed.info/sct|981000124106,http://hl7.org/fhir/sid/icd-10|I20.0"}

	o := searcher.createQueryObject(q)
	c.Assert(o, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"code.coding": bson.M{
					"$elemMatch": bson.M{
						"system": "http://hl7.org/fhir/sid/icd-9", // not a regex
						"code":   "428.0",                         // not a regex
					}},
			},
			bson.M{
				"code.coding": bson.M{
					"$elemMatch": bson.M{
						"system": "http://snomed.info/sct", // not a regex
						"code":   "981000124106",           // not a regex
					}},
			},
			bson.M{
				"code.coding": bson.M{
					"$elemMatch": bson.M{
						"system": "http://hl7.org/fhir/sid/icd-10", // not a regex
						"code":   "I20.0",                          // not a regex
					}},
			},
		},
	})
}

func (m *MongoSearchSuite) TestCacheSearchCount(c *C) {
	db := m.Session.DB("fhir-test")
	searcher := NewMongoSearcherForUri(m.MongoUri, db.Name, true, true, false, true) // countTotalResults = true, enableCISearches = true, readonly = true
	defer searcher.Close()

	q := Query{"Device", "manufacturer=Acme"}
	expectedHash := fmt.Sprintf("%x", md5.Sum([]byte("Device?manufacturer=Acme")))

	results, total, err := searcher.Search(q)
	util.CheckErr(err)
	c.Assert(total, Equals, uint32(1))
	c.Assert(results, NotNil)

	// Check that the total was cached.
	cc := &CountCache{}
	err = db.C("countcache").FindId(expectedHash).One(cc)
	util.CheckErr(err)
	c.Assert(cc.Id, Equals, expectedHash)
	c.Assert(cc.Count, Equals, uint32(1))
}

func (m *MongoSearchSuite) TestSummaryCount(c *C) {
	q := Query{"Patient", "_summary=count"}
	results, total, err := m.MongoSearcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
	c.Assert(total, Equals, uint32(2))
}

func (m *MongoSearchSuite) TestSummaryCountWithCountsDisabled(c *C) {
	// The count should still be returned when requesting _summary=count, even if counts are disabled.
	db := m.Session.DB("fhir-test")
	searcher := NewMongoSearcherForUri(m.MongoUri, db.Name, false, true, false, false) // countTotalResults = false, enableCISearches = true, readonly = false
	defer searcher.Close()

	q := Query{"Patient", "_summary=count"}
	results, total, err := searcher.Search(q)
	util.CheckErr(err)
	c.Assert(len(results), Equals, 0)
	c.Assert(total, Equals, uint32(2))
}

// Test internally used functions

func (m *MongoSearchSuite) TestBuildBsonForCompositeCriteriaAndPathWithArrayAncestor(c *C) {
	b := buildBSON("a.[]b.c.d.e", bson.M{"x": 1, "y": 2})
	c.Assert(b, DeepEquals, bson.M{
		"a.b": bson.M{
			"$elemMatch": bson.M{
				"c.d.e.x": 1,
				"c.d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonForCompositeCriteriaAndPathWithArrayAncestors(c *C) {
	b := buildBSON("a.[]b.[]c.d.e", bson.M{"x": 1, "y": 2})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c": bson.M{
			"$elemMatch": bson.M{
				"d.e.x": 1,
				"d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonForCompositeCriteriaAndPathWithArrayAncestorsAndLeaf(c *C) {
	b := buildBSON("a.[]b.[]c.d.[]e", bson.M{"x": 1, "y": 2})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": bson.M{
			"$elemMatch": bson.M{
				"x": 1,
				"y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonForCompositeCriteriaAndPathWithArrayLeaf(c *C) {
	b := buildBSON("a.b.c.d.[]e", bson.M{"x": 1, "y": 2})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": bson.M{
			"$elemMatch": bson.M{
				"x": 1,
				"y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonForSingleKeyCriteriaAndPathWithArrayAncestor(c *C) {
	b := buildBSON("a.[]b.c.d.e", bson.M{"x": 1})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e.x": 1,
	})
}

func (m *MongoSearchSuite) TestBuildBsonForSingleKeyCriteriaAndPathWithArrayLeaf(c *C) {
	b := buildBSON("a.b.c.d.[]e", bson.M{"x": 1})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e.x": 1,
	})
}

func (m *MongoSearchSuite) TestBuildBsonForSingularCriteriaAndPathWithArrayAncestor(c *C) {
	b := buildBSON("a.[]b.c.d.e", 1)
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": 1,
	})
}

func (m *MongoSearchSuite) TestBuildBsonForSingularCriteriaAndPathWithArrayLeaf(c *C) {
	b := buildBSON("a.b.c.d.[]e", 1)
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": 1,
	})
}

func (m *MongoSearchSuite) TestBuildBsonForCompositeCriteriaAndPathWithoutArrays(c *C) {
	b := buildBSON("a.b.c.d.e", bson.M{"x": 1, "y": 2})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e.x": 1,
		"a.b.c.d.e.y": 2,
	})
}

func (m *MongoSearchSuite) TestBuildBsonForSingleKeyCriteriaAndPathWithoutArrays(c *C) {
	b := buildBSON("a.b.c.d.e", bson.M{"x": 1})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e.x": 1,
	})
}

func (m *MongoSearchSuite) TestBuildBsonForSingularCriteriaAndPathWithoutArrays(c *C) {
	b := buildBSON("a.b.c.d.e", 1)
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": 1,
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithSingleOrCriteria(c *C) {
	b := buildBSON("a.b.c.d.e", bson.M{"$or": []bson.M{bson.M{"x": 1}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"a.b.c.d.e.x": 1,
			},
			bson.M{
				"a.b.c.d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithAndOrCriteria(c *C) {
	b := buildBSON("a.b.c.d.e", bson.M{"z": 3, "$or": []bson.M{bson.M{"x": 1}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e.z": 3,
		"$or": []bson.M{
			bson.M{
				"a.b.c.d.e.x": 1,
			},
			bson.M{
				"a.b.c.d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithAndComplexOrCriteria(c *C) {
	b := buildBSON("a.b.c.d.e", bson.M{"z": 3, "$or": []bson.M{bson.M{"x": 1, "a": 99}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e.z": 3,
		"$or": []bson.M{
			bson.M{
				"a.b.c.d.e.x": 1,
				"a.b.c.d.e.a": 99,
			},
			bson.M{
				"a.b.c.d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithSingleOrCriteriaAndArrayAncestors(c *C) {
	b := buildBSON("a.b.[]c.d.e", bson.M{"$or": []bson.M{bson.M{"x": 1}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"a.b.c.d.e.x": 1,
			},
			bson.M{
				"a.b.c.d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithSingleOrCriteriaAndArrayLeaf(c *C) {
	b := buildBSON("a.b.c.d.[]e", bson.M{"$or": []bson.M{bson.M{"x": 1}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"$or": []bson.M{
			bson.M{
				"a.b.c.d.e.x": 1,
			},
			bson.M{
				"a.b.c.d.e.y": 2,
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithAndOrCriteriaAndArrayAncenstors(c *C) {
	b := buildBSON("a.b.[]c.d.e", bson.M{"z": 3, "$or": []bson.M{bson.M{"x": 1, "a": 99}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c": bson.M{
			"$elemMatch": bson.M{
				"d.e.z": 3,
				"$or": []bson.M{
					bson.M{
						"d.e.x": 1,
						"d.e.a": 99,
					},
					bson.M{
						"d.e.y": 2,
					},
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithAndOrCriteriaAndArrayLeaf(c *C) {
	b := buildBSON("a.b.c.d.[]e", bson.M{"z": 3, "$or": []bson.M{bson.M{"x": 1, "a": 99}, bson.M{"y": 2}}})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": bson.M{
			"$elemMatch": bson.M{
				"z": 3,
				"$or": []bson.M{
					bson.M{
						"x": 1,
						"a": 99,
					},
					bson.M{
						"y": 2,
					},
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithQueryOperators(c *C) {
	b := buildBSON("a.b.c.d.e", bson.M{"$gte": 0, "$lt": 1})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": bson.M{
			"$gte": 0,
			"$lt":  1,
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithQueryOperatorsAndArrayAncestors(c *C) {
	b := buildBSON("a.b.[]c.d.e", bson.M{"$gte": 0, "$lt": 1})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c": bson.M{
			"$elemMatch": bson.M{
				"d.e": bson.M{
					"$gte": 0,
					"$lt":  1,
				},
			},
		},
	})
}

func (m *MongoSearchSuite) TestBuildBsonWithQueryOperatorsAndArrayLeaf(c *C) {
	b := buildBSON("a.b.c.d.[]e", bson.M{"$gte": 0, "$lt": 1})
	c.Assert(b, DeepEquals, bson.M{
		"a.b.c.d.e": bson.M{
			"$elemMatch": bson.M{
				"$gte": 0,
				"$lt":  1,
			},
		},
	})
}

func (m *MongoSearchSuite) TestMergeObjectsWithNoCommonKeys(c *C) {
	o1 := bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
	}
	o2 := bson.M{
		"g.h.i": 3,
		"j.k.l": 4,
	}
	merge(o1, o2)

	c.Assert(o1, DeepEquals, bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
		"g.h.i": 3,
		"j.k.l": 4,
	})
}

func (m *MongoSearchSuite) TestMergeObjectsWithCommonKeys(c *C) {
	o1 := bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
	}
	o2 := bson.M{
		"g.h.i": 3,
		"a.b.c": 4,
	}
	merge(o1, o2)

	c.Assert(o1, DeepEquals, bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
		"g.h.i": 3,
		"$and": []bson.M{
			bson.M{"a.b.c": 4},
		},
	})
}

func (m *MongoSearchSuite) TestMergeObjectsWithExistingAndOnLeft(c *C) {
	o1 := bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
		"$and": []bson.M{
			bson.M{"a.b.c": 5},
		},
	}
	o2 := bson.M{
		"g.h.i": 3,
		"a.b.c": 4,
	}
	merge(o1, o2)

	c.Assert(o1, DeepEquals, bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
		"g.h.i": 3,
		"$and": []bson.M{
			bson.M{"a.b.c": 5},
			bson.M{"a.b.c": 4},
		},
	})
}

func (m *MongoSearchSuite) TestMergeObjectsWithExistingAndOnRight(c *C) {
	o1 := bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
	}
	o2 := bson.M{
		"g.h.i": 3,
		"a.b.c": 4,
		"$and": []bson.M{
			bson.M{"a.b.c": 5},
		},
	}
	merge(o1, o2)

	// Can't guarantee order of $and, so check one-by-one
	c.Assert(o1, HasLen, 4)
	c.Assert(o1["a.b.c"], Equals, 1)
	c.Assert(o1["d.e.f"], Equals, 2)
	c.Assert(o1["g.h.i"], Equals, 3)
	c.Assert(o1["$and"], FitsTypeOf, []bson.M{})
	found4, found5 := false, false
	for _, m := range o1["$and"].([]bson.M) {
		c.Assert(m, HasLen, 1)
		switch m["a.b.c"] {
		case 4:
			found4 = true
		case 5:
			found5 = true
		}
	}
	c.Assert(found4 && found5, Equals, true)
}

func (m *MongoSearchSuite) TestMergeObjectsWithExistingAndOnLeftAndRight(c *C) {
	o1 := bson.M{
		"a.b.c": 1,
		"d.e.f": 2,
		"$and": []bson.M{
			bson.M{"a.b.c": 6},
		},
	}
	o2 := bson.M{
		"g.h.i": 3,
		"a.b.c": 4,
		"$and": []bson.M{
			bson.M{"a.b.c": 5},
		},
	}
	merge(o1, o2)

	// Can't guarantee order of $and, so check one-by-one
	c.Assert(o1, HasLen, 4)
	c.Assert(o1["a.b.c"], Equals, 1)
	c.Assert(o1["d.e.f"], Equals, 2)
	c.Assert(o1["g.h.i"], Equals, 3)
	c.Assert(o1["$and"], FitsTypeOf, []bson.M{})
	found4, found5, found6 := false, false, false
	for _, m := range o1["$and"].([]bson.M) {
		c.Assert(m, HasLen, 1)
		switch m["a.b.c"] {
		case 4:
			found4 = true
		case 5:
			found5 = true
		case 6:
			found6 = true
		}
	}
	c.Assert(found4 && found5 && found6, Equals, true)
}
