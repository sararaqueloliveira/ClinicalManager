package models2

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/buger/jsonparser"
	"github.com/pkg/errors"
)

type Resource struct {
	jsonBytes    []byte
	resourceType string
	id           string
	versionId    string
	lastUpdated  string

	searchIncludes []*Resource

	idChanged              bool
	versionIdChanged       bool
	lastUpdatedChanged     bool
	transformReferencesMap map[string]string
	cachedBson             *[]bson.E
	whatToEncrypt          WhatToEncrypt
}

func (r *Resource) JsonBytes() []byte {
	return r.jsonBytes
}
func (r *Resource) ResourceType() string {
	return r.resourceType
}
func (r *Resource) Id() string {
	return r.id
}
func (r *Resource) VersionId() string {
	return r.versionId
}
func (r *Resource) LastUpdated() string {
	return r.lastUpdated
}
func (r *Resource) LastUpdatedTime() time.Time {
	t := time.Time{}
	err := t.UnmarshalJSON([]byte("\"" + r.lastUpdated + "\""))
	if err != nil {
		panic(fmt.Errorf("failed to parse FHIR meta.lastUpdated field: %s", r.lastUpdated))
	}
	return t
}
func (r *Resource) SearchIncludes() []*Resource {
	return r.searchIncludes
}
func (r *Resource) SearchIncludesOfType(resourceType string) []*Resource {
	var out []*Resource
	for _, included := range r.searchIncludes {
		if included.resourceType == resourceType {
			out = append(out, included)
		}
	}
	return out
}

func (r *Resource) Unmarshal(v interface{}) error {
	// debug("Resource.Unmarshal: %s", r.jsonBytes)
	return json.Unmarshal(r.jsonBytes, v)
}

func (r *Resource) SetId(id string) {
	r.id = id
	r.idChanged = true
	r.cachedBson = nil
}
func (r *Resource) SetVersionId(versionId int) {
	r.versionId = strconv.Itoa(versionId)
	r.versionIdChanged = true
	r.cachedBson = nil
}
func (r *Resource) SetLastUpdated(lastUpdated string) {
	r.lastUpdated = lastUpdated
	r.lastUpdatedChanged = true
	r.cachedBson = nil
}
func (r *Resource) SetLastUpdatedTime(t time.Time) {
	r.lastUpdated = t.Format(time.RFC3339)
	r.lastUpdatedChanged = true
	r.cachedBson = nil
}
func (r *Resource) SetTransformReferencesMap(transformReferencesMap map[string]string) {
	r.transformReferencesMap = transformReferencesMap
	r.cachedBson = nil
}

func (r *Resource) SetWhatToEncrypt(whatToEncrypt WhatToEncrypt) {
	r.whatToEncrypt = whatToEncrypt
}

func dumpMalformedJson(jsonBytes []byte, jsonError error, failedRequestsDir string) error {
	currentTime := time.Now()
	timestamp := currentTime.Format("2006-01-02-15-04-05.000000")

	f, ferr := os.Create(path.Join(failedRequestsDir, timestamp+".error.txt"))
	if ferr != nil {
		return ferr
	}
	defer f.Close()

	_, ferr = f.WriteString("AsShallowBundle: json.Unmarshal failed: ")
	if ferr != nil {
		return ferr
	}
	_, ferr = f.WriteString(jsonError.Error())
	if ferr != nil {
		return ferr
	}
	_, ferr = f.WriteString("\n")
	if ferr != nil {
		return ferr
	}
	_, ferr = f.WriteString("\n")
	if ferr != nil {
		return ferr
	}
	_, ferr = f.Write(jsonBytes)
	if ferr != nil {
		return ferr
	}
	ferr = f.Sync()
	if ferr != nil {
		return ferr
	}

	return nil
}

func (r *Resource) AsShallowBundle(failedRequestsDir string) (bundle *ShallowBundle, err error) {
	bundle = &ShallowBundle{}
	err = json.Unmarshal(r.jsonBytes, bundle)
	if err != nil {
		if failedRequestsDir != "" {

			// previously tried dumping to stderr but Kubernetes and Stackdriver truncate it..
			ferr := dumpMalformedJson(r.jsonBytes, err, failedRequestsDir)
			if ferr != nil {
				fmt.Fprintf(os.Stderr, "json.Unmarshal failed: %s and failed to write to failedRequestsDir (%s)", err.Error(), ferr.Error())
				return nil, errors.Wrap(err, "json.Unmarshal failed - see stderr for more details")
			}

			return nil, errors.Wrapf(err, "json.Unmarshal failed - see %s for the culprit string", failedRequestsDir)
		} else {
			return nil, errors.Wrap(err, "json.Unmarshal failed - enable failedRequestsDir to see the culprit string")
		}
	}
	for _, entry := range bundle.Entry {
		if entry.Resource != nil {
			entry.Resource.SetWhatToEncrypt(r.whatToEncrypt)
		}
	}
	return
}

func (r *Resource) UnmarshalJSON(data []byte) (err error) {
	newResource, err := NewResourceFromJsonBytes(data)
	if err != nil {
		return errors.Wrap(err, "Resource.UnmarshalJSON: NewResourceFromJsonBytes failed")
	} else {
		*r = *newResource
		return nil
	}
}

func (r *Resource) MarshalJSON() ([]byte, error) {
	if len(r.jsonBytes) <= 1 {
		return nil, fmt.Errorf("Resource.MarshalJSON: jsonBytes is invalid (%d bytes)", len(r.jsonBytes))
	}

	// TODO optimise: first converting to BSON or using a cached bson doc
	// since the conversion to BSON applies the transformReferencesMap
	var err error

	if r.cachedBson == nil {
		cachedBson, err := r.GetBSON()
		if err != nil {
			return nil, errors.Wrap(err, "Resource.MarshalJSON: GetBSON failed")
		}
		cachedBson2 := cachedBson.([]bson.E)
		r.cachedBson = &cachedBson2
	}

	json, _, err := ConvertGoFhirBSONToJSON(*r.cachedBson)
	if err != nil {
		err = errors.Wrapf(err, "Resource.MarshalJSON: ConvertGoFhirBSONToJSON failed (%s/%s/%s)", r.resourceType, r.id, r.versionId)
	}
	return json, err

	// json := r.jsonBytes
	// fmt.Printf("MarshalJSON at start have %s\n", string(json))
	// if r.idChanged {
	// 	json, err = jsonparser.Set(json, []byte(fmt.Sprintf(`"%s"`, r.Id())), "id")
	// 	if err != nil {
	// 		return nil, errors.Wrap(err, "jsonparser.Set (id) failed")
	// 	}
	// }
	// if r.lastUpdatedChanged {
	// 	json, err = jsonparser.Set(json, []byte(fmt.Sprintf(`"%s"`, r.LastUpdated())), "meta", "lastUpdated")
	// 	if err != nil {
	// 		return nil, errors.Wrap(err, "jsonparser.Set (lastUpdated) failed")
	// 	}
	// }
	// if r.versionIdChanged {
	// 	json, err = jsonparser.Set(json, []byte(fmt.Sprintf(`"%s"`, r.VersionId())), "meta", "versionId")
	// 	if err != nil {
	// 		return nil, errors.Wrap(err, "jsonparser.Set (versionId) failed")
	// 	}
	// }

	// fmt.Printf("MarshalJSON returning %s\n", string(json))
	// return json, nil
}

// Implements bson2.Marshaler
func (r *Resource) MarshalBSON() ([]byte, error) {
	bson1, err := r.GetBSON()
	if err != nil {
		return nil, err
	}
	return bson.Marshal(bson1)
}

func (r *Resource) GetBSON() (interface{}, error) {
	// debug("GetBSON: transformReferencesMap: %#v", r.transformReferencesMap)
	bsonDoc, err := ConvertJsonToGoFhirBSON(r.jsonBytes, r.whatToEncrypt, r.transformReferencesMap)
	bsonDoc2 := []bson.E(bsonDoc)
	if err != nil {
		return nil, errors.Wrap(err, "ConvertJsonToGoFhirBSON failed")
	}

	if r.idChanged {
		debug("GetBSON: setting _id to %s", r.id)
		setBsonValue(&bsonDoc2, "_id", r.id, 0)
		// debug("GetBSON:   %#v --> %#v", bsonDoc, bsonDoc2)
	}
	// debug("setBson: lastUpdated: %t, versionChanged: %t", r.lastUpdatedChanged, r.versionIdChanged)
	if r.lastUpdatedChanged || r.versionIdChanged {
		// debug("setBson: bsonDoc2 now %+v", bsonDoc2)
		meta, err := getOrInsertBsonEmbeddedDoc(&bsonDoc2, "meta", 2)
		debug("setBson: meta is %+v", meta)
		if err != nil {
			return nil, errors.Wrap(err, "getOrInsertBsonEmbeddedDoc failed")
		}
		if r.versionIdChanged {
			setBsonValue(meta, "versionId", r.versionId, 0)
		}
		if r.lastUpdatedChanged {
			setBsonValue(meta, "lastUpdated", r.LastUpdatedTime(), 1)
		}
		debug("setBson: meta now %+v", meta)
		// debug("setBson: bsonDoc2 now %+v", bsonDoc2)
	}

	r.cachedBson = &bsonDoc2
	return bsonDoc2, err
}

func getOrInsertBsonEmbeddedDoc(doc *[]bson.E, name string, rawInsertPos int) (*[]bson.E, error) {

	for i, _ := range *doc {
		elem := &(*doc)[i]
		if elem.Key == name {
			val, ok := elem.Value.([]bson.E)
			if !ok {
				return nil, fmt.Errorf("findBsonEmbeddedDoc: bad type: %T", elem.Value)
			}
			elem.Value = &val
			return &val, nil
		}
	}

	*doc = append(*doc, bson.E{})
	ins := min(rawInsertPos, len(*doc)-1)
	if ins < len(*doc)-1 {
		copy((*doc)[ins+1:], (*doc)[ins:])
	}

	subdoc := make([]bson.E, 0, 2)
	(*doc)[ins] = bson.E{Key: name, Value: &subdoc}
	return &subdoc, nil
}

func setBsonValue(doc *[]bson.E, name string, valueToSet interface{}, rawInsertPos int) {

	for i, _ := range *doc {
		elem := &(*doc)[i]
		if elem.Key == name {
			debug("     setBsonValue: %s -> %s", name, valueToSet)
			elem.Value = valueToSet
			return
		} else {
			debug("     setBsonValue: %s", name)
		}
	}

	*doc = append(*doc, bson.E{})
	ins := min(rawInsertPos, len(*doc)-1)
	if ins < len(*doc)-1 {
		copy((*doc)[ins+1:], (*doc)[ins:])
	}
	(*doc)[ins] = bson.E{Key: name, Value: valueToSet}
	debug("     setBsonValue: inserted %s --> %s", name, valueToSet)
}

func min(x int, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func NewResourceFromBSON(bsonDoc []bson.E) (resource *Resource, err error) {
	jsonBytes, includedJsons, err := ConvertGoFhirBSONToJSON(bsonDoc)
	if err != nil {
		return nil, errors.Wrap(err, "NewResourceFromBSON: ConvertGoFhirBSONToJSON failed")
	}
	resource, err = NewResourceFromJsonBytes(jsonBytes)
	if err != nil {
		return nil, errors.Wrap(err, "NewResourceFromBSON: NewResourceFromJsonBytes failed on output of ConvertGoFhirBSONToJSON")
	}

	if includedJsons != nil && len(includedJsons) > 0 {
		for _, includedJson := range includedJsons {
			included, err := NewResourceFromJsonBytes(includedJson)
			if err != nil {
				return nil, errors.Wrap(err, "NewResourceFromBSON: NewResourceFromJsonBytes failed on included resource")
			}
			resource.searchIncludes = append(resource.searchIncludes, included)
		}
	}

	return
}

func NewResourceFromJsonBytes(jsonBytes []byte) (resource *Resource, err error) {

	// debug("NewResourceFromJsonBytes: %s", string(jsonBytes))

	paths := [][]string{
		[]string{"resourceType"},
		[]string{"id"},
		[]string{"meta", "lastUpdated"},
		[]string{"meta", "versionId"},
	}
	var resourceType, id, lastUpdated, versionId string
	jsonparser.EachKey(jsonBytes, func(idx int, value []byte, vt jsonparser.ValueType, err2 error) {
		if err != nil {
			return
		}
		err = err2
		if err != nil {
			return
		}

		switch idx {
		case 0:
			resourceType = string(value)
		case 1:
			id = string(value)
		case 2:
			lastUpdated = string(value)
		case 3:
			versionId = string(value)
		default:
			panic("invalid index")
		}
	}, paths...)
	if err != nil {
		return nil, errors.Wrap(err, "jsonparser.EachKey failed")
	}

	if resourceType == "" {
		return nil, fmt.Errorf("JSON missing resourceType element")
	}

	resource = &Resource{
		jsonBytes:    jsonBytes,
		resourceType: resourceType,
		id:           id,
		lastUpdated:  lastUpdated,
		versionId:    versionId,
	}
	return resource, nil
}
