package models2

import (
	"fmt"
	"strings"

	"github.com/buger/jsonparser"
)

type FhirSchemaError struct {
	msg   string
	at  string
}
func (e FhirSchemaError) Error() string {
	return fmt.Sprintf("FHIR schema error at %s: %s", e.at, e.msg)
}

type positionInfo struct {
	// FHIR 'element' that we're currently parsing - found in fhir_types.go
	element                string

	// For contained & included resources we need to peek into each JSON array element to get the resource type
	needToReadResourceType bool

	// Current path through the JSON - only for debugging
	pathHere               string
}
func (p *positionInfo) atReference() bool {
	return p.element == "Reference"
}
func (p *positionInfo) atExtension() bool {
	return p.element == "Extension"
}
func (p *positionInfo) atDecimal() bool {
	return p.element == "decimal"
}
func (p *positionInfo) atDate() bool {
	return p.element == "date" || p.element == "dateTime"
}
func (p *positionInfo) atInstant() bool {
	return p.element == "instant"
}
func (p *positionInfo) downTo(key string, valueJson []byte) positionInfo {
	result := p.__downTo(key, valueJson)
	debug("downTo %s --> %#v", key, result)
	return result
}
func (p *positionInfo) __downTo(key string, valueJson []byte) positionInfo {

	if p.needToReadResourceType {
		panic(fmt.Errorf("error parsing JSON: need to acquire resource type"))
	}
	var needToAcquireResourceType bool

	nextPath := p.pathHere + "." + key

	// resourceType key isn't included in our schema extract
	if key == "resourceType" && !strings.Contains(p.element, ".") {
		return positionInfo{
			pathHere: nextPath,
			element:  "string",
		}
	}

	if strings.HasPrefix(key, "_") {
		// primitive extension
		return positionInfo{
			pathHere: nextPath,
			element:  "_",
		}
	}

	nextElement := p.element + "." + key
	t, found := fhirTypes[nextElement]
	if !found {
		panic(p.schemaError("failed to get type for %s (at %s --> %s)", nextElement, p.element, key))
	}

	if t != "BackboneElement" && t != "Element" {
		// For BackboneElement and Element we continue adding to the current 'path'
		// e.g.
		//   if we're at "DataRequirement.codeFilter": "Element",
		//   we don't want to move to element but at the next field we can move downTo
		//   "DataRequirement.codeFilter.valueCodeableConcept": "CodeableConcept",

		nextElement = t
	}

	if nextElement == "Resource" {
		_, dataType, _, err := jsonparser.Get(valueJson)
		if err != nil {
			panic(p.schemaError("failed to get json type of nested resource (%s)", key))
		}

		if dataType == jsonparser.Array {
			needToAcquireResourceType = true // not in array element yet

		} else if dataType == jsonparser.Object {
			resourceType, err := jsonparser.GetString(valueJson, "resourceType")
			if err != nil {
				panic(p.schemaError("failed to get resourceType of nested resource (%s)", key))
			}
			nextElement = resourceType
			nextPath = nextPath + "(" + nextElement + ")"
			_, found = fhirTypes[nextElement+".id"]
			if !found {
				panic(p.schemaError("failed to get type for contained resource %s (%s)", nextPath, key))
			}
		} else {
			panic(p.schemaError("failed to get JSON of nested resource (%s): neither object nor array (%d)", key, dataType))
		}
	}

	return positionInfo{
		pathHere:               nextPath,
		element:                nextElement,
		needToReadResourceType: needToAcquireResourceType,
	}
}
func (p *positionInfo) intoArray(valueJson []byte) positionInfo {
	result := p.__intoArray(valueJson)
	debug("intoArray --> %#v", result)
	return result
}
func (p *positionInfo) __intoArray(valueJson []byte) positionInfo {

	nextPath := p.pathHere + ".[]"
	nextElement := p.element

	if p.needToReadResourceType {
		p.needToReadResourceType = false

		resourceType, err := jsonparser.GetString(valueJson, "resourceType")
		if err != nil {
			panic(p.schemaError("failed to get resourceType of array resource: %+v", err))
		}
		var found bool
		nextElement = resourceType
		nextPath = nextPath + "(" + nextElement + ")"
		_, found = fhirTypes[nextElement+".id"]
		if !found {
			panic(p.schemaError("failed to get type for contained resource at array (%s)", resourceType))
		}
	}
	return positionInfo{
		pathHere: nextPath,
		element:  nextElement,
	}
}

func (p *positionInfo) schemaError(format string, a ...interface{}) FhirSchemaError {
	return FhirSchemaError{
		at: p.pathHere,
		msg: fmt.Sprintf(format, a...),
	}
}

func init() {
	// TODO: add to map
	fhirTypes["_.id"] = "string"
	fhirTypes["_.extension"] = "Extension"
}
