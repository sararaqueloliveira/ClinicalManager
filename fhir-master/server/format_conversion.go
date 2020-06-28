package server

import (
	"fmt"
	"encoding/json"
	"github.com/dop251/goja"
	"github.com/gin-gonic/gin"
)

// Converts between FHIR JSON and XML encodings using the
// FHIR.js library developed by the Lantana Consulting Group
// (https://github.com/lantanagroup/FHIR.js)
// It is executed using the goja JavaScript engine
type FhirFormatConverter struct {
	runtime *goja.Runtime
}

func NewFhirFormatConverter() *FhirFormatConverter {
	converter := &FhirFormatConverter{
		runtime: goja.New(),
	}

	polyfills := `
		if (!String.prototype.startsWith) {
			// polyfill from MDN: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/startsWith
			String.prototype.startsWith = function(search, pos) {
				return this.substr(!pos || pos < 0 ? 0 : +pos, search.length) === search;
			};
		}
		`

	prg := goja.MustCompile("bundle.js", FhirJsFormatConverterJavascript() + polyfills, true)

	_, err := converter.runtime.RunProgram(prg)
	if err != nil { panic(err) }

	_, err = converter.runtime.RunString("var fhir = new Fhir();")
	if err != nil { panic(err) }

	return converter
}

func (c *FhirFormatConverter) XmlToJson(xml string) (json string, err error) {

	c.runtime.Set("strXML", c.runtime.ToValue(xml))
	jsonVal, err := c.runtime.RunString("fhir.xmlToJson(strXML);")
	if err != nil {
		return
	}
	var isString bool
	json, isString = jsonVal.Export().(string)
	if !isString {
		return "", fmt.Errorf("fhir.xmlToJson: return value is not a string but %T", jsonVal.Export())
	}
	return
}

func (c *FhirFormatConverter) JsonToXml(json string) (xml string, err error) {

	if json == "" {
		return "", nil
	}
	// fmt.Printf("[JsonToXML] json: %s\n", json)
	c.runtime.Set("strJSON", c.runtime.ToValue(json))
	// FIXME: JSON.parse doesn't correctly parse FHIR decimals..
	xmlVal, err := c.runtime.RunString("fhir.objToXml(JSON.parse(strJSON));")
	if err != nil {
		return
	}
	var isString bool
	xml, isString = xmlVal.Export().(string)
	if !isString {
		return "", fmt.Errorf("fhir.JsonToXml: return value is not a string but %T", xmlVal.Export())
	}
	return
}

func (c *FhirFormatConverter) SendXML(statusCode int, obj interface{}, context *gin.Context) error {
	jsonStr, err := json.Marshal(obj)
	if err != nil {
		return context.AbortWithError(500, err)
	}
	xml, err := c.JsonToXml(string(jsonStr))
	if err != nil {
		return context.AbortWithError(500, err)
	}
	context.Data(statusCode, "application/fhir+xml; charset=utf-8", []byte(xml))
	return err
}