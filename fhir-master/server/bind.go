package server

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/gin-gonic/gin"
	"github.com/eug48/fhir/models2"
)

var validatorNetTransport = &http.Transport{
	// thanks to https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	Dial: (&net.Dialer{
		Timeout: 5 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 5 * time.Second,
}
var validatorHttpClient = &http.Client{
	Timeout: time.Second * 10,
}

func FHIRBind(c *gin.Context, validatorURL string) (resource *models2.Resource, err error) {
	if c.Request.Method == "GET" {
		panic("FHIRBind called for a GET request")
		// return c.BindWith(obj, binding.Form)
	}

	contentType := c.ContentType()
	bodyBytes, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		return nil, errors.Wrap(err, "FHIRBind: failed to read request body")
	}
	// fmt.Printf("FHIRBind: read %d bytes\n", len(bodyBytes))

	encryptPatientDetails := shouldEncryptPatientDetails(c)

	// validate
	if validatorURL != "" {
		if c.Request.Body != nil {
			bodyBuffer := bytes.NewBuffer(bodyBytes)
			resp, err := validatorHttpClient.Post(validatorURL, contentType, ioutil.NopCloser(bodyBuffer))
			if err != nil {
				return nil, errors.Wrapf(err, "FHIRBind: error calling validator (%s)", validatorURL)
			}
			resp.Location()
		}
	}

	// JSON
	if strings.Contains(contentType, "json") {
		resource, err = models2.NewResourceFromJsonBytes(bodyBytes)
		if encryptPatientDetails && resource != nil {
			resource.SetWhatToEncrypt(models2.WhatToEncrypt { PatientDetails: true })
		}
		return
	}

	// XML
	if strings.Contains(contentType, "application/fhir+xml") || strings.Contains(contentType, "application/xml+fhir") {
		converterInterface, enabled := c.Get("FhirFormatConverter")
		if enabled {
			converter := converterInterface.(*FhirFormatConverter)
			var jsonStr string
			jsonStr, err = converter.XmlToJson(string(bodyBytes))
			if err != nil {
				return nil, err
			}
			resource, err = models2.NewResourceFromJsonBytes([]byte(jsonStr))
			if encryptPatientDetails && resource != nil {
				resource.SetWhatToEncrypt(models2.WhatToEncrypt { PatientDetails: true })
			}
			return
		}
	}

	return nil, fmt.Errorf("unknown content type")
}


func shouldEncryptPatientDetails(c *gin.Context) bool {
	str := c.GetHeader("X-GoFHIR-Encrypt-Patient-Details")

	switch strings.ToLower(str) {
	case "1", "yes", "true":
		return true
	default:
		return false
	}
}