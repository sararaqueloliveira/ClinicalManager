package server

import (
	"fmt"
	"net/http"
	runtime_debug "runtime/debug"

	"github.com/golang/glog"

	"github.com/eug48/fhir/models"
	"github.com/eug48/fhir/models2"
	"github.com/eug48/fhir/search"
	"github.com/pkg/errors"
)

func ErrorToOpOutcome(err interface{}) (statusCode int, outcome *models.OperationOutcome) {
	switch x := err.(type) {
	case *search.Error:
		return x.HTTPStatus, x.OperationOutcome
	case error:
		cause := errors.Cause(x)
		_, isSchemaError := cause.(models2.FhirSchemaError)
		_, isVersionConflict := cause.(ErrConflict)
		if isSchemaError {
			outcome := models.NewOperationOutcome("fatal", "structure", cause.Error())
			return http.StatusBadRequest, outcome
		} else if isVersionConflict {
			outcome := models.NewOperationOutcome("error", "conflict", cause.Error())
			return http.StatusConflict, outcome // TODO (FHIR R4): changed to 412
		} else {
			stacktrace := string(runtime_debug.Stack())
			glog.Errorf("ErrorToOpOutcome: %+v\n%s", x, stacktrace)

			outcome := models.NewOperationOutcome("fatal", "exception", x.Error()+stacktrace)
			return http.StatusInternalServerError, outcome
		}
	default:
		stacktrace := string(runtime_debug.Stack())
		glog.Errorf("ErrorToOpOutcome: %+v\n%s", x, stacktrace)

		str := fmt.Sprintf("%#v", err)
		outcome := models.NewOperationOutcome("fatal", "exception", str)
		return http.StatusInternalServerError, outcome
	}
}
