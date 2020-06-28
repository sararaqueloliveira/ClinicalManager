package middleware

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"


	"github.com/DataDog/zstd"
	"github.com/pkg/errors"
	"go.opencensus.io/trace"
)

type responseTeeWriter struct {
	http.ResponseWriter
	statusCode int
	requestId  string
	body       *bytes.Buffer
}

func (w *responseTeeWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}
func (w *responseTeeWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
}
func (w *responseTeeWriter) SendResponse() {
	w.ResponseWriter.Header().Set("X-GoFHIR-Request-ID", w.requestId)
	if w.statusCode == 0 {
		panic("file_logger.go: SendResponse: header not yet written")
	}
	w.ResponseWriter.WriteHeader(w.statusCode)
	w.ResponseWriter.Write(w.body.Bytes())
}
func (w *responseTeeWriter) AbortWithError(statusCode int, err error) {
	w.ResponseWriter.Header().Set("X-GoFHIR-Request-ID", w.requestId)
	w.ResponseWriter.WriteHeader(statusCode)
	w.ResponseWriter.Write([]byte(err.Error()))
}

// counter to add to filenames to prevent name collisions when doing many concurrent requests
var globalCounter uint64

/*
	Writes raw requests and responses to files for archiving.
	Compression using Zstandard (which is famous for high compression speed)

	Sets several xattrs: http_status, latency_ms, requestor_details, mutex_name
*/
func FileLoggerMiddleware(outputDirectory string, dumpHttpGET bool, httpHandler http.Handler) http.HandlerFunc {

	// writing to temp directory and then moving into outputDirectory
	// so that monitoring tools don't see a partially-written file
	tempDirectory := path.Join(outputDirectory, "temp")

	if err := os.MkdirAll(tempDirectory, 0777); err != nil {
		panic(fmt.Sprintf("file_logger.go: failed to create temp directory (%s): %s", tempDirectory, err.Error()))
	}

	return func(resp http.ResponseWriter, req *http.Request) {

		if !dumpHttpGET && req.Method == "GET" {
			httpHandler.ServeHTTP(resp, req)
			return
		}

		_, span := trace.StartSpan(req.Context(), "file_logger")
		defer span.End()

		mutexName := req.Header.Get("X-Mutex-Name")
		requestorDetails := req.Header.Get("X-Requestor-Details")

		// read request
		reqBytes, err := httputil.DumpRequest(req, true)
		if err != nil {
			resp.WriteHeader(500)
			resp.Write([]byte("FileLoggerMiddleware: error dumping request: " + err.Error()))
			return
		}

		// form filenames
		hasher := sha1.New()
		hasher.Write(reqBytes)
		hashBytes := hasher.Sum(nil)

		counter := atomic.AddUint64(&globalCounter, 1)
		currentTime := time.Now()
		timestamp := currentTime.Format("2006-01-02T15-04-05.000000")

		filename := fmt.Sprintf("%s-%x-%d", timestamp, hashBytes, counter)
		requestFilename := filename + ".req.zst"
		responseFilename := filename + ".res.zst"

		// write request
		requestWritten := make(chan error)
		go func() {

			ferr := writeCompressedData(tempDirectory, requestFilename, reqBytes)
			if ferr != nil {
				requestWritten <- ferr
				return
			}

			ferr = moveToDir(requestFilename, tempDirectory, outputDirectory)
			if ferr != nil {
				requestWritten <- ferr
				return
			}

			span.Annotate([]trace.Attribute{trace.StringAttribute("filename", filename)}, "request written")
			requestWritten <- nil
		}()

		// hook the response
		tee := &responseTeeWriter{body: bytes.NewBufferString(""), ResponseWriter: resp, requestId: filename}

		// do the work
		started := time.Now()
		httpHandler.ServeHTTP(tee, req)
		latencyMsecs := time.Since(started).Nanoseconds() / 1e6

		// write the response

		err = writeCompressedData(tempDirectory, responseFilename, tee.body.Bytes())
		if err != nil {
			tee.AbortWithError(500, errors.Wrap(err, "FileLoggerMiddleware: writeCompressedData failed"))
			return
		}

		err = setXattrInt(tempDirectory, responseFilename, "user.http_status", int64(tee.statusCode), tee)
		if err != nil {
			return
		}
		err = setXattrInt(tempDirectory, responseFilename, "user.latency_ms", latencyMsecs, tee)
		if err != nil {
			return
		}
		err = setXattr(tempDirectory, responseFilename, "user.mutex_name", mutexName, tee)
		if err != nil {
			return
		}
		err = setXattr(tempDirectory, responseFilename, "user.requestor_details", requestorDetails, tee)
		if err != nil {
			return
		}

		err = moveToDir(responseFilename, tempDirectory, outputDirectory)
		if err != nil {
			tee.AbortWithError(500, errors.Wrapf(err, "FileLoggerMiddleware: os.Rename for response failed: %s", responseFilename))
			return
		}

		// wait for request to be written
		err = <-requestWritten
		if err != nil {
			tee.AbortWithError(500, errors.Wrap(err, "FileLoggerMiddleware: failed to write request"))
			return
		}

		// write response
		tee.SendResponse()
	}
}

func setXattr(tempDirectory string, filename string, xattrName string, xattrValue string, resp *responseTeeWriter) error {
	if xattrValue == "" {
		return nil
	}
	/*
	err := unix.Setxattr(path.Join(tempDirectory, filename), xattrName, []byte(xattrValue), 0)
	if err != nil {
		resp.AbortWithError(500, errors.Wrapf(err, "FileLoggerMiddleware: setxattr (%s) (%d bytes) failed on %s", xattrName, len(xattrValue), filename))
	}
	return err*/
	return nil
}
func setXattrInt(tempDirectory string, filename string, xattrName string, xattrValue int64, resp *responseTeeWriter) error {
	return setXattr(tempDirectory, filename, xattrName, strconv.FormatInt(xattrValue, 10), resp)
}
func moveToDir(filename string, fromDir string, toDir string) error {
	return os.Rename(path.Join(fromDir, filename), path.Join(toDir, filename))
}
func writeCompressedData(dirName string, filename string, data []byte) error {
	f, ferr := os.Create(path.Join(dirName, filename))
	if ferr != nil {
		return errors.Wrap(ferr, "os.Create failed")
	}
	defer f.Close()

	compressor := zstd.NewWriterLevel(f, 5)
	_, ferr = compressor.Write(data)
	if ferr != nil {
		return errors.Wrap(ferr, "compressor.Write failed")
	}
	ferr = compressor.Close()
	if ferr != nil {
		return errors.Wrap(ferr, "compressor.Close failed")
	}

	return nil
}
