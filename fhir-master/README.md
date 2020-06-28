GoFHIR - Intervention Engine FHIR Server
=====================================================================

This project provides [HL7 FHIR STU3](http://hl7.org/fhir/STU3/index.html) models and server components implemented in Go and using MongoDB for storage. It aims to work well when multiple instances are run in containers - starting quickly, having modest resource utilisation and giving good performance.

This is a continuation of the [FHIR Server](https://github.com/mitre/fhir-server) developed by the MITRE Corporation. It is not a complete implementation of FHIR, as features are driven by the
[Intervention Engine](https://github.com/intervention-engine/ie), [eCQM Engine](https://github.com/mitre/ecqm), [Patient Matching Test Harness](https://github.com/mitre/ptmatch),
[Synthetic Mass](https://github.com/synthetichealth/syntheticmass) and [Patient Assistance Tool](https://www.patsoftware.com.au/) projects.

Currently this server should be considered experimental, with preliminary support for:

-	JSON representations of all resources
-	XML representations of all resources via [FHIR.js](https://github.com/lantanagroup/FHIR.js) (except for primitive extensions)
-	Transaction bundles (requires a MongoDB 4.0 replica set)
-	Create/Read/Update/Delete (CRUD) operations with versioning
-	Conditional update and delete
-	Resource-level history (basic support - lacks paging and filtering)
-	Batch bundles (POST, PUT and DELETE entries)
-	X-Provenance header (transactions only)
-	Arbitrary-precision storage for decimals
-	Some search features
	-	All defined resource-specific search parameters except composite types and contact (email/phone) searches
	-	Chained searches
	-	Reverse chained searches using `_has`
	-	`_include` and `_revinclude` searches (*without* `_recurse`)

Currently this server does not support the following features:

-	Validation
-	Terminology
-	Resource summaries
-	Whole-system and whole-resource history
-	Advanced search
	-	Custom search parameters
	-	Full-text search
	-	Filter expressions
	-	Whole-system search
-	GraphQL

The following relatively basic items are next in line for development:

- Conditional reads (`If-Modified-Since` and `If-None-Match`)
- History support for paging, `_since`, `_at` and `_count`
- Batch interdependency validation
- Validation (probably by proxying the request to a reference FHIR server)
- Search for quantities with the system unspecified (i.e. by both unit and code)


Users are strongly encouraged to test thoroughly and contributions (including more tests) would be most welcome. Please note that MongoDB 4.0 is quite new and the [MongoDB Go Driver](https://github.com/mongodb/mongo-go-driver) is still in its alpha stage.


Future work
-------------------------------

In addition to the unimplemented parts of the FHIR spec above major topics to consider are:

1. Improvements to indexing. Currently MongoDB searches & indexes "look inside" the resources for each path which is inefficient for parameters like `Observation.combo-code-value-quantity` since several paths/indexes need to be checked. Solutions to investigate would be to create compound indexes or extract search parameters into a sub-document and index that.
2. PostgreSQL support - [see here for some ideas](./docs/PostgreSQL_ideas.md).


Transactions
-------------------------------

MongoDB is used as the underlying database and has recently acquired multi-document transaction features in version 4.0. Note that transactions are only supported when MongoDB is run as a replica set.

This project also implements a partial workaround. Clients can send a `X-Mutex-Name` header and two requests with the same value of this header will execute serially (provided there is only one active instance of this server). Please note that this won't give you the all-or-nothing behaviour of real transactions.


Multi-database mode
-------------------------------

A single server can store multiple datasets with the `--enableMultiDB` switch. This allows requests to specificy the name of a MongoDB database to use. This is done in the base URL, e.g. http://fhir-server/db/test4_fhir/Patient?name=alex

The database should already exist and indexes will not be created automatically. MongoDB transactions also require that collections are pre-created and this server will attempt to do that the first time the database is used. An existing database can also be copied with MongoDB's `copyDatabase` command, or you can run this server with the `initdb --databaseName db-name` flags.


## Encryption

To mitigate the effects of the database being compromised clients can request that
[some Patient data](https://github.com/eug48/fhir/blob/master/models2/encryption.go) be encrypted when stored by setting an HTTP header `X-GoFHIR-Encrypt-Patient-Details: 1`. 
However this currently prevents searches by these fields from working.

Encryption is done using AES-GCM with a random nonce. The 32-byte key is specified in an environment variable `GOFHIR_ENCRYPTION_KEY_BASE64`. A name should be given to the key via `GOFHIR_ENCRYPTION_KEY_ID` and this is checked for consistency prior to decryption.

## Tracing

Calls to MongoDB have been instrumented using OpenCensus.
There is currently support for Google StackDriver (`--enableStackdriverTracing`) and Jaeger (`--enableJaegerTracing` and set `JAEGER_AGENT_ENDPOINT_URI` and `JAEGER_COLLECTOR_ENDPOINT_URI`).


Getting started using Docker
-------------------------------

1. Install Docker
2. Run an image of this FHIR server that includes MongoDB (`--rm` means all data will be deleted after the container exits):
		
		docker run --rm -it -p 3001:3001 gcr.io/eug48-fhir/fhir-server-with-mongo


3. You can also run MongoDB and this FHIR server in separate containers:

		docker run --name fhir-mongo -v /my/own/datadir:/data/db -d mongo --replSet rs0
		docker run --rm --link fhir-mongo:mongo mongo mongo --host mongo --eval "rs.initiate()"
		docker run --rm -it --link fhir-mongo:mongo -e GIN_MODE=release -e "MONGODB_URI=mongodb://fhir-mongo:27017/?replicaSet=rs0" -p 3001:3001 gcr.io/eug48-fhir/fhir-server


See MongoDB's Docker image documentation for more information, including how to persist data: https://hub.docker.com/_/mongo/


Building and running from source
---------------------------------

1. Install the Go programming language (at least version 1.10)
2. Install and start MongoDB
3. Install the [Dep](https://github.com/golang/dep) package-management tool for Go: https://golang.github.io/dep/docs/installation.html
4. Clone or download this repository under GOPATH/src (run `go env` to see your current GOPATH)
5. Run `dep ensure -vendor-only -v` to download dependencies into a `vendor` sub-directory (can take a long time)
6. Build and run the `fhir-server` executable

		$ cd fhir-server
		$ go build
		$ ./fhir-server --help
		Usage of ./fhir-server:
		-databaseName string
				MongoDB database name to use by default (default "fhir")
		-enableXML
				Enable support for the FHIR XML encoding
		-databaseSuffix string
				Request-specific MongoDB database name has to end with this (optional, e.g. '_fhir')
		-enableMultiDB
				Allow request to specify a specific Mongo database instead of the default, e.g. http://fhir-server/db/test4_fhir/Patient?name=alex
		-enableHistory
				Keep previous versions of every resource
		-disableSearchTotals
				Don't query for all results of a search to return Bundle.total, only do paging
		-tokenParametersCaseSensitive
				Whether token-type search parameters should be case sensitive (faster and R4 leans towards case-sensitive, whereas STU3 text suggests case-insensitive)
		-mongodbURI string
				MongoDB connection URI - a replica set is required for transactions support (default "mongodb://mongo:27017/?replicaSet=rs0")
		-port int
				Port to listen on (default 3001)
		-reqlog
				Enables request logging -- use with caution in production
		-failedRequestsDir string
				Directory where to dump failed requests (e.g. with malformed json)
		-enableJaegerTracing
				Enable OpenCensus tracing to Jaeger
		-enableStackdriverTracing
				Enable OpenCensus tracing to StackDriver
		-startMongod
				Run mongod (for 'getting started' docker images - development only)


MongoDB 4.0 only supports transactions when run as a replica set. To create a single-node replica set:

1. Add the `--replSet` option to the MongoDB daemon: e.g. `mongod --replSet rs0`
2. Run `rs.initiate()` from the MongoDB shell (`mongo`)

If you wish to test the server with synthetic patient data, please reference [Generate and Upload Synthetic Patient Data](https://github.com/intervention-engine/ie/blob/master/docs/dev_install.md#generate-and-upload-synthetic-patient-data).


Building docker images
--------------------------

Dockerfiles as well as a Google Cloud Container Builder `cloudBuild.json` spec are in the root of the repository.

To build from the Dockerfiles:

```
$ docker build -t my-fhir-server:2018-07-12a .
$ docker build -f Dockerfile-with-mongo -t my-fhir-server-with-mongo:2018-07-12a .
```

To build using Google Container Builder (after setting up the gcloud tool):

```
# The main Dockerfile
$ gcloud builds submit -t gcr.io/your-project-id/fhir-server:2018-07-12a

# Both Dockerfiles
$ gcloud builds submit --config=cloudBuild.json  --substitutions=COMMIT_SHA=yyyy-mm-dd
```

You can also set up a trigger via https://console.cloud.google.com/gcr/triggers 


Development
-------------

To run the server whilst it is under development it is often easier to combine the build and run steps into a single command from the `fhir-server` directory:

```
$ cd fhir-server
$ go run server.go
```

You can invoke the golang test suite by running:

```
$ go test ./...
```

More tests have been written in F#:

```
$ cd fhir-server
$ go build
$ cd ..

$ cd fsharp-fhir-tools/HttpTests/
$ dotnet run start-gofhir
```

As a library
--------------

This package can also be used as a library. Examples of usage can be found in the [server set up of the eCQM Engine](https://github.com/mitre/ecqm/blob/master/server.go), the
[server set up of Intervention Engine](https://github.com/intervention-engine/ie/blob/master/server.go), or the [GoFHIR server used by SyntheticMass](https://github.com/synthetichealth/gofhir/blob/master/main.go).

License
-------

Copyright 2017 The MITRE Corporation

Copyright 2018 PAT Pty Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
