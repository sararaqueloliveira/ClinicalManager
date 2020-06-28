FROM golang:1.12.6-alpine as builder
RUN apk add --no-cache ca-certificates curl git build-base

# Download dependencies first for docker caching
WORKDIR /gofhir-src
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build
WORKDIR /gofhir-src/fhir-server
ARG GIT_COMMIT=dev
RUN go build -ldflags "-X main.gitCommit=$GIT_COMMIT"

# Copy to lightweight runtime image
FROM alpine:3.8.4
RUN apk add --no-cache ca-certificates tini
COPY --from=builder /gofhir-src/fhir-server/fhir-server /
COPY --from=builder /gofhir-src/fhir-server/config/ /config
COPY --from=builder /gofhir-src/conformance/ /conformance

ENV MONGODB_URI mongodb://fhir-mongo:27017/?replicaSet=rs0
CMD ["sh", "-c", "/fhir-server -port 3001 -disableSearchTotals -enableXML -databaseName fhir -mongodbURI $MONGODB_URI"]