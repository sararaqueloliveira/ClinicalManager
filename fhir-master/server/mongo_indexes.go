package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	mongowrapper "github.com/opencensus-integrations/gomongowrapper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Indexer is the top-level interface for managing MongoDB indexes.
type Indexer struct {
	idxPath string
	dbName  string
	debug   bool
}

// NewIndexer returns a pointer to a newly configured Indexer.
func NewIndexer(dbName string, config Config) *Indexer {
	return &Indexer{
		idxPath: config.IndexConfigPath,
		dbName:  dbName,
		debug:   config.Debug,
	}
}

// IndexMap is a map of index arrays with the collection name as the key. Each index array
// contains one or more indexes.
type IndexMap map[string][]mongo.IndexModel

// ConfigureIndexes ensures that all indexes listed in the provided indexes.conf file
// are part of the Mongodb fhir database. If an index does not exist yet ConfigureIndexes
// creates a new index in the background using mgo.collection.EnsureIndex(). Depending
// on the size of the collection it may take some time before the index is created.
// This will block the current thread until the indexing completes, but will not block
// other connections to the mongo database.
func (i *Indexer) ConfigureIndexes(db *mongowrapper.WrappedDatabase) {
	var err error
	fmt.Println("Indexer: Ensuring indexes")

	// TODO?
	// worker.SetTimeout(5 * time.Minute) // Some indexes take a long time to build

	// Read the config file
	f, err := os.Open(i.idxPath)
	if err != nil {
		i.log("[WARNING] Could not find indexes configuration file")
		return
	}
	defer f.Close()

	// parse the config file
	var indexMap = make(IndexMap)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip blank lines or lines with bash-style comments
		if line != "" && !strings.HasPrefix(line, "#") {

			collectionName, index, err := parseIndex(line)

			if err != nil {
				i.log(fmt.Sprintf("[ERROR] %s\n", err.Error()))
				panic(err)
			}

			indexMap[collectionName] = append(indexMap[collectionName], *index)
		}
	}

	// ensure all indexes in the config file
	for k := range indexMap {
		collection := db.Collection(k)

		indexes := indexMap[k]
		for _, index := range indexes {
			i.log(fmt.Sprintf("Ensuring index: %s.%s: %s", i.dbName, k, sprintIndexKeys(&index)))
		}

		_, err = collection.Indexes().CreateMany(context.Background(), indexes)
		if err != nil {
			i.log(fmt.Sprintf("[WARNING] Could not ensure indexes for: %s.%s: %s\n", i.dbName, k, err.Error()))
		}

	}
}

func (i *Indexer) log(msg string) {
	if i.debug {
		log.Printf("Indexer: %s\n", msg)
	}
}

// parseIndex parses a line from the index config file and returns a new *mongo.IndexModel struct
func parseIndex(line string) (collectionName string, newIndex *mongo.IndexModel, err error) {

	// Begin parsing new index from next line of file
	// format: <collection_name>.<index(es)>
	config := strings.SplitN(line, ".", 2)
	if len(config) < 2 {
		// Bad index format
		return "", nil, newParseIndexError(line, "Not of format <collection_name>.<index(es)>")
	}

	collectionName = config[0]
	if len(collectionName) == 0 {
		// No collection name provided
		return "", nil, newParseIndexError(line, "No collection name given")
	}

	indexSpec := config[1]
	if len(indexSpec) == 0 {
		// No index specification provided
		return "", nil, newParseIndexError(line, "No index key(s) given")
	}

	if string(indexSpec[0]) == "(" {
		// this is a compound index spec
		newIndex, err = parseCompoundIndex(indexSpec)
	} else {
		// this is a standard index spec
		newIndex, err = parseStandardIndex(indexSpec)
	}

	if err != nil {
		return "", nil, newParseIndexError(line, err.Error())
	}

	// build the index in the background; do not block other connections
	backgroundIndex := true
	newIndex.Options = &options.IndexOptions{Background: &backgroundIndex}
	return collectionName, newIndex, nil
}

// parseStandardIndex parses an index of the form:
// <db_name>.<collection_name>.<key>_(-)1
func parseStandardIndex(indexSpec string) (*mongo.IndexModel, error) {

	key, direction := parseIndexKey(indexSpec)

	if key == "" {
		// invalid key format, was not parsed successfully
		return nil, errors.New("Standard key not of format: <key>_(-)1")
	}

	return &mongo.IndexModel{
		Keys: bson.D{{Key: key, Value: direction}},
	}, nil
}

// parseCompoundIndex parses an index of the form:
// <db_name>.<collection_name>.(<key1>_(-)1, <key2>_(-)1, ...)
func parseCompoundIndex(indexSpec string) (*mongo.IndexModel, error) {

	// Check that the compound indexes are listed inside parentheses
	if !strings.HasPrefix(indexSpec, "(") || !strings.HasSuffix(indexSpec, ")") {
		return nil, errors.New("Compound key not of format: (<key1>_(-)1, <key2>_(-)1, ...)")
	}

	// Each element of specs is a standard key of the format <key>_(-)1
	// Note: if only one key is specified in the compound format a standard (not compound) key will be returned
	specs := strings.Split(indexSpec[1:len(indexSpec)-1], ",")

	var keys bson.D

	for _, spec := range specs {
		key, direction := parseIndexKey(strings.Trim(spec, " ")) // trim leading and trailing whitespace before parsing
		if key == "" {
			return nil, errors.New("Compound key sub-key not of format: <key>_(-)1")
		}
		keys = append(keys, bson.E{Key: key, Value: direction})
	}
	return &mongo.IndexModel{
		Keys: keys,
	}, nil
}

// parseIndexKey converts the standard mongo index key format: "<key>_(-)1"
// to the format used by mongo.IndexModel: "(-)<key>"
func parseIndexKey(spec string) (key string, direction int32) {

	if strings.HasSuffix(spec, "_1") {
		// ascending
		direction = 1
		key = strings.TrimSuffix(spec, "_1")
	} else if strings.HasSuffix(spec, "_-1") {
		// descending
		direction = -1
		key = strings.TrimSuffix(spec, "_-1")
	} else {
		return "", 0 // error
	}

	return
}

func newParseIndexError(indexName, reason string) error {
	return fmt.Errorf("Index '%s' is invalid: %s", indexName, reason)
}

func sprintIndexKeys(index *mongo.IndexModel) string {
	return fmt.Sprintf("%v", index.Keys)
	// return fmt.Sprintf("%+v (%+v)", index.Keys, index.Options)
}
