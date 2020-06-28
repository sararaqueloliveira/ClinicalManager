package middleware

import (
	"context"
	"strings"
	"sync"

	"github.com/golang/glog"

	"github.com/eug48/fhir/models2"
	"github.com/gin-gonic/gin"
	mongowrapper "github.com/opencensus-integrations/gomongowrapper"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opencensus.io/trace"
)

// Pre-create collections as required by MongoDB transactions
func PrecreateCollectionsMiddleware(mongoDBuri string) gin.HandlerFunc {

	client, err := mongowrapper.Connect(context.Background(), options.Client().ApplyURI(mongoDBuri))
	if err != nil {
		panic(errors.Wrap(err, "PrecreateCollectionsMiddleware can't connect to MongoDB"))
	}

	collectionsToCreate := models2.AllFhirResourceCollectionNames()

	// cached databases that we've already checked/created
	var dbsAlreadyDone sync.Map

	return func(c *gin.Context) {

		dbName := c.GetHeader("Db")
		if dbName == "" {
			dbName = "fhir"
		}

		_, done := dbsAlreadyDone.Load(dbName)

		if !done {
			ctx, span := trace.StartSpan(c.Request.Context(), "create_collections")
			span.AddAttributes(trace.StringAttribute("db", dbName))
			defer span.End()

			err := CreateCollections(ctx, collectionsToCreate, dbName, client)
			if err != nil {
				panic(errors.Wrap(err, "PrecreateCollectionsMiddleware failed"))
			}
			dbsAlreadyDone.Store(dbName, true)
		}

		c.Next()
	}
}

func CreateCollections(ctx context.Context, collectionsToCreate []string, dbName string, client *mongowrapper.WrappedClient) error {

	db := client.Database(dbName)

	// get existing collections
	existingCollections := map[string]bool{}
	cursor, err := db.ListCollections(ctx, bson.D{}, options.ListCollections().SetNameOnly(true))
	if err != nil {
		return errors.Wrap(err, "ListCollections failed")
	}
	for cursor.Next(ctx) {
		var doc bson.Raw
		err = cursor.Decode(&doc)
		nameBSON, err := doc.LookupErr("name")
		if err != nil {
			return errors.Wrap(err, "ListCollections LookupErr failed")
		}
		name, ok := nameBSON.StringValueOK()
		if !ok {
			return errors.New("ListCollections StringValueOK failed")
		}
		existingCollections[name] = true
	}
	if err := cursor.Err(); err != nil {
		return errors.Wrap(err, "ListCollections cursor failed")
	}

	createIfDoesntExist := func(name string) error {
		_, exists := existingCollections[name]
		if !exists {
			glog.V(1).Infof("pre-creating collection %s.%s\n", dbName, name)
			res := db.RunCommand(ctx, bson.D{{"create", name}})
			if res.Err() != nil && !strings.Contains(res.Err().Error(), "already exists") {
				return errors.Wrap(res.Err(), "failed to create collection "+name)
			}
		}
		return nil
	}

	for _, name := range collectionsToCreate {
		err = createIfDoesntExist(name)
		if err != nil {
			return err
		}

		err = createIfDoesntExist(name + "_prev")
		if err != nil {
			return err
		}
	}

	return nil
}
