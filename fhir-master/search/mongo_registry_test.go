package search

import (
	"github.com/pebbe/util"
	"go.mongodb.org/mongo-driver/bson"
	. "gopkg.in/check.v1"
)

type MongoRegistrySuite struct{}

var _ = Suite(&MongoRegistrySuite{})

func (s *MongoRegistrySuite) TestRegisterAndLookupBSONBuilder(c *C) {
	build := func(param SearchParam, search *MongoSearcher) (bson.M, error) {
		return bson.M{"foo": param.(*StringParam).String}, nil
	}

	GlobalMongoRegistry().RegisterBSONBuilder("test", build)
	obtained, err := GlobalMongoRegistry().LookupBSONBuilder("test")
	util.CheckErr(err)
	searcher := NewMongoSearcher(nil, nil, true, true, false, false) // countTotalResults = true, enableCISearches = true, tokenParametersCaseSensitive = false, readonly = false
	bmap, err := obtained(&StringParam{String: "bar"}, searcher)
	util.CheckErr(err)
	c.Assert(bmap, HasLen, 1)
	c.Assert(bmap["foo"], Equals, "bar")
}

func (s *MongoRegistrySuite) TestLookupNonExistingBSONBuilder(c *C) {
	obtained, err := GlobalMongoRegistry().LookupBSONBuilder("nope")
	c.Assert(err, Not(IsNil))
	c.Assert(obtained, IsNil)
}
