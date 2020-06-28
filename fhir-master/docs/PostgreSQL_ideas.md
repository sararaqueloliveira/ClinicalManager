Ideas for FHIR on PostgreSQL
====


1. A table for every type of resource
   1. An indexed column for most search parameters
   2. Can use arrays, structs, range types, full-text indexes
   3. NULLs are apparently stored very efficiently
1. Store resources as JSON BLOBs
   1. Every blob is immutable & identified by (_id, _versionId) → can be cached in some highly scalable non-ACID system with PostgreSQL handling cache misses
   2. JSONB is problematic as “jsonb will reject numbers that are outside the range of the PostgreSQL numeric data type” (link) but FHIR requires arbitrary-precision decimals..
   3. PostgreSQL already does compression, but compressing application-side could reduce CPU / network traffic and enable caching of already-compressed blobs
1. Single history table
   1. Stores all historical versions, and perhaps even current versions (see below)
   2. Indexed _lastUpdated column can satisfy whole-system history _since/_at queries
1. Storage of current-version blobs has several possibilities:
   1. Only store in history table
      1. Queries get ids (_id + _versionId) from index tables
      2. Can immediately join to history table to get resource blobs
      3. Or can cache all the ids application-side (e.g. in Redis as auto-expiring keys), and retrieve resources in pages from PostgreSQL or another cache
         1. Probably better if many queries returning large numbers of paged results - query only run once, less load on PostgreSQL
         2. For queries returning too many ids (e.g. all Observations) this may not be appropriate so likely to still need a way to batch results from PostgreSQL (e.g. sort & batch by _lastModified as a last resort)
   1. Only store in the resource-specific tables
      1. Slightly more efficient current-version queries?
         1. Perhaps better if many queries returning small numbers of non-paged results?
         2. PostgreSQL still needs to go from index value → table row → blob, whereas with (a) will go index value → history table index → history table row → blob (?)
      2. Less efficient history queries
1. Id generation
   1. Global counter table so every instance of GoFHIR can pre-allocate a bunch of ids - if the instance is stopped or crashes there will be holes but that should be ok
   2. Or can use a strategy like MongoDB’s ObjectIds that perhaps shard better?
1. Would be nice to also support CockroachDB which is partially PostgreSQL-compatible https://github.com/cockroachdb/cockroach