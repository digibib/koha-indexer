# Koha-indexer

Koha-indexer is a daemon that periodically queries Koha's MySQL database for popularity and availability data, persists this data on disk, and exposes it via a simple HTTP API queryable by biblionumber.

## Usage

```
Usage of koha-indexer:
  -db string
        Path to db file, will be created if not existing (default "koharecords.db")
  -dsn string
        DSN for connecting to MySQL (username:password@hostname/dbname)
  -force-refetch
        Discard persisted state and refetch data
  -freq duration
        Query frequency (default 15m0s)
  -http string
        HTTP serve address (default ":8009")
```


## HTTP API

#### GET /record/{biblionumber}

Example response:

```
{
	"Biblionumber": 1931218,
	"Updated": "2017-02-19T12:18:38.548237905+01:00",
	"ItemsTotal": 54,
	"Checkouts1m": 43,
	"Checkouts6m": 252,
	"Availability": ["fbjo", "fopp"]
}
```
