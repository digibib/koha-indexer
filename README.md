# Koha-indexer

Koha-indexer is a daemon that periodically queries Koha's MySQL database for popularity and availability data, persists this data on disk, and exposes it via a simple HTTP API queryable by biblionumber.

## Usage

```
Usage of koha-indexer:
  -db string
      Path to db file, will be created if not existing (default "koharecords.db")
  -dsn string
      DSN for connecting to MySQL (username:password@hostname/dbname)
  -freq duration
      Query frequency (default 15m0s)
  -http string
      HTTP serve address (default ":8009")
  -initial-import
      Perform inital import of all availability data via SPARQL
  -services string
      Services availablity endpoint (default "http://services:8005/publication/")
  -sparql string
      Fuseki SPARQL endpoint (default "http://fuseki:3030/ds/sparql")
  -update
      Send changes in availability to services
```


## HTTP API

#### GET /record/{biblionumber}

Example response:

```
{
  "Biblionumber": 1931218,
  "Updated": "2017-04-11T10:11:05.783324589Z",
  "ItemsTotal": 57,
  "Checkouts1m": 89,
  "Checkouts6m": 454,
  "Branches": [
    "fbje",
    "fbjo",
    "fbol",
    "ffur",
    "fgam",
    "fgry",
    "fhol",
    "flam",
    "fmaj",
    "fnor",
    "fnyd",
    "fopp",
    "frik",
    "frmm",
    "froa",
    "from",
    "fsto",
    "ftor",
    "hutl"
  ],
  "Availability": [
    "fbjo",
    "flam"
  ]
}
```
