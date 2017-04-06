package main

import (
	"database/sql"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/boltdb/bolt"
)

func main() {
	dsn := flag.String("dsn", "", "DSN for connecting to MySQL (username:password@hostname/dbname)")
	freq := flag.Duration("freq", 15*time.Minute, "Query frequency")
	dbpath := flag.String("db", "koharecords.db", "Path to db file, will be created if not existing")
	httpAddr := flag.String("http", ":8009", "HTTP serve address")
	initalImport := flag.Bool("initial-import", false, "Perform inital import of all availability data via SPARQL")
	fusekiEndpoint := flag.String("sparql", "http://fuseki:3030/ds/sparql", "Fuseki SPARQL endpoint")
	sendUpdates := flag.Bool("update", false, "Send changes in availability to services")
	servicesEndpoint := flag.String("services", "http://services:8005/publication/", "Services availablity endpoint")

	flag.Parse()

	mysql, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}

	newDB := false
	if _, err := os.Stat(*dbpath); os.IsNotExist(err) {
		newDB = true
	}

	db, err := bolt.Open(*dbpath, 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	c := newCollector(db, mysql, *freq)
	c.services = *servicesEndpoint
	c.fuseki = *fusekiEndpoint
	c.sendUpdates = *sendUpdates
	c.initialImport = *initalImport

	if err := c.setup(); err != nil {
		log.Fatal(err)
	}

	if newDB {
		log.Printf("Initialized new DB: %q", *dbpath)
	} else {
		stats := c.stats()
		log.Printf("Found %d records in DB %q, last updated=%v",
			stats.NumRecords,
			*dbpath,
			stats.LastUpdated.Format(time.RFC3339))
	}

	c.waitForMySQL()
	log.Println("Starting collector")
	go c.run()
	log.Printf("Starting HTTP server listeing at %v", *httpAddr)
	log.Fatal(http.ListenAndServe(*httpAddr, newServer(db)))
}
