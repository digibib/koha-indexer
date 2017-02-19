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
	refetch := flag.Bool("force-refetch", false, "Discard persisted state and refetch data")
	httpAddr := flag.String("http", ":8009", "HTTP serve address")
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

	c.waitForMySQL()

	if err := c.setup(); err != nil {
		log.Fatal(err)
	}
	if newDB {
		log.Printf("Initialized new DB: %q", *dbpath)
	} else {
		log.Printf("Loaded %d records from disk, last updated=%v", len(c.records), c.lastUpdated.Format(time.RFC3339))
	}

	log.Println("Starting collector")
	go c.run(*refetch)
	log.Printf("Starting HTTP server listeing at %v", *httpAddr)
	log.Fatal(http.ListenAndServe(*httpAddr, newServer(db)))
}
