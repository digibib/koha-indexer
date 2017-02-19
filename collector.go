package main

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/boltdb/bolt"
	_ "github.com/go-sql-driver/mysql"
)

// collector represents the process that collects billioitems availabilty and
// popularity data from Koha. The data is persisted transactionally on disk.
type collector struct {
	db          *bolt.DB       // handle to key-value store
	mysql       *sql.DB        // db handle to Koha's MySQL
	freq        time.Duration  // polling frequency
	records     map[int]record // in-memory version of current persisted record state
	lastUpdated time.Time      // timestamp of last persisted record updates
}

func newCollector(db *bolt.DB, mysql *sql.DB, freq time.Duration) collector {
	return collector{
		db:      db,
		mysql:   mysql,
		freq:    freq,
		records: make(map[int]record),
	}
}

var (
	bktMeta   = []byte("meta")
	bktBiblio = []byte("biblio")
)

// setup ensures DB is set up and loads persisted records into memory.
func (c collector) setup() error {

	// Make sure DB is correctly set up
	if err := c.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktMeta, bktBiblio} {
			_, err := tx.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Load persisted records into memory
	if err := c.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bktBiblio).Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			rec, err := decode(v)
			if err != nil {
				return err
			}
			c.records[int(btou32(k))] = rec
		}

		// Read timestamp of last update
		if b := tx.Bucket(bktMeta).Get([]byte("updated")); b != nil {
			c.lastUpdated = time.Unix(int64(btou64(b)), 0)
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// persistUpdated stores updated records on disk
func (c collector) persistUpdated(records map[int]record, timestamp time.Time) (int, error) {
	n := 0
	err := c.db.Update(func(tx *bolt.Tx) error {
		for _, record := range records {
			if !timestamp.Equal(record.Updated) {
				// no changes
				continue
			}
			b, err := encode(record)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bktBiblio).Put(u32tob(uint32(record.Biblionumber)), b); err != nil {
				return err
			}
			n++
		}

		// Store timestamp
		return tx.Bucket(bktMeta).Put(
			[]byte("updated"),
			u64tob(uint64(timestamp.Unix())),
		)
	})
	return n, err
}

func (c collector) fetchItemCounts() (map[int]int, error) {
	var biblionumber, count int
	counts := make(map[int]int)

	rows, err := c.mysql.Query(sqlItemsPerBiblio)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&biblionumber, &count); err != nil {
			return nil, err
		}
		counts[biblionumber] = count

	}
	defer rows.Close()
	return counts, nil
}

func (c collector) fetchAvailability() (map[int]string, error) {
	var biblionumber int
	var branches string
	avail := make(map[int]string)

	rows, err := c.mysql.Query(sqlBranchAvailabilty)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&biblionumber, &branches); err != nil {
			return nil, err
		}
		avail[biblionumber] = branches

	}
	rows.Close()
	return avail, nil
}

func (c collector) fetchCheckouts1m() (map[int]int, error) {
	return c.fetchCheckoutsNMonth(sqlCheckouts1m)
}
func (c collector) fetchCheckouts6m() (map[int]int, error) {
	return c.fetchCheckoutsNMonth(sqlCheckouts6m)
}

func (c collector) fetchCheckoutsNMonth(q string) (map[int]int, error) {
	var biblionumber, count int
	checkouts := make(map[int]int)

	rows, err := c.mysql.Query(q)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		if err := rows.Scan(&biblionumber, &count); err != nil {
			return nil, err
		}
		checkouts[biblionumber] = count

	}
	defer rows.Close()
	return checkouts, nil
}

func (c collector) waitForMySQL() {
	log.Println("Verifying MySQL connection")

	i := 1
	for {
		err := c.mysql.Ping()
		if err == nil {
			break
		}
		log.Println(err)
		log.Printf("Retrying MySQL ping in %d seconds", i)
		time.Sleep(time.Second * time.Duration(i))
		i++
	}
	log.Println("MySQL connection OK")
}

func (c collector) run(refetch bool) error {

	firstLoop := true
	for {
		if !firstLoop {
			log.Printf("Sleeping %v before fetching data", c.freq)
			time.Sleep(c.freq)
			firstLoop = false
		}

		log.Println("Fetching data from Koha...")

		// This will timestamp be stored on the updated records
		timestamp := time.Now()

		counts, err := c.fetchItemCounts()
		if err != nil {
			log.Printf("Failed to fetch items count: %v", err)
			continue
		}

		avail, err := c.fetchAvailability()
		if err != nil {
			log.Printf("Failed to fetch availlability data: %v", err)
			continue
		}

		checkouts1m, err := c.fetchCheckouts1m()
		if err != nil {
			log.Printf("Failed to fetch last month checkout data: %v", err)
			continue
		}

		checkouts6m, err := c.fetchCheckouts6m()
		if err != nil {
			log.Printf("Failed to fetch last 6 months checkout data: %v", err)
			continue
		}

		log.Println("Fetched data OK")
		log.Println("Processing data...")

		newRecords := make(map[int]record, len(counts))

		for biblio, n := range counts {
			newRecords[biblio] = record{
				Biblionumber: biblio,
				ItemsTotal:   n,
			}
		}

		for biblio, branches := range avail {
			rec := newRecords[biblio]
			rec.Availability = strings.Split(branches, ",")
			sort.Strings(rec.Availability)
			newRecords[biblio] = rec
		}

		for biblio, n := range checkouts1m {
			rec := newRecords[biblio]
			rec.Checkouts1m = n
			newRecords[biblio] = rec
		}

		for biblio, n := range checkouts6m {
			rec := newRecords[biblio]
			rec.Checkouts6m = n
			newRecords[biblio] = rec
		}

		if refetch {
			c.records = make(map[int]record)
			refetch = false
		}

		var (
			totalCount   = len(newRecords)
			deletedCount = len(c.records) - totalCount
			updatedCount = 0
			sameCount    = 0
			newCount     = 0
		)
		if deletedCount < 0 {
			// We are refetching all, or fetching for the first time.
			deletedCount = 0
		}

		// Loop over records, and find which is updated
		for biblio, newRecord := range newRecords {
			if oldRecord, found := c.records[biblio]; found {
				if newRecord.sameAs(oldRecord) {
					// record is same, keep old timestamp
					newRecord.Updated = oldRecord.Updated
					sameCount++
				} else {
					// record is updated
					newRecord.Updated = timestamp
					updatedCount++
				}
			} else {
				// record is new
				newRecord.Updated = timestamp
				newCount++
			}
			newRecords[biblio] = newRecord
		}

		// Persist all updated records to disk
		log.Println("Persisting to disk...")
		n, err := c.persistUpdated(newRecords, timestamp)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Persisted %d records to disk, with timestamp=%v", n, timestamp.Format(time.RFC3339))

		// We can now swap the old records with the updated records
		c.records = newRecords
		c.lastUpdated = timestamp

		log.Printf("Done processing %d records", totalCount)
		log.Printf("Stats: same=%d updated=%d new=%d deleted=%d", sameCount, updatedCount, newCount, deletedCount)
		firstLoop = false
	}

	return nil
}

// record represent a biblioitem in Koha, with values
// aggregated from its items and transactions on those items.
type record struct {
	Biblionumber int
	Updated      time.Time // timestamp of when any of its data was changed
	ItemsTotal   int       // number of items on biblioitem
	Checkouts1m  int       // number of checkouts during the last month
	Checkouts6m  int       // number of checkouts during the last 6 months
	Availability []string  // sorted list of branchcodes (homebranch) where items are available
}

func encode(r record) ([]byte, error) {
	return json.Marshal(r)
}

func decode(b []byte) (record, error) {
	var r record
	err := json.Unmarshal(b, &r)
	return r, err
}

// sameAs checks if two records are same, that is, all
// fields are equal disregarding the timestamp.
func (r record) sameAs(stored record) bool {
	if r.ItemsTotal != stored.ItemsTotal ||
		r.Checkouts1m != stored.Checkouts1m ||
		r.Checkouts6m != stored.Checkouts6m {
		return false
	}
	if len(r.Availability) != len(stored.Availability) {
		return false
	}
	// Availabilty branchcodes are assumed to be sorted alphabetically
	for i, branch := range r.Availability {
		if stored.Availability[i] != branch {
			return false
		}
	}
	return true
}

// SQL queries
const (
	sqlItemsPerBiblio = `
  SELECT biblionumber, count(*) AS num
    FROM items
GROUP BY biblionumber;`

	sqlBranchAvailabilty = `
   SELECT I.biblioitemnumber, GROUP_CONCAT(DISTINCT I.homebranch)
     FROM items I
LEFT JOIN reserves R USING(itemnumber)
    WHERE I.onloan IS NULL
      AND I.homebranch = I.holdingbranch
      AND I.notforloan = 0
      AND I.itemlost = 0
      AND R.reserve_id IS NULL
 GROUP BY biblioitemnumber;`

	sqlCheckouts1m = `
  SELECT biblionumber, count(*) AS num
    FROM (
         (SELECT issue_id, items.biblionumber AS biblionumber
            FROM issues
            JOIN items USING(itemnumber)
           WHERE issues.issuedate > (NOW() - INTERVAL 1 MONTH) )
         UNION
         (SELECT issue_id, items.biblionumber AS biblionumber
            FROM old_issues
            JOIN items USING(itemnumber)
           WHERE old_issues.issuedate > (NOW() - INTERVAL 1 MONTH) )
          ) AS combined
GROUP BY biblionumber;`

	sqlCheckouts6m = `
  SELECT biblionumber, count(*) AS num
    FROM (
         (SELECT issue_id, items.biblionumber AS biblionumber
            FROM issues
            JOIN items USING(itemnumber)
           WHERE issues.issuedate > (NOW() - INTERVAL 6 MONTH) )
         UNION
         (SELECT issue_id, items.biblionumber AS biblionumber
            FROM old_issues
            JOIN items USING(itemnumber)
           WHERE old_issues.issuedate > (NOW() - INTERVAL 6 MONTH) )
         ) AS combined
GROUP BY biblionumber;`
)

// u32tob converts a uint32 into a 4-byte slice.
func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// btou32 converts a 4-byte slice into an uint32.
func btou32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// u64tob converts a uint64 into a 8-byte slice.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 converts a 8-byte slice into an uint64.
func btou64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
