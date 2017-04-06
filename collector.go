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

// collector represents the process that collects billioitems availability and
// popularity data from Koha. The data is persisted transactionally on disk.
type collector struct {
	db    *bolt.DB      // handle to key-value store
	mysql *sql.DB       // db handle to Koha's MySQL
	freq  time.Duration // polling frequency
}

func newCollector(db *bolt.DB, mysql *sql.DB, freq time.Duration) collector {
	return collector{
		db:    db,
		mysql: mysql,
		freq:  freq,
	}
}

var (
	bktMeta   = []byte("meta")
	bktBiblio = []byte("biblio")
)

type stats struct {
	LastUpdated time.Time
	NumRecords  int
}

func (c collector) stats() (s stats) {
	c.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bktMeta).Get([]byte("updated")); b != nil {
			s.LastUpdated = time.Unix(int64(btou64(b)), 0)
		}
		s.NumRecords = tx.Bucket(bktBiblio).Stats().KeyN
		return nil
	})
	return s
}

// setup ensures DB is set up with required buckets.
func (c collector) setup() error {
	err := c.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bktMeta, bktBiblio} {
			_, err := tx.CreateBucketIfNotExists(b)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// persistUpdated stores updated records on disk
func (c collector) persistUpdated(newRecords map[uint32]record, timestamp time.Time) (nNew, nUpdated, nDeleted int, finalErr error) {
	finalErr = c.db.Update(func(tx *bolt.Tx) error {
		cur := tx.Bucket(bktBiblio).Cursor()

		updates := make(map[uint32]bool)

		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			oldRec, err := decode(v)
			if err != nil {
				return err
			}
			if newRec, found := newRecords[oldRec.Biblionumber]; found {
				if newRec.sameAs(oldRec) {
					// no changes
					delete(newRecords, newRec.Biblionumber)
					continue
				}
				// Mark record as to be updated. It's not safe to mutate the key/values
				// while iterating.
				updates[newRec.Biblionumber] = true
			} else {
				// Record is no longer in Koha and must be deleted.
				if err := cur.Delete(); err != nil {
					return err
				}
				nDeleted++
			}
		}

		// Update records
		for biblionr, _ := range updates {
			rec := newRecords[biblionr]
			rec.Updated = timestamp
			b, err := encode(rec)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bktBiblio).Put(u32tob(biblionr), b); err != nil {
				return err
			}
			nUpdated++
		}

		// Insert new records
		for biblionr, rec := range newRecords {
			if updates[biblionr] {
				continue
			}
			rec.Updated = timestamp
			// Unchanged records are deleted, so remaining must be new records.
			b, err := encode(rec)
			if err != nil {
				return err
			}
			if err := tx.Bucket(bktBiblio).Put(u32tob(biblionr), b); err != nil {
				return err
			}
			nNew++
		}

		// Store timestamp
		return tx.Bucket(bktMeta).Put(
			[]byte("updated"),
			u64tob(uint64(timestamp.Unix())),
		)
	})
	return
}

func (c collector) fetchItemCounts() (map[uint32]int, error) {
	var biblionumber uint32
	var count int
	counts := make(map[uint32]int)

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

func (c collector) fetchAvailability() (map[uint32]string, error) {
	var biblionumber uint32
	var branches string
	avail := make(map[uint32]string)

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

func (c collector) fetchBranches() (map[uint32]string, error) {
	var biblionumber uint32
	var branches string
	avail := make(map[uint32]string)

	rows, err := c.mysql.Query(sqlHomeBranches)
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

func (c collector) fetchCheckouts1m() (map[uint32]int, error) {
	return c.fetchCheckoutsNMonth(sqlCheckouts1m)
}
func (c collector) fetchCheckouts6m() (map[uint32]int, error) {
	return c.fetchCheckoutsNMonth(sqlCheckouts6m)
}

func (c collector) fetchCheckoutsNMonth(q string) (map[uint32]int, error) {
	var biblionumber uint32
	var count int
	checkouts := make(map[uint32]int)

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

func (c collector) run() error {

	firstLoop := true
	for {
		if !firstLoop {
			log.Printf("Sleeping %v before fetching data", c.freq)
			time.Sleep(c.freq)
			firstLoop = false
		}

		log.Println("Fetching data from Koha...")

		// This timestamp be stored on the updated records
		timestamp := time.Now()

		counts, err := c.fetchItemCounts()
		if err != nil {
			log.Printf("Failed to fetch items count: %v", err)
			continue
		}

		branches, err := c.fetchBranches()
		if err != nil {
			log.Printf("Failed to fetch branches data: %v", err)
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

		newRecords := make(map[uint32]record, len(counts))

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

		for biblio, branches := range branches {
			rec := newRecords[biblio]
			rec.Branches = strings.Split(branches, ",")
			sort.Strings(rec.Branches)
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

		totalCount := len(newRecords)

		// Persist all updated records to disk
		log.Println("Persisting to disk...")
		nNew, nUpdated, nDeleted, err := c.persistUpdated(newRecords, timestamp)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Persisted all changes to disk, with timestamp=%v", timestamp.Format(time.RFC3339))
		log.Printf("Stats: updated=%d new=%d deleted=%d unchanged=%d", nUpdated, nNew, nDeleted, totalCount-(nUpdated+nNew))
		log.Printf("Done processing %d records", totalCount)

		firstLoop = false
	}

	return nil
}

// record represent a biblioitem in Koha, with values
// aggregated from its items and transactions on those items.
type record struct {
	Biblionumber uint32
	Updated      time.Time // timestamp of when any of its data was changed
	ItemsTotal   int       // number of items on biblioitem
	Checkouts1m  int       // number of checkouts during the last month
	Checkouts6m  int       // number of checkouts during the last 6 months
	Branches     []string  // sorted list of branchcodes (homebranch) with items
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
	// Availability branchcodes are assumed to be sorted alphabetically
	for i, branch := range r.Availability {
		if stored.Availability[i] != branch {
			return false
		}
	}
	if len(r.Branches) != len(stored.Branches) {
		return false
	}
	for i, branch := range r.Branches {
		if stored.Branches[i] != branch {
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

	sqlHomeBranches = `
  SELECT biblionumber, GROUP_CONCAT(DISTINCT homebranch)
    FROM items
GROUP BY biblionumber;`

	sqlBranchAvailabilty = `
   SELECT I.biblionumber, GROUP_CONCAT(DISTINCT I.homebranch)
     FROM items I
LEFT JOIN reserves R USING(itemnumber)
    WHERE I.onloan IS NULL
      AND I.homebranch = I.holdingbranch
      AND I.notforloan = 0
      AND I.itemlost = 0
      AND R.reserve_id IS NULL
 GROUP BY biblionumber;`

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
