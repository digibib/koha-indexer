package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
)

var errNotFound = errors.New("not found")

type server struct {
	db *bolt.DB
}

func newServer(db *bolt.DB) server {
	return server{db: db}
}

func (s server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Split(r.URL.Path, "/")
	if len(path) != 3 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if path[1] != "record" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	biblionr, err := strconv.Atoi(path[2])
	if err != nil {
		http.Error(w, "record ID must be an integer", http.StatusBadRequest)
		return
	}

	var rec record
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktBiblio).Get(u32tob(uint32(biblionr)))
		if b == nil {
			return errNotFound
		}
		var err2 error
		rec, err2 = decode(b)
		if err2 != nil {
			return err2
		}
		return nil
	})

	if err == errNotFound {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("Content-Encoding", "gzip")

	gz := gzip.NewWriter(w)
	defer gz.Close()
	if err := json.NewEncoder(gz).Encode(rec); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
