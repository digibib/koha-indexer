package main

import (
	"compress/gzip"
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
	if len(path) < 3 || path[1] != "record" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	biblionr, err := strconv.Atoi(path[2])
	if err != nil {
		http.Error(w, "record ID must be an integer", http.StatusBadRequest)
		return
	}

	var recJSON []byte
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bktBiblio).Get(u32tob(uint32(biblionr)))
		if b == nil {
			return errNotFound
		}
		recJSON = make([]byte, len(b))
		copy(recJSON, b)
		return nil
	}); err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("Content-Encoding", "gzip")

	gz := gzip.NewWriter(w)
	defer gz.Close()
	if _, err := gz.Write(recJSON); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
