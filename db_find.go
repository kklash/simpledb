package simpledb

import (
	"errors"
)

// ErrNotFound is returned by db.Find, db.Pop, and db.Drop if no match was found when
// searching for a given id number.
var ErrNotFound = errors.New("given ID was not found in the DB")

// Find searches the DB index for the given id number. If the ID is not found, it returns ErrNotFound.
// If a match is found, Find unmarshals the encoded value from the DB source into the given pointer.
func (db *DB) Find(id uint64, destPtr interface{}) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	cursor, ok := db.index[id]
	if !ok {
		return ErrNotFound
	}

	if err := db.decodeAt(cursor, destPtr); err != nil {
		return err
	}

	return nil
}
