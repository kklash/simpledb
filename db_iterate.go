package simpledb

import (
	"reflect"
)

// RowGenerator is a generator function which yields a new Row, and a possible decoding
// error, each time it is called. It returns two nil values when iteration is complete.
type RowGenerator = func() (*Row, error)

// Iterate returns a RowGenerator function which can be used to iterate over every row currently in the database.
// The returned generator caches every ID currently in the DB and decodes a new one each time it is called.
// If a row is dropped from the DB before the generator can reach it, the generator will ignore that row.
func (db *DB) Iterate() RowGenerator {
	// Pull all ids ahead of time to prevent concurrent map read/writes
	ids := make([]uint64, db.RowCount())
	i := 0
	for id, _ := range db.index {
		ids[i] = id
		i += 1
	}

	i = 0
	var iter RowGenerator
	iter = func() (*Row, error) {
		if i >= len(ids) {
			return nil, nil
		}

		db.mutex.Lock()
		defer db.mutex.Unlock()

		id := ids[i]
		cursor, ok := db.index[id]
		if !ok {
			// row was dropped, continue to next row
			i += 1
			return iter()
		}

		valuePtr := reflect.New(db.schema.dataType).Interface()

		err := db.decodeAt(cursor, valuePtr)
		if err != nil {
			return nil, err
		}

		row := &Row{ID: id, Value: valuePtr}
		i += 1

		return row, nil
	}

	return iter
}
