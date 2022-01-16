package simpledb

import (
	"bytes"
	"io"
)

// newID generates a new ID number which is not already present in the DB.
func (db *DB) newID() uint64 {
	var id uint64
	for id == DeletedID {
		id = randUint64()

		// Make sure IDs are unique
		if _, ok := db.index[id]; ok {
			continue
		}
	}
	return id
}

func (db *DB) insert(value interface{}, id uint64) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	cursor, err := db.source.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	bytesWritten, err := db.schema.Encode(buf, value)
	if err != nil {
		return err
	}

	rowHeader := encodeRowHeader(id, uint64(bytesWritten))
	if _, err := db.source.Write(rowHeader); err != nil {
		return err
	}

	if _, err := buf.WriteTo(db.source); err != nil {
		return err
	}

	err = db.addToIndex(id, cursor, func() (interface{}, error) {
		return value, nil
	})
	if err != nil {
		return err
	}

	return nil
}

// Insert inserts a given value into the DB. The value must be the same
// struct type that was given to NewDB or db.ReflectSchema most recently.
// The value can be a value or a pointer to a value, of that type.
func (db *DB) Insert(value interface{}) (uint64, error) {
	id := db.newID()
	if err := db.insert(value, id); err != nil {
		return DeletedID, err
	}

	return id, nil
}

// Update drops the given row and reinserts a new one with the same ID.
func (db *DB) Update(id uint64, value interface{}) error {
	if err := db.Drop(id); err != nil {
		return err
	}

	if err := db.insert(value, id); err != nil {
		return err
	}

	return nil
}
