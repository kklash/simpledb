package simpledb

import (
	"encoding/binary"
	"io"
)

// decodeAt decodes a struct from the given cursor in the DB source. It assumes
// the caller is handling db.mutex.
func (db *DB) decodeAt(cursor int64, destPtr interface{}) error {
	// +8 bytes for the uint64 id part of the header
	if _, err := db.source.Seek(cursor+8, io.SeekStart); err != nil {
		return err
	}

	if _, err := binary.ReadUvarint(&wrappedByteReader{db.source}); err != nil {
		return err
	}

	if _, err := db.schema.Decode(db.source, destPtr); err != nil {
		return err
	}

	return nil
}
