package simpledb

import (
	"encoding/binary"
	"io"
)

func (db *DB) drop(id uint64) error {
	if id == DeletedID {
		return ErrNotFound
	}

	cursor, ok := db.index[id]
	if !ok {
		return ErrNotFound
	}

	if _, err := db.source.Seek(cursor, io.SeekStart); err != nil {
		return err
	}

	if _, err := db.source.Write(encodeUint64(DeletedID)); err != nil {
		return err
	}

	size, err := binary.ReadUvarint(&wrappedByteReader{db.source})
	if err != nil {
		return err
	}

	blankData := make([]byte, size)
	if _, err := db.source.Write(blankData); err != nil {
		return err
	}

	db.removeFromIndex(id)

	return nil
}

// Drop removes the row with the given ID from the database by zeroing it on-disk and removing it
// from the index. If the row does not exist, it returns ErrNotFound.
func (db *DB) Drop(id uint64) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.drop(id)
}
