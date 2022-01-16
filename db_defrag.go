package simpledb

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Defrag seeks through the database source and cleans out the sections of zero bytes from deleted rows.
// Defrag copies the database temporarily to os.TempDir(), ensuring the tempfile is cleaned up even if
// a panic occurs. Defrag calls should be performed after large numbers of rows have been dropped, as this
// will reduce the on-disk size of the DB and thus improve performance.
func (db *DB) Defrag() error {
	tempFile, err := os.CreateTemp(os.TempDir(), "simpledb-defrag-")
	if err != nil {
		return fmt.Errorf("Failed to create temp file for defrag: %s", err)
	}

	defer func() {
		panicValue := recover()
		tempFile.Close()
		os.Remove(tempFile.Name())
		if panicValue != nil {
			panic(panicValue)
		}
	}()

	db.mutex.Lock()
	defer db.mutex.Unlock()

	newIndex := make(map[uint64]int64)
	offset := int64(0)
	byteReader := &wrappedByteReader{db.source}

	for id, cursor := range db.index {
		// +8 bytes to bypass the id part of the row header
		if _, err := db.source.Seek(cursor+8, io.SeekStart); err != nil {
			return err
		}

		size, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return err
		}

		rowHeader := encodeRowHeader(id, size)
		if _, err := tempFile.Write(rowHeader); err != nil {
			return err
		}

		if _, err := io.CopyN(tempFile, db.source, int64(size)); err != nil {
			return err
		}

		newIndex[id] = offset
		offset += int64(size) + int64(len(rowHeader))
	}

	if _, err = db.source.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err = tempFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	newSize, err := io.Copy(db.source, tempFile)
	if err != nil {
		return err
	}

	db.index = newIndex

	if err := db.source.Truncate(newSize); err != nil {
		return err
	}

	return nil
}
