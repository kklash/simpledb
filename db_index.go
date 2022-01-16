package simpledb

import (
	"encoding/binary"
	"io"
	"reflect"
)

func (db *DB) addToIndex(id uint64, cursor int64, decodeValue func() (interface{}, error)) error {
	if db.index == nil {
		db.index = make(map[uint64]int64)
	}

	if len(db.customIndices) > 0 {
		value, err := decodeValue()
		if err != nil {
			return err
		}
		valueReflected := reflect.Indirect(reflect.ValueOf(value))

		for fieldName, customIndex := range db.customIndices {
			customIndex[id] = valueReflected.FieldByName(fieldName).Interface()
		}
	}

	db.index[id] = cursor
	return nil
}

func (db *DB) removeFromIndex(id uint64) {
	delete(db.index, id)

	for _, customIndex := range db.customIndices {
		delete(customIndex, id)
	}
}

// PopulateIndex reads through the underlying database source to populate the in-memory index.
func (db *DB) PopulateIndex() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if _, err := db.source.Seek(0, io.SeekStart); err != nil {
		return err
	}

	db.index = make(map[uint64]int64)

	offset := int64(0)
	byteReader := &wrappedByteReader{db.source}
	for {
		id, err := decodeUint64(db.source)
		if err != nil {
			// END of DB
			if err == io.EOF {
				return nil
			}
			return err
		}

		size, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return err
		}

		decodeValue := func() (interface{}, error) {
			destPtr := reflect.New(db.schema.dataType).Interface()
			if _, err := db.schema.Decode(db.source, destPtr); err != nil {
				return nil, err
			}
			return destPtr, nil
		}

		if err := db.addToIndex(id, offset, decodeValue); err != nil {
			return err
		}

		rowHeaderSize := int64(len(encodeUvarint(size))) + 8
		offset += int64(size) + rowHeaderSize

		if _, err = db.source.Seek(offset, io.SeekStart); err != nil {
			return err
		}
	}
}
