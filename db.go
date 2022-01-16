// Package simpledb provides a minimal, single-table No-SQL database.
package simpledb

import (
	"io"
	"reflect"
	"sync"
)

const (
	// DeletedID is the ID used on-disk to represent a deleted row.
	DeletedID uint64 = 0

	// StructTag is the struct tag inspected by DB when evaluating a value for the DB's schema.
	StructTag = "simpledb"
)

// Source is an interface for the long-term storage used by DB.
// Usually, this is an *os.File. Read and Write calls should both
// move the same cursor of the Seeker. Seek calls should
// support all three whence values.
type Source interface {
	io.Reader
	io.Writer
	io.Seeker
	io.Closer
	Truncate(size int64) error
}

// DB is a simple database with a single table. The table stores golang struct types as binary data on-disk.
// It maintains an in-memory index of where specific data structures are stored on-disk, for faster lookup.
// Reads from and writes to the DB block one-another with a mutex.
//
// Rows can be dropped, which zeros their data on-disk. If many drops have occurred, defragging should be
// performed to reduce the DB size and speed up performance on later opening. It is good practice to call
// db.Defrag() before quitting the application.
//
// If you wish to do frequent Filter calls on the DB using a particular field, you should add a tag to
// that struct field: `simpledb:"indexed"`. This causes the DB to cache values from that field in-memory,
// so that they can be looked up faster. This causes Filter calls which query that indexed field to
// finish much faster. The trade-off is a significant increase in memory consumption, and a slow-down
// on first-open for large databases.
type DB struct {
	schema        *tableSchema
	source        Source
	mutex         sync.Mutex
	index         map[uint64]int64
	customIndices map[string]map[uint64]interface{}
}

// ReflectSchema sets the schema of the DB based on the given struct type value.
//  type Car struct {
//    Color uint8
//    Year  uint16
//    Maker string
//    Serial [16]byte
//  }
//  if err := db.ReflectSchema(Car{}); err != nil {
//    panic(err)
//  }
func (db *DB) ReflectSchema(value interface{}) error {
	db.schema = new(tableSchema)
	if err := db.schema.Reflect(value); err != nil {
		return err
	}

	db.customIndices = make(map[string]map[uint64]interface{})
	for _, fieldName := range getExportedIndexedFields(reflect.TypeOf(value)) {
		db.customIndices[fieldName] = make(map[uint64]interface{})
	}

	return nil
}

// NewDB opens a DB on the target Source, usually an os.File pointer. Upon opening, NewDB reads the
// the source from start to finish and in doing so, populates its in-memory index for faster lookups later.
func NewDB(source Source, exampleValue interface{}) (*DB, error) {
	db := &DB{
		source: source,
		index:  make(map[uint64]int64),
	}

	if err := db.ReflectSchema(exampleValue); err != nil {
		return nil, err
	}

	if err := db.PopulateIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

// Size returns the byte-size (disk usage) of the DB.
func (db *DB) Size() (int64, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.source.Seek(0, io.SeekEnd)
}

// RowCount returns the number of rows in the DB.
func (db *DB) RowCount() int {
	return len(db.index)
}

// Close closes the underlying DB Source.
func (db *DB) Close() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.source.Close()
}
