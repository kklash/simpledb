package simpledb

import "reflect"

// Pop combines db.Find and db.Drop into one atomic operation.
// The result is not set in destPtr until the row is decoded & dropped
// cleanly from the database.
func (db *DB) Pop(id uint64, destPtr interface{}) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	cursor, ok := db.index[id]
	if !ok {
		return ErrNotFound
	}

	// do not set destPtr until we have dropped the row from the DB without error
	indirectDestValue := reflect.Indirect(reflect.ValueOf(destPtr))
	holdingPtr := reflect.New(indirectDestValue.Type())

	if err := db.decodeAt(cursor, holdingPtr.Interface()); err != nil {
		return err
	}

	if err := db.drop(id); err != nil {
		return err
	}

	indirectDestValue.Set(reflect.Indirect(holdingPtr))

	return nil
}
