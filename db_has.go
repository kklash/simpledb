package simpledb

// Has returns true if the given ID is stored in the DB's index.
func (db *DB) Has(id uint64) bool {
	_, ok := db.index[id]
	return ok
}
