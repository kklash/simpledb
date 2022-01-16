package simpledb

import (
	"reflect"
)

// FilterQuery is a set of strict equal requirements which are passed to db.Filter.
type FilterQuery = map[string]interface{}

// Row is a struct representing a row in the DB, including the struct Value
// and the unique ID number.
type Row struct {
	Value interface{}
	ID    uint64
}

// Filter searches the database for all rows which match the FilterQuery.
// Decoded rows are checked against the query, each column compared with the value in
// the query. If the row matches all queried values, it is included.
//
// TODO extend FilterQuery type as an interface.
func (db *DB) Filter(query FilterQuery) ([]*Row, error) {
	results := make([]*Row, 0)

	db.mutex.Lock()
	defer db.mutex.Unlock()

nextRow:
	for id, cursor := range db.index {
		if len(db.customIndices) > 0 {
			for fieldName, queryValue := range query {
				if customIndex, ok := db.customIndices[fieldName]; ok {
					// query is using an indexed field
					if indexedValue, ok := customIndex[id]; !ok || !reflect.DeepEqual(queryValue, indexedValue) {
						continue nextRow
					}
				}
			}
		}

		destPtr := reflect.New(db.schema.dataType).Interface()

		if err := db.decodeAt(cursor, destPtr); err != nil {
			return nil, err
		}

		if matchesFilterQuery(destPtr, query) {
			results = append(results, &Row{
				Value: destPtr,
				ID:    id,
			})
		}
	}

	return results, nil
}

func matchesFilterQuery(value interface{}, query FilterQuery) bool {
	for columnName, columnValue := range query {
		fieldValue := reflect.Indirect(reflect.ValueOf(value)).FieldByName(columnName)
		if !reflect.DeepEqual(fieldValue.Interface(), columnValue) {
			return false
		}
	}

	return true
}
