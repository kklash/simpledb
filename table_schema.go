package simpledb

import (
	"fmt"
	"io"
	"reflect"
)

type tableSchema struct {
	dataType reflect.Type
}

func (schema *tableSchema) ColumnNames() []string {
	return getExportedFieldNames(schema.dataType)
}

func (schema *tableSchema) Reflect(strct interface{}) error {
	dataType := reflect.TypeOf(strct)
	if dataType.Kind() != reflect.Struct {
		return fmt.Errorf("Only struct types can be passed to schema.Reflect. Got: %s", dataType)
	}

	fields := reflect.VisibleFields(dataType)

	for _, field := range fields {
		if field.IsExported() && !isValidColumnType(field.Type) {
			return fmt.Errorf("Only structs exporting fields of fixed-size types, "+
				"strings, or slices of such types can be passed to schema.Reflect; "+
				"Found kind '%s' in struct '%s'", field.Type, dataType)
		}
	}

	schema.dataType = dataType
	return nil
}

func (schema *tableSchema) Encode(w io.Writer, e interface{}) (int, error) {
	value := reflect.ValueOf(e)
	if value.Type() == reflect.PtrTo(schema.dataType) {
		value = reflect.Indirect(value)
	} else if value.Type() != schema.dataType {
		return 0, fmt.Errorf("invalid data type for DB encoding '%s'", value.Type())
	}

	return encodeStructToBinary(w, value)
}

func (schema *tableSchema) Decode(r io.Reader, e interface{}) (int, error) {
	value := reflect.ValueOf(e)
	if value.Type() != reflect.PtrTo(schema.dataType) {
		return 0, fmt.Errorf("invalid data type for DB decoding '%s'", value.Type())
	}

	return decodeStructFromBinary(r, value)
}
