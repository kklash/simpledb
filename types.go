package simpledb

import (
	"reflect"
	"sort"
)

// PrimitiveFixedSizeKinds are the accepted primitive types, usable for columns in a DB schema struct.
// Arrays are also available, and are considered fixed-size, as long as the array's element's kind is
// a member of PrimitiveFixedSizeKinds.
var PrimitiveFixedSizeKinds = []reflect.Kind{
	reflect.Bool,
	reflect.Int8,
	reflect.Int16,
	reflect.Int32,
	reflect.Int64,
	reflect.Uint8,
	reflect.Uint16,
	reflect.Uint32,
	reflect.Uint64,
	reflect.Float32,
	reflect.Float64,
	reflect.Complex64,
	reflect.Complex128,
}

// VariableSizeKinds are the accepted variable-size types.
var VariableSizeKinds = []reflect.Kind{
	reflect.String,
	reflect.Slice,
}

func isFixedSizeType(t reflect.Type) bool {
	// numeric types
	for _, kind := range PrimitiveFixedSizeKinds {
		if t.Kind() == kind {
			return true
		}
	}

	// fixed-size arrays only
	if t.Kind() == reflect.Array {
		if isFixedSizeType(t.Elem()) {
			return true
		}
	}

	return false
}

func isVariableSizeType(t reflect.Type) bool {
	if t.Kind() == reflect.String {
		return true
	}

	if t.Kind() == reflect.Slice {
		return true
	}

	return false
}

func isValidColumnType(t reflect.Type) bool {
	if t.Kind() == reflect.String {
		return true
	}

	// strings & slices of bytes are allowed
	if t.Kind() == reflect.Slice {
		return isValidColumnType(t.Elem())
	}

	return isFixedSizeType(t)
}

func needsRecursiveEncoding(t reflect.Type) bool {
	return t.Kind() == reflect.Slice && isVariableSizeType(t.Elem())
}

func getExportedFieldNames(t reflect.Type) []string {
	fields := reflect.VisibleFields(t)
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.IsExported() {
			fieldNames = append(fieldNames, field.Name)
		}
	}
	sort.Strings(fieldNames)
	return fieldNames
}

func getExportedIndexedFields(t reflect.Type) []string {
	fields := reflect.VisibleFields(t)
	fieldNames := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.IsExported() && field.Tag.Get(StructTag) == "indexed" {
			fieldNames = append(fieldNames, field.Name)
		}
	}
	sort.Strings(fieldNames)
	return fieldNames
}
