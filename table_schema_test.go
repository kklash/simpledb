package simpledb

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"
)

func TestTableSchema(t *testing.T) {
	failIfNoError := func(value interface{}) {
		err := new(tableSchema).Reflect(value)
		if err == nil {
			t.Errorf("failed to return error when calling table.Reflect on incompatible struct type")
		}
	}

	failIfNoError([]int{})                                     // only structs
	failIfNoError(struct{ Age uint }{})                        // no unsized integer types
	failIfNoError(struct{ IntArray []int }{})                  // no slices unless they are fixed-size element types
	failIfNoError(struct{ SubStruct struct{ Name string } }{}) // no composite field types

	type Person struct {
		private uint
		Name    string
		Arr     [4]byte
		Ints    []uint16
		Age     uint32
		Data    []byte
		Empty   []byte
		Strings []string
	}

	table := new(tableSchema)
	table.Reflect(Person{})

	person := &Person{
		private: 20,
		Name:    "bob",
		Age:     20,
		Arr:     [4]byte{1, 2, 3, 4},
		Ints:    []uint16{0xffff, 8},
		Data:    []byte{1, 2, 3, 4},
		Empty:   nil,
		Strings: []string{"welcome", "home"},
	}

	buf := new(bytes.Buffer)
	nBytesEncoded, err := table.Encode(buf, person)
	if err != nil {
		t.Errorf("Failed to encode Person struct: %s", err)
		return
	}

	if nBytesEncoded != buf.Len() {
		t.Errorf(
			"encoded size does not match reported number of bytes written\nEncoded data: %d\nByte count:   %d",
			buf.Len(),
			nBytesEncoded,
		)
		return
	}

	encodedPersonHex := hex.EncodeToString(buf.Bytes())
	expectedHex := "000000140102030404010203040002ffff000803626f62020777656c636f6d6504686f6d65"
	if encodedPersonHex != expectedHex {
		t.Errorf("expected encoded Person does not match\nWanted %s\nGot    %s", expectedHex, encodedPersonHex)
		return
	}

	decodedPerson := new(Person)

	nBytesDecoded, err := table.Decode(bytes.NewReader(buf.Bytes()), decodedPerson)
	if err != nil {
		t.Errorf("Failed to decode person struct: %s", err)
		return
	}

	if nBytesDecoded != nBytesEncoded {
		t.Errorf("decoded byte count does not match encoded byte count\nEncoded: %d\nDecoded: %d", nBytesEncoded, nBytesDecoded)
		return
	}

	expectedPerson := &(*person)
	expectedPerson.private = 0      // unexported fields are not encoded
	expectedPerson.Empty = []byte{} // nil byte slices are decoded as empty byte slices

	if !reflect.DeepEqual(decodedPerson, expectedPerson) {
		t.Errorf("decoded person does not match expected\nWanted %v\nGot    %v", expectedPerson, decodedPerson)
		return
	}
}
