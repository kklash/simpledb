package simpledb

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"
)

func TestBinaryEncoding(t *testing.T) {
	type Fixture struct {
		inputValue  interface{}
		expectedHex string
	}

	fixtures := []Fixture{
		{
			int32(-1),
			"ffffffff",
		},
		{
			int32(-0xffff),
			"ffff0001",
		},
		{
			uint32(10),
			"0000000a",
		},
		{
			byte(1),
			"01",
		},
		{
			[10]byte{},
			"00000000000000000000",
		},
		{
			[]byte{},
			"00",
		},
		{
			"foobar",
			"06666f6f626172", // 0x06 + []byte("foobar")
		},
		{
			[]byte{1, 2, 3},
			"03010203", // 0x03 + [1, 2, 3]
		},
		{
			[5]uint16{0xffff, 1, 2, 3, 4},
			"ffff0001000200030004",
		},
		{
			[]uint32{0xffffaaaa, 0, 0xccccdddd},
			"03ffffaaaa00000000ccccdddd",
		},
		{
			[][2]uint16{
				{1, 2},
				{3, 4},
			},
			"020001000200030004",
		},
		{
			[][]int32{
				{1, 2, 3},
				{4, 5, 6, 7},
			},
			"02030000000100000002000000030400000004000000050000000600000007",
		},
		{
			[][]string{
				{"foo", "bar"},
				{"", "hello"},
			},
			"020203666f6f0362617202000568656c6c6f",
		},
	}

	for _, fixture := range fixtures {
		buf := new(bytes.Buffer)
		bytesWritten, err := encodeToBinary(buf, reflect.ValueOf(fixture.inputValue))
		if err != nil {
			t.Errorf("Failed to encode fixture to binary: %s", err)
			continue
		}

		if bytesWritten != buf.Len() {
			t.Errorf("incorrect number of bytes written returned\nWanted %d\nGot    %d", buf.Len(), bytesWritten)
			continue
		}

		encoded := buf.Bytes()
		encodedHex := fmt.Sprintf("%x", encoded)
		if encodedHex != fixture.expectedHex {
			t.Errorf("encoding does not match expected\nWanted %s\nGot    %s", fixture.expectedHex, encodedHex)
			continue
		}

		decodedValue := reflect.Indirect(reflect.New(reflect.TypeOf(fixture.inputValue)))
		bytesRead, err := decodeFromBinary(buf, decodedValue)
		if err != nil {
			t.Errorf("Failed to decode fixture from binary: %s", err)
			continue
		}

		if bytesRead != bytesWritten {
			t.Errorf("incorrect number of bytes read returned\nWanted %d\nGot    %d", bytesWritten, bytesRead)
			continue
		}

		decoded := decodedValue.Interface()
		if !reflect.DeepEqual(decoded, fixture.inputValue) {
			t.Errorf("Decoded value does not match original input\nWanted %v\nGot    %v", fixture.inputValue, decoded)
			continue
		}
	}
}
