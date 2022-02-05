package simpledb

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
)

// wrappedByteReader wraps an io.Reader with a ReadByte method, needed for binary.ReadUvarint
type wrappedByteReader struct {
	io.Reader
}

func (r *wrappedByteReader) ReadByte() (byte, error) {
	p := make([]byte, 1)
	_, err := r.Read(p)
	return p[0], err
}

func encodeUvarint(n uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	written := binary.PutUvarint(buf, n)
	return buf[:written]
}

// Encodes a uint64 as a big-endian byte slice.
func encodeUint64(n uint64) []byte {
	buf := make([]byte, 8)
	for i := 7; n > 0; i-- {
		buf[i] = byte(n & 0xff)
		n >>= 8
	}
	return buf
}

// Reads a uint64 from an io.Reader.
func decodeUint64(r io.Reader) (n uint64, err error) {
	buf := make([]byte, 8)
	if _, err = io.ReadFull(r, buf); err != nil {
		return
	}

	for i := 0; i < 8; i++ {
		byteShift := 64 - ((i + 1) * 8)
		n |= uint64(buf[i]) << byteShift
	}
	return
}

func encodeRowHeader(id, size uint64) []byte {
	return append(encodeUint64(id), encodeUvarint(size)...)
}

// encodeToBinary writes a given value to an io.Writer in simpledb's binary encoding scheme.
// It returns the number of bytes written to w and any error encountered while encoding or writing.
//
// Encodeable types:
//   - bool
//   - uint8 (byte), uint16, uint32, uint64
//   - int8, int16, int32, int64
//   - float32, float64, complex64, complex128
//   - slices of any of these types
//   - string
//
// If the value is a fixed size type (either [n]byte or a sized numerical type), then the
// value is encoded in binary in BigEndian format. If the value is a variable-size type, it is
// encoded as the concatenation of an unsigned length varint and the serialized bytes of the value.
func encodeToBinary(w io.Writer, fieldValue reflect.Value) (int, error) {
	buf := new(bytes.Buffer)
	fieldValueInterface := fieldValue.Interface()
	fieldType := fieldValue.Type()

	if isVariableSizeType(fieldType) {
		// encoding/binary can't handle UTF8 strings
		if fieldAsString, ok := fieldValueInterface.(string); ok {
			fieldValueInterface = []byte(fieldAsString)
		}

		fieldValueLength := fieldValue.Len()

		if _, err := buf.Write(encodeUvarint(uint64(fieldValueLength))); err != nil {
			return 0, err
		}

		if needsRecursiveEncoding(fieldType) {
			for i := 0; i < fieldValueLength; i++ {
				if _, err := encodeToBinary(buf, fieldValue.Index(i)); err != nil {
					return 0, err
				}
			}
			bytesWritten, err := buf.WriteTo(w)
			return int(bytesWritten), err
		}
	}

	if err := binary.Write(buf, binary.BigEndian, fieldValueInterface); err != nil {
		return 0, err
	}

	bytesWritten, err := buf.WriteTo(w)
	return int(bytesWritten), err
}

// encodeStructToBinary encodes a given struct value using simpledb encoding.
// Only exported fields are encoded.
func encodeStructToBinary(w io.Writer, value reflect.Value) (int, error) {
	value = reflect.Indirect(value)
	bytesWritten := 0
	for _, fieldName := range getExportedFieldNames(value.Type()) {
		n, err := encodeToBinary(w, value.FieldByName(fieldName))
		bytesWritten += n
		if err != nil {
			return bytesWritten, err
		}
	}

	return bytesWritten, nil
}

// decodeFromBinary reads values from an io.Reader to populate a given
// value using simpledb's binary encoding scheme. This is the reverse
// of encodeToBinary. Returns the number of bytes read from r and any
// error encountered while reading or decoding.
func decodeFromBinary(r io.Reader, fieldValue reflect.Value) (int, error) {
	fieldValue = reflect.Indirect(fieldValue)
	fieldType := fieldValue.Type()

	// Short circuit for decoding strings: just decode as a byte-slice and convert the result to a string
	if fieldType.Kind() == reflect.String {
		var data []byte
		bytesRead, err := decodeFromBinary(r, reflect.ValueOf(&data))
		fieldValue.Set(reflect.ValueOf(string(data)))
		return bytesRead, err
	}

	byteReader := &wrappedByteReader{r}

	bytesRead := 0

	if isVariableSizeType(fieldType) {
		length, err := binary.ReadUvarint(byteReader)
		if err != nil {
			return bytesRead, err
		}
		bytesRead += len(encodeUvarint(length))

		fieldValueLength := int(length)
		fieldValue.Set(reflect.MakeSlice(fieldType, fieldValueLength, fieldValueLength))

		if needsRecursiveEncoding(fieldType) {
			for i := 0; i < fieldValueLength; i++ {
				n, err := decodeFromBinary(byteReader, fieldValue.Index(i))
				bytesRead += n
				if err != nil {
					return bytesRead, err
				}
			}
			return bytesRead, nil
		}
	}

	if err := binary.Read(byteReader, binary.BigEndian, fieldValue.Addr().Interface()); err != nil {
		return bytesRead, err
	}
	bytesRead += binary.Size(fieldValue.Interface())

	return bytesRead, nil
}

// decodeStructFromBinary decodes binary data and unmarshals it
// into the given struct value pointer using simpledb encoding.
// Only exported fields are decoded & populated.
func decodeStructFromBinary(r io.Reader, value reflect.Value) (int, error) {
	value = reflect.Indirect(value)

	bytesRead := 0
	for _, fieldName := range getExportedFieldNames(value.Type()) {
		fieldValue := value.FieldByName(fieldName)
		n, err := decodeFromBinary(r, fieldValue)
		bytesRead += n
		if err != nil {
			return bytesRead, err
		}
	}

	return bytesRead, nil
}
