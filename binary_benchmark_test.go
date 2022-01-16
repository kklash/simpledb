package simpledb

import (
	"bytes"
	"reflect"
	"testing"
)

func BenchmarkEncodeRowHeader(b *testing.B) {
	for i := 0; i < b.N; i++ {
		encodeRowHeader(randUint64(), randUint64())
	}
}

func BenchmarkDecodeFromBinary(b *testing.B) {
	encodeExample := func(v interface{}) []byte {
		buf := new(bytes.Buffer)
		encodeToBinary(buf, reflect.ValueOf(v))
		return buf.Bytes()
	}

	stringExample := encodeExample("abcdefg222ccjjjjjjjj")
	byteSliceExample := encodeExample([]byte("hello i am a secret string - \x00\x00\xff\xff\xff"))
	byteArrayExample := encodeExample([20]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	uint32Example := encodeExample(uint32(0x12345678))
	int64Example := encodeExample(int64(0x78ffccddeeffaa00))
	sliceOfSliceOfStringsExample := encodeExample([][]string{{"foobar"}, {"once", "upon", "a", "time"}})

	b.ResetTimer()
	b.Run("string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := new(string)
			_, err := decodeFromBinary(bytes.NewReader(stringExample), reflect.ValueOf(s))
			if err != nil {
				b.Fatalf("failed to decode string: %s", err)
			}
		}
	})

	b.Run("[]byte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := new([]byte)
			_, err := decodeFromBinary(bytes.NewReader(byteSliceExample), reflect.ValueOf(s))
			if err != nil {
				b.Fatalf("failed to decode []byte: %s", err)
			}
		}
	})

	b.Run("[20]byte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			s := new([20]byte)
			_, err := decodeFromBinary(bytes.NewReader(byteArrayExample), reflect.ValueOf(s))
			if err != nil {
				b.Fatalf("failed to decode [20]byte: %s", err)
			}
		}
	})

	b.Run("uint32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			i := new(uint32)
			_, err := decodeFromBinary(bytes.NewReader(uint32Example), reflect.ValueOf(i))
			if err != nil {
				b.Fatalf("failed to decode uint32: %s", err)
			}
		}
	})

	b.Run("int64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			i := new(int64)
			_, err := decodeFromBinary(bytes.NewReader(int64Example), reflect.ValueOf(i))
			if err != nil {
				b.Fatalf("failed to decode int64: %s", err)
			}
		}
	})

	b.Run("[][]string", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			slice := new([][]string)
			_, err := decodeFromBinary(bytes.NewReader(sliceOfSliceOfStringsExample), reflect.ValueOf(slice))
			if err != nil {
				b.Fatalf("failed to decode [][]string: %s", err)
			}
		}
	})
}

func BenchmarkDecodeStructFromBinary(b *testing.B) {
	type Person struct {
		Name       string
		Age        uint32
		SecretData []byte
		Array      [10]int32
	}

	encodePerson := func(p *Person) []byte {
		buf := new(bytes.Buffer)
		encodeStructToBinary(buf, reflect.ValueOf(p))
		return buf.Bytes()
	}

	examples := [][]byte{
		encodePerson(&Person{
			Name:       "James",
			Age:        32,
			SecretData: []byte("Secret info"),
			Array:      [10]int32{},
		}),
		encodePerson(&Person{
			Name:       "Bertha",
			Age:        3,
			SecretData: make([]byte, 500),
			Array:      [10]int32{0, 1, 2, 3, 4, 5},
		}),
		encodePerson(&Person{
			Name:       "1111111111111111111111111111111111111111111111111111111111",
			Age:        0,
			SecretData: []byte{1, 2, 3, 4, 5},
			Array:      [10]int32{0xffff, 0xaf11223, 0xff2233, 0xf1, 0x283, 0x28391, 0xfa29b},
		}),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(examples[i%len(examples)])
		person := new(Person)
		_, err := decodeStructFromBinary(reader, reflect.ValueOf(person))
		if err != nil {
			b.Fatalf("failed to decode person struct: %s", err)
		}
	}
}
