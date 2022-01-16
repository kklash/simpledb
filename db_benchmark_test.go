package simpledb

import (
	"os"
	"reflect"
	"testing"
)

func BenchmarkDB(b *testing.B) {
	tempFile, err := os.CreateTemp(os.TempDir(), "simpledb-")
	if err != nil {
		b.Fatalf("Failed to create temp file: %s", err)
	}

	b.Cleanup(func() {
		tempFile.Close()
		// data, _ := os.ReadFile(tempFile.Name())
		// fmt.Printf("%x\n", data)
		os.Remove(tempFile.Name())
	})

	type Human struct {
		id          uint64 // private for testing
		Age         uint16
		Name        string `simpledb:"indexed"`
		Information []byte
	}
	db, err := NewDB(tempFile, Human{})
	if err != nil {
		b.Fatalf("failed to create DB: %s", err)
	}

	humanNames := []string{"George", "Marg", "Bill", "Wendy", "Ace"}

	randomHuman := func() *Human {
		info := make([]byte, randomData.Intn(30))
		randomData.Read(info)
		return &Human{
			Name:        humanNames[randomData.Intn(len(humanNames))],
			Age:         uint16(randomData.Uint32() >> 24),
			Information: info,
		}
	}

	allHumans := make([]*Human, 0, 100_000)

	insertNewHuman := func() {
		human := randomHuman()
		allHumans = append(allHumans, human)
		id, err := db.Insert(human)
		if err != nil {
			b.Fatalf("failed to insert human: %s", err)
		}
		human.id = id
	}

	b.Run("insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			insertNewHuman()
		}
	})

	randomData.Shuffle(len(allHumans), func(i, j int) {
		allHumans[i], allHumans[j] = allHumans[j], allHumans[i]
	})

	b.Run("find", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			var foundHuman Human
			err := db.Find(allHumans[i].id, &foundHuman)
			if err != nil {
				b.Errorf("Failed to find human 0x%x: %s", allHumans[i].id, err)
				return
			}
		}
	})

	b.Run("filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			exampleHuman := allHumans[i]

			_, err := db.Filter(map[string]interface{}{
				"Name": exampleHuman.Name,
				"Age":  exampleHuman.Age,
			})
			if err != nil {
				b.Errorf("Failed to filter humans: %s", err)
				return
			}
		}
	})

	for i := 0; i < 400_000; i++ {
		insertNewHuman()
	}
	randomData.Shuffle(len(allHumans), func(i, j int) {
		allHumans[i], allHumans[j] = allHumans[j], allHumans[i]
	})

	b.Run("drop", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			human := allHumans[0]
			allHumans = allHumans[1:]
			err := db.Drop(human.id)
			if err != nil {
				b.Fatalf("Failed to drop human row: %s", err)
			}
		}
	})

	b.Run("defrag", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := db.Defrag()
			if err != nil {
				b.Fatalf("Failed to defrag DB: %s", err)
			}
		}
	})

	b.Run("PopulateIndex", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := db.PopulateIndex()
			if err != nil {
				b.Fatalf("Failed to populate DB index: %s", err)
			}
		}
	})

	b.Run("update", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			newHuman := randomHuman()
			newHuman.id = allHumans[i].id
			if err := db.Update(newHuman.id, newHuman); err != nil {
				b.Fatalf("failed to run Update call: %s", err)
			}
			allHumans[i] = newHuman
		}
	})

	b.Run("iterate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			nextRow := db.Iterate()

			for {
				row, err := nextRow()
				if err != nil {
					b.Fatalf("failed to get next row from iterator: %s", err)
				}
				if row == nil {
					break
				}
			}
		}
	})
}

func BenchmarkIndexedFields(b *testing.B) {
	benchWithUserType := func(b *testing.B, t reflect.Type) {
		tempFile, err := os.CreateTemp(os.TempDir(), "simpledb-")
		if err != nil {
			b.Fatalf("Failed to create temp file: %s", err)
		}

		b.Cleanup(func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		})

		db, err := NewDB(tempFile, reflect.Indirect(reflect.New(t)).Interface())
		if err != nil {
			b.Fatalf("failed to create DB: %s", err)
		}

		for i := 0; i < 100_000; i++ {
			userValue := reflect.Indirect(reflect.New(t))
			if i == 25_000 {
				userValue.FieldByName("Email").Set(reflect.ValueOf("jamesbond@mi6.gov.uk"))
			} else {
				userValue.FieldByName("Email").Set(reflect.ValueOf("whatever@wherever.co.uk"))
			}

			if _, err := db.Insert(userValue.Interface()); err != nil {
				b.Fatalf("failed to insert user: %s", err)
			}
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			filterResults, err := db.Filter(map[string]interface{}{
				"Email": "jamesbond@mi6.gov.uk",
			})
			if err != nil {
				b.Fatalf("failed to filter for user: %s", err)
			}
			if len(filterResults) != 1 {
				b.Fatalf("filter results incorrect length: %d", len(filterResults))
			}
		}
	}

	b.Run("Without indexing Email field", func(b *testing.B) {
		type User struct {
			Email        string
			PasswordHash []byte
		}

		benchWithUserType(b, reflect.TypeOf(User{}))
	})

	b.Run("With index on Email field", func(b *testing.B) {
		type User struct {
			Email        string `simpledb:"indexed"`
			PasswordHash []byte
		}

		benchWithUserType(b, reflect.TypeOf(User{}))
	})
}
