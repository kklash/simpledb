package simpledb_test

import (
	"fmt"
	"local/simpledb"
	"os"
)

func ExampleDB() {
	type Car struct {
		Year  uint16
		Color string
		Make  string
		Model string
	}

	tempFile, err := os.CreateTemp(os.TempDir(), "simpledb-")
	if err != nil {
		panic(err)
	}

	defer os.Remove(tempFile.Name())

	db, err := simpledb.NewDB(tempFile, Car{})
	if err != nil {
		panic(err)
	}

	carID, err := db.Insert(&Car{
		Year:  2008,
		Color: "brown",
		Make:  "Mazda",
		Model: "Miata",
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("ID type: %T\n", carID)

	var car Car
	err = db.Find(carID, &car)
	if err != nil {
		panic(err)
	}

	fmt.Printf("car.Year: %d\n", car.Year)
	fmt.Printf("car.Color: %s\n", car.Color)
	fmt.Printf("car.Make: %s\n", car.Make)
	fmt.Printf("car.Model: %s\n", car.Model)

	// Output:
	// ID type: uint64
	// car.Year: 2008
	// car.Color: brown
	// car.Make: Mazda
	// car.Model: Miata
}
