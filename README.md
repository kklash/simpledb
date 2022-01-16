# SimpleDB

SimpleDB is a very basic No-SQL database format for long-term data storage in Golang. It is WIP, has a LOT of drawbacks, and definitely is not production-grade:

- not very performant
- not scaleable, and it isn't safe for multiple connections at once
- limited to one table per DB
- simplistic query support

Depending on your use-case, the benefits may be worthwhile:

- very easy to reimplement and maintain
- zero dependencies
- helps keep large amounts of data out of memory when not needed
- very minimal, no daemons to spin up or configuration to learn


Example usage:

```go
type Car struct {
  Year  uint16
  Color string
  Make  string
  Model string
}

tempFile, _ := os.CreateTemp(os.TempDir(), "simpledb-")

db, err := simpledb.NewDB(tempFile, Car{})
if err != nil {
  // ...
}

carID, err := db.Insert(&Car{
  Year:  2008,
  Color: "brown",
  Make:  "Mazda",
  Model: "Miata",
})
if err != nil {
  // ...
}

var car Car
err = db.Find(carID, &car)
if err != nil {
  // ...
}

fmt.Printf("car.Year: %d\n", car.Year)
fmt.Printf("car.Color: %s\n", car.Color)
fmt.Printf("car.Make: %s\n", car.Make)
fmt.Printf("car.Model: %s\n", car.Model)
```

To create a SimpleDB, you must provide a data source which satisfies the `simpledb.Source` interface:

```go
type Source interface {
  io.Reader
  io.Writer
  io.Seeker
  io.Closer
  Truncate(size int64) error
}
```

`simpledb.Source` is an interface for the long-term storage used by DB. Usually, this is an `*os.File`, but you could also design a source which reads and writes through some other means. Read and Write calls should both move the same cursor of the Seeker, and Seek calls should support all three whence values.

You must also pass a zero-value struct instance, whose exported fields will define the table schema. SimpleDB is, for the moment, a single-table database.

Guidelines for struct types which can define valid SimpleDB tables:

- The struct type must export only fields whose types are fixed-size, or are slices which boil down to those types.
- `simpledb.PrimitiveFixedSizeKinds` defines the set of usable fixed-size types.
- `string` is also allowed.
- Arrays of fixed-size types are considered to also be fixed-size types and can be used.
- The sequence in which struct fields are declared does not matter - they are sorted alphabetically to decide encoding order.

Additional type support (e.g. for maps and structs) is forthcoming.

### How does it work?

When first opened on a new file, the database will not write any data, because an empty SimpleDB has zero size. As values are inserted into the table, SimpleDB encodes and writes the values directly to the `Source` file. First it writes the 'row header', consisting of a random `uint64` ID, and the size of the row, encoded as a unsigned varint. The _index_ of that row is its offset from the start, which for the first row would be zero; For the second row, the _index_ would be the size of the first row, etc.

Slices are encoded first by writing their slice length encoded as a unsigned varint, then each element is written. All values are encoded with `binary.BigEndian`.

As each row is inserted, their indices are cached in memory, mapped to by their ID numbers. A caller who retains the ID number can thus quickly look-up and decode the stored value. However, perhaps you don't have the ID number, or you want to find multiple rows...

### Filtering

You can use the `db.Filter` method to return all rows which match a certain query. Currently this is limited to deep-equality-based checks, but in the future I plan to extend the query functionality quite a bit.


```go
rows, err := usersDB.Filter(map[string]interface{}{
  "UserName": "josh89",
})
if err != nil {
  // ...
} else if len(rows) == 0 {
  // username not found
}

id := rows[0].ID
user := rows[0].Value.(*User)
```

### Indexing

If you will need to look up rows using certain fields frequently, you can add an index to that field.

```go
type User struct {
  UserName string `simpledb:"indexed"`
}
```

Adding the tag `simpledb:"indexed"` to a struct field used to define a SimpleDB Schema will add an in-memory cache for that field to the database. The cache records the row's ID number, mapping it to the value of the field upon insertion or reading from disk.

When calling `db.Filter`, SimpleDB will compare the cached value with the queried value using `reflect.DeepEqual`.

### Dropping

You can drop rows using `db.Drop(id)`, but this alone does not reduce the on-disk size of the database. It only zeros the given row on-disk. Dropped rows on-disk look like big sectors of zeros which are skipped when reading the database from disk.

### Defragging

To re-compact the database on-disk back down to its optimal size, you should call `db.Defrag()`. This operation removes all zero'd rows from the database file on-disk and thus reduces file size. Best practice is to call `db.Defrag()` before closing an application which uses a SimpleDB.
