package simpledb

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
)

func TestDB(t *testing.T) {
	type Session struct {
		Email  string `simpledb:"indexed"`
		Secret [8]byte
	}

	tempFile, err := os.CreateTemp(os.TempDir(), "simpledb-")
	if err != nil {
		t.Fatalf("Failed to create temp file: %s", err)
	}

	t.Cleanup(func() {
		tempFile.Close()
		// data, _ := os.ReadFile(tempFile.Name())
		// fmt.Printf("%x\n", data)
		os.Remove(tempFile.Name())
	})

	db, err := NewDB(tempFile, Session{})
	if err != nil {
		t.Fatalf("failed to create DB: %s", err)
	}

	session1 := Session{
		Email:  "foo@bar.com",
		Secret: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
	}
	session2 := Session{
		Email:  "james@bond.com",
		Secret: [8]byte{9, 10, 11, 12, 13, 14, 15, 16},
	}

	id1, err := db.Insert(&session1)
	if err != nil {
		t.Fatalf("Failed to insert *session1: %s", err)
	}

	if !db.Has(id1) {
		t.Fatalf("expected db.Has(id1) to be true")
	}

	if db.RowCount() != 1 {
		t.Fatalf("Expected to get row count of 1 after inserting session")
	}

	id2, err := db.Insert(session2)
	if err != nil {
		t.Fatalf("Failed to insert session2: %s", err)
	}

	if db.RowCount() != 2 {
		t.Fatalf("Expected to get row count of 2 after inserting 2nd session")
	}

	if id1 == 0 || id2 == 0 {
		t.Fatal("Received zero ID for session")
	}

	var unknownID uint64 = 21832
	var foundSession1 Session

	if err = db.Find(unknownID, &foundSession1); err != ErrNotFound {
		t.Fatalf("Unexpected error returned when finding unknown ID: %s", err)
	}

	if err = db.Find(id1, &foundSession1); err != nil {
		t.Fatalf("Failed to find session1: %s", err)
	}

	if foundSession1.Secret != session1.Secret {
		t.Fatal("found session1 secret does not match")
	} else if foundSession1.Email != session1.Email {
		t.Fatal("found session1 email does not match")
	}

	if err := db.Drop(unknownID); err != ErrNotFound {
		t.Fatalf("Unexpected error returned when dropping unknown ID: %s", err)
	}

	if err := db.Drop(id1); err != nil {
		t.Fatalf("Failed to drop session1: %s", err)
	}

	if db.RowCount() != 1 {
		t.Fatalf("Expected to get row count of 2 after dropping session")
	}

	dbContents := new(bytes.Buffer)
	db.source.Seek(0, io.SeekStart)
	io.Copy(dbContents, db.source)
	if bytes.Contains(dbContents.Bytes(), session1.Secret[:]) {
		t.Fatal("db data source still contains secret after dropping session1")
	}

	if err = db.Find(id1, &foundSession1); err != ErrNotFound {
		t.Fatalf("Unexpected error returned when finding dropped ID: %s", err)
	}

	filterResults, err := db.Filter(map[string]interface{}{
		"Email": "james@bond.com",
	})
	if err != nil {
		t.Fatalf("Failed to filter for session2: %s", err)
	}
	if len(filterResults) != 1 {
		t.Fatal("unexpected results returned by db.Filter")
	}

	foundSession2 := filterResults[0].Value.(*Session)
	if foundSession2.Secret != session2.Secret {
		t.Fatal("found session2 secret does not match")
	} else if foundSession2.Email != session2.Email {
		t.Fatal("found session2 email does not match")
	}

	if err := db.Defrag(); err != nil {
		t.Fatalf("Failed to defrag DB: %s", err)
	}

	if err = db.Find(id2, foundSession2); err != nil {
		t.Fatalf("Failed to find session2: %s", err)
	}

	if foundSession2.Secret != session2.Secret {
		t.Fatal("found session2 secret does not match")
	} else if foundSession2.Email != session2.Email {
		t.Fatal("found session2 email does not match")
	}

	db, err = NewDB(tempFile, Session{})
	if err != nil {
		t.Fatalf("failed to reflect schema: %s", err)
	}
	if err := db.PopulateIndex(); err != nil {
		t.Fatalf("failed to populate db index from disk: %s", err)
	}

	if err = db.Find(id2, foundSession2); err != nil {
		t.Fatalf("Failed to find session2 on newly reopened DB: %s", err)
	}

	session2.Email = "welcome@bob.it"

	if err = db.Update(id2, session2); err != nil {
		t.Fatalf("Failed to update session2 email: %s", err)
	}

	if err = db.Find(id2, foundSession2); err != nil {
		t.Fatalf("Failed to find session2 after Update call: %s", err)
	}

	if foundSession2.Email != session2.Email {
		t.Fatalf("Updated session2 Email does not match")
	}

	id1, err = db.Insert(&session1)
	if err != nil {
		t.Fatalf("Failed to reinsert &session1: %s", err)
	}

	if err := db.Drop(id1); err != nil {
		t.Fatalf("Failed to drop session1 again: %s", err)
	}

	t.Run("db.Iterate()", func(t *testing.T) {
		iter := db.Iterate()
		for {
			row, err := iter()
			if err != nil {
				t.Fatalf("failed to get next row: %s", err)
			}
			if row == nil {
				break
			}
			sessionFromIter, ok := row.Value.(*Session)
			if !ok {
				t.Fatalf("failed to cast row.Value")
			}

			if sessionFromIter.Email != session2.Email {
				t.Fatalf("session from iterator email does not match")
			}
		}
	})

	t.Run("db.Pop(id)", func(t *testing.T) {
		id1, err = db.Insert(&session1)
		if err != nil {
			t.Fatalf("Failed to insert session1 for Pop test: %s", err)
		}

		attempts := 3
		sessionChan := make(chan *Session, attempts)
		errChan := make(chan error, attempts)

		var wg sync.WaitGroup
		for i := 0; i < attempts; i++ {
			wg.Add(1)
			go func() {
				session1Copy := new(Session)
				err := db.Pop(id1, session1Copy)
				if err == nil {
					sessionChan <- session1Copy
				} else {
					errChan <- err
				}
				wg.Done()
			}()
		}
		wg.Wait()

		var session1Copy *Session
		for i := 0; i < attempts; i++ {
			select {
			case session1CopyFromChan := <-sessionChan:
				if session1Copy != nil {
					t.Fatalf("Expected only one db.Pop() call to succeed")
				}
				session1Copy = session1CopyFromChan
			case err := <-errChan:
				if err != ErrNotFound {
					t.Fatalf("expected ErrNotFound when calling db.Pop() multiple times: %s", err)
				}
			}
		}

		if session1Copy == nil {
			t.Fatalf("Failed to Pop at least one Session")
		}

		if session1Copy.Email != session1.Email {
			t.Fatalf("session from Pop does not match")
		}
	})
}
