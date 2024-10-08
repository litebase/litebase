package database_test

import (
	"litebase/internal/test"
	"litebase/server/database"
	"testing"
)

func TestDatabaseCanBeCreated(t *testing.T) {
	test.Setup(t)

	databaseId := "test"
	branchId := "test"

	db, err := database.Create(
		databaseId,
		branchId,
	)

	if db == nil {
		t.Fail()
	}

	if err != nil {
		t.Error(err)
	}
}

// func TestDatabaseCanBeClosed(t *testing.T) {
// 	os.Setenv("DATABASE_DIRECTORY", "../data")
// 	os.Setenv("DATABASE_NAME", "test")

// 	db := database.NewConnection()

// 	db.Close()

// 	if db.Connection() != nil {
// 		t.Fail()
// 	}
// }

// func TestDatabaseCanBeInitialized(t *testing.T) {
// 	os.Setenv("DATABASE_DIRECTORY", "../data")
// 	os.Setenv("DATABASE_NAME", "test")
// 	db := database.NewConnection()

// 	if db.Connection() == nil {
// 		t.Fail()
// 	}

// 	db.Close()

// 	if db.Connection() != nil {
// 		t.Fail()
// 	}
// }

// func TestDatabaseCanBeQueried(t *testing.T) {
// 	test.Setup(t)

// 	var (
// 		// result sqlite3.Result
// 		err error
// 	)

// 	db := database.NewConnection()

// 	_, err = db.Query("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

// 	if err != nil {
// 		t.Error(err)
// 	}

// 	db.Close()
// 	db = nil

// 	db = database.NewConnection()

// 	_, err = db.Query("INSERT INTO test (id, name) VALUES (1, 'test')")

// 	if err != nil {
// 		t.Error(err)
// 	}

// 	result, err := db.Query("SELECT * FROM test")

// 	log.Println("RESULT", result)

// 	if err != nil {
// 		t.Error(err)
// 	}

// 	if len(result) != 1 {
// 		t.Errorf("Expected 1 result, got %d", len(result))
// 	}
// }
