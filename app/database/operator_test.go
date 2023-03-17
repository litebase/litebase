package database_test

import (
	"litebasedb/app/database"
	"litebasedb/app/sqlite3"
	"litebasedb/internal/test"
	"testing"
)

func TestNewOperator(t *testing.T) {
	// wal := NewWAL()
	// operator := NewOperator(wal)

	// if operator.InTransaction() {
	// 	t.Fatal("Should not be in transaction")
	// }

	// if operator.IsWriting() {
	// 	t.Fatal("Should not be writing")
	// }
}

func TestMonitor(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			t.Fatal(err)
		}

		wal := database.NewWAL(db.GetConnection().Path)
		operator := database.NewOperator(wal)

		_, err = operator.Monitor(false, func() (sqlite3.Result, error) {
			if !operator.IsWriting() {
				t.Fatal("Should be writing")
			}

			statement, err := db.GetConnection().Prepare("SELECT 1")

			if err != nil {
				return nil, err
			}

			return db.GetConnection().Query(statement, []interface{}{}...)
		})

		if operator.IsWriting() {
			t.Fatal("Should not be writing")
		}

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestRecord(t *testing.T) {
	test.Run(func() {
		mock := test.MockDatabase()
		db, err := database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			t.Fatal(err)
		}

		test.RunQuery(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", []interface{}{})

		db, err = database.Get(mock["databaseUuid"], mock["branchUuid"], nil, false)

		if err != nil {
			t.Fatal(err)
		}

		sqliteStatement, err := db.GetConnection().Prepare("INSERT INTO users (name) VALUES (?)")

		if err != nil {
			t.Fatal(err)
		}

		_, err = db.GetConnection().Query(sqliteStatement, []interface{}{"John"}...)

		if err != nil {
			t.Fatal(err)
		}

		db.GetConnection().Operator.Transmit()

		recordedPages := db.GetConnection().Operator.Record()

		if recordedPages == 0 {
			t.Fatalf("Should have recorded pages")
		}
	})
}
