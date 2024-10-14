package database_test

import (
	"context"
	"litebase/internal/test"
	"litebase/server"
	"log"
	"sync"
	"testing"
)

func TestDatabaseConnectionIsolationDuringCheckpoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		connection2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatal(err)
		}

		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 100000; i++ {
				_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", "test")

				if err != nil {
					t.Error(err)
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 10; i++ {
				_, err := connection2.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

				if err != nil {
					t.Error(err)
				}
			}
		}()

		wg.Wait()

		resut, err := connection1.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

		if err != nil {
			t.Error(err)
		}

		log.Println(resut)

		resut, err = connection2.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

		if err != nil {
			t.Error(err)
		}

		log.Println(resut)
	})
}

/*
This test is useful in ensuring the database can be properly written to and read
from in an interleaved manner without issue.

TODO: Test times out.
*/
func TestDatabaseConnectionsInterleaved(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		connection2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatal(err)
		}

		app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)
		app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection2)

		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 500000; i++ {
				db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Error(err)
					break
				}

				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", "test")

				if err != nil {
					t.Error(err)
					break
				}

				if db.GetConnection().SqliteConnection().Changes() != 1 {
					t.Error("Expected 1 row affected")
					break
				}

				app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 500000; i++ {
				db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Error(err)
					break
				}

				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

				if err != nil {
					t.Error(err)
					break
				}

				app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)
			}
		}()

		wg.Wait()

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Error(err)
		}

		resut, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

		if err != nil {
			t.Error(err)
		}

		log.Println(resut)

		db, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Error(err)
		}

		resut, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

		if err != nil {
			t.Error(err)
		}

		log.Println(resut)

		db, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Error(err)
		}

		resut, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

		if err != nil {
			t.Error(err)
		}

		log.Println(resut)
	})
}
