package database_test

import (
	"sync"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestDatabaseConnectionWithMultipleWriters(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

		if err != nil {
			t.Fatal(err)
		}

		connection.GetConnection().Exec("CREATE TABLE test (name TEXT)", nil)

		app.DatabaseManager.ConnectionManager().Release(connection)

		wg := sync.WaitGroup{}

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				connection, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

				defer app.DatabaseManager.ConnectionManager().Release(connection)

				statement, _ := connection.GetConnection().Statement("INSERT INTO test (name) VALUES (?)")
				result := connection.GetConnection().ResultPool().Get()

				for range 10 {
					result.Reset()

					err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
						err = statement.Sqlite3Statement.Exec(result, sqlite3.StatementParameter{
							Type:  "TEXT",
							Value: []byte("test"),
						})

						return err
					})

					if err != nil {
						t.Error(err)
					}
				}
			}()
		}

		wg.Wait()

		connection, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(connection)

		// Check integrity of the database
		result, err := connection.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

		if err != nil {
			t.Error(err)
		}

		if result.Rows[0][0].Int64() != 1000 {
			t.Errorf("Expected 1000 rows, got %d", result.Rows[0][0].Int64())
		}

	})
}

func TestDatabaseConnectionWithMultipleWritersWhileCheckPointing(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(connection)

		connection.GetConnection().Exec("CREATE TABLE test (name TEXT)", nil)

		for round := range 10 {
			wg := sync.WaitGroup{}

			for range 100 {
				wg.Add(1)
				go func() {
					defer wg.Done()

					connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

					if err != nil {
						t.Error(err)
						return
					}

					defer app.DatabaseManager.ConnectionManager().Release(connection)

					statement, _ := connection.GetConnection().Statement("INSERT INTO test (name) VALUES (?)")
					result := connection.GetConnection().ResultPool().Get()

					for range 10 {
						result.Reset()
						connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
							err = statement.Sqlite3Statement.Exec(result, sqlite3.StatementParameter{
								Type:  "TEXT",
								Value: []byte("test"),
							})

							return err
						})

						if err != nil {
							t.Error(err)
						}
					}
				}()
			}

			wg.Wait()

			connection, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatal(err)
			}

			// Check integrity of the database
			result, err := connection.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Error(err)
			}

			if len(result.Rows) > 0 && result.Rows[0][0].Int64() != (1000*int64(round+1)) {
				t.Errorf("Expected %d rows, got %d", 1000*int64(round+1), result.Rows[0][0].Int64())
			}

			app.DatabaseManager.ConnectionManager().Release(connection)
		}
	})
}
