package database_test

import (
	"fmt"
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

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		if _, err := connection.GetConnection().Exec("CREATE TABLE test (name TEXT)", nil); err != nil {
			t.Fatal(err)
		}

		app.DatabaseManager.ConnectionManager().Release(connection)

		wg := sync.WaitGroup{}

		for range 100 {
			wg.Add(1)
			go func() {
				defer wg.Done()

				connection, _ := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				defer app.DatabaseManager.ConnectionManager().Release(connection)

				statement, _ := connection.GetConnection().Statement("INSERT INTO test (name) VALUES (?)")
				result := connection.GetConnection().ResultPool().Get()

				for range 10 {
					result.Reset()

					err := connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
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

				connection.GetConnection().ResultPool().Put(result)
			}()
		}

		wg.Wait()

		connection, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

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

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(connection)

		if _, err := connection.GetConnection().Exec("CREATE TABLE test (name TEXT)", nil); err != nil {
			t.Fatal(err)
		}

		for round := range 10 {
			wg := sync.WaitGroup{}

			for range 100 {
				wg.Add(1)
				go func() {
					defer wg.Done()

					connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						t.Error(err)
						return
					}

					defer app.DatabaseManager.ConnectionManager().Release(connection)

					statement, _ := connection.GetConnection().Statement("INSERT INTO test (name) VALUES (?)")
					result := connection.GetConnection().ResultPool().Get()

					for range 10 {
						result.Reset()

						err := connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
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

			connection, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

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

// TestDatabaseConnectionConcurrentReads verifies that multiple read queries
// can execute simultaneously without blocking each other
func TestDatabaseConnectionConcurrentReads(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Setup test data
		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		// Insert test data
		for i := 0; i < 100; i++ {
			_, err = connection.GetConnection().Exec("INSERT INTO test (id, name) VALUES (?, ?)", []sqlite3.StatementParameter{
				{Type: sqlite3.ParameterTypeInteger, Value: int64(i)},
				{Type: sqlite3.ParameterTypeText, Value: []byte("test")},
			})

			if err != nil {
				t.Fatal(err)
			}
		}

		app.DatabaseManager.ConnectionManager().Release(connection)

		// Now perform concurrent reads
		wg := sync.WaitGroup{}
		successCount := sync.Map{}

		for i := 0; i < 50; i++ {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					t.Error(err)
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(connection)

				// Each goroutine performs 10 reads
				for j := 0; j < 10; j++ {
					result, err := connection.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						t.Errorf("Read failed: %v", err)
						return
					}

					if len(result.Rows) > 0 && result.Rows[0][0].Int64() == 100 {
						successCount.Store(fmt.Sprintf("%d-%d", id, j), true)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify all reads succeeded
		count := 0
		successCount.Range(func(key, value interface{}) bool {
			count++
			return true
		})

		if count != 500 {
			t.Errorf("Expected 500 successful reads, got %d", count)
		}
	})
}

// TestDatabaseConnectionReadWhileWrite verifies that read queries can execute
// while write operations are in progress
func TestDatabaseConnectionReadWhileWrite(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Setup table
		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		app.DatabaseManager.ConnectionManager().Release(connection)

		wg := sync.WaitGroup{}

		// Start writers
		for i := 0; i < 10; i++ {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					t.Error(err)
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(connection)

				for j := 0; j < 50; j++ {
					_, err := connection.GetConnection().Exec("INSERT INTO test (id, name) VALUES (?, ?)", []sqlite3.StatementParameter{
						{Type: sqlite3.ParameterTypeInteger, Value: int64(id*50 + j)},
						{Type: sqlite3.ParameterTypeText, Value: []byte("test")},
					})

					if err != nil {
						t.Errorf("Write failed: %v", err)
					}
				}
			}(i)
		}

		// Start readers concurrently with writers
		readErrors := make(chan error, 20)

		for i := 0; i < 20; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					readErrors <- err
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(connection)

				for j := 0; j < 25; j++ {
					_, err := connection.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						readErrors <- fmt.Errorf("read failed: %w", err)
						return
					}
				}
			}()
		}

		wg.Wait()
		close(readErrors)

		// Check if any reads failed
		for err := range readErrors {
			t.Error(err)
		}

		// Verify final count
		connection, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(connection)

		result, err := connection.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

		if err != nil {
			t.Fatal(err)
		}

		if len(result.Rows) > 0 && result.Rows[0][0].Int64() != 500 {
			t.Errorf("Expected 500 rows, got %d", result.Rows[0][0].Int64())
		}
	})
}
