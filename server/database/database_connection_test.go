package database_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/database"
	"github.com/litebase/litebase/server/sqlite3"
)

func TestNewDatabaseConnection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection == nil {
			t.Fatal("Expected connection to be non-nil")
		}

	})
}

func TestDatabaseConnection_Changes(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

		_, err = connection.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		// Insert a row
		_, err = connection.GetConnection().SqliteConnection().Exec(context.Background(), []byte("INSERT INTO test (name) VALUES (?)"), sqlite3.StatementParameter{
			Type:  "TEXT",
			Value: []byte("test"),
		})

		if err != nil {
			t.Fatal(err)
		}

		if connection.GetConnection().Changes() != 1 {
			t.Fatalf("Expected 1 change but got %d", connection.GetConnection().Changes())
		}
	})
}

func TestDatabaseConnection_Checkpoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

		_, err = connection.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Checkpoint()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDatabaseConnection_Checkpoint_WithMultipleConnections(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

		_, err = connection.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Checkpoint()

		if err != nil {
			t.Fatal(err)
		}

		wg := sync.WaitGroup{}
		rounds := 100
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range rounds {
				db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Error(err)
					continue
				}

				statement, err := db.GetConnection().Prepare(db.GetConnection().Context(), []byte("INSERT INTO test (name) VALUES (?)"))

				if err != nil {
					t.Error(err)
					continue
				}

				err = db.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
					return statement.Sqlite3Statement.Exec(nil, []sqlite3.StatementParameter{
						{
							Type:  "TEXT",
							Value: []byte("test"),
						},
					}...)
				})

				if err != nil {
					t.Error(err)
					continue
				}

				err = db.Checkpoint()

				if err != nil {
					t.Log(err)
				}

				app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for range rounds {
				db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Error(err)
					continue
				}

				statement, err := db.GetConnection().Prepare(db.GetConnection().Context(), []byte("INSERT INTO test (name) VALUES (?)"))

				if err != nil {
					t.Error(err)
					continue
				}

				// err = db.GetConnection().Query(nil, statement.Sqlite3Statement, []sqlite3.StatementParameter{
				// 	{
				// 		Type:  "TEXT",
				// 		Value: []byte("test"),
				// 	},
				// })

				err = db.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
					return statement.Sqlite3Statement.Exec(nil, []sqlite3.StatementParameter{
						{
							Type:  "TEXT",
							Value: []byte("test"),
						},
					}...)
				})

				if err != nil {
					t.Error(err)
					continue
				}

				app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)
			}
		}()

		wg.Wait()

		//  Ensure the count is correct
		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		result, err := db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

		if err != nil {
			t.Fatal(err)
		}

		if len(result.Rows) != 1 {
			t.Fatal("Expected 1 row")
		}

		if result.Rows[0][0].Int64() != int64(rounds*2) {
			t.Fatalf("Expected %d rows", rounds*2)
		}
	})
}

func TestDatabaseConnection_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		// Create a table
		_, err = connection.SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		// Insert a row
		_, err = connection.SqliteConnection().Exec(context.Background(), []byte("INSERT INTO test (name) VALUES (?)"), sqlite3.StatementParameter{
			Type:  "TEXT",
			Value: []byte("test"),
		})

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Close()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDatabaseConnection_Closed(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.Closed() {
			t.Fatal("Expected connection to be open")
		}

		err = connection.Close()

		if err != nil {
			t.Fatal(err)
		}

		if !connection.Closed() {
			t.Fatal("Expected connection to be closed")
		}
	})
}

func TestDatabaseConnection_Context(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.Context() == nil {
			t.Fatal("Expected connection to have a context")
		}
	})
}

func TestDatabaseConnection_Exec(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.Exec("INSERT INTO test (name) VALUES (?)", []sqlite3.StatementParameter{
			{
				Type:  "TEXT",
				Value: []byte("test"),
			},
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDatabaseConnection_FileSystem(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.FileSystem() == nil {
			t.Fatal("Expected connection to have a file system")
		}
	})
}

func TestDatabaseConnectionIsolationDuringCheckpoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)

		connection2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection2)

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 750 {
				_, err = connection1.GetConnection().SqliteConnection().Exec(
					context.Background(),
					[]byte("INSERT INTO test (name) VALUES (?)"),
					sqlite3.StatementParameter{
						Type:  "TEXT",
						Value: []byte("test"),
					},
				)

				if err != nil {
					t.Error(err)
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 10 {
				_, err := connection2.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

				if err != nil {
					t.Error(err)
				}
			}
		}()

		wg.Wait()

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

		if err != nil {
			t.Error(err)
		}

		_, err = connection2.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

		if err != nil {
			t.Error(err)
		}
	})
}

func TestDatabaseConnection_Id(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.Id() == "" {
			t.Fatal("Expected connection to have an ID")
		}
	})
}

func TestDatabaseConnection_Prepare(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		statement, err := connection.Prepare(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		if statement == (database.Statement{}) {
			t.Fatal("Expected statement to not be empty")
		}
	})
}

func TestDatabaseConnection_Query(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		result := sqlite3.NewResult()

		statement, err := connection.Prepare(context.Background(), []byte("INSERT INTO test (name) VALUES (?)"))

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Query(result, statement.Sqlite3Statement, []sqlite3.StatementParameter{
			{
				Type:  "TEXT",
				Value: []byte("test"),
			},
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDatabaseConnection_ResultPool(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.ResultPool() == nil {
			t.Fatal("Expected connection to have a result pool")
		}
	})
}

func TestDatabaseConnection_SqliteConnection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.SqliteConnection() == nil {
			t.Fatal("Expected connection to have a SQLite connection")
		}
	})
}

func TestDatabaseConnection_Statement(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		statement1, err := connection.Statement([]byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		if statement1 == (database.Statement{}) {
			t.Fatal("Expected statement to not be empty")
		}

		statement2, err := connection.Statement([]byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		if statement2 == (database.Statement{}) {
			t.Fatal("Expected statement to not be empty")
		}

		if statement1 != statement2 {
			t.Fatal("Expected statement to be the same")
		}
	})
}

func TestDatabaseConnection_Transaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Transaction(false, func(con *database.DatabaseConnection) error {
			_, err := con.SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

			if err != nil {
				return err
			}

			return nil
		})

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDatabaseConnection_Transaction_WithError(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Transaction(false, func(con *database.DatabaseConnection) error {
			_, err := con.SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

			if err != nil {
				return err
			}

			return fmt.Errorf("test error")
		})

		if err == nil {
			t.Fatal("Expected error but got nil")
		}
	})
}

func TestDatabaseConnection_Transaction_WithRollback(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		err = connection.Transaction(true, func(con *database.DatabaseConnection) error {
			_, err := con.SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

			if err != nil {
				return err
			}

			return errors.New("test error")
		})

		if err == nil {
			t.Fatal("Expected error but got nil")
		}

		// Check if the table was created
		_, err = connection.SqliteConnection().Exec(context.Background(), []byte("SELECT * FROM test"))

		if err == nil {
			t.Fatal("Expected error but got nil")
		}
	})
}

func TestDatabaseConnection_VFSDatabaseHash(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.VFSDatabaseHash() == "" {
			t.Fatal("Expected connection to have a VFS database hash")
		}
	})
}

func TestDatabaseConnection_VFSHash(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		if connection.VFSHash() == "" {
			t.Fatal("Expected connection to have a VFS hash")
		}
	})
}

func TestDatabaseConnection_WithAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", nil)

		connection.WithAccessKey(accessKey)

		if connection.AccessKey == nil {
			t.Fatal("Expected connection to have an access key")
		}

		if connection.AccessKey.AccessKeyId != accessKey.AccessKeyId {
			t.Fatal("Expected connection to have the same access key")
		}
	})
}

// This test is useful in ensuring the database can be properly written to and read
// from in an interleaved manner without issue.
func TestDatabaseConnectionsInterleaved(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		wg := sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 10000 {
				db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Error(err)
					break
				}

				_, err = db.GetConnection().SqliteConnection().Exec(
					context.Background(),
					[]byte("INSERT INTO test (name) VALUES (?)"),
					sqlite3.StatementParameter{
						Type:  "TEXT",
						Value: []byte("test"),
					},
				)

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

			for range 10000 {
				db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Error(err)
					break
				}

				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

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

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

		if err != nil {
			t.Error(err)
		}

		db, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Error(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

		if err != nil {
			t.Error(err)
		}

		db, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Error(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

		if err != nil {
			t.Error(err)
		}
	})
}

func TestDatabaseConnectionReadSnapshotIsolation(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, text TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

		wg := sync.WaitGroup{}
		var errors []error
		mutex := sync.Mutex{}

		recordError := func(err error) {
			mutex.Lock()
			defer mutex.Unlock()

			errors = append(errors, err)
		}

		_, err = connection.GetConnection().Exec("INSERT INTO test (text) VALUES (?)",
			[]sqlite3.StatementParameter{
				{
					Type:  "TEXT",
					Value: []byte("test"),
				},
			})

		if err != nil {
			t.Fatal(err)
		}

		// Start multiple read transactions at different points
		for i := range 3 {
			wg.Add(1)

			go func(readerID int) {
				defer wg.Done()

				conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					recordError(err)
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, conn)

				var firstCount int64

				// Start a read transaction that should maintain its snapshot
				err = conn.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
					for j := range 10 {
						result, err := con.Exec("SELECT COUNT(*) FROM test", nil)

						if err != nil {
							return err
						}

						// Each reader should see consistent results throughout its transaction
						count := result.Rows[0][0].Int64()

						if j == 0 {
							firstCount = count
						}

						if j > 0 && count != firstCount {
							return fmt.Errorf("reader %d: count changed within transaction from %d to %d", readerID, firstCount, count)
						}

						time.Sleep(5 * time.Millisecond) // Stagger reads
					}

					return nil
				})

				if err != nil {
					recordError(err)
				}
			}(i)
		}

		// Concurrent writer
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

			if err != nil {
				recordError(err)
				return
			}

			defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, conn)

			for range 10 {
				err = conn.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
					_, err := con.Exec("INSERT INTO test (text) VALUES (?)",
						[]sqlite3.StatementParameter{
							{
								Type:  "TEXT",
								Value: []byte("test"),
							},
						})

					return err
				})

				if err != nil {
					recordError(err)
					continue
				}

				time.Sleep(10 * time.Millisecond)
			}
		}()

		wg.Wait()

		// Verify final state
		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, conn)

		result, err := conn.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

		if err != nil {
			t.Fatal(err)
		}

		if count := result.Rows[0][0].Int64(); count != 11 {
			t.Errorf("expected 11 rows, got %d", count)
		}

		for _, err := range errors {
			t.Error(err)
		}
	})
}

func TestDatabaseConnectionReadSnapshotIsolationWithLargerDataSet(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatal(err)
		}

		statement, err := connection1.GetConnection().Prepare(context.Background(), []byte("INSERT INTO test (name) VALUES ('test')"))

		err = connection1.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
			for range 100000 {
				err = statement.Sqlite3Statement.Exec(nil)

				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			t.Fatal(err)
		}

		err = connection1.Checkpoint()

		if err != nil {
			t.Error(err)
		}

		app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)

		wg := sync.WaitGroup{}
		var connection1Error error
		var connection2Error error

		wg.Add(1)
		go func() {
			defer wg.Done()

			connection1, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

			if err != nil {
				connection1Error = err

				return
			}

			statement, err := connection1.GetConnection().Prepare(context.Background(), []byte("UPDATE test SET name = 'updated' WHERE id = ?"))

			err = connection1.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
				for i := 1; i <= 10000; i++ {
					err = statement.Sqlite3Statement.Exec(nil, sqlite3.StatementParameter{
						Type:  "INTEGER",
						Value: int64(i),
					})

					if err != nil {
						connection1Error = err
						break
					}
				}

				return connection1Error
			})

			err = connection1.Checkpoint()

			if err != nil {
				t.Error(err)
			}

			app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			connection2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

			if err != nil {
				connection2Error = err

				return
			}

			statement, err := connection2.GetConnection().Prepare(context.Background(), []byte("SELECT name FROM test where id = ?"))

			if err != nil {
				connection2Error = err
				return
			}

			result := sqlite3.NewResult()

			err = connection2.GetConnection().Transaction(true, func(con *database.DatabaseConnection) error {
				for i := 1; i <= 10000; i++ {
					err = statement.Sqlite3Statement.Exec(result, sqlite3.StatementParameter{
						Type:  "INTEGER",
						Value: int64(i),
					})

					if err != nil {
						return err
					}

					if len(result.Rows) != 1 {
						t.Error("Expected 1 row")
					}

					if string(result.Rows[0][0].Text()) != "test" {
						return fmt.Errorf("Expected %s, got %s", "test", result.Rows[0][0].Text())
					}
				}

				return nil
			})

			if err != nil {
				connection2Error = err

				return
			}

			app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection2)
		}()

		wg.Wait()

		if connection1Error != nil {
			t.Fatal(connection1Error)
		}

		if connection2Error != nil {
			t.Fatal(connection2Error)
		}
	})
}

func TestDatabaseConnectionReadSnapshotIsolationWhileWriting(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		err = connection1.Checkpoint()

		if err != nil {
			t.Error(err)
		}

		app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)

		var wg sync.WaitGroup
		var insertError error
		var selectError error
		var insertingName = make(chan struct{}, 1)
		var readingName = make(chan struct{}, 1)

		insertName := func() error {
			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

			if err != nil {
				return err
			}

			statement, err := connection.GetConnection().Prepare(context.Background(), []byte("INSERT INTO test (name) VALUES ('test')"))

			if err != nil {
				log.Println(err)
				return err
			}

			insertingName <- struct{}{}

			<-readingName

			// Checkpoint
			err = connection.Checkpoint()

			if err != nil {
				log.Println(err)
				return err
			}

			// Insert 1 row
			err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
				err = statement.Sqlite3Statement.Exec(nil)

				if err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				log.Println(err)
				return err
			}

			app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

			return nil
		}

		// Insert the rows and checkpoint after each insert
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 50 {
				err := insertName()

				if err != nil {
					insertError = err
					log.Println(err)
				}
			}

			close(insertingName)
		}()

		var namesInserted = 0

		// Each time a name is inserted, start a new read transaction
		for range insertingName {
			wg.Add(1)

			go func(namesInserted int) {
				defer wg.Done()

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					selectError = err
					log.Println(err)
					return
				}

				statement, err := connection.GetConnection().Prepare(context.Background(), []byte("SELECT COUNT(*) as count FROM test"))

				if err != nil {
					selectError = err
					log.Println(err)
					return
				}

				result := sqlite3.NewResult()

				// Start a new read transaction
				err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
					readingName <- struct{}{}

					err = statement.Sqlite3Statement.Exec(result)

					if err != nil {
						log.Println(err)
						return err
					}

					if len(result.Rows) != 1 {
						return fmt.Errorf("Expected 1 row, got %d", len(result.Rows))
					}

					// Read the expected number of rows
					if result.Rows[0][0].Int64() != int64(namesInserted) {
						return fmt.Errorf("Expected %d, got %d", namesInserted, result.Rows[0][0].Int64())
					}

					app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

					return nil
				})

				if err != nil {
					selectError = err
					log.Println(err)
				}
			}(namesInserted)

			namesInserted++
		}

		// Wait for all inserts to complete
		wg.Wait()

		if insertError != nil {
			t.Fatal(insertError)
		}

		if selectError != nil {
			t.Fatal(selectError)
		}
	})
}

func TestDatabaseConnectionReadSnapshotIsolationOnReplicaServer(t *testing.T) {
	t.Skip("Test is hanging, needs investigation")
	test.Run(t, func() {
		primaryServer := test.NewTestServer(t)
		replicaServer := test.NewTestServer(t)

		if !primaryServer.App.Cluster.Node().IsPrimary() {
			t.Fatal("Primary server is not primary")
		}

		if !replicaServer.App.Cluster.Node().IsReplica() {
			t.Fatal("Replica server is not replica")
		}

		mock := test.MockDatabase(primaryServer.App)

		// Create a database table
		connection1, err := primaryServer.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatal(err)
		}

		_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		// err = connection1.Checkpoint()

		// if err != nil {
		// 	t.Error(err)
		// }

		primaryServer.App.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)

		var wg sync.WaitGroup
		var insertError error
		var selectError error
		var insertingName = make(chan struct{}, 1)
		var readingName = make(chan struct{}, 1)

		insertName := func() error {
			connection, err := primaryServer.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

			if err != nil {
				return err
			}

			statement, err := connection.GetConnection().Prepare(context.Background(), []byte("INSERT INTO test (name) VALUES ('test')"))

			if err != nil {
				log.Println(err)
				return err
			}

			insertingName <- struct{}{}

			<-readingName

			// Checkpoint
			// err = connection.Checkpoint()

			// if err != nil {
			// 	log.Println(err)
			// 	return err
			// }

			// Insert 1 row
			err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
				err = statement.Sqlite3Statement.Exec(nil)

				if err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				log.Println(err)
				return err
			}

			primaryServer.App.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

			return nil
		}

		// Insert the rows and checkpoint after each insert
		wg.Add(1)
		go func() {
			defer wg.Done()

			for range 50 {
				err := insertName()

				if err != nil {
					insertError = err
					log.Println(err)
				}
			}
		}()

		var namesInserted = 0

		// Before each time a name is inserted, start a new read transaction to
		// ensure the read transaction is started before the write transaction.
		// This is to ensure the read transaction is able to see the data with
		// a consistent snapshot.
		wg.Add(1)

		go func() {
			defer wg.Done()

			connection, err := replicaServer.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

			if err != nil {
				selectError = err
				log.Println(err)
				return
			}

			defer replicaServer.App.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

			statement, err := connection.GetConnection().Prepare(context.Background(), []byte("SELECT COUNT(*) as count FROM test"))

			if err != nil {
				selectError = err
				log.Println(err)
				return
			}

			result := sqlite3.NewResult()

			// Start a new read transaction
			err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
				for range insertingName {
					readingName <- struct{}{}

					err = statement.Sqlite3Statement.Exec(result)

					if err != nil {
						log.Println(err)
						return err
					}

					if len(result.Rows) != 1 {
						return fmt.Errorf("Expected 1 row, got %d", len(result.Rows))
					}

					// Read the expected number of rows
					// if result.Rows[0][0].Int64() != int64(namesInserted) {
					// 	return fmt.Errorf("Expected %d, got %d", namesInserted, result.Rows[0][0].Int64())
					// }

					namesInserted++
				}
				return nil
			})

			if err != nil {
				selectError = err
				log.Println(err)
				// close(readingName)
				// close(insertingName)
			}
		}()

		// Wait for all inserts to complete
		wg.Wait()

		if insertError != nil {
			t.Fatal(insertError)
		}

		if selectError != nil {
			t.Fatal(selectError)
		}
	})
}

// func TestDatabaseConnectionReadSnapshotIsolationOnReplicaServer(t *testing.T) {
// 	test.Run(t, func() {
// 		primaryServer := test.NewTestServer(t)
// 		replicaServer := test.NewTestServer(t)

// 		if !primaryServer.App.Cluster.Node().IsPrimary() {
// 			t.Fatal("Primary server is not primary")
// 		}

// 		if !replicaServer.App.Cluster.Node().IsReplica() {
// 			t.Fatal("Replica server is not replica")
// 		}

// 		mock := test.MockDatabase(primaryServer.App)

// 		// Create a database table
// 		connection1, err := primaryServer.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		err = connection1.Checkpoint()

// 		if err != nil {
// 			t.Error(err)
// 		}

// 		primaryServer.App.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection1)

// 		var wg sync.WaitGroup
// 		var insertError error
// 		var selectError error
// 		var insertingName = make(chan struct{}, 1)
// 		var readingName = make(chan struct{}, 1)

// 		insertName := func() error {
// 			connection, err := primaryServer.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

// 			if err != nil {
// 				return err
// 			}

// 			statement, err := connection.GetConnection().Prepare(context.Background(), []byte("INSERT INTO test (name) VALUES ('test')"))

// 			if err != nil {
// 				log.Println(err)
// 				return err
// 			}

// 			insertingName <- struct{}{}

// 			<-readingName

// 			// Checkpoint
// 			err = connection.Checkpoint()

// 			if err != nil {
// 				log.Println(err)
// 				return err
// 			}

// 			// Insert 1 row
// 			err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
// 				err = statement.Sqlite3Statement.Exec(nil)

// 				if err != nil {
// 					return err
// 				}

// 				return nil
// 			})

// 			if err != nil {
// 				log.Println(err)
// 				return err
// 			}

// 			primaryServer.App.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

// 			return nil
// 		}

// 		// Insert the rows and checkpoint after each insert
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()

// 			for range 50 {
// 				err := insertName()

// 				if err != nil {
// 					insertError = err
// 					log.Println(err)
// 				}
// 			}

// 			close(insertingName)
// 		}()

// 		var namesInserted = 0

// 		// Before each time a name is inserted, start a new read transaction to
// 		// ensure the read transaction is started before the write transaction.
// 		// This is to ensure the read transaction is able to see the data with
// 		// a consistent snapshot.
// 		for range insertingName {
// 			wg.Add(1)

// 			go func(namesInserted int) {
// 				defer wg.Done()

// 				connection, err := replicaServer.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

// 				if err != nil {
// 					selectError = err
// 					log.Println(err)
// 					return
// 				}

// 				defer replicaServer.App.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

// 				statement, err := connection.GetConnection().Prepare(context.Background(), []byte("SELECT COUNT(*) as count FROM test"))

// 				if err != nil {
// 					selectError = err
// 					log.Println(err)
// 					return
// 				}

// 				result := sqlite3.NewResult()

// 				// Start a new read transaction
// 				err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
// 					readingName <- struct{}{}

// 					err = statement.Sqlite3Statement.Exec(result)

// 					if err != nil {
// 						log.Println(err)
// 						return err
// 					}

// 					if len(result.Rows) != 1 {
// 						return fmt.Errorf("Expected 1 row, got %d", len(result.Rows))
// 					}

// 					// Read the expected number of rows
// 					if result.Rows[0][0].Int64() != int64(namesInserted) {
// 						return fmt.Errorf("Expected %d, got %d", namesInserted, result.Rows[0][0].Int64())
// 					}

// 					return nil
// 				})

// 				if err != nil {
// 					selectError = err
// 					log.Println(err)
// 				}
// 			}(namesInserted)

// 			namesInserted++
// 		}

// 		// Wait for all inserts to complete
// 		wg.Wait()

// 		if insertError != nil {
// 			t.Fatal(insertError)
// 		}

// 		if selectError != nil {
// 			t.Fatal(selectError)
// 		}
// 	})
// }
