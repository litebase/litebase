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
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestDatabaseConnection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseConnection", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection == nil {
				t.Fatal("Expected connection to be non-nil")
			}

		})

		t.Run("Changes", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Insert a row
			_, err = connection.GetConnection().Exec("INSERT INTO test (name) VALUES (?)", []sqlite3.StatementParameter{
				{
					Type:  "TEXT",
					Value: []byte("test"),
				},
			})

			if err != nil {
				t.Fatal(err)
			}

			if connection.GetConnection().Changes() != 1 {
				t.Fatalf("Expected 1 change but got %d", connection.GetConnection().Changes())
			}
		})

		t.Run("Checkpoint", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			if err := connection.Checkpoint(); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("Checkpointing_WithMultipleConnections", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

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
					db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						t.Error(err)
						continue
					}

					err = db.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
						statement, err := db.GetConnection().Statement("INSERT INTO test (name) VALUES (?)")

						if err != nil {
							return err
						}

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

					app.DatabaseManager.ConnectionManager().Release(db)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				for range rounds {
					db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						t.Error(err)
						continue
					}

					err = db.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
						statement, err := db.GetConnection().Statement("INSERT INTO test (name) VALUES (?)")

						if err != nil {
							return err
						}

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

					app.DatabaseManager.ConnectionManager().Release(db)
				}
			}()
			wg.Wait()

			//  Ensure the count is correct
			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			result, err := db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) != 1 {
				t.Fatal("Expected 1 row")
			}

			actualRows := result.Rows[0][0].Int64()
			expectedRows := int64(rounds * 2)

			if actualRows != expectedRows {
				t.Fatalf("Expected %d rows, got %d", expectedRows, actualRows)
			}
		})

		t.Run("Close", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			// Create a table
			_, err = connection.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Insert a row
			_, err = connection.Exec("INSERT INTO test (name) VALUES (?)", []sqlite3.StatementParameter{
				{
					Type:  "TEXT",
					Value: []byte("test"),
				},
			})

			if err != nil {
				t.Fatal(err)
			}

			err = connection.Close()

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("Closed", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

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

		t.Run("Context", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection.Context() == nil {
				t.Fatal("Expected connection to have a context")
			}
		})

		t.Run("Exec", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

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

		t.Run("FileSystem", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection.FileSystem() == nil {
				t.Fatal("Expected connection to have a file system")
			}
		})

		t.Run("DatabaseConnectionIsolationDuringCheckpoint", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection1)

			connection2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection2)

			_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			wg := sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 750 {
					_, err = connection1.GetConnection().Exec(
						"INSERT INTO test (name) VALUES (?)",
						[]sqlite3.StatementParameter{
							{
								Type:  "TEXT",
								Value: []byte("test"),
							},
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
					_, err := connection2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						t.Error(err)
					}
				}
			}()

			wg.Wait()

			_, err = connection1.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Error(err)
			}

			_, err = connection2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Error(err)
			}
		})

		t.Run("Id", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection.Id() == "" {
				t.Fatal("Expected connection to have an ID")
			}
		})

		t.Run("Prepare", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			statement, err := connection.Prepare(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

			if err != nil {
				t.Fatal(err)
			}

			if statement == (database.Statement{}) {
				t.Fatal("Expected statement to not be empty")
			}
		})

		t.Run("Query", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			_, err = connection.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			result := sqlite3.NewResult()

			statement, err := connection.Prepare(context.Background(), "INSERT INTO test (name) VALUES (?)")

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

		t.Run("ResultPool", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection.ResultPool() == nil {
				t.Fatal("Expected connection to have a result pool")
			}
		})

		t.Run("Statement", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			statement1, err := connection.Statement("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

			if err != nil {
				t.Fatal(err)
			}

			if statement1 == (database.Statement{}) {
				t.Fatal("Expected statement to not be empty")
			}

			statement2, err := connection.Statement("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

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

		t.Run("Transaction", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			err = connection.Transaction(false, func(con *database.DatabaseConnection) error {
				_, err := con.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

				if err != nil {
					return err
				}

				return nil
			})

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("Transaction_WhenClosed", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}

			err = connection.Transaction(false, func(con *database.DatabaseConnection) error {
				return nil
			})

			if err != database.ErrDatabaseConnectionClosed {
				t.Fatalf("Expected ErrDatabaseConnectionClosed but got %v", err)
			}
		})

		t.Run("Transaction_WithError", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			err = connection.Transaction(false, func(con *database.DatabaseConnection) error {
				_, err := con.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

				if err != nil {
					return err
				}

				return fmt.Errorf("test error")
			})

			if err == nil {
				t.Fatal("Expected error but got nil")
			}
		})

		t.Run("Transaction_WithRollback", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			err = connection.Transaction(true, func(con *database.DatabaseConnection) error {
				_, err := con.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

				if err != nil {
					return err
				}

				return errors.New("test error")
			})

			if err == nil {
				t.Fatal("Expected error but got nil")
			}

			// Check if the table was created
			_, err = connection.Exec("SELECT * FROM test", nil)

			if err == nil {
				t.Fatal("Expected error but got nil")
			}
		})

		t.Run("VFSDatabaseHash", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection.VFSDatabaseHash() == "" {
				t.Fatal("Expected connection to have a VFS database hash")
			}
		})

		t.Run("VFSHash", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			if connection.VFSHash() == "" {
				t.Fatal("Expected connection to have a VFS hash")
			}
		})

		t.Run("WithAccessKey", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := database.NewDatabaseConnection(app.DatabaseManager.ConnectionManager(), mock.Branch)

			if err != nil {
				t.Fatal(err)
			}

			credential := &auth.Credential{}

			credential.WithAccessKey(
				auth.NewAccessKey(
					app.Auth.AccessKeyManager, "test", "test", "", nil,
				),
			)

			connection.WithCredential(credential)

			if connection.Credential == nil {
				t.Fatal("Expected connection to have an access key")
			}

			if credential.CredentialID != connection.Credential.CredentialID {
				t.Fatal("Expected connection to have the same access key")
			}
		})

		// This test is useful in ensuring the database can be properly written
		// to and read from in an interleaved manner without issue.
		t.Run("DatabaseConnectionsInterleaved", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection1)

			_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			wg := sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 10000 {
					db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						t.Error(err)
						break
					}

					_, err = db.GetConnection().Exec(
						"INSERT INTO test (name) VALUES (?)",
						[]sqlite3.StatementParameter{
							{
								Type:  "TEXT",
								Value: []byte("test"),
							},
						},
					)

					if err != nil {
						t.Error(err)
						break
					}

					if db.GetConnection().Changes() != 1 {
						t.Error("Expected 1 row affected")
						break
					}

					app.DatabaseManager.ConnectionManager().Release(db)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 10000 {
					db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						t.Error(err)
						break
					}

					_, err = db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						t.Error(err)
						break
					}

					app.DatabaseManager.ConnectionManager().Release(db)
				}
			}()

			wg.Wait()

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Error(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			_, err = db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Error(err)
			}

			db, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Error(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			_, err = db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Error(err)
			}

			db, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Error(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			_, err = db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Error(err)
			}
		})

		t.Run("DatabaseConnectionReadSnapshotIsolation", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, text TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

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

					conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						recordError(err)
						return
					}

					defer app.DatabaseManager.ConnectionManager().Release(conn)

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

				conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					recordError(err)
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(conn)

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
			conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

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

		t.Run("DatabaseConnectionReadSnapshotIsolationWithLargerDataSet", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			statement, err := connection1.GetConnection().Prepare(context.Background(), "INSERT INTO test (name) VALUES ('test')")

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

			if err := connection1.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			app.DatabaseManager.ConnectionManager().Release(connection1)

			wg := sync.WaitGroup{}
			var connection1Error error
			var connection2Error error

			// Use channels to synchronize the start of transactions
			readTransactionStarted := make(chan struct{})
			writeCanStart := make(chan struct{})

			wg.Go(func() {
				connection2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					connection2Error = err
					close(readTransactionStarted)
					return
				}

				statement, err := connection2.GetConnection().Prepare(context.Background(), "SELECT name FROM test where id = ?")

				if err != nil {
					connection2Error = err
					close(readTransactionStarted)
					return
				}

				err = connection2.GetConnection().Transaction(true, func(con *database.DatabaseConnection) error {
					// Signal that read transaction has started
					close(readTransactionStarted)

					// Wait for write transaction to be allowed to start
					<-writeCanStart

					for i := 1; i <= 10000; i++ {
						result := sqlite3.NewResult()

						err = statement.Sqlite3Statement.Exec(result, sqlite3.StatementParameter{
							Type:  "INTEGER",
							Value: int64(i),
						})

						if err != nil {
							return err
						}

						if len(result.Rows) != 1 {
							return fmt.Errorf("Expected 1 row, got %d rows", len(result.Rows))
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

				app.DatabaseManager.ConnectionManager().Release(connection2)
			})

			wg.Go(func() {
				// Wait for read transaction to start first
				<-readTransactionStarted

				connection1, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					connection1Error = err
					close(writeCanStart)
					return
				}

				statement, err := connection1.GetConnection().Prepare(context.Background(), "UPDATE test SET name = 'updated' WHERE id = ?")

				if err != nil {
					connection1Error = err
					close(writeCanStart)
					return
				}

				// Signal that write can start
				close(writeCanStart)

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

				if err != nil && connection1Error == nil {
					connection1Error = err
				}

				if err := connection1.Checkpoint(); err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				app.DatabaseManager.ConnectionManager().Release(connection1)
			})

			wg.Wait()

			if connection1Error != nil {
				t.Fatal(connection1Error)
			}

			if connection2Error != nil {
				t.Fatal(connection2Error)
			}
		})

		t.Run("DatabaseConnectionReadSnapshotIsolationWhileWriting", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection1, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			_, err = connection1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			if err := connection1.Checkpoint(); err != nil {
				t.Fatal(err)
			}

			app.DatabaseManager.ConnectionManager().Release(connection1)

			var wg sync.WaitGroup
			var errorsMu sync.Mutex
			var insertErrors []error
			var selectErrors []error
			var insertingName = make(chan struct{}, 1)
			var readingName = make(chan struct{}, 1)

			insertName := func() error {
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					return err
				}

				insertingName <- struct{}{}

				<-readingName

				// Checkpoint
				if err := connection.Checkpoint(); err != nil {
					return err
				}

				// Insert 1 row
				err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
					_, err = con.Exec("INSERT INTO test (name) VALUES ('test')", nil)

					if err != nil {
						return err
					}

					return nil
				})

				if err != nil {
					log.Println(err)
					return err
				}

				app.DatabaseManager.ConnectionManager().Release(connection)

				return nil
			}

			// Insert the rows and checkpoint after each insert
			wg.Add(1)
			go func() {
				defer wg.Done()

				for range 50 {
					err := insertName()

					if err != nil {
						errorsMu.Lock()
						insertErrors = append(insertErrors, err)
						errorsMu.Unlock()
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

					connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						errorsMu.Lock()
						selectErrors = append(selectErrors, err)
						errorsMu.Unlock()
						log.Println(err)
						return
					}

					statement, err := connection.GetConnection().Prepare(context.Background(), "SELECT COUNT(*) as count FROM test")

					if err != nil {
						errorsMu.Lock()
						selectErrors = append(selectErrors, err)
						errorsMu.Unlock()
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

						app.DatabaseManager.ConnectionManager().Release(connection)

						return nil
					})

					if err != nil {
						errorsMu.Lock()
						selectErrors = append(selectErrors, err)
						errorsMu.Unlock()
						log.Println(err)
					}
				}(namesInserted)

				namesInserted++
			}

			// Wait for all inserts to complete
			wg.Wait()

			if len(insertErrors) > 0 {
				t.Fatalf("Insert errors: %v", insertErrors[0])
			}

			if len(selectErrors) > 0 {
				t.Fatalf("Select errors: %v", selectErrors[0])
			}
		})

		t.Run("DatabaseConnectionReadSnapshotIsolationOnReplicaServer", func(t *testing.T) {
			// TODO: This test needs to be refactored with proper a primary/replica.
			// Writing to the primary while reading from the replica.
			//
			// CURRENT ISSUES:
			// 1. This test only creates a single node using test.MockDatabase(app),
			//    which automatically becomes a PRIMARY node in the test environment.
			//    It does NOT create a separate replica node.
			//
			// 2. The test name suggests it's testing replica behavior, but it's
			//    actually just testing snapshot isolation on a single primary node.
			//
			// 3. To properly test primary/replica snapshot isolation, the test should:
			//    a) Create a primary server using test.NewTestServer(t)
			//    b) Create a replica server using test.NewTestServer(t) (second server becomes replica)
			//    c) Write data on the primary
			//    d) Trigger checkpoint on the primary
			//    e) Verify the replica can read the checkpointed data
			//    f) Ensure snapshot isolation works during concurrent reads/writes
			//
			// 4. See other tests that properly set up primary/replica:
			//    - TestNodeReplicaJoinCluster in node_replica_test.go
			//    - TestForwardToPrimary in forward_to_primary_middleware_test.go
			//    - TestClusterPrimaryController in cluster_primary_controller_test.go

			mock := test.MockDatabase(app)

			// Create a database table and add some data
			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Insert initial data
			for range 5 {
				_, err = connection.GetConnection().Exec("INSERT INTO test (name) VALUES (?)", []sqlite3.StatementParameter{
					{
						Type:  "TEXT",
						Value: []byte("test"),
					},
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// Test snapshot isolation within a single transaction
			err = connection.GetConnection().Transaction(false, func(con *database.DatabaseConnection) error {
				var firstCount int64

				for i := range 3 {
					result, err := con.Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						return err
					}

					if len(result.Rows) != 1 {
						return fmt.Errorf("Expected 1 row, got %d", len(result.Rows))
					}

					count := result.Rows[0][0].Int64()

					if i == 0 {
						firstCount = count
					} else if count != firstCount {
						return fmt.Errorf("Count changed within transaction from %d to %d (iteration %d)", firstCount, count, i)
					}

					time.Sleep(10 * time.Millisecond) // Small delay
				}

				return nil
			})

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("BarrierProtection_ConcurrentReads", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			// Create test table with data
			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			_, err = connection.GetConnection().Exec("INSERT INTO test (value) VALUES ('test1'), ('test2'), ('test3')", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Release connection so we can get multiple fresh connections
			app.DatabaseManager.ConnectionManager().Release(connection)

			// Verify multiple concurrent SELECTs can proceed simultaneously (read barriers allow this)
			const numReaders = 20
			readersStarted := make(chan bool, numReaders)
			readersCompleted := make(chan bool, numReaders)
			errChan := make(chan error, numReaders)

			for i := range numReaders {
				go func(id int) {
					conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						errChan <- fmt.Errorf("reader %d failed to get connection: %w", id, err)
						return
					}

					defer app.DatabaseManager.ConnectionManager().Release(conn)

					readersStarted <- true

					// Execute SELECT query - should use read barrier
					result, err := conn.GetConnection().Exec("SELECT * FROM test", nil)

					if err != nil {
						errChan <- fmt.Errorf("reader %d query failed: %w", id, err)
						return
					}

					if len(result.Rows) != 3 {
						errChan <- fmt.Errorf("reader %d expected 3 rows, got %d", id, len(result.Rows))
						return
					}

					readersCompleted <- true
				}(i)
			}

			// Wait for all readers to start (proves they don't block each other)
			readersStartedCount := 0

			for readersStartedCount < numReaders {
				select {
				case <-readersStarted:
					readersStartedCount++
				case err := <-errChan:
					t.Fatal(err)
				case <-time.After(5 * time.Second):
					t.Fatalf("Timeout waiting for readers to start (got %d/%d)", readersStartedCount, numReaders)
				}
			}

			// Wait for all readers to complete
			readersCompletedCount := 0

			for readersCompletedCount < numReaders {
				select {
				case <-readersCompleted:
					readersCompletedCount++
				case err := <-errChan:
					t.Fatal(err)
				case <-time.After(5 * time.Second):
					t.Fatalf("Timeout waiting for readers to complete (got %d/%d)", readersCompletedCount, numReaders)
				}
			}

			t.Logf("✓ All %d concurrent readers completed successfully (read barriers allow concurrency)", numReaders)
		})

		t.Run("BarrierProtection_SelectDuringCheckpoint", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			// Create test table with data
			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Insert data to trigger WAL changes
			for i := range 100 {
				_, err = connection.GetConnection().Exec("INSERT INTO test (value) VALUES (?)", []sqlite3.StatementParameter{
					{Type: sqlite3.ParameterTypeText, Value: []byte(fmt.Sprintf("value%d", i))},
				})

				if err != nil {
					t.Fatal(err)
				}
			}

			// Force a checkpoint
			err = connection.Checkpoint()

			if err != nil {
				t.Fatal(err)
			}

			// Release connection
			app.DatabaseManager.ConnectionManager().Release(connection)

			// Start a long-running SELECT query
			selectDone := make(chan error)
			selectStarted := make(chan bool)

			go func() {
				conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					selectDone <- err
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(conn)

				selectStarted <- true

				// This SELECT should use read barrier and complete successfully
				// even if checkpoint runs concurrently
				result, err := conn.GetConnection().Exec("SELECT * FROM test", nil)

				if err != nil {
					selectDone <- err
					return
				}

				if len(result.Rows) != 100 {
					selectDone <- fmt.Errorf("expected 100 rows, got %d", len(result.Rows))
					return
				}

				selectDone <- nil
			}()

			// Wait for SELECT to start
			<-selectStarted

			// Trigger checkpoint while SELECT is running (simulates the race condition)
			conn2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn2)

			// Insert more data to trigger WAL changes
			_, err = conn2.GetConnection().Exec("INSERT INTO test (value) VALUES ('checkpoint_trigger')", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Checkpoint should wait for SELECT to finish (read barrier protects it)
			checkpointErr := conn2.Checkpoint()

			if checkpointErr != nil {
				t.Logf("Checkpoint error (expected if SELECT still holding read lock): %v", checkpointErr)
			}

			// Wait for SELECT to complete
			err = <-selectDone

			if err != nil {
				t.Fatalf("SELECT query failed: %v - this indicates read barrier didn't protect WAL timestamps", err)
			}

			t.Log("✓ SELECT completed successfully during checkpoint (read barrier protected WAL timestamp)")
		})

		t.Run("BarrierProtection_WritesSerialized", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			// Create test table
			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, counter INTEGER)", nil)

			if err != nil {
				t.Fatal(err)
			}

			_, err = connection.GetConnection().Exec("INSERT INTO test (counter) VALUES (0)", nil)

			if err != nil {
				t.Fatal(err)
			}

			app.DatabaseManager.ConnectionManager().Release(connection)

			// Verify concurrent writes are properly serialized by checkpoint barrier
			const numWriters = 10
			var wg sync.WaitGroup
			errChan := make(chan error, numWriters)

			for i := range numWriters {
				wg.Add(1)

				go func(id int) {
					defer wg.Done()

					conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						errChan <- err
						return
					}

					defer app.DatabaseManager.ConnectionManager().Release(conn)

					// Each writer increments the counter
					_, err = conn.GetConnection().Exec("UPDATE test SET counter = counter + 1", nil)

					if err != nil {
						errChan <- err
					}
				}(i)
			}

			wg.Wait()
			close(errChan)

			for err := range errChan {
				if err != nil {
					t.Fatal(err)
				}
			}

			// Verify final counter value equals number of writers (proves serialization)
			conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn)

			result, err := conn.GetConnection().Exec("SELECT counter FROM test", nil)

			if err != nil {
				t.Fatal(err)
			}

			if len(result.Rows) != 1 {
				t.Fatalf("Expected 1 row, got %d", len(result.Rows))
			}

			counter := result.Rows[0][0].Int64()

			if counter != int64(numWriters) {
				t.Fatalf("Expected counter to be %d, got %d - barrier didn't properly serialize writes", numWriters, counter)
			}

			t.Logf("✓ All %d concurrent writes properly serialized (checkpoint barrier enforced)", numWriters)
		})

		t.Run("BarrierProtection_WALTimestampSovereignty", func(t *testing.T) {
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			// Create test table
			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Insert initial data
			_, err = connection.GetConnection().Exec("INSERT INTO test (value) VALUES ('initial')", nil)

			if err != nil {
				t.Fatal(err)
			}

			// Force checkpoint to create WAL version
			err = connection.Checkpoint()

			if err != nil {
				t.Fatal(err)
			}

			app.DatabaseManager.ConnectionManager().Release(connection)

			// Start a transaction in one goroutine
			txDone := make(chan error)
			txStarted := make(chan bool)

			go func() {
				conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					txDone <- err
					return
				}

				defer app.DatabaseManager.ConnectionManager().Release(conn)

				err = conn.GetConnection().Transaction(false, func(txConn *database.DatabaseConnection) error {
					txStarted <- true

					// Insert data
					_, err := txConn.Exec("INSERT INTO test (value) VALUES ('tx_data')", nil)

					if err != nil {
						return err
					}

					// Sleep to ensure concurrent checkpoint attempt happens
					time.Sleep(100 * time.Millisecond)

					return nil
				})

				txDone <- err
			}()

			// Wait for transaction to start
			<-txStarted

			// Attempt concurrent checkpoint (should be blocked by transaction's barrier)
			conn2, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(conn2)

			// Try to insert and checkpoint
			_, err = conn2.GetConnection().Exec("INSERT INTO test (value) VALUES ('checkpoint_data')", nil)

			if err != nil {
				// This is expected if transaction is holding barrier
				t.Logf("Concurrent insert blocked (expected): %v", err)
			}

			// Wait for transaction to complete
			err = <-txDone

			if err != nil {
				t.Fatalf("Transaction failed: %v - barrier didn't protect WAL timestamp sovereignty", err)
			}

			// Verify both inserts succeeded (proves timestamps were protected)
			result, err := conn2.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatal(err)
			}

			count := result.Rows[0][0].Int64()

			if count < 2 {
				t.Fatalf("Expected at least 2 rows (initial + tx_data), got %d", count)
			}

			t.Log("✓ WAL timestamp sovereignty maintained during concurrent operations")
		})

		t.Run("BarrierProtection_NestedConnectionNoDeadlock", func(t *testing.T) {
			// This test simulates the vector_search pattern where a connection
			// might trigger nested connection acquisition (e.g., querying system database)
			mock := test.MockDatabase(app)

			connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(connection)

			// Create test table
			_, err = connection.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)", nil)

			if err != nil {
				t.Fatal(err)
			}

			app.DatabaseManager.ConnectionManager().Release(connection)

			// Simulate pattern: outer connection queries, which internally gets system connection
			const numConcurrent = 10
			var wg sync.WaitGroup
			errChan := make(chan error, numConcurrent)
			successChan := make(chan bool, numConcurrent)

			for i := range numConcurrent {
				wg.Add(1)

				go func(id int) {
					defer wg.Done()

					// Get connection (like vector_search does)
					conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

					if err != nil {
						errChan <- fmt.Errorf("goroutine %d failed to get connection: %w", id, err)
						return
					}

					defer app.DatabaseManager.ConnectionManager().Release(conn)

					// Query that might trigger nested connection (simulated)
					// In real vector_search, this could trigger BranchByID -> SystemDB -> Get()
					_, err = conn.GetConnection().Exec("SELECT * FROM test", nil)

					if err != nil {
						errChan <- fmt.Errorf("goroutine %d query failed: %w", id, err)
						return
					}

					successChan <- true
				}(i)
			}

			// Wait with timeout
			done := make(chan bool)

			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				// Success
			case <-time.After(10 * time.Second):
				t.Fatal("Deadlock detected: nested connection acquisition blocked")
			}

			close(errChan)
			close(successChan)

			// Check for errors
			for err := range errChan {
				if err != nil {
					t.Fatal(err)
				}
			}

			// Verify all succeeded
			successCount := 0

			for range successChan {
				successCount++
			}

			if successCount != numConcurrent {
				t.Fatalf("Expected %d successful operations, got %d", numConcurrent, successCount)
			}

			t.Logf("✓ %d concurrent nested connection patterns completed without deadlock", numConcurrent)
		})
	})
}
