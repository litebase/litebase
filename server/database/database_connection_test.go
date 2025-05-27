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

func TestDatabaseConnection_AuthorizerDatabasePrivilege_AlterTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeAlterTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "ALTER TABLE test ADD COLUMN age INTEGER", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"*"}},
				},
			},
			{
				name: auth.DatabasePrivilegeAlterTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "ALTER TABLE test ADD COLUMN age INTEGER", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{
						Resource: "*",
						Actions:  []string{"ALTER_TABLE", "CREATE_TABLE", "FUNCTION", "READ", "UPDATE"},
					},
				},
			},
			{
				name: auth.DatabasePrivilegeAlterTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "ALTER TABLE test ADD COLUMN age INTEGER", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TABLE", "FUNCTION", "READ", "UPDATE"}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Analyze(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeAnalyze,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "ANALYZE test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TABLE", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeAnalyze,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "ANALYZE test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"ANALYZE", "CREATE_TABLE", "READ", "UPDATE"}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Attach(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeAttach,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "ATTACH DATABASE 'test.db' AS test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX test_index ON test (name)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TABLE", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX test_index ON test (name)", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_INDEX",
						"READ",
						"REINDEX",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TABLE", "READ", "UPDATE"}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateTempIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX temp.test_index ON test (name)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX temp.test_index ON test (name)", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_INDEX",
						"INSERT",
						"READ",
						"REINDEX",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateTempTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateTempTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN INSERT INTO test (name) VALUES ('test'); END", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN INSERT INTO test (name) VALUES ('test'); END", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_TRIGGER",
						"INSERT",
						"READ",
						"REINDEX",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateTempView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TEMP VIEW test_view AS SELECT * FROM test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TEMP VIEW test_view AS SELECT * FROM test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_VIEW",
						"INSERT",
						"READ",
						"REINDEX",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN INSERT INTO test (name) VALUES ('test'); END", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_TRIGGER",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE VIEW test_view AS SELECT * FROM test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE VIEW test_view AS SELECT * FROM test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_VIEW",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_CreateVTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateVTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)", ExpectError: false},
					{SQL: "CREATE VIRTUAL TABLE test_vtable USING fts5 (name)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateVTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE VIRTUAL TABLE test_vtable USING fts5 (name)", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_VTABLE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Delete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			name        auth.DatabasePrivilege
			commands    []test.TestDatabaseAuthorizationCommand
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDelete,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "DELETE FROM test WHERE name = 'test'", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
			{
				name: auth.DatabasePrivilegeDelete,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "DELETE FROM test WHERE name = 'test'", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"DELETE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))
					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Detach(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDetach,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "DETACH DATABASE 'test.db'", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)

				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)

				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)

				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "DELETE FROM test WHERE name = 'test'", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
			{
				name: auth.DatabasePrivilegeDropIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "DELETE FROM test WHERE name = 'test'", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"DELETE",
						"INSERT",
						"READ",
						"UPDATE",
					},
					},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))
					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "DROP TABLE test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "DROP TABLE test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"DELETE",
						"DROP_TABLE",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))
					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropTempIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX test_index ON  test (name)", ExpectError: false},
					{SQL: "DROP INDEX test_index", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_INDEX",
						"INSERT",
						"READ",
						"REINDEX",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropTempIndex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX test_index ON test (name)", ExpectError: false},
					{SQL: "DROP INDEX test_index", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_INDEX",
						"DELETE",
						"DROP_TEMP_INDEX",
						"INSERT",
						"READ",
						"REINDEX",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))
					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropTempTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "DROP TABLE test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropTempTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "DROP TABLE test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"DELETE",
						"DROP_TEMP_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))
					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropTempTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TEMP TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END", ExpectError: false},
					{SQL: "DROP TRIGGER test_trigger", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_TRIGGER",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropTempTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TEMP TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END", ExpectError: false},
					{SQL: "DROP TRIGGER test_trigger", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_TRIGGER",
						"DELETE",
						"DROP_TEMP_TRIGGER",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropTempView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TEMP VIEW test_view AS SELECT * FROM test", ExpectError: false},
					{SQL: "DROP TEMP VIEW test_view", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_VIEW",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropTempView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TEMP VIEW test_view AS SELECT * FROM test", ExpectError: false},
					{SQL: "DROP VIEW test_view", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TEMP_TABLE",
						"CREATE_TEMP_VIEW",
						"DELETE",
						"DROP_TEMP_VIEW",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END", ExpectError: false},
					{SQL: "DROP TRIGGER test_trigger", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_TRIGGER",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropTrigger,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END", ExpectError: false},
					{SQL: "DROP TRIGGER test_trigger", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_TRIGGER",
						"DELETE",
						"DROP_TRIGGER",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE VIEW test_view AS SELECT * FROM test", ExpectError: false},
					{SQL: "DROP VIEW test_view", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_VIEW",
						"DELETE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropView,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE VIEW test_view AS SELECT * FROM test", ExpectError: false},
					{SQL: "DROP VIEW test_view", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_VIEW",
						"DELETE",
						"DROP_VIEW",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_DropVTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropVTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE VIRTUAL TABLE test_vtable USING fts5 (name)", ExpectError: false},
					{SQL: "DROP TABLE test_vtable", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_INDEX",
						"CREATE_VTABLE",
						"DELETE",
						"INSERT",
						"PRAGMA",
						"READ",
						"SELECT",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeDropVTable,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE VIRTUAL TABLE test_vtable USING fts5 (name)", ExpectError: false},
					{SQL: "DROP TABLE test_vtable", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_INDEX",
						"CREATE_VTABLE",
						"DELETE",
						"DROP_VTABLE",
						"INSERT",
						"PRAGMA",
						"READ",
						"SELECT",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Function(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeFunction,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "SELECT sqlite_version()", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"SELECT",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeFunction,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "SELECT sqlite_version()", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"FUNCTION",
						"SELECT",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Insert(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeInsert,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeInsert,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Pragma(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegePragma,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "PRAGMA database_list", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegePragma,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "PRAGMA database_list", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"PRAGMA"}},
				},
			},
			{
				name: auth.DatabasePrivilegePragma,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "PRAGMA page_size", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"PRAGMA"}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Read(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeRead,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"SELECT",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeRead,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "SELECT * FROM test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"SELECT",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Recursive(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeRecursive,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "WITH RECURSIVE cte(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM cte WHERE id < 10) SELECT * FROM cte", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"SELECT",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeRecursive,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "WITH RECURSIVE cte(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM cte WHERE id < 10) SELECT * FROM cte", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"RECURSIVE",
						"SELECT",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Reindex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeReindex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX test_index ON test (name)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_INDEX",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeReindex,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "CREATE INDEX test_index ON test (name)", ExpectError: false},
					{SQL: "REINDEX test_index", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"CREATE_INDEX",
						"READ",
						"REINDEX",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Savepoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeSavepoint,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "SAVEPOINT test_savepoint", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"TRANSACTION",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeSavepoint,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "SAVEPOINT test_savepoint", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"SAVEPOINT",
						"TRANSACTION",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Select(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeSelect,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "SELECT * FROM test", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeSelect,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "SELECT * FROM test", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"SELECT",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Transaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeTransaction,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "BEGIN TRANSACTION", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegeTransaction,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "BEGIN TRANSACTION", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"TRANSACTION"}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnection_AuthorizerDatabasePrivilege_Update(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    []test.TestDatabaseAuthorizationCommand
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeUpdate,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: true},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeUpdate,
				commands: []test.TestDatabaseAuthorizationCommand{
					{SQL: "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", ExpectError: false},
					{SQL: "INSERT INTO test (name) VALUES ('test')", ExpectError: false},
					{SQL: "UPDATE test SET name = 'updated' WHERE name = 'test'", ExpectError: false},
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"CREATE_TABLE",
						"INSERT",
						"READ",
						"UPDATE",
					}},
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(string(testCase.name), func(t *testing.T) {
				mock := test.MockDatabase(app)
				connection, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)
				if err != nil {
					t.Fatal(err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, connection)
				accessKey := auth.NewAccessKey(app.Auth.AccessKeyManager, "test", "test", testCase.permissions)
				con := connection.GetConnection().WithAccessKey(accessKey)

				for _, command := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command.SQL))

					if err != nil && !command.ExpectError {
						t.Fatal(err)
					}

					if err == nil && command.ExpectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
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

			for range 50000 {
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

			for range 50000 {
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

					log.Println("CONNECTION TIMESTAMP:", con.Timestamp(), con.VFSHash())

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
