package database_test

import (
	"context"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/auth"
	"litebase/server/sqlite3"
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

			for i := 0; i < 100000; i++ {
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

			for i := 0; i < 10; i++ {
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

// This test is useful in ensuring the database can be properly written to and read
// from in an interleaved manner without issue.
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

		_, err = connection1.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

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

			for i := 0; i < 500000; i++ {
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

		_, err = app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

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

func TestTestDatabaseConnectionDatabasePrivilegeAlterTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeAlterTable,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"ALTER TABLE test ADD COLUMN age INTEGER":               false,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"*"}},
				},
			},
			{
				name: auth.DatabasePrivilegeAlterTable,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"ALTER TABLE test ADD COLUMN age INTEGER":               false,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"ALTER TABLE test ADD COLUMN age INTEGER":               true,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeAnalyze(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeAnalyze,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"ANALYZE test": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TABLE", "READ", "UPDATE"}},
				},
			},
			{
				name:     auth.DatabasePrivilegeAnalyze,
				commands: map[string]bool{"ANALYZE": false},
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeAttach(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeAttach,
				commands: map[string]bool{
					"ATTACH DATABASE 'test.db' AS test": true,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateIndex,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX test_index ON test (name)":                true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TABLE", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateIndex,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX test_index ON test (name)":                false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTable,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTable,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateTempIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempIndex,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX temp.test_index ON test (name)":                true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempIndex,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX temp.test_index ON test (name)":                false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateTempTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempTable,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempTable,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateTempTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempTrigger,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)":                                          false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN INSERT INTO test (name) VALUES ('test'); END": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempTrigger,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)":                                          false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN INSERT INTO test (name) VALUES ('test'); END": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateTempView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTempView,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE TEMP VIEW test_view AS SELECT * FROM test":           true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"CREATE_TEMP_TABLE", "INSERT", "READ", "UPDATE"}},
				},
			},
			{
				name: auth.DatabasePrivilegeCreateTempView,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE TEMP VIEW test_view AS SELECT * FROM test":           false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateTrigger,
				commands: map[string]bool{
					"CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)":                                                false,
					"CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)":                                                false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END": true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)":                                               false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test BEGIN INSERT INTO test (name) VALUES ('test'); END": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateView,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE VIEW test_view AS SELECT * FROM test":           true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE VIEW test_view AS SELECT * FROM test":           false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeCreateVTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeCreateVTable,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)": false,
					"CREATE VIRTUAL TABLE test_vtable USING fts5 (name)":     true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE VIRTUAL TABLE test_vtable USING fts5 (name)":    false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDelete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDelete,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":                false,
					"DELETE FROM test WHERE name = 'test'":                   true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":                false,
					"DELETE FROM test WHERE name = 'test'":                   false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))
					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDetach(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDetach,
				commands: map[string]bool{
					"DETACH DATABASE 'test.db'": true,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropIndex,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":                false,
					"DELETE FROM test WHERE name = 'test'":                   true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY,  name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":                false,
					"DELETE FROM test WHERE name = 'test'":                   false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))
					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTable,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"DROP TABLE test": true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"DROP TABLE test": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))
					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropTempIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempIndex,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX test_index ON  test (name)":                    false,
					"DROP INDEX test_index":                                      true,
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
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX test_index ON test (name)":                     false,
					"DROP INDEX test_index":                                      false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))
					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropTempTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempTable,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"DROP TABLE test": true,
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
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"DROP TABLE test": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))
					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropTempTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempTrigger,
				commands: map[string]bool{
					"CREATE TEMP TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)":                                           false,
					"CREATE TEMP TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)":                                           false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END": false,
					"DROP TRIGGER test_trigger": true,
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
				commands: map[string]bool{
					"CREATE TEMP TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)":                                           false,
					"CREATE TEMP TABLE tes2 (id INTEGER PRIMARY KEY, name TEXT)":                                            false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END": false,
					"DROP TRIGGER test_trigger": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropTempView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTempView,
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE TEMP VIEW test_view AS SELECT * FROM test":           false,
					"DROP TEMP VIEW test_view":                                   true,
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
				commands: map[string]bool{
					"CREATE TEMP TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE TEMP VIEW test_view AS SELECT * FROM test":           false,
					"DROP VIEW test_view": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropTrigger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropTrigger,
				commands: map[string]bool{
					"CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)":                                                 false,
					"CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)":                                                 false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END;": false,
					"DROP TRIGGER test_trigger": true,
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
				commands: map[string]bool{
					"CREATE TABLE test1 (id INTEGER PRIMARY KEY, name TEXT)":                                                false,
					"CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)":                                                false,
					"CREATE TRIGGER test_trigger AFTER INSERT ON test1 BEGIN INSERT INTO test2 (name) VALUES ('test'); END": false,
					"DROP TRIGGER test_trigger": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeDropView(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropView,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE VIEW test_view AS SELECT * FROM test":           false,
					"DROP VIEW test_view":                                   true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE VIEW test_view AS SELECT * FROM test":           false,
					"DROP VIEW test_view":                                   false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnectionAuthorizerDatabasePrivilegeDropVTable(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeDropVTable,
				commands: map[string]bool{
					// "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE VIRTUAL TABLE test_vtable USING fts5 (name)": false,
					"DROP TABLE test_vtable":                             true,
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
				commands: map[string]bool{
					"CREATE VIRTUAL TABLE test_vtable USING fts5 (name)": false,
					"DROP TABLE test_vtable":                             false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestDatabaseConnectionAuthorizerDatabasePrivilegeFunction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeFunction,
				commands: map[string]bool{
					"SELECT sqlite_version()": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{
						"SELECT",
					}},
				},
			},
			{
				name: auth.DatabasePrivilegeFunction,
				commands: map[string]bool{
					"SELECT sqlite_version()": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeInsert(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeInsert,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":               true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":               false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegePragma(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegePragma,
				commands: map[string]bool{
					"PRAGMA database_list": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegePragma,
				commands: map[string]bool{
					"PRAGMA database_list": false,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{"PRAGMA"}},
				},
			},
			{
				name: auth.DatabasePrivilegePragma,
				commands: map[string]bool{
					"PRAGMA page_size": true,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeRead(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeRead,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":               false,
					"SELECT * FROM test":                                    false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeRecursive(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeRecursive,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					// "WITH RECURSIVE cte(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM cte WHERE id < 10) SELECT * FROM cte": true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					// "WITH RECURSIVE cte(id) AS (SELECT 1 UNION ALL SELECT id + 1 FROM cte WHERE id < 10) SELECT * FROM cte": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeReindex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeReindex,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX test_index ON test (name)":                true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"CREATE INDEX test_index ON test (name)":                false,
					"REINDEX test_index":                                    false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeSavepoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeSavepoint,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"SAVEPOINT test_savepoint":                              true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"SAVEPOINT test_savepoint":                              false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeSelect(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeSelect,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":               false,
					"SELECT * FROM test":                                    true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":               false,
					"SELECT * FROM test":                                    false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeTransaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeTransaction,
				commands: map[string]bool{
					"BEGIN TRANSACTION": true,
				},
				permissions: []*auth.AccessKeyPermission{
					{Resource: "*", Actions: []string{}},
				},
			},
			{
				name: auth.DatabasePrivilegeTransaction,
				commands: map[string]bool{
					"BEGIN TRANSACTION": false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}

func TestTestDatabaseConnectionAuthorizerDatabasePrivilegeUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testCases := []struct {
			commands    map[string]bool
			name        auth.DatabasePrivilege
			permissions []*auth.AccessKeyPermission
		}{
			{
				name: auth.DatabasePrivilegeUpdate,
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": true,
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
				commands: map[string]bool{
					"CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)": false,
					"INSERT INTO test (name) VALUES ('test')":               false,
					"UPDATE test SET name = 'updated' WHERE name = 'test'":  false,
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

				for command, expectError := range testCase.commands {
					_, err := con.SqliteConnection().Exec(context.Background(), []byte(command))

					if err != nil && !expectError {
						t.Fatal(err)
					}

					if err == nil && expectError {
						t.Fatal("Expected error but got none")
					}
				}
			})
		}
	})
}
