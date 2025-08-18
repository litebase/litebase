package cmd_test

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestDatabaseQueryCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// insert 50 users
		for range 50 {
			_, err = con.GetConnection().Exec("INSERT INTO users (name, role) VALUES (?, ?)", []sqlite3.StatementParameter{
				{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte("user"),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = 'user' LIMIT 1")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestDatabaseQueryCmdBatchInsert(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Insert
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "INSERT INTO users (name, role) VALUES ('testuser1', 'user'), ('testuser2', 'user')")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Show users
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("testuser") {
			t.Fatalf("expected to see 'testuser', but it was not found")
		}

		if cli.GetOutputLine("Row Count") != "2" {
			t.Fatalf("expected row count to be 2, got %s", cli.GetOutputLine("Row Count"))
		}
	})
}

func TestDatabaseQueryCmdInteractiveTransaction(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Begin transaction
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "BEGIN")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		transactionID := cli.GetOutputLine("Transaction ID")

		if transactionID == "" {
			t.Fatalf("expected transaction ID to be set")
		}

		// Insert
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "INSERT INTO users (name, role) VALUES ('testuser', 'user')", "--transaction-id", transactionID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// End transaction
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "COMMIT", "--transaction-id", transactionID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Show users
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("testuser") {
			t.Fatalf("expected to see 'testuser', but it was not found")
		}
	})
}

func TestDatabaseQueryCmdWithParameterSets(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run(
			"database",
			"query",
			fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName),
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"--parameter-sets", "[[{\"value\": \"bob\", \"type\": \"TEXT\"}, {\"value\": 10, \"type\": \"INTEGER\"}], [{\"value\": \"sally\", \"type\": \"TEXT\"}, {\"value\": 20, \"type\": \"INTEGER\"}]]",
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check if the users were inserted
		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("bob") {
			t.Fatalf("expected to see 'bob', but it was not found")
		}

		if cli.DoesntSee("sally") {
			t.Fatalf("expected to see 'sally', but it was not found")
		}

		if cli.GetOutputLine("Row Count") != "2" {
			t.Fatalf("expected row count to be 2, got %s", cli.GetOutputLine("Row Count"))
		}
	})
}

func TestDatabaseQueryCmdWithParameterSetsAndParametersFails(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test that providing both --parameters and --parameter-sets results in an error
		err = cli.Run(
			"database",
			"query",
			fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName),
			"INSERT INTO users (name, age) VALUES (?, ?)",
			"--parameters", "[{\"value\": \"test\", \"type\": \"TEXT\"}, {\"value\": 25, \"type\": \"INTEGER\"}]",
			"--parameter-sets", "[[{\"value\": \"bob\", \"type\": \"TEXT\"}, {\"value\": 10, \"type\": \"INTEGER\"}]]",
		)

		if err == nil {
			t.Fatal("expected error when providing both parameters and parameter-sets, but got none")
		}

		expectedErrorMessage := "cannot specify both parameters and parameter sets"
		if err.Error() != expectedErrorMessage {
			t.Fatalf("expected error message '%s', got '%s'", expectedErrorMessage, err.Error())
		}
	})
}

func TestDatabaseQueryCmdWithPositionalParameters(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// insert 50 users
		for range 50 {
			_, err = con.GetConnection().Exec("INSERT INTO users (name, role) VALUES (?, ?)", []sqlite3.StatementParameter{
				{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte("user"),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = ? LIMIT ?", "--parameters", "[{\"value\": \"user\"}, {\"value\": 10}]")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestDatabaseQueryCmdWithNamedParameters(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// insert 50 users
		for range 50 {
			_, err = con.GetConnection().Exec("INSERT INTO users (name, role) VALUES (:name, :role)", []sqlite3.StatementParameter{
				{
					Type:  sqlite3.ParameterTypeText,
					Name:  ":name",
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Name:  ":role",
					Value: []byte("user"),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = :role LIMIT :limit", "--parameters", "[{\"name\": \"role\", \"value\": \"user\", \"type\": \"TEXT\"}, {\"name\": \"limit\", \"value\": 10, \"type\": \"INTEGER\"}]")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestDatabaseQueryCmdWithJSONParameters(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, settings TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// insert 50 users
		for range 50 {
			_, err = con.GetConnection().Exec("INSERT INTO users (name, settings) VALUES (?, ?)", []sqlite3.StatementParameter{
				{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte(`{"theme": "dark", "notifications": true}`),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE settings->>'$.theme' = ? LIMIT ?", "--parameters", "[{\"value\": \"dark\", \"type\": \"TEXT\"}, {\"value\": 10, \"type\": \"INTEGER\"}]")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Row Count: 10") {
			t.Log(cli.GetOutput())
			t.Fatal("expected to see 'Row Count: 10', got none")
		}

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "UPDATE users SET settings = json_set(settings, '$.theme', ?) WHERE id = ?", "--parameters", "[{\"value\": \"light\", \"type\": \"TEXT\"}, {\"value\": 1, \"type\": \"INTEGER\"}]")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE settings->>'$.theme' = ? LIMIT ?", "--parameters", "[{\"value\": \"light\", \"type\": \"TEXT\"}, {\"value\": 10, \"type\": \"INTEGER\"}]")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("light") {
			t.Log(cli.GetOutput())
			t.Fatal("expected to see 'light', got none")
		}
	})
}

func TestDatabaseQueryCmdWithInvalidParameters(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, role TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// insert 50 users
		for range 50 {
			_, err = con.GetConnection().Exec("INSERT INTO users (name, role) VALUES (:name, :role)", []sqlite3.StatementParameter{
				{
					Type:  sqlite3.ParameterTypeText,
					Name:  ":name",
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Name:  ":role",
					Value: []byte("user"),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = :role LIMIT :limit", "--parameters", "[]")

		if err == nil {
			t.Fatal("expected error, got none")
		}

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = :role LIMIT :limit", "--parameters", "[{\"value\": 10, \"type\": \"INTEGER\"}, { \"value\": \"user\", \"type\": \"TEXT\"} ]")

		if err == nil {
			t.Fatal("expected error, got none")
		}
	})
}
