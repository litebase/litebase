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
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = 'user' LIMIT 1")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
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
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
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
					Name:  "name",
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Name:  "role",
					Value: []byte("user"),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "query", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), "SELECT * FROM users WHERE role = :role LIMIT :limit", "--parameters", "[{\"name\": \"role\", \"value\": \"user\", \"type\": \"TEXT\"}, {\"name\": \"limit\", \"value\": 10, \"type\": \"INTEGER\"}]")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
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
					Name:  "name",
					Value: []byte(uuid.NewString()),
				},
				{
					Type:  sqlite3.ParameterTypeText,
					Name:  "role",
					Value: []byte("user"),
				},
			})

			if err != nil {
				t.Fatalf("failed to insert user: %v", err)
			}
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
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
