package cmd_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestDatabaseErrorLogListSuccess(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(testDB.DatabaseID, testDB.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create table
		_, err = db.GetConnection().Exec("CREATE TABLE test_errors (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Execute multiple queries that will generate errors
		err = db.GetConnection().Transaction(false, func(dbConn *database.DatabaseConnection) error {
			for i := range 5 {
				_, _ = dbConn.Exec("INSERT INTO test_errors (nonexistent_column) VALUES (?)", []sqlite3.StatementParameter{
					{
						Type:  sqlite3.ParameterTypeText,
						Value: fmt.Appendf(nil, "test value %d", i),
					},
				})
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Failed to execute transaction: %v", err)
		}

		// Give some time for errors to be recorded
		time.Sleep(100 * time.Millisecond)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with default time range (last hour)
		err = cli.Run("database", "error-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName))

		if err != nil {
			t.Logf("Error output: %s", cli.GetOutput())
			t.Fatalf("expected no error, got %v", err)
		}

		// Check for expected headers
		if cli.DoesNotSee("Timestamp") {
			t.Error("expected output to contain 'Timestamp' header")
		}

		if cli.DoesNotSee("Credential ID") {
			t.Error("expected output to contain 'Credential ID' header")
		}

		if cli.DoesNotSee("Error") {
			t.Error("expected output to contain 'Error' header")
		}

		if cli.DoesNotSee("Statement") {
			t.Error("expected output to contain 'Statement' header")
		}

		if cli.DoesNotSee("Latency") {
			t.Error("expected output to contain 'Latency' header")
		}
	})
}

func TestDatabaseErrorLogListWithTimeRange(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with specific time range
		now := time.Now()
		start := now.Add(-1 * time.Hour)
		end := now

		err := cli.Run(
			"database", "error-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--start", strconv.FormatInt(start.Unix(), 10),
			"--end", strconv.FormatInt(end.Unix(), 10),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should either show error entries or "No error entries found" message
		hasErrors := cli.Sees("Timestamp")
		hasNoErrorsMessage := cli.Sees("No error entries found")

		if !hasErrors && !hasNoErrorsMessage {
			t.Error("expected either error entries table or 'No error entries found' message")
		}
	})
}

func TestDatabaseErrorLogListInvalidDatabasePath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "error-logs", "list", "invalid-path")

		if err == nil {
			t.Error("expected error when database path is invalid, got none")
		}
	})
}

func TestDatabaseErrorLogListInvalidTimeStamps(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with invalid start timestamp
		err := cli.Run(
			"database", "error-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--start", "invalid",
			"--end", strconv.FormatInt(time.Now().Unix(), 10),
		)

		if err == nil {
			t.Error("expected error with invalid start timestamp, got none")
		}

		// Test with invalid end timestamp
		err = cli.Run(
			"database", "error-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--start", strconv.FormatInt(time.Now().Add(-1*time.Hour).Unix(), 10),
			"--end", "invalid",
		)

		if err == nil {
			t.Error("expected error with invalid end timestamp, got none")
		}
	})
}

func TestDatabaseErrorLogListNoPermissions(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectDeny, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "error-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName))

		if err == nil {
			t.Error("expected error due to lack of permissions, got none")
		}
	})
}

func TestDatabaseErrorLogListNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "error-logs", "list", "nonexistent/main")

		if err == nil {
			t.Error("expected error for non-existent database, got none")
		}
	})
}

func TestDatabaseErrorLogCommandHelp(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server)

		err := cli.Run("database", "error-logs", "--help")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("View database error logs") {
			t.Error("expected help text to contain command description")
		}
	})
}
