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

func TestDatabaseQueryLogListSuccess(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(testDB.DatabaseID, testDB.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create table and execute some queries to generate metrics
		_, err = db.GetConnection().Exec("CREATE TABLE test_metrics (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		// Execute multiple queries to generate metrics
		err = db.GetConnection().Transaction(false, func(dbConn *database.DatabaseConnection) error {
			for i := range 10 {
				_, err = dbConn.Exec("INSERT INTO test_metrics (value) VALUES (?)", []sqlite3.StatementParameter{
					{
						Type:  sqlite3.ParameterTypeText,
						Value: fmt.Appendf(nil, "test value %d", i),
					},
				})

				if err != nil {
					return err
				}
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}

		// Give some time for metrics to be recorded
		time.Sleep(100 * time.Millisecond)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with default time range (last hour)
		err = cli.Run("database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName))

		if err != nil {
			t.Logf("Error output: %s", cli.GetOutput())
			t.Fatalf("expected no error, got %v", err)
		}

		// Check for expected headers
		if cli.DoesNotSee("Query ID") {
			t.Error("expected output to contain 'Query ID' header")
		}

		if cli.DoesNotSee("Count") {
			t.Error("expected output to contain 'Count' header")
		}

		if cli.DoesNotSee("Avg Latency") {
			t.Error("expected output to contain 'Avg Latency' header")
		}
	})
}

func TestDatabaseQueryLogListWithTimeRange(t *testing.T) {
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
			"database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--start", strconv.FormatInt(start.Unix(), 10),
			"--end", strconv.FormatInt(end.Unix(), 10),
			"--step", "60",
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should either show metrics or "No query metrics found" message
		hasMetrics := cli.Sees("Query ID")
		hasNoMetricsMessage := cli.DoesNotSee("No query metrics found")

		if !hasMetrics && !hasNoMetricsMessage {
			t.Error("expected either query metrics table or 'No query metrics found' message")
		}
	})
}

func TestDatabaseQueryLogListInvalidDatabasePath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "query-logs", "list", "invalid-path")

		if err == nil {
			t.Error("expected error when database path is invalid, got none")
		}
	})
}

func TestDatabaseQueryLogListInvalidTimeStamps(t *testing.T) {
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
			"database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--start", "invalid-timestamp",
			"--end", strconv.FormatInt(time.Now().Unix(), 10),
		)

		if err == nil {
			t.Error("expected error when start timestamp is invalid, got none")
		}

		// Test with invalid end timestamp
		err = cli.Run(
			"database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--start", strconv.FormatInt(time.Now().Add(-1*time.Hour).Unix(), 10),
			"--end", "invalid-timestamp",
		)

		if err == nil {
			t.Error("expected error when end timestamp is invalid, got none")
		}
	})
}

func TestDatabaseQueryLogListInvalidStep(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with invalid step (less than 1)
		err := cli.Run(
			"database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--step", "0",
		)

		if err == nil {
			t.Error("expected error when step is less than 1, got none")
		}

		// Test with negative step
		err = cli.Run(
			"database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName),
			"--step", "-1",
		)

		if err == nil {
			t.Error("expected error when step is negative, got none")
		}
	})
}

func TestDatabaseQueryLogListNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with non-existent database
		err := cli.Run("database", "query-logs", "list", "non-existent-db/main")

		if err == nil {
			t.Error("expected error when database does not exist, got none")
		}
	})
}

func TestDatabaseQueryLogListInsufficientPrivileges(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDB := test.MockDatabase(server.App)

		// Create CLI with limited privileges (no read privilege)
		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeQuery}},
			})

		err := cli.Run("database", "query-logs", "list", fmt.Sprintf("%s/%s", testDB.DatabaseName, testDB.BranchName))

		if err == nil {
			t.Error("expected error when user lacks read privileges, got none")
		}
	})
}

func TestDatabaseQueryLogListWrongArgumentCount(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test no arguments
		err := cli.Run("database", "query-logs", "list")

		if err == nil {
			t.Error("expected error when no arguments provided, got none")
		}

		// Test too many arguments
		err = cli.Run("database", "query-logs", "list", "db/main", "extra-arg")

		if err == nil {
			t.Error("expected error when too many arguments provided, got none")
		}
	})
}

func TestDatabaseQueryLogMainCommand(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test main command shows help
		err := cli.Run("database", "query-logs")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should show help with available commands
		if cli.DoesNotSee("Available Commands") {
			t.Logf("Actual output: %s", cli.GetOutput())
			t.Error("expected output to contain 'Available Commands' section")
		}

		if cli.DoesNotSee("list") {
			t.Error("expected output to contain 'list' command")
		}
	})
}
