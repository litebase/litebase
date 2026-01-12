package database_test

import (
	"os"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestQueryResolver_Handle(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		if _, err := db.GetConnection().Exec("CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{}); err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		cases := []struct {
			statement  string
			parameters []sqlite3.StatementParameter
			expected   string
		}{
			{
				"SELECT * FROM users",
				[]sqlite3.StatementParameter{},
				"success",
			},
			{
				"SELECT * FROM users LIMIT ?",
				[]sqlite3.StatementParameter{
					{
						Type:  "INTEGER",
						Value: int64(1),
					},
				},
				"success",
			},
			{
				"?SELECT * FROM users",
				[]sqlite3.StatementParameter{},
				"error",
			},
		}

		queryResponse := &database.QueryResponse{}

		for _, c := range cases {
			q, err := database.NewQuery(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				auth.NewDatabaseKey(mock.DatabaseID, mock.DatabaseName, mock.DatabaseBranchID, mock.BranchName),
				mock.Credential,
				&database.QueryInput{
					Statement:  c.statement,
					Parameters: c.parameters,
					ID:         "",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			queryResponse.Reset()

			_, err = q.Resolve(queryResponse)

			if err != nil && c.expected == `success` {
				t.Fatal(err)
			}
		}
	})
}

func TestQueryResolver_NoQueryLogsWhenDisabled(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Disable query logs
		mock.Branch.Settings.QueryLogsEnabled = false

		err := mock.Branch.UpdateBranchSettings(mock.Branch.Settings)

		if err != nil {
			t.Fatal(err)
		}

		// Reload the branch to get updated settings
		branch, err := app.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatal(err)
		}

		updatedBranch, err := branch.BranchByID(mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		// Execute some queries
		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(db)

		if _, err := db.GetConnection().Exec("CREATE TABLE users (id INT, name TEXT)", []sqlite3.StatementParameter{}); err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		queryResponse := &database.QueryResponse{}

		// Execute several queries
		queries := []string{
			"INSERT INTO users VALUES (1, 'Alice')",
			"INSERT INTO users VALUES (2, 'Bob')",
			"SELECT * FROM users",
			"SELECT COUNT(*) FROM users",
		}

		for _, stmt := range queries {
			q, err := database.NewQuery(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				auth.NewDatabaseKey(mock.DatabaseID, mock.DatabaseName, mock.DatabaseBranchID, updatedBranch.Name),
				mock.Credential,
				&database.QueryInput{
					Statement:  stmt,
					Parameters: []sqlite3.StatementParameter{},
					ID:         "",
				},
			)

			if err != nil {
				t.Fatal(err)
			}

			queryResponse.Reset()

			_, err = q.Resolve(queryResponse)

			if err != nil {
				t.Fatal(err)
			}
		}

		// Give the async query logger time to potentially write (even though it shouldn't)
		time.Sleep(100 * time.Millisecond)

		// Verify that no query logs were created by checking the query log directory
		queryLogPath := file.GetDatabaseFileBaseDir(mock.DatabaseID, mock.DatabaseBranchID) + "logs/query"

		entries, err := app.Cluster.TmpTieredFS().ReadDir(queryLogPath)

		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}

		// The directory might not exist or be empty - both are valid
		if len(entries) > 0 {
			t.Fatalf("Expected no query log entries when query logs disabled, found %d", len(entries))
		}
	})
}
