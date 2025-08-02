package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBackupCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		// Create a new table
		_, err = con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = con.GetConnection().Checkpoint()

		if err != nil {
			t.Fatalf("failed to checkpoint: %v", err)
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "backup", "create", fmt.Sprintf("%s/%s", mock.DatabaseName, mock.BranchName))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}
