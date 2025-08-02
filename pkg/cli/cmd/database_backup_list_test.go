package cmd_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestNewDatabaseBackupListCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create an initial checkpoint before creating the table
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		var createdBackups []*backups.Backup

		// Create 10 different backups by inserting data and creating backups in each iteration
		for i := range 10 {
			// Insert some test data to ensure the database has actual content
			for j := range 100 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-data-backup-%d-record-%d", i, j),
						},
					},
				)

				if err != nil {
					t.Fatalf("expected no error inserting data, got %v", err)
				}
			}

			err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a backup
			backup, err := backups.Run(
				server.App.Config,
				server.App.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error creating backup %d, got %v", i, err)
			}

			err = server.App.DatabaseManager.SystemDatabase().StoreDatabaseBackup(
				mock.ID,
				mock.BranchID,
				mock.DatabaseID,
				mock.DatabaseBranchID,
				backup.RestorePoint.Timestamp,
				backup.RestorePoint.PageCount,
				backup.GetSize(),
			)

			if err != nil {
				t.Fatalf("expected no error storing backup %d, got %v", i, err)
			}

			createdBackups = append(createdBackups, backup)

			// Add a small delay to ensure different timestamps
			time.Sleep(10 * time.Millisecond)
		}

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "backup", "list", fmt.Sprintf("%s/%s", mock.DatabaseName, mock.BranchName))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check if the output contains the backup information
		output := cli.GetOutput()

		if output == "" {
			t.Fatal("expected output to contain backup information, but it was empty")
		}

		for _, backup := range createdBackups {
			if cli.DoesntSee(fmt.Sprintf("%d", backup.RestorePoint.Timestamp)) {
				t.Errorf("expected output to contain backup timestamp %d and size %d, but it did not", backup.RestorePoint.Timestamp, backup.Size)
			}
		}
	})
}
