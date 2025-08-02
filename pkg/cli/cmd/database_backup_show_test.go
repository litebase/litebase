package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

func TestDatabaseBackupShowCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		backup, err := backups.Run(
			server.App.Config,
			server.App.Cluster.ObjectFS(),
			db.DatabaseID,
			db.DatabaseBranchID,
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).SnapshotLogger(),
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem(),
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("failed to create backup: %v", err)
		}

		server.App.DatabaseManager.SystemDatabase().StoreDatabaseBackup(
			db.ID,
			db.BranchID,
			db.DatabaseID,
			db.DatabaseBranchID,
			backup.RestorePoint.Timestamp,
			backup.RestorePoint.PageCount,
			backup.GetSize(),
		)

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run("database", "backup", "show", fmt.Sprintf("%s/%s", db.DatabaseName, db.BranchName), fmt.Sprintf("%d", backup.RestorePoint.Timestamp))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Database Backup") {
			t.Fatal("expected to see 'Database Backup' in output")
		}

		if cli.DoesntSee(fmt.Sprintf("Database ID %s", db.DatabaseID)) {
			t.Fatal("expected to see database ID in output")
		}

		if cli.DoesntSee(fmt.Sprintf("Database Branch ID %s", db.DatabaseBranchID)) {
			t.Fatal("expected to see branch ID in output")
		}

		if cli.DoesntSee(fmt.Sprintf("Timestamp %d", backup.RestorePoint.Timestamp)) {
			t.Log("Output was:", cli.GetOutput())
			t.Fatal("expected to see timestamp in output", backup.RestorePoint.Timestamp)
		}
	})
}
