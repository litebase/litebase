package logs_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewLogManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		logManager := logs.NewLogManager(app.Cluster.Node().Context())

		if logManager == nil {
			t.Fatal("Log manager is nil")
		}
	})
}

func TestLogManager_GetQueryLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		l := app.LogManager.GetQueryLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		if l == nil {
			t.Fatal("Query log is nil")
		}
	})
}

func TestLogManager_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.LogManager.Close()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestLogManager_Query(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		err := app.LogManager.Query(
			logs.QueryLogEntry{
				Cluster:      app.Cluster,
				DatabaseHash: db.DatabaseKey.DatabaseHash,
				DatabaseID:   db.DatabaseID,
				BranchID:     db.DatabaseBranchID,
				CredentialID: db.Credential.CredentialID,
				Statement:    "SELECT * FROM test",
				Latency:      0.01,
			},
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestLogManager_Run(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		go app.LogManager.Run()
	})
}

func TestLogManager_GetErrorLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		if errorLog == nil {
			t.Fatal("Error log is nil")
		}
	})
}

func TestLogManager_Error(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		err := app.LogManager.Error(
			logs.ErrorLogEntry{
				Cluster:      app.Cluster,
				DatabaseHash: db.DatabaseKey.DatabaseHash,
				DatabaseID:   db.DatabaseID,
				BranchID:     db.DatabaseBranchID,
				CredentialID: db.Credential.CredentialID,
				Statement:    "SELECT * FROM nonexistent",
				Error:        "no such table: nonexistent",
				Latency:      0.02,
			},
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestLogManager_GetErrorLogEncrypted(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockEncryptedDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		if errorLog == nil {
			t.Fatal("Error log is nil")
		}

		// For encrypted databases, verify we can write and read error entries
		err := errorLog.Write(
			db.Credential.CredentialID,
			"SELECT * FROM encrypted_test",
			"no such table: encrypted_test",
			0.01,
		)

		if err != nil {
			t.Fatal(err)
		}

		err = errorLog.Flush(true)

		if err != nil {
			t.Fatal(err)
		}

		// Verify file was created and has content
		file, err := errorLog.GetFile()

		if err != nil {
			t.Fatal(err)
		}

		fileInfo, err := file.Stat()

		if err != nil {
			t.Fatal(err)
		}

		if fileInfo.Size() == 0 {
			t.Fatal("Error log file should have content for encrypted database")
		}
	})
}

func TestLogManager_ErrorEncrypted(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockEncryptedDatabase(app)

		// Log an error through the manager for an encrypted database
		err := app.LogManager.Error(
			logs.ErrorLogEntry{
				Cluster:      app.Cluster,
				DatabaseHash: db.DatabaseKey.DatabaseHash,
				DatabaseID:   db.DatabaseID,
				BranchID:     db.DatabaseBranchID,
				CredentialID: db.Credential.CredentialID,
				Statement:    "SELECT * FROM encrypted_table",
				Error:        "no such table: encrypted_table",
				Latency:      0.02,
			},
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}
