package logs_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/server"
)

func TestErrorLog_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		err := errorLog.Close()

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestErrorLog_Write(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		logs.ErrorLogFlushInterval = time.Millisecond * 1

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		err := errorLog.Write(
			db.Credential.CredentialID,
			"INSERT INTO test (invalid_column) VALUES (?)",
			"no such column: invalid_column",
			0.05,
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestErrorLog_Flush(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		logs.ErrorLogFlushInterval = time.Millisecond * 1

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		errorLog.Flush(true)

		file, err := errorLog.GetFile()

		if err != nil {
			t.Fatal(err)
		}

		fileInfo, err := file.Stat()

		if err != nil {
			t.Fatal(err)
		}

		// Ensure file is initially empty
		if fileInfo.Size() != 0 {
			t.Fatal("File size should be 0 initially")
		}

		err = errorLog.Write(
			db.Credential.CredentialID,
			"SELECT * FROM nonexistent",
			"no such table: nonexistent",
			0.01,
		)

		if err != nil {
			t.Fatal(err)
		}

		errorLog.Flush(true)

		file, err = errorLog.GetFile()

		if err != nil {
			t.Fatal(err)
		}

		fileInfo, err = file.Stat()

		if err != nil {
			t.Fatal(err)
		}

		// Ensure data was written to the file
		if fileInfo.Size() == 0 {
			t.Fatal("File size should not be 0 after writing error entry")
		}
	})
}

func TestErrorLog_Read(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		logs.ErrorLogFlushInterval = time.Millisecond * 1

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		startTime := time.Now().UTC().Truncate(time.Second)

		// Write some test errors
		testErrors := []struct {
			statement string
			error     string
			latency   float64
		}{
			{"SELECT * FROM test1", "no such table: test1", 0.01},
			{"INSERT INTO test2 (col) VALUES (?)", "no such table: test2", 0.02},
			{"UPDATE test3 SET col = ?", "no such table: test3", 0.03},
		}

		for _, te := range testErrors {
			err := errorLog.Write(
				db.Credential.CredentialID,
				te.statement,
				te.error,
				te.latency,
			)

			if err != nil {
				t.Fatal(err)
			}
		}

		errorLog.Flush(true)

		endTime := time.Now().UTC().Add(time.Second)

		uint32StartTime, err := utils.SafeUint64ToUint32(uint64(startTime.Unix()))

		if err != nil {
			t.Fatal(err)
		}

		uint32EndTime, err := utils.SafeUint64ToUint32(uint64(endTime.Unix()))

		if err != nil {
			t.Fatal(err)
		}

		// Read the errors back
		entries, err := errorLog.Read(uint32StartTime, uint32EndTime)

		if err != nil {
			t.Fatal(err)
		}

		if len(entries) != len(testErrors) {
			t.Fatalf("Expected %d entries, got %d", len(testErrors), len(entries))
		}

		// Verify the entries
		for i, entry := range entries {
			if entry.Statement != testErrors[i].statement {
				t.Errorf("Expected statement '%s', got '%s'", testErrors[i].statement, entry.Statement)
			}

			if entry.Error != testErrors[i].error {
				t.Errorf("Expected error '%s', got '%s'", testErrors[i].error, entry.Error)
			}

			if entry.CredentialID != db.Credential.CredentialID {
				t.Errorf("Expected credential ID '%s', got '%s'", db.Credential.CredentialID, entry.CredentialID)
			}

			if entry.Latency != testErrors[i].latency {
				t.Errorf("Expected latency %f, got %f", testErrors[i].latency, entry.Latency)
			}
		}
	})
}

func TestErrorLog_ReadEmptyRange(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		// Read from a time range in the future
		futureTime := time.Now().UTC().Add(24 * time.Hour)

		uint32Start, err := utils.SafeUint64ToUint32(uint64(futureTime.Unix()))

		if err != nil {
			t.Fatal(err)
		}

		uint32End, err := utils.SafeUint64ToUint32(uint64(futureTime.Add(time.Hour).Unix()))

		if err != nil {
			t.Fatal(err)
		}

		entries, err := errorLog.Read(uint32Start, uint32End)

		if err != nil {
			t.Fatal(err)
		}

		if len(entries) != 0 {
			t.Fatalf("Expected 0 entries, got %d", len(entries))
		}
	})
}

func TestErrorLog_MultipleWrites(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		logs.ErrorLogFlushInterval = time.Millisecond * 1

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		// Write multiple errors
		for i := range 10 {
			err := errorLog.Write(
				db.Credential.CredentialID,
				"SELECT * FROM test",
				"test error",
				float64(i)*0.01,
			)

			if err != nil {
				t.Fatal(err)
			}
		}

		errorLog.Flush(true)

		file, err := errorLog.GetFile()

		if err != nil {
			t.Fatal(err)
		}

		fileInfo, err := file.Stat()

		if err != nil {
			t.Fatal(err)
		}

		// Ensure data was written
		if fileInfo.Size() == 0 {
			t.Fatal("File size should not be 0 after writing multiple entries")
		}
	})
}

func TestErrorLog_GetFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		file, err := errorLog.GetFile()

		if err != nil {
			t.Fatal(err)
		}

		if file == nil {
			t.Fatal("File is nil")
		}
	})
}

func TestErrorLog_EncryptionConfiguration(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		// Initially, encryption should not be configured
		if errorLog.IsEncrypted() {
			t.Fatal("Error log should not be encrypted initially")
		}

		// Configure encryption
		dataKey := make([]byte, 32)

		for i := range dataKey {
			dataKey[i] = byte(i)
		}

		var keyHash [32]byte

		for i := range keyHash {
			keyHash[i] = byte(i + 32)
		}

		err := errorLog.ConfigureEncryption(dataKey, keyHash)

		if err != nil {
			t.Fatal(err)
		}

		// Verify encryption is now enabled
		if !errorLog.IsEncrypted() {
			t.Fatal("Error log should be encrypted after configuration")
		}
	})
}

func TestErrorLog_WriteAndReadEncrypted(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockEncryptedDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		startTime := time.Now().UTC()

		// Write encrypted error entries
		testErrors := []struct {
			statement string
			error     string
			latency   float64
		}{
			{"SELECT * FROM encrypted1", "no such table: encrypted1", 0.01},
			{"INSERT INTO encrypted2 (col) VALUES (?)", "no such table: encrypted2", 0.02},
			{"UPDATE encrypted3 SET col = ?", "no such table: encrypted3", 0.03},
		}

		for _, te := range testErrors {
			err := errorLog.Write(
				db.Credential.CredentialID,
				te.statement,
				te.error,
				te.latency,
			)

			if err != nil {
				t.Fatal(err)
			}
		}

		errorLog.Flush(true)

		endTime := time.Now().UTC().Add(time.Second)

		uint32StartTime, err := utils.SafeUint64ToUint32(uint64(startTime.Unix()))

		if err != nil {
			t.Fatal(err)
		}

		uint32EndTime, err := utils.SafeUint64ToUint32(uint64(endTime.Unix()))

		if err != nil {
			t.Fatal(err)
		}

		// Read the encrypted errors back
		entries, err := errorLog.Read(uint32StartTime, uint32EndTime)

		if err != nil {
			t.Fatal(err)
		}

		if len(entries) != len(testErrors) {
			t.Fatalf("Expected %d entries, got %d", len(testErrors), len(entries))
		}

		// Verify the decrypted entries match the original data
		for i, entry := range entries {
			if entry.Statement != testErrors[i].statement {
				t.Errorf("Expected statement '%s', got '%s'", testErrors[i].statement, entry.Statement)
			}

			if entry.Error != testErrors[i].error {
				t.Errorf("Expected error '%s', got '%s'", testErrors[i].error, entry.Error)
			}

			if entry.CredentialID != db.Credential.CredentialID {
				t.Errorf("Expected credential ID '%s', got '%s'", db.Credential.CredentialID, entry.CredentialID)
			}
		}
	})
}

func TestErrorLog_MultipleWritesEncrypted(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockEncryptedDatabase(app)

		errorLog := app.LogManager.GetErrorLog(
			app.Cluster,
			db.DatabaseKey.DatabaseHash,
			db.DatabaseID,
			db.DatabaseBranchID,
		)

		// Write multiple error entries to encrypted database
		for i := range 10 {
			err := errorLog.Write(
				db.Credential.CredentialID,
				fmt.Sprintf("SELECT * FROM encrypted_test%d", i),
				fmt.Sprintf("no such table: encrypted_test%d", i),
				0.01,
			)

			if err != nil {
				t.Fatal(err)
			}
		}

		errorLog.Flush(true)

		file, err := errorLog.GetFile()

		if err != nil {
			t.Fatal(err)
		}

		fileInfo, err := file.Stat()

		if err != nil {
			t.Fatal(err)
		}

		// Verify data was written (encrypted data should have size > 0)
		if fileInfo.Size() == 0 {
			t.Fatal("File size should not be 0 after writing encrypted entries")
		}
	})
}
