package database_test

import (
	"database/sql"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseImport(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseImport", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 10)

			if importRecord == nil {
				t.Fatal("Expected import record to be non-nil")
			}

			if importRecord.Status != database.DatabaseImportStatusPending {
				t.Fatalf("Expected status to be pending, got %s", importRecord.Status)
			}

			if importRecord.ChunkCount != 10 {
				t.Fatalf("Expected chunk count to be 10, got %d", importRecord.ChunkCount)
			}

			if !importRecord.DatabaseReferenceID.Valid {
				t.Fatal("Expected database reference ID to be valid")
			}

			if !importRecord.DatabaseBranchReferenceID.Valid {
				t.Fatal("Expected database branch reference ID to be valid")
			}
		})

		t.Run("InsertDatabaseImport", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 5)

			err = database.InsertDatabaseImport(importRecord)

			if err != nil {
				t.Fatal(err)
			}

			if importRecord.ID == 0 {
				t.Fatal("Expected import record to have an ID after insert")
			}
		})

		t.Run("UpdateDatabaseImport", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 5)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Update the status
			importRecord.Status = database.DatabaseImportStatusUploading

			err = database.UpdateDatabaseImport(importRecord)

			if err != nil {
				t.Fatal(err)
			}

			// Verify the update
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var status string
			err = systemDB.QueryRow("SELECT status FROM database_imports WHERE id = ?", importRecord.ID).Scan(&status)

			if err != nil {
				t.Fatal(err)
			}

			if status != string(database.DatabaseImportStatusUploading) {
				t.Fatalf("Expected status to be uploading, got %s", status)
			}
		})

		t.Run("Save", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 5)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			if importRecord.ID == 0 {
				t.Fatal("Expected import record to have an ID after save")
			}

			// Update and save again
			importRecord.Status = database.DatabaseImportStatusCompleted

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("GetChunks", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Add some chunks
			for i := range int64(3) {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, i, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatal(err)
				}
			}

			chunks, err := importRecord.GetChunks()

			if err != nil {
				t.Fatal(err)
			}

			if len(chunks) != 3 {
				t.Fatalf("Expected 3 chunks, got %d", len(chunks))
			}

			// Verify chunks are in order
			for i, chunk := range chunks {
				if chunk.ChunkIndex != int64(i) {
					t.Fatalf("Expected chunk index %d, got %d", i, chunk.ChunkIndex)
				}
			}
		})

		t.Run("GetUploadedChunkCount", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 5)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Add 3 chunks
			for i := range int64(3) {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, i, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatal(err)
				}
			}

			count, err := importRecord.GetUploadedChunkCount()

			if err != nil {
				t.Fatal(err)
			}

			if count != 3 {
				t.Fatalf("Expected 3 uploaded chunks, got %d", count)
			}
		})

		t.Run("IsComplete", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Check incomplete
			complete, err := importRecord.IsComplete()

			if err != nil {
				t.Fatal(err)
			}

			if complete {
				t.Fatal("Expected import to be incomplete")
			}

			// Add all chunks
			for i := range int64(3) {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, i, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatal(err)
				}
			}

			// Check complete
			complete, err = importRecord.IsComplete()

			if err != nil {
				t.Fatal(err)
			}

			if !complete {
				t.Fatal("Expected import to be complete")
			}
		})

		t.Run("MarshalJSON", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Add 2 chunks
			for i := range int64(2) {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, i, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatal(err)
				}
			}

			jsonBytes, err := importRecord.MarshalJSON()

			if err != nil {
				t.Fatal(err)
			}

			if len(jsonBytes) == 0 {
				t.Fatal("Expected JSON output to be non-empty")
			}

			// Check that it includes uploadedChunks
			jsonString := string(jsonBytes)

			if !contains(jsonString, "uploadedChunks") {
				t.Fatal("Expected JSON to contain 'uploadedChunks' field")
			}
		})

		t.Run("StatusTransitions", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Transition through statuses
			statuses := []database.DatabaseImportStatus{
				database.DatabaseImportStatusUploading,
				database.DatabaseImportStatusProcessing,
				database.DatabaseImportStatusCompleted,
			}

			for _, status := range statuses {
				importRecord.Status = status

				err = importRecord.Save()

				if err != nil {
					t.Fatalf("Failed to save status %s: %v", status, err)
				}
			}
		})

		t.Run("ErrorHandling", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			importRecord.Status = database.DatabaseImportStatusFailed
			importRecord.ErrorMessage = sql.NullString{
				String: "Test error message",
				Valid:  true,
			}

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Verify error message is saved
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var errorMsg sql.NullString
			err = systemDB.QueryRow("SELECT error_message FROM database_imports WHERE id = ?", importRecord.ID).Scan(&errorMsg)

			if err != nil {
				t.Fatal(err)
			}

			if !errorMsg.Valid || errorMsg.String != "Test error message" {
				t.Fatalf("Expected error message 'Test error message', got %s", errorMsg.String)
			}
		})
	})
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsInMiddle(s, substr)))
}

func containsInMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
