package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseImportManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseImportManager", func(t *testing.T) {
			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			if manager == nil {
				t.Fatal("Expected manager to be non-nil")
			}
		})

		t.Run("Create", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			importRecord, err := manager.Create(db.ID, branch.ID, 10)

			if err != nil {
				t.Fatal(err)
			}

			if importRecord == nil {
				t.Fatal("Expected import record to be non-nil")
			}

			if importRecord.ID == 0 {
				t.Fatal("Expected import record to have an ID")
			}

			if importRecord.ChunkCount != 10 {
				t.Fatalf("Expected chunk count to be 10, got %d", importRecord.ChunkCount)
			}

			if importRecord.Status != database.DatabaseImportStatusPending {
				t.Fatalf("Expected status to be pending, got %s", importRecord.Status)
			}
		})

		t.Run("Get", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import
			created, err := manager.Create(db.ID, branch.ID, 5)

			if err != nil {
				t.Fatal(err)
			}

			// Get the import
			retrieved, err := manager.Get(created.ID)

			if err != nil {
				t.Fatal(err)
			}

			if retrieved.ID != created.ID {
				t.Fatalf("Expected import ID %d, got %d", created.ID, retrieved.ID)
			}

			if retrieved.ChunkCount != created.ChunkCount {
				t.Fatalf("Expected chunk count %d, got %d", created.ChunkCount, retrieved.ChunkCount)
			}
		})

		t.Run("Get_NotFound", func(t *testing.T) {
			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			_, err := manager.Get(999999)

			if err == nil {
				t.Fatal("Expected error when getting non-existent import")
			}
		})

		t.Run("Delete", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import
			importRecord, err := manager.Create(db.ID, branch.ID, 5)

			if err != nil {
				t.Fatal(err)
			}

			// Delete the import
			err = manager.Delete(importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			// Verify it's deleted
			_, err = manager.Get(importRecord.ID)

			if err == nil {
				t.Fatal("Expected error when getting deleted import")
			}
		})

		t.Run("Delete_WithChunks", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import
			importRecord, err := manager.Create(db.ID, branch.ID, 3)

			if err != nil {
				t.Fatal(err)
			}

			// Add some chunks
			for i := range int64(3) {
				_, err = manager.AddChunk(importRecord.ID, i, 16*1024*1024, "")

				if err != nil {
					t.Fatal(err)
				}
			}

			// Delete the import (should cascade delete chunks)
			err = manager.Delete(importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			// Verify chunks were deleted
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var count int64
			err = systemDB.QueryRow("SELECT COUNT(*) FROM database_import_chunks WHERE import_reference_id = ?", importRecord.ID).Scan(&count)

			if err != nil {
				t.Fatal(err)
			}

			if count != 0 {
				t.Fatalf("Expected 0 chunks after delete, got %d", count)
			}
		})

		t.Run("List", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Get initial count
			initialList, err := manager.List()

			if err != nil {
				t.Fatal(err)
			}

			initialCount := len(initialList)

			// Create multiple imports
			for i := range int64(3) {
				_, err = manager.Create(db.ID, branch.ID, int64(i+1))
				if err != nil {
					t.Fatal(err)
				}
			}

			// List all imports
			imports, err := manager.List()

			if err != nil {
				t.Fatal(err)
			}

			if len(imports) != initialCount+3 {
				t.Fatalf("Expected %d imports, got %d", initialCount+3, len(imports))
			}
		})

		t.Run("List_OrderedByCreatedAt", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create imports
			var createdIDs []int64

			for i := range int64(3) {
				importRecord, err := manager.Create(db.ID, branch.ID, int64(i+1))

				if err != nil {
					t.Fatal(err)
				}

				createdIDs = append(createdIDs, importRecord.ID)
			}

			// List should be ordered by created_at DESC (newest first)
			imports, err := manager.List()

			if err != nil {
				t.Fatal(err)
			}

			// Check that our recently created imports appear first
			if len(imports) < 3 {
				t.Fatal("Expected at least 3 imports")
			}

			// Verify the first import is newer than the last
			if !imports[0].CreatedAt.After(imports[len(imports)-1].CreatedAt) && !imports[0].CreatedAt.Equal(imports[len(imports)-1].CreatedAt) {
				t.Fatal("Expected imports to be ordered by created_at DESC")
			}
		})

		t.Run("AddChunk", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import
			importRecord, err := manager.Create(db.ID, branch.ID, 5)

			if err != nil {
				t.Fatal(err)
			}

			// Add a chunk
			chunk, err := manager.AddChunk(importRecord.ID, 0, 16*1024*1024, "")

			if err != nil {
				t.Fatal(err)
			}

			if chunk == nil {
				t.Fatal("Expected chunk to be non-nil")
			}

			if chunk.ID == 0 {
				t.Fatal("Expected chunk to have an ID")
			}

			if chunk.ChunkIndex != 0 {
				t.Fatalf("Expected chunk index to be 0, got %d", chunk.ChunkIndex)
			}
		})

		t.Run("AddChunk_WithChecksum", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import
			importRecord, err := manager.Create(db.ID, branch.ID, 5)

			if err != nil {
				t.Fatal(err)
			}

			// Add a chunk with checksum
			checksum := "abc123def456"

			chunk, err := manager.AddChunk(importRecord.ID, 0, 16*1024*1024, checksum)

			if err != nil {
				t.Fatal(err)
			}

			if !chunk.Checksum.Valid || chunk.Checksum.String != checksum {
				t.Fatalf("Expected checksum %s, got %s", checksum, chunk.Checksum.String)
			}
		})

		t.Run("GetMissingChunks", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import with 5 chunks
			importRecord, err := manager.Create(db.ID, branch.ID, 5)

			if err != nil {
				t.Fatal(err)
			}

			// Initially all chunks should be missing
			missing, err := manager.GetMissingChunks(importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			if len(missing) != 5 {
				t.Fatalf("Expected 5 missing chunks, got %d", len(missing))
			}

			// Add chunks 0, 2, 4
			uploadedIndices := []int64{0, 2, 4}

			for _, idx := range uploadedIndices {
				_, err = manager.AddChunk(importRecord.ID, idx, 16*1024*1024, "")

				if err != nil {
					t.Fatal(err)
				}
			}

			// Now chunks 1 and 3 should be missing
			missing, err = manager.GetMissingChunks(importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			if len(missing) != 2 {
				t.Fatalf("Expected 2 missing chunks, got %d", len(missing))
			}

			expectedMissing := map[int64]bool{1: true, 3: true}

			for _, idx := range missing {
				if !expectedMissing[idx] {
					t.Fatalf("Unexpected missing chunk index: %d", idx)
				}
			}
		})

		t.Run("GetMissingChunks_AllUploaded", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import with 3 chunks
			importRecord, err := manager.Create(db.ID, branch.ID, 3)

			if err != nil {
				t.Fatal(err)
			}

			// Upload all chunks
			for i := range int64(3) {
				_, err = manager.AddChunk(importRecord.ID, i, 16*1024*1024, "")

				if err != nil {
					t.Fatal(err)
				}
			}

			// No chunks should be missing
			missing, err := manager.GetMissingChunks(importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			if len(missing) != 0 {
				t.Fatalf("Expected 0 missing chunks, got %d", len(missing))
			}
		})

		t.Run("ConcurrentCreate", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create multiple imports concurrently
			done := make(chan bool, 10)

			for i := range 10 {
				go func(index int) {
					_, err := manager.Create(db.ID, branch.ID, int64(index+1))

					if err != nil {
						t.Errorf("Failed to create import: %v", err)
					}

					done <- true
				}(i)
			}

			// Wait for all to complete
			for range 10 {
				<-done
			}
		})

		t.Run("PartialUploadResumability", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create an import
			importRecord, err := manager.Create(db.ID, branch.ID, 10)

			if err != nil {
				t.Fatal(err)
			}

			// Simulate partial upload (upload chunks 0-4)
			for i := range 5 {
				_, err = manager.AddChunk(importRecord.ID, int64(i), 16*1024*1024, "")

				if err != nil {
					t.Fatal(err)
				}
			}

			// Check progress
			complete, err := importRecord.IsComplete()

			if err != nil {
				t.Fatal(err)
			}

			if complete {
				t.Fatal("Expected import to be incomplete")
			}

			// Get missing chunks for resume
			missing, err := manager.GetMissingChunks(importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			if len(missing) != 5 {
				t.Fatalf("Expected 5 missing chunks, got %d", len(missing))
			}

			// Resume upload (upload remaining chunks)
			for _, idx := range missing {
				_, err = manager.AddChunk(importRecord.ID, idx, 16*1024*1024, "")

				if err != nil {
					t.Fatal(err)
				}
			}

			// Verify complete
			complete, err = importRecord.IsComplete()

			if err != nil {
				t.Fatal(err)
			}

			if !complete {
				t.Fatal("Expected import to be complete")
			}
		})

		t.Run("MultipleImportsForSameDatabase", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			manager := database.NewDatabaseImportManager(app.DatabaseManager)

			// Create multiple imports for the same database
			import1, err := manager.Create(db.ID, branch.ID, 5)

			if err != nil {
				t.Fatal(err)
			}

			import2, err := manager.Create(db.ID, branch.ID, 10)

			if err != nil {
				t.Fatal(err)
			}

			if import1.ID == import2.ID {
				t.Fatal("Expected different import IDs")
			}

			// Both should reference the same database
			if import1.DatabaseReferenceID.Int64 != import2.DatabaseReferenceID.Int64 {
				t.Fatal("Expected both imports to reference the same database")
			}
		})
	})
}
