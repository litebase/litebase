package database_test

import (
	"database/sql"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseImportChunk(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseImportChunk", func(t *testing.T) {
			chunk := database.NewDatabaseImportChunk(app.DatabaseManager, 1, 0, 16*1024*1024)

			if chunk == nil {
				t.Fatal("Expected chunk to be non-nil")
			}

			if chunk.ImportReferenceID != 1 {
				t.Fatalf("Expected import reference ID to be 1, got %d", chunk.ImportReferenceID)
			}

			if chunk.ChunkIndex != 0 {
				t.Fatalf("Expected chunk index to be 0, got %d", chunk.ChunkIndex)
			}

			if chunk.ChunkSize != 16*1024*1024 {
				t.Fatalf("Expected chunk size to be 16MB, got %d", chunk.ChunkSize)
			}
		})

		t.Run("InsertDatabaseImportChunk", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import first
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create and insert a chunk
			chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, 0, 16*1024*1024)

			err = database.InsertDatabaseImportChunk(chunk)

			if err != nil {
				t.Fatal(err)
			}

			if chunk.ID == 0 {
				t.Fatal("Expected chunk to have an ID after insert")
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

			// Create an import first
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create and save a chunk
			chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, 1, 16*1024*1024)

			err = chunk.Save()

			if err != nil {
				t.Fatal(err)
			}

			if chunk.ID == 0 {
				t.Fatal("Expected chunk to have an ID after save")
			}
		})

		t.Run("SaveWithChecksum", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import first
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create a chunk with checksum
			chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, 2, 16*1024*1024)

			chunk.Checksum = sql.NullString{
				String: "abc123def456",
				Valid:  true,
			}

			err = chunk.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Verify checksum was saved
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var checksum sql.NullString

			err = systemDB.QueryRow("SELECT checksum FROM database_import_chunks WHERE id = ?", chunk.ID).Scan(&checksum)

			if err != nil {
				t.Fatal(err)
			}

			if !checksum.Valid || checksum.String != "abc123def456" {
				t.Fatalf("Expected checksum 'abc123def456', got %s", checksum.String)
			}
		})

		t.Run("UniqueConstraint", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import first
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create and save first chunk
			chunk1 := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, 0, 16*1024*1024)

			err = chunk1.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Try to save duplicate chunk (same import_reference_id and chunk_index)
			chunk2 := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, 0, 16*1024*1024)

			err = chunk2.Save()

			if err == nil {
				t.Fatal("Expected error when saving duplicate chunk, but got nil")
			}
		})

		t.Run("MultipleChunksForSameImport", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 5)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create multiple chunks with different indices
			for i := int64(0); i < 5; i++ {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, i, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatalf("Failed to save chunk %d: %v", i, err)
				}
			}

			// Verify all chunks were saved
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var count int64

			err = systemDB.QueryRow("SELECT COUNT(*) FROM database_import_chunks WHERE import_reference_id = ?", importRecord.ID).Scan(&count)

			if err != nil {
				t.Fatal(err)
			}

			if count != 5 {
				t.Fatalf("Expected 5 chunks, got %d", count)
			}
		})

		t.Run("ChunkImmutability", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import first
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create and save a chunk
			chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, 0, 16*1024*1024)

			err = chunk.Save()

			if err != nil {
				t.Fatal(err)
			}

			originalSize := chunk.ChunkSize

			// Try to save again (should be a no-op since chunks are immutable)
			chunk.ChunkSize = 32 * 1024 * 1024

			err = chunk.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Verify the size wasn't updated in the database
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var dbSize int64
			err = systemDB.QueryRow("SELECT chunk_size FROM database_import_chunks WHERE id = ?", chunk.ID).Scan(&dbSize)

			if err != nil {
				t.Fatal(err)
			}

			if dbSize != originalSize {
				t.Fatalf("Expected chunk size to remain %d, but got %d", originalSize, dbSize)
			}
		})

		t.Run("CascadeDelete", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 3)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Create chunks
			for i := int64(0); i < 3; i++ {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, i, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatal(err)
				}
			}

			// Delete the import
			systemDB, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			_, err = systemDB.Exec("DELETE FROM database_imports WHERE id = ?", importRecord.ID)

			if err != nil {
				t.Fatal(err)
			}

			// Verify chunks were deleted via CASCADE
			var count int64

			err = systemDB.QueryRow("SELECT COUNT(*) FROM database_import_chunks WHERE import_reference_id = ?", importRecord.ID).Scan(&count)

			if err != nil {
				t.Fatal(err)
			}

			if count != 0 {
				t.Fatalf("Expected 0 chunks after cascade delete, got %d", count)
			}
		})

		t.Run("ChunkOrdering", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(mock.BranchName)

			if err != nil {
				t.Fatal(err)
			}

			// Create an import
			importRecord := database.NewDatabaseImport(app.DatabaseManager, db.ID, branch.ID, 5)

			err = importRecord.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Save chunks in random order
			indices := []int64{3, 0, 4, 1, 2}

			for _, idx := range indices {
				chunk := database.NewDatabaseImportChunk(app.DatabaseManager, importRecord.ID, idx, 16*1024*1024)

				err = chunk.Save()

				if err != nil {
					t.Fatal(err)
				}
			}

			// Retrieve chunks and verify they're ordered by chunk_index
			chunks, err := importRecord.GetChunks()

			if err != nil {
				t.Fatal(err)
			}

			if len(chunks) != 5 {
				t.Fatalf("Expected 5 chunks, got %d", len(chunks))
			}

			for i, chunk := range chunks {
				if chunk.ChunkIndex != int64(i) {
					t.Fatalf("Expected chunk at position %d to have index %d, got %d", i, i, chunk.ChunkIndex)
				}
			}
		})
	})
}
