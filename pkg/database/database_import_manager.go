package database

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

type DatabaseImportManager struct {
	databaseManager *DatabaseManager
	mutex           *sync.Mutex
}

func NewDatabaseImportManager(databaseManager *DatabaseManager) *DatabaseImportManager {
	return &DatabaseImportManager{
		databaseManager: databaseManager,
		mutex:           &sync.Mutex{},
	}
}

// Create a new database import.
func (dim *DatabaseImportManager) Create(databaseReferenceID, databaseBranchReferenceID int64, chunkCount int64) (*DatabaseImport, error) {
	dim.mutex.Lock()
	defer dim.mutex.Unlock()

	importRecord := NewDatabaseImport(dim.databaseManager, databaseReferenceID, databaseBranchReferenceID, chunkCount)

	err := importRecord.Save()

	if err != nil {
		return nil, fmt.Errorf("failed to save database import: %w", err)
	}

	return importRecord, nil
}

// Delete a database import and all its chunks.
func (dim *DatabaseImportManager) Delete(importID int64) error {
	dim.mutex.Lock()
	defer dim.mutex.Unlock()

	db, err := dim.databaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	// The database_import_chunks will be deleted automatically via CASCADE
	_, err = db.Exec(
		`DELETE FROM database_imports WHERE id = ?`,
		importID,
	)

	if err != nil {
		return fmt.Errorf("failed to delete database import: %w", err)
	}

	return nil
}

// Get a database import by its ID.
func (dim *DatabaseImportManager) Get(importID int64) (*DatabaseImport, error) {
	db, err := dim.databaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	importRecord := &DatabaseImport{
		DatabaseManager: dim.databaseManager,
	}

	err = db.QueryRow(
		`SELECT id, database_reference_id, database_branch_reference_id, status, total_size, chunk_count, error_message, created_at, updated_at, completed_at 
		FROM database_imports 
		WHERE id = ?`,
		importID,
	).Scan(
		&importRecord.ID,
		&importRecord.DatabaseReferenceID,
		&importRecord.DatabaseBranchReferenceID,
		&importRecord.Status,
		&importRecord.TotalSize,
		&importRecord.ChunkCount,
		&importRecord.ErrorMessage,
		&importRecord.CreatedAt,
		&importRecord.UpdatedAt,
		&importRecord.CompletedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("database import not found")
		}

		return nil, err
	}

	importRecord.exists = true

	return importRecord, nil
}

// List all database imports.
func (dim *DatabaseImportManager) List() ([]*DatabaseImport, error) {
	db, err := dim.databaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	rows, err := db.Query(
		`SELECT id, database_reference_id, database_branch_reference_id, status, total_size, chunk_count, error_message, created_at, updated_at, completed_at 
		FROM database_imports 
		ORDER BY created_at DESC`,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var imports []*DatabaseImport
	for rows.Next() {
		importRecord := &DatabaseImport{
			DatabaseManager: dim.databaseManager,
		}

		err := rows.Scan(
			&importRecord.ID,
			&importRecord.DatabaseReferenceID,
			&importRecord.DatabaseBranchReferenceID,
			&importRecord.Status,
			&importRecord.TotalSize,
			&importRecord.ChunkCount,
			&importRecord.ErrorMessage,
			&importRecord.CreatedAt,
			&importRecord.UpdatedAt,
			&importRecord.CompletedAt,
		)

		if err != nil {
			return nil, err
		}

		importRecord.exists = true
		imports = append(imports, importRecord)
	}

	return imports, nil
}

// AddChunk adds a chunk to an import and writes it directly to the database file system.
// This method is safe for concurrent use - multiple chunks can be uploaded in parallel.
func (dim *DatabaseImportManager) AddChunk(importID, chunkIndex int64, chunkData []byte, checksum string) (*DatabaseImportChunk, error) {
	importRecord, err := dim.Get(importID)

	if err != nil {
		return nil, fmt.Errorf("failed to get import: %w", err)
	}

	// Validate checksum if provided
	if checksum != "" {
		computed := sha256.Sum256(chunkData)
		computedHex := hex.EncodeToString(computed[:])

		if computedHex != checksum {
			return nil, fmt.Errorf("checksum mismatch: expected %s, got %s", checksum, computedHex)
		}
	}

	// Validate chunk index
	if chunkIndex < 0 || chunkIndex >= importRecord.ChunkCount {
		return nil, fmt.Errorf("invalid chunk index: %d (expected 0-%d)", chunkIndex, importRecord.ChunkCount-1)
	}

	// Get the database and branch IDs
	db, err := dim.databaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database: %w", err)
	}

	var databaseID, branchID string

	err = db.QueryRow(`
		SELECT d.database_id, b.database_branch_id
		FROM databases d
		JOIN database_branches b ON b.id = ?
		WHERE d.id = ?
	`,
		importRecord.DatabaseBranchReferenceID.Int64,
		importRecord.DatabaseReferenceID.Int64,
	).Scan(&databaseID, &branchID)

	if err != nil {
		return nil, fmt.Errorf("failed to get database and branch IDs: %w", err)
	}

	// Get the database resources and file system
	resources := dim.databaseManager.Resources(databaseID, branchID)
	dfs := resources.FileSystem()

	// Write the chunk data to the appropriate ranges
	const pageSize = 4096 // SQLite page size
	// Pages are 1-indexed in Litebase, so chunk 0 starts at page 1
	startPageNumber := (chunkIndex * (16 * 1024 * 1024)) / pageSize + 1

	// Pre-allocate padding buffer once (only used if needed for last page)
	var paddingBuffer []byte

	// Write the data page by page
	for offset := int64(0); offset < int64(len(chunkData)); offset += pageSize {
		pageNumber := startPageNumber + (offset / pageSize)
		endOffset := min(offset+pageSize, int64(len(chunkData)))
		pageData := chunkData[offset:endOffset]

		// Pad the last page if necessary
		if len(pageData) < int(pageSize) {
			if paddingBuffer == nil {
				paddingBuffer = make([]byte, pageSize)
			}

			// Clear and reuse the padding buffer
			for i := range paddingBuffer {
				paddingBuffer[i] = 0
			}

			copy(paddingBuffer, pageData)
			pageData = paddingBuffer
		}

		// Write to the appropriate range
		err = dfs.WriteToRange(pageNumber, pageData)

		if err != nil {
			return nil, fmt.Errorf("failed to write page %d: %w", pageNumber, err)
		}
	}

	// Now save the chunk metadata
	chunk := NewDatabaseImportChunk(dim.databaseManager, importID, chunkIndex, int64(len(chunkData)))

	if checksum != "" {
		chunk.Checksum = sql.NullString{String: checksum, Valid: true}
	}

	err = chunk.Save()

	if err != nil {
		return nil, fmt.Errorf("failed to save chunk: %w", err)
	}

	// Use mutex to prevent race conditions when updating import status
	dim.mutex.Lock()
	defer dim.mutex.Unlock()

	// Reload import record to get latest status
	importRecord, err = dim.Get(importID)

	if err != nil {
		return nil, fmt.Errorf("failed to reload import: %w", err)
	}

	// Update import status to uploading if this is the first chunk
	if importRecord.Status == DatabaseImportStatusPending {
		importRecord.Status = DatabaseImportStatusUploading
		err = importRecord.Save()

		if err != nil {
			return nil, fmt.Errorf("failed to update import status: %w", err)
		}
	}

	// Check if all chunks are uploaded and mark as completed
	if importRecord.Status == DatabaseImportStatusUploading {
		complete, err := importRecord.IsComplete()

		if err != nil {
			return nil, fmt.Errorf("failed to check completion: %w", err)
		}

		if complete {
			// Calculate total size from all chunks
			chunks, err := importRecord.GetChunks()

			if err != nil {
				return nil, fmt.Errorf("failed to get chunks: %w", err)
			}

			var totalSize int64

			for _, c := range chunks {
				totalSize += c.ChunkSize
			}

			importRecord.Status = DatabaseImportStatusCompleted
			importRecord.CompletedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
			importRecord.TotalSize = sql.NullInt64{Int64: totalSize, Valid: true}
			err = importRecord.Save()

			if err != nil {
				return nil, fmt.Errorf("failed to update import status: %w", err)
			}
		}
	}

	return chunk, nil
}

// GetMissingChunks returns a list of chunk indices that have not been uploaded yet.
func (dim *DatabaseImportManager) GetMissingChunks(importID int64) ([]int64, error) {
	importRecord, err := dim.Get(importID)

	if err != nil {
		return nil, err
	}

	chunks, err := importRecord.GetChunks()

	if err != nil {
		return nil, err
	}

	// Create a map of uploaded chunk indices
	uploadedMap := make(map[int64]bool)

	for _, chunk := range chunks {
		uploadedMap[chunk.ChunkIndex] = true
	}

	// Find missing chunks
	var missing []int64

	for i := int64(0); i < importRecord.ChunkCount; i++ {
		if !uploadedMap[i] {
			missing = append(missing, i)
		}
	}

	return missing, nil
}
