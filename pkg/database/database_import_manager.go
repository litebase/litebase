package database

import (
	"database/sql"
	"fmt"
	"sync"
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

// AddChunk adds a chunk to an import.
func (dim *DatabaseImportManager) AddChunk(importID, chunkIndex, chunkSize int64, checksum string) (*DatabaseImportChunk, error) {
	chunk := NewDatabaseImportChunk(dim.databaseManager, importID, chunkIndex, chunkSize)

	if checksum != "" {
		chunk.Checksum = sql.NullString{String: checksum, Valid: true}
	}

	err := chunk.Save()

	if err != nil {
		return nil, fmt.Errorf("failed to save chunk: %w", err)
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
