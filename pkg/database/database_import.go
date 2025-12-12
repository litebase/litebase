package database

import (
	"database/sql"
	"encoding/json"
	"time"
)

type DatabaseImportStatus string

const (
	DatabaseImportStatusPending    DatabaseImportStatus = "pending"
	DatabaseImportStatusUploading  DatabaseImportStatus = "uploading"
	DatabaseImportStatusProcessing DatabaseImportStatus = "processing"
	DatabaseImportStatusCompleted  DatabaseImportStatus = "completed"
	DatabaseImportStatusFailed     DatabaseImportStatus = "failed"
)

type DatabaseImport struct {
	ID                        int64                `json:"-"`
	DatabaseReferenceID       sql.NullInt64        `json:"-"`
	DatabaseBranchReferenceID sql.NullInt64        `json:"-"`
	Status                    DatabaseImportStatus `json:"status"`
	TotalSize                 sql.NullInt64        `json:"totalSize"`
	ChunkCount                int64                `json:"chunkCount"`
	ErrorMessage              sql.NullString       `json:"errorMessage,omitempty"`
	CreatedAt                 time.Time            `json:"createdAt"`
	UpdatedAt                 time.Time            `json:"updatedAt"`
	CompletedAt               sql.NullTime         `json:"completedAt,omitempty"`
	DatabaseManager           *DatabaseManager     `json:"-"`
	exists                    bool
}

func NewDatabaseImport(databaseManager *DatabaseManager, databaseReferenceID, databaseBranchReferenceID int64, chunkCount int64) *DatabaseImport {
	return &DatabaseImport{
		DatabaseManager:           databaseManager,
		DatabaseReferenceID:       sql.NullInt64{Int64: databaseReferenceID, Valid: true},
		DatabaseBranchReferenceID: sql.NullInt64{Int64: databaseBranchReferenceID, Valid: true},
		Status:                    DatabaseImportStatusPending,
		ChunkCount:                chunkCount,
		CreatedAt:                 time.Now().UTC(),
		UpdatedAt:                 time.Now().UTC(),
	}
}

// Insert a new database import into the system database.
func InsertDatabaseImport(importRecord *DatabaseImport) error {
	db, err := importRecord.DatabaseManager.SystemDatabase().DB()
	if err != nil {
		return err
	}

	var errorMsg sql.NullString

	if importRecord.ErrorMessage.Valid {
		errorMsg = importRecord.ErrorMessage
	}

	result, err := db.Exec(
		`INSERT INTO database_imports (
			database_reference_id,
			database_branch_reference_id,
			status,
			total_size,
			chunk_count,
			error_message,
			created_at,
			updated_at,
			completed_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
		importRecord.DatabaseReferenceID,
		importRecord.DatabaseBranchReferenceID,
		importRecord.Status,
		importRecord.TotalSize,
		importRecord.ChunkCount,
		errorMsg,
		importRecord.CreatedAt,
		importRecord.UpdatedAt,
		importRecord.CompletedAt,
	)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return err
	}

	importRecord.ID = id
	importRecord.exists = true

	return nil
}

// Update an existing database import in the system database.
func UpdateDatabaseImport(importRecord *DatabaseImport) error {
	db, err := importRecord.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	updatedAt := time.Now().UTC()

	_, err = db.Exec(
		`UPDATE database_imports 
		SET 
			database_reference_id = ?,
			database_branch_reference_id = ?,
			status = ?,
			total_size = ?,
			chunk_count = ?,
			error_message = ?,
			updated_at = ?,
			completed_at = ?
		WHERE id = ?
		`,
		importRecord.DatabaseReferenceID,
		importRecord.DatabaseBranchReferenceID,
		importRecord.Status,
		importRecord.TotalSize,
		importRecord.ChunkCount,
		importRecord.ErrorMessage,
		updatedAt,
		importRecord.CompletedAt,
		importRecord.ID,
	)

	if err != nil {
		return err
	}

	importRecord.UpdatedAt = updatedAt

	return nil
}

// Save the database import to the system database.
func (di *DatabaseImport) Save() error {
	if di.exists {
		return UpdateDatabaseImport(di)
	} else {
		return InsertDatabaseImport(di)
	}
}

// GetChunks returns all chunks for this import.
func (di *DatabaseImport) GetChunks() ([]*DatabaseImportChunk, error) {
	db, err := di.DatabaseManager.SystemDatabase().DB()
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(
		`SELECT id, import_reference_id, chunk_index, chunk_size, checksum, uploaded_at 
		FROM database_import_chunks 
		WHERE import_reference_id = ?
		ORDER BY chunk_index ASC`,
		di.ID,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var chunks []*DatabaseImportChunk

	for rows.Next() {
		chunk := &DatabaseImportChunk{
			DatabaseManager: di.DatabaseManager,
		}

		err := rows.Scan(
			&chunk.ID,
			&chunk.ImportReferenceID,
			&chunk.ChunkIndex,
			&chunk.ChunkSize,
			&chunk.Checksum,
			&chunk.UploadedAt,
		)

		if err != nil {
			return nil, err
		}

		chunk.exists = true
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// GetUploadedChunkCount returns the number of chunks that have been uploaded.
func (di *DatabaseImport) GetUploadedChunkCount() (int64, error) {
	db, err := di.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return 0, err
	}

	var count int64

	err = db.QueryRow(
		`SELECT COUNT(*) FROM database_import_chunks WHERE import_reference_id = ?`,
		di.ID,
	).Scan(&count)

	if err != nil {
		return 0, err
	}

	return count, nil
}

// IsComplete returns true if all chunks have been uploaded.
func (di *DatabaseImport) IsComplete() (bool, error) {
	count, err := di.GetUploadedChunkCount()

	if err != nil {
		return false, err
	}

	return count == di.ChunkCount, nil
}

// MarshalJSON customizes the JSON representation.
func (di *DatabaseImport) MarshalJSON() ([]byte, error) {
	type Alias DatabaseImport

	uploadedCount, _ := di.GetUploadedChunkCount()

	return json.Marshal(&struct {
		*Alias
		UploadedChunks int64 `json:"uploadedChunks"`
	}{
		Alias:          (*Alias)(di),
		UploadedChunks: uploadedCount,
	})
}
