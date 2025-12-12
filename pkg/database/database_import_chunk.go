package database

import (
	"database/sql"
	"time"
)

type DatabaseImportChunk struct {
	ID                int64            `json:"id"`
	ImportReferenceID int64            `json:"importReferenceId"`
	ChunkIndex        int64            `json:"chunkIndex"`
	ChunkSize         int64            `json:"chunkSize"`
	Checksum          sql.NullString   `json:"checksum,omitempty"`
	UploadedAt        time.Time        `json:"uploadedAt"`
	DatabaseManager   *DatabaseManager `json:"-"`
	exists            bool
}

func NewDatabaseImportChunk(databaseManager *DatabaseManager, importReferenceID, chunkIndex, chunkSize int64) *DatabaseImportChunk {
	return &DatabaseImportChunk{
		DatabaseManager:   databaseManager,
		ImportReferenceID: importReferenceID,
		ChunkIndex:        chunkIndex,
		ChunkSize:         chunkSize,
		UploadedAt:        time.Now().UTC(),
	}
}

// Insert a new database import chunk into the system database.
func InsertDatabaseImportChunk(chunk *DatabaseImportChunk) error {
	db, err := chunk.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	result, err := db.Exec(
		`INSERT INTO database_import_chunks (
			import_reference_id,
			chunk_index,
			chunk_size,
			checksum,
			uploaded_at
		) VALUES (?, ?, ?, ?, ?)
		`,
		chunk.ImportReferenceID,
		chunk.ChunkIndex,
		chunk.ChunkSize,
		chunk.Checksum,
		chunk.UploadedAt,
	)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return err
	}

	chunk.ID = id
	chunk.exists = true

	return nil
}

// Save the database import chunk to the system database.
func (dic *DatabaseImportChunk) Save() error {
	if dic.exists {
		// Chunks are immutable once created, so we don't support updates
		return nil
	} else {
		return InsertDatabaseImportChunk(dic)
	}
}
