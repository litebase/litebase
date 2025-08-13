package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
)

type SystemDatabaseAccessKeyStorage struct {
	config         *config.Config
	secretsManager *auth.SecretsManager
	systemDatabase *SystemDatabase
}

// Create a new instance of SystemDatabaseAccessKeyStorage
func NewSystemDatabaseAccessKeyStorage(
	config *config.Config,
	secretsManager *auth.SecretsManager,
	systemDatabase *SystemDatabase,
) *SystemDatabaseAccessKeyStorage {
	return &SystemDatabaseAccessKeyStorage{
		config:         config,
		secretsManager: secretsManager,
		systemDatabase: systemDatabase,
	}
}

// Delete an access key from storage.
func (s *SystemDatabaseAccessKeyStorage) Delete(id string) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	result, err := db.Exec("DELETE FROM access_keys WHERE access_key_id = ?", id)

	if err != nil {
		return fmt.Errorf("failed to delete access key: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("access key with id %s not found", id)
	}

	return nil
}

// Get an access key from storage by its ID.
func (s *SystemDatabaseAccessKeyStorage) Get(id string) (*auth.AccessKey, error) {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return nil, err
	}

	var (
		accessKeyID              string
		encryptedAccessKeySecret string
		description              sql.NullString
		statementsJSON           string
		createdAtStr             string
		updatedAtStr             string
	)

	err = db.QueryRow("SELECT access_key_id, access_key_secret, description, statements, created_at, updated_at FROM access_keys WHERE access_key_id = ?",
		id,
	).Scan(
		&accessKeyID,
		&encryptedAccessKeySecret,
		&description,
		&statementsJSON,
		&createdAtStr,
		&updatedAtStr,
	)

	if err != nil {
		return nil, err
	}

	// Decrypt the access key secret
	decryptedSecret, err := s.secretsManager.Decrypt(
		s.config.EncryptionKey,
		[]byte(encryptedAccessKeySecret),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to decrypt access key secret: %w", err)
	}

	// Unmarshal statements from JSON
	var statements []auth.AccessKeyStatement
	err = json.Unmarshal([]byte(statementsJSON), &statements)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal access key statements: %w", err)
	}

	// Parse timestamps
	createdAt, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse created_at timestamp: %w", err)
	}

	updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse updated_at timestamp: %w", err)
	}

	// Handle NULL description
	var descriptionValue string
	if description.Valid {
		descriptionValue = description.String
	}

	accessKey := &auth.AccessKey{
		AccessKeyID:     accessKeyID,
		AccessKeySecret: decryptedSecret.Value,
		Description:     descriptionValue,
		Statements:      statements,
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
	}

	return accessKey, nil
}

// List access keys from storage.
func (s *SystemDatabaseAccessKeyStorage) List() ([]*auth.AccessKey, error) {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database connection: %w", err)
	}

	rows, err := db.Query("SELECT access_key_id, access_key_secret, description, statements, created_at, updated_at FROM access_keys ORDER BY created_at ASC")

	if err != nil {
		return nil, fmt.Errorf("failed to query access keys: %w", err)
	}

	defer rows.Close()

	// Initialize with empty slice instead of nil
	accessKeys := make([]*auth.AccessKey, 0)

	for rows.Next() {
		var (
			accessKeyID              string
			encryptedAccessKeySecret string
			description              sql.NullString
			statementsJSON           string
			createdAtStr             string
			updatedAtStr             string
		)

		err := rows.Scan(
			&accessKeyID,
			&encryptedAccessKeySecret,
			&description,
			&statementsJSON,
			&createdAtStr,
			&updatedAtStr,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan access key row: %w", err)
		}

		// Decrypt the access key secret
		decryptedSecret, err := s.secretsManager.Decrypt(
			s.config.EncryptionKey,
			[]byte(encryptedAccessKeySecret),
		)

		if err != nil {
			return nil, fmt.Errorf("failed to decrypt access key secret for key %s: %w", accessKeyID, err)
		}

		var statements []auth.AccessKeyStatement

		err = json.Unmarshal([]byte(statementsJSON), &statements)

		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal access key statements for key %s: %w", accessKeyID, err)
		}

		createdAt, err := time.Parse(time.RFC3339, createdAtStr)

		if err != nil {
			return nil, fmt.Errorf("failed to parse created_at timestamp for key %s: %w", accessKeyID, err)
		}

		updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)

		if err != nil {
			return nil, fmt.Errorf("failed to parse updated_at timestamp for key %s: %w", accessKeyID, err)
		}

		// Handle NULL description
		var descriptionValue string
		if description.Valid {
			descriptionValue = description.String
		}

		accessKey := &auth.AccessKey{
			AccessKeyID:     accessKeyID,
			AccessKeySecret: decryptedSecret.Value,
			Description:     descriptionValue,
			Statements:      statements,
			CreatedAt:       createdAt,
			UpdatedAt:       updatedAt,
		}

		accessKeys = append(accessKeys, accessKey)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating over access key rows: %w", err)
	}

	return accessKeys, nil
}

// Store access key in the database.
func (s *SystemDatabaseAccessKeyStorage) Store(accessKey *auth.AccessKey) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	statementsJSON, err := json.Marshal(accessKey.Statements)

	if err != nil {
		return fmt.Errorf("failed to marshal access key statements: %w", err)
	}

	encryptedAccessKeySecret, err := s.secretsManager.Encrypt(
		s.config.EncryptionKey,
		[]byte(accessKey.AccessKeySecret),
	)

	if err != nil {
		return fmt.Errorf("failed to encrypt access key secret before storing: %w", err)
	}

	_, err = db.Exec("INSERT INTO access_keys (access_key_id, access_key_secret, description, statements, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		accessKey.AccessKeyID,
		string(encryptedAccessKeySecret),
		accessKey.Description,
		string(statementsJSON),
		accessKey.CreatedAt.UTC().Format(time.RFC3339),
		accessKey.UpdatedAt.UTC().Format(time.RFC3339),
	)

	if err != nil {
		return fmt.Errorf("failed to insert access key: %w", err)
	}

	return nil
}

// Update an access key in storage.
func (s *SystemDatabaseAccessKeyStorage) Update(accessKey *auth.AccessKey) error {
	return s.update(s.config.EncryptionKey, accessKey)
}

// Internal method to update access keys with the given encryption key.
func (s *SystemDatabaseAccessKeyStorage) update(encryptionKey string, accessKey *auth.AccessKey) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	statementsJSON, err := json.Marshal(accessKey.Statements)

	if err != nil {
		return fmt.Errorf("failed to marshal access key statements: %w", err)
	}

	encryptedAccessKeySecret, err := s.secretsManager.Encrypt(
		encryptionKey,
		[]byte(accessKey.AccessKeySecret),
	)

	if err != nil {
		return fmt.Errorf("failed to encrypt access key secret before updating: %w", err)
	}

	result, err := db.Exec("UPDATE access_keys SET access_key_secret = ?, description = ?, statements = ?, updated_at = ? WHERE access_key_id = ?",
		string(encryptedAccessKeySecret),
		accessKey.Description,
		string(statementsJSON),
		accessKey.UpdatedAt.UTC().Format(time.RFC3339),
		accessKey.AccessKeyID,
	)

	if err != nil {
		return fmt.Errorf("failed to update access key: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("access key with id %s not found", accessKey.AccessKeyID)
	}

	return nil
}

// UpdateNext updates the the access key with the next encryption key.
func (s *SystemDatabaseAccessKeyStorage) UpdateNext(accessKey *auth.AccessKey) error {
	if s.config.EncryptionKeyNext == "" {
		return fmt.Errorf("next encryption key is not set")
	}

	if accessKey.AccessKeySecret == "" {
		return fmt.Errorf("access key secret is not set")
	}

	return s.update(s.config.EncryptionKeyNext, accessKey)
}
