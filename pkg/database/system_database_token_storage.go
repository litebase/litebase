package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
)

type SystemDatabaseTokenStorage struct {
	config         *config.Config
	secretsManager *auth.SecretsManager
	systemDatabase *SystemDatabase
}

// Create a new instance of SystemDatabaseTokenStorage
func NewSystemDatabaseTokenStorage(
	config *config.Config,
	secretsManager *auth.SecretsManager,
	systemDatabase *SystemDatabase,
) *SystemDatabaseTokenStorage {
	return &SystemDatabaseTokenStorage{
		config:         config,
		secretsManager: secretsManager,
		systemDatabase: systemDatabase,
	}
}

// Delete a token from storage.
func (s *SystemDatabaseTokenStorage) Delete(id string) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	result, err := db.Exec("DELETE FROM tokens WHERE token_id = ?", id)

	if err != nil {
		return fmt.Errorf("failed to delete token: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("token with id %s not found", id)
	}

	return nil
}

// Get a token from storage by its ID.
func (s *SystemDatabaseTokenStorage) Get(id string) (*auth.Token, error) {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return nil, err
	}

	var (
		tokenID       string
		tokenHash     string
		description   sql.NullString
		statementsJSON string
		createdAtStr  string
		updatedAtStr  string
	)

	err = db.QueryRow("SELECT token_id, token_hash, description, statements, created_at, updated_at FROM tokens WHERE token_id = ?",
		id,
	).Scan(
		&tokenID,
		&tokenHash,
		&description,
		&statementsJSON,
		&createdAtStr,
		&updatedAtStr,
	)

	if err != nil {
		return nil, err
	}

	// Unmarshal statements from JSON
	var statements []auth.AccessKeyStatement
	err = json.Unmarshal([]byte(statementsJSON), &statements)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token statements: %w", err)
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

	token := &auth.Token{
		TokenID:     tokenID,
		TokenHash:   tokenHash,
		Description: descriptionValue,
		Statements:  statements,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}

	return token, nil
}

// List tokens from storage.
func (s *SystemDatabaseTokenStorage) List() ([]*auth.Token, error) {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database connection: %w", err)
	}

	rows, err := db.Query("SELECT token_id, token_hash, description, statements, created_at, updated_at FROM tokens ORDER BY created_at ASC")

	if err != nil {
		return nil, fmt.Errorf("failed to query tokens: %w", err)
	}

	defer rows.Close()

	// Initialize with empty slice instead of nil
	tokens := make([]*auth.Token, 0)

	for rows.Next() {
		var (
			tokenID       string
			tokenHash     string
			description   sql.NullString
			statementsJSON string
			createdAtStr  string
			updatedAtStr  string
		)

		err := rows.Scan(
			&tokenID,
			&tokenHash,
			&description,
			&statementsJSON,
			&createdAtStr,
			&updatedAtStr,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan token row: %w", err)
		}

		var statements []auth.AccessKeyStatement

		err = json.Unmarshal([]byte(statementsJSON), &statements)

		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal token statements for token %s: %w", tokenID, err)
		}

		createdAt, err := time.Parse(time.RFC3339, createdAtStr)

		if err != nil {
			return nil, fmt.Errorf("failed to parse created_at timestamp for token %s: %w", tokenID, err)
		}

		updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)

		if err != nil {
			return nil, fmt.Errorf("failed to parse updated_at timestamp for token %s: %w", tokenID, err)
		}

		// Handle NULL description
		var descriptionValue string
		if description.Valid {
			descriptionValue = description.String
		}

		token := &auth.Token{
			TokenID:     tokenID,
			TokenHash:   tokenHash,
			Description: descriptionValue,
			Statements:  statements,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
		}

		tokens = append(tokens, token)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating over token rows: %w", err)
	}

	return tokens, nil
}

// Store token in the database.
func (s *SystemDatabaseTokenStorage) Store(token *auth.Token) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	statementsJSON, err := json.Marshal(token.Statements)

	if err != nil {
		return fmt.Errorf("failed to marshal token statements: %w", err)
	}

	_, err = db.Exec("INSERT INTO tokens (token_id, token_hash, description, statements, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		token.TokenID,
		token.TokenHash,
		token.Description,
		string(statementsJSON),
		token.CreatedAt.UTC().Format(time.RFC3339),
		token.UpdatedAt.UTC().Format(time.RFC3339),
	)

	if err != nil {
		return fmt.Errorf("failed to insert token: %w", err)
	}

	return nil
}

// Update a token in storage.
func (s *SystemDatabaseTokenStorage) Update(token *auth.Token) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	statementsJSON, err := json.Marshal(token.Statements)

	if err != nil {
		return fmt.Errorf("failed to marshal token statements: %w", err)
	}

	result, err := db.Exec("UPDATE tokens SET token_hash = ?, description = ?, statements = ?, updated_at = ? WHERE token_id = ?",
		token.TokenHash,
		token.Description,
		string(statementsJSON),
		token.UpdatedAt.UTC().Format(time.RFC3339),
		token.TokenID,
	)

	if err != nil {
		return fmt.Errorf("failed to update token: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("token with id %s not found", token.TokenID)
	}

	return nil
}