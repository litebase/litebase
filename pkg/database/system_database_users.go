package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

type SystemDatabaseUserStorage struct {
	systemDatabase *SystemDatabase
}

// Create a new instance of SystemDatabaseUserStorage
func NewSystemDatabaseUserStorage(
	systemDatabase *SystemDatabase,
) *SystemDatabaseUserStorage {
	return &SystemDatabaseUserStorage{
		systemDatabase: systemDatabase,
	}
}

// Delete a user from storage.
func (s *SystemDatabaseUserStorage) Delete(username string) error {
	if username == "" {
		return fmt.Errorf("username cannot be empty")
	}

	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	result, err := db.Exec("DELETE FROM users WHERE username = ?", username)

	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("user not found")
	}

	return nil
}

// Get a user from storage by its username.
func (s *SystemDatabaseUserStorage) Get(username string) (*auth.User, error) {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return nil, err
	}

	var (
		storedUsername string
		password       string
		description    sql.NullString
		statementsJSON string
		createdAtStr   string
		updatedAtStr   string
	)

	err = db.QueryRow("SELECT username, password, description, statements, created_at, updated_at FROM users WHERE username = ?",
		username,
	).Scan(
		&storedUsername,
		&password,
		&description,
		&statementsJSON,
		&createdAtStr,
		&updatedAtStr,
	)

	if err != nil {
		return nil, err
	}

	// Unmarshal statements from JSON
	var statements []auth.Statement

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

	user := &auth.User{
		Username:    storedUsername,
		Password:    password,
		Description: descriptionValue,
		Statements:  statements,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
	}

	return user, nil
}

// List users from storage.
func (s *SystemDatabaseUserStorage) List() ([]*auth.User, error) {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database connection: %w", err)
	}

	rows, err := db.Query("SELECT username, password, description, statements, created_at, updated_at FROM users ORDER BY created_at ASC")

	if err != nil {
		return nil, fmt.Errorf("failed to query users: %w", err)
	}

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("Error closing rows", "error", err)
		}
	}()

	// Initialize with empty slice instead of nil
	users := make([]*auth.User, 0)

	for rows.Next() {
		var (
			username       string
			password       string
			description    sql.NullString
			statementsJSON string
			createdAtStr   string
			updatedAtStr   string
		)

		err := rows.Scan(
			&username,
			&password,
			&description,
			&statementsJSON,
			&createdAtStr,
			&updatedAtStr,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan user row: %w", err)
		}

		var statements []auth.Statement

		err = json.Unmarshal([]byte(statementsJSON), &statements)

		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal user statements for user %s: %w", username, err)
		}

		createdAt, err := time.Parse(time.RFC3339, createdAtStr)

		if err != nil {
			return nil, fmt.Errorf("failed to parse created_at timestamp for user %s: %w", username, err)
		}

		updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)

		if err != nil {
			return nil, fmt.Errorf("failed to parse updated_at timestamp for user %s: %w", username, err)
		}

		// Handle NULL description
		var descriptionValue string

		if description.Valid {
			descriptionValue = description.String
		}

		user := &auth.User{
			Username:    username,
			Password:    password,
			Description: descriptionValue,
			Statements:  statements,
			CreatedAt:   createdAt,
			UpdatedAt:   updatedAt,
		}

		users = append(users, user)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while iterating over user rows: %w", err)
	}

	return users, nil
}

// Store user in the database.
func (s *SystemDatabaseUserStorage) Store(user *auth.User) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	statementsJSON, err := json.Marshal(user.Statements)

	if err != nil {
		return fmt.Errorf("failed to marshal user statements: %w", err)
	}

	_, err = db.Exec("INSERT INTO users (username, password, description, statements, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
		user.Username,
		user.Password,
		user.Description,
		string(statementsJSON),
		user.CreatedAt.UTC().Format(time.RFC3339),
		user.UpdatedAt.UTC().Format(time.RFC3339),
	)

	if err != nil {
		return fmt.Errorf("failed to insert user: %w", err)
	}

	return nil
}

// Update a user in storage.
func (s *SystemDatabaseUserStorage) Update(user *auth.User) error {
	db, err := s.systemDatabase.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	statementsJSON, err := json.Marshal(user.Statements)

	if err != nil {
		return fmt.Errorf("failed to marshal user statements: %w", err)
	}

	result, err := db.Exec("UPDATE users SET password = ?, description = ?, statements = ?, updated_at = ? WHERE username = ?",
		user.Password,
		user.Description,
		string(statementsJSON),
		user.UpdatedAt.UTC().Format(time.RFC3339),
		user.Username,
	)

	if err != nil {
		return fmt.Errorf("failed to update user: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("user with username %s not found", user.Username)
	}

	return nil
}
