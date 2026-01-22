package migrations

import "database/sql"

// Migration0000000005DatabaseEncryption adds encryption fields to database_branch_settings.
func Migration0000000005DatabaseEncryption(db *sql.DB) error {
	// Add encrypted boolean field
	_, err := db.Exec(`
		ALTER TABLE database_branch_settings
		ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0
	`)

	if err != nil {
		return err
	}

	// Add data_encryption_key_hash field for storing the SHA256 hash of the encryption key
	_, err = db.Exec(`
		ALTER TABLE database_branch_settings
		ADD COLUMN data_encryption_key_hash TEXT
	`)

	return err
}
