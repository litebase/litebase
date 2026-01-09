package migrations

import "database/sql"

// Migration001InitialSchema creates all initial system database tables.
func Migration001InitialSchema(db *sql.DB) error {
	// Create the metadata table if it doesn't exist.
	_, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS metadata
		(
			id INTEGER PRIMARY KEY, 
			key TEXT UNIQUE, 
			value TEXT
		)`,
	)

	if err != nil {
		return err
	}

	// Create the databases table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS databases
		(
			id INTEGER PRIMARY KEY, 
			database_id TEXT UNIQUE, 
			name TEXT UNIQUE,
			primary_branch_reference_id INTEGER,
			settings TEXT,
			created_at TEXT,
			updated_at TEXT
		)`,
	)

	if err != nil {
		return err
	}

	// Create the branches table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_branches
		(
			id INTEGER PRIMARY KEY, 
			database_reference_id INTEGER,
			parent_database_branch_reference_id INTEGER,
			database_id TEXT,
			database_branch_id TEXT,
			name TEXT,
			settings TEXT,
			created_at TEXT,
			updated_at TEXT,
			FOREIGN KEY (database_reference_id) REFERENCES databases(id) ON DELETE CASCADE
		)`,
	)

	if err != nil {
		return err
	}

	// Create index on database_reference_id and the branch name.
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_database_branches_database_reference_id_name 
		ON database_branches (database_reference_id, name)`,
	)

	if err != nil {
		return err
	}

	// Create the database backups table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_backups
		(
			id INTEGER PRIMARY KEY, 
			database_reference_id INTEGER,
			database_branch_reference_id INTEGER,
			database_id TEXT,
			database_branch_id TEXT,
			restore_point_timestamp INTEGER,
			restore_point_page_count INTEGER,
			size INTEGER,
			created_at TEXT,
			FOREIGN KEY (database_reference_id) REFERENCES databases(id) ON DELETE CASCADE,
			FOREIGN KEY (database_branch_reference_id) REFERENCES database_branches(id) ON DELETE CASCADE
		)`,
	)

	if err != nil {
		return err
	}

	// Create a table for access keys
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS access_keys
		(
			id INTEGER PRIMARY KEY,
			access_key_id TEXT UNIQUE,
			access_key_secret TEXT NOT NULL,
			description TEXT,
			statements TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	)
	if err != nil {
		return err
	}

	// Create a table for tokens
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS tokens
		(
			id INTEGER PRIMARY KEY,
			token_id TEXT UNIQUE,
			token_hash TEXT NOT NULL,
			description TEXT,
			statements TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	)

	if err != nil {
		return err
	}

	// Create a table for users
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS users
		(
			id INTEGER PRIMARY KEY,
			username TEXT UNIQUE,
			password TEXT NOT NULL,
			description TEXT,
			statements TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	)

	if err != nil {
		return err
	}

	// Create a table for database imports
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_imports
		(
			id INTEGER PRIMARY KEY,
			database_reference_id INTEGER,
			database_branch_reference_id INTEGER,
			status TEXT NOT NULL,
			total_size INTEGER,
			chunk_count INTEGER NOT NULL,
			error_message TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT,
			FOREIGN KEY (database_reference_id) REFERENCES databases(id) ON DELETE CASCADE,
			FOREIGN KEY (database_branch_reference_id) REFERENCES database_branches(id) ON DELETE CASCADE
		)`,
	)

	if err != nil {
		return err
	}

	// Create a table for database import chunks
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_import_chunks
		(
			id INTEGER PRIMARY KEY,
			import_reference_id INTEGER NOT NULL,
			chunk_index INTEGER NOT NULL,
			chunk_size INTEGER NOT NULL,
			checksum TEXT,
			uploaded_at TEXT NOT NULL,
			FOREIGN KEY (import_reference_id) REFERENCES database_imports(id) ON DELETE CASCADE,
			UNIQUE(import_reference_id, chunk_index)
		)`,
	)

	if err != nil {
		return err
	}

	// Create index on import_reference_id for database_import_chunks
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_database_import_chunks_import_reference_id 
		ON database_import_chunks (import_reference_id)`,
	)

	if err != nil {
		return err
	}

	return nil
}
