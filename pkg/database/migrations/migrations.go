package migrations

import "database/sql"

// Migration represents a database migration with a name and up function.
type Migration struct {
	Name string
	Up   func(*sql.DB) error
}

// GetAllMigrations returns all migrations in order.
func GetAllMigrations() []Migration {
	return []Migration{
		{
			Name: "001_initial_schema",
			Up:   Migration001InitialSchema,
		},
		// Add new migrations here
	}
}
