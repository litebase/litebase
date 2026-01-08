package migrations

import "database/sql"

// Migration represents a database migration with a name and up function.
type Migration struct {
	Name string
	Up   func(*sql.DB) error
}

// AllMigrations contains all migrations in order.
// This can be modified in tests to simulate new migrations.
var AllMigrations = []Migration{
	{
		Name: "001_initial_schema",
		Up:   Migration001InitialSchema,
	},
	// Add new migrations here
}

// GetAllMigrations returns all migrations in order.
func GetAllMigrations() []Migration {
	return AllMigrations
}
