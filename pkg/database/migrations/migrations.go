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
		Name: "0000000001_initial_schema",
		Up:   Migration0000000001InitialSchema,
	},
	{
		Name: "0000000002_database_branch_settings",
		Up:   Migration0000000002DatabaseBranchSettings,
	},
	{
		Name: "0000000003_queued_jobs",
		Up:   Migration0000000003QueuedJobs,
	},
	{
		Name: "0000000004_batched_jobs",
		Up:   Migration0000000004BatchedJobs,
	},
	// Add new migrations here
}

// GetAllMigrations returns all migrations in order.
func GetAllMigrations() []Migration {
	return AllMigrations
}
