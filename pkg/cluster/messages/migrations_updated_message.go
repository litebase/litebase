package messages

// MigrationsUpdatedMessage is sent by the primary when new migrations are applied
type MigrationsUpdatedMessage struct {
	LatestMigration string `json:"latest_migration"`
	MigrationsHash  string `json:"migrations_hash"`
}
