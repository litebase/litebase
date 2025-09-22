package openapi

// GetTags returns the tag definitions for organizing API endpoints
func GetTags() []Tag {
	return []Tag{
		{Name: "Health", Description: "Health check endpoints"},
		{Name: "Cluster", Description: "Cluster management and status operations"},
		{Name: "Users", Description: "User management operations"},
		{Name: "Access Keys", Description: "Access key management for authentication"},
		{Name: "Databases", Description: "Database management operations"},
		{Name: "Database Branches", Description: "Database branch management operations"},
		{Name: "Queries", Description: "SQL query execution operations"},
		{Name: "Backups", Description: "Database backup and restore operations"},
		{Name: "Snapshots", Description: "Database snapshot operations"},
		{Name: "Metrics", Description: "Performance and usage metrics"},
		{Name: "Keys", Description: "Encryption key management"},
		{Name: "Events", Description: "Internal cluster event management"},
	}
}