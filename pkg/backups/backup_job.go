package backups

import (
	"context"
	"fmt"
)

// BackupJob executes a backup job with the given data
func BackupJob(ctx context.Context, data map[string]any) error {
	databaseID, ok := data["database_id"].(string)

	if !ok || databaseID == "" {
		return fmt.Errorf("database_id is required")
	}

	branchID, ok := data["branch_id"].(string)

	if !ok || branchID == "" {
		return fmt.Errorf("branch_id is required")
	}

	// TODO: Implement backup logic here
	// This will have access to app dependencies via closure when registered
	fmt.Printf("Backing up database %s, branch %s\n", databaseID, branchID)

	return nil
}
