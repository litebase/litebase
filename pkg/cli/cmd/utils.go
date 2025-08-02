package cmd

import (
	"fmt"
	"strings"
)

// Split a string that is formated as "databaseName/branchName" into its components.
func splitDatabasePath(path string) (string, string, error) {
	if path == "" {
		return "", "", fmt.Errorf("database path is required")
	}

	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid database path format")
	}

	return parts[0], parts[1], nil
}
