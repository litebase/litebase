package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

// Hide authentication-related flags from the help output of the given command.
func hideAuthFlags(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		cmd.Flags().MarkHidden("access-key-id")
		cmd.Flags().MarkHidden("access-key-secret")
		cmd.Flags().MarkHidden("profile")
		cmd.Flags().MarkHidden("token")
		cmd.Flags().MarkHidden("username")
		cmd.Flags().MarkHidden("password")
		cmd.Parent().HelpFunc()(cmd, args)
	})
}

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
