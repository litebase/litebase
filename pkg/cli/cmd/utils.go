package cmd

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/cobra"
)

// Hide authentication-related flags from the help output of the given command.
func hideAuthFlags(cmd *cobra.Command) {
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		if err := cmd.Flags().MarkHidden("access-key-id"); err != nil {
			slog.Error("failed to hide access-key-id flag", "error", err)
		}

		if err := cmd.Flags().MarkHidden("access-key-secret"); err != nil {
			slog.Error("failed to hide access-key-secret flag", "error", err)
		}

		if err := cmd.Flags().MarkHidden("profile"); err != nil {
			slog.Error("failed to hide profile flag", "error", err)
		}

		if err := cmd.Flags().MarkHidden("token"); err != nil {
			slog.Error("failed to hide token flag", "error", err)
		}

		if err := cmd.Flags().MarkHidden("username"); err != nil {
			slog.Error("failed to hide username flag", "error", err)
		}

		if err := cmd.Flags().MarkHidden("password"); err != nil {
			slog.Error("failed to hide password flag", "error", err)
		}

		if err := cmd.Flags().MarkHidden("url"); err != nil {
			slog.Error("failed to hide url flag", "error", err)
		}

		// Find the root command to get the original styled help function
		root := cmd

		for root.Parent() != nil {
			root = root.Parent()
		}

		// Use the root command's help function to preserve Fang styling
		root.HelpFunc()(cmd, args)
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
