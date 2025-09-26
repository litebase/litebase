package cmd

import (
	"fmt"
	"strconv"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseRestoreCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <source-database/branch> <target-database/branch>",
		Args:  cobra.ExactArgs(2),
		Short: "Restore a database from a specific timestamp",
		Long:  "Restore a database from a specific timestamp to a target database and branch.\n\nThe timestamp should be a Unix timestamp in nanoseconds (int64).\nYou can get available timestamps from the 'database snapshot show' command.",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.MarkFlagRequired("timestamp")

			if err != nil {
				return fmt.Errorf("failed to mark timestamp flag as required: %w", err)
			}

			sourceDatabaseName, sourceBranchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid source database path: %w", err)
			}

			targetDatabaseName, targetBranchName, err := splitDatabasePath(args[1])

			if err != nil {
				return fmt.Errorf("invalid target database path: %w", err)
			}

			timestampStr, err := cmd.Flags().GetString("timestamp")

			if err != nil {
				return fmt.Errorf("failed to get timestamp: %w", err)
			}

			if timestampStr == "" {
				return fmt.Errorf("timestamp is required")
			}

			_, err = strconv.ParseInt(timestampStr, 10, 64)

			if err != nil {
				return fmt.Errorf("invalid timestamp format - must be a Unix timestamp in nanoseconds: %w", err)
			}

			requestData := map[string]any{
				"target_database":        targetDatabaseName,
				"target_database_branch": targetBranchName,
				"timestamp":              timestampStr,
			}

			res, apiErrors, err := api.Post(
				config,
				fmt.Sprintf("/v1/databases/%s/branches/%s/restore", sourceDatabaseName, sourceBranchName),
				requestData,
			)

			if err != nil {
				return err
			}

			if len(apiErrors) > 0 {
				return fmt.Errorf("failed to restore database: %v", apiErrors)
			}

			rows := []components.CardRow{
				{
					Key:   "Source Database",
					Value: fmt.Sprintf("%s/%s", sourceDatabaseName, sourceBranchName),
				},
				{
					Key:   "Target Database",
					Value: fmt.Sprintf("%s/%s", targetDatabaseName, targetBranchName),
				},
				{
					Key:   "Timestamp",
					Value: timestampStr,
				},
			}

			if status, ok := res["status"].(string); ok {
				rows = append(rows, components.CardRow{
					Key:   "Status",
					Value: status,
				})
			}

			if message, ok := res["message"].(string); ok {
				rows = append(rows, components.CardRow{
					Key:   "Message",
					Value: message,
				})
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert("Database restore completed successfully"),
					components.NewCard(
						components.WithCardTitle("Database Restore"),
						components.WithCardRows(rows),
					).Render(),
				),
			)

			return err
		},
	}

	cmd.Flags().String("timestamp", "", "Unix timestamp in nanoseconds to restore from (required)")

	return cmd
}
