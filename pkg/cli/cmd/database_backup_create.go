package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBackupCreateCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "create <database/branch>",
		Short: "Create a new database backup",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			res, apiErrors, err := api.Post(config, fmt.Sprintf("/v1/databases/%s/branches/%s/backups", databaseName, branchName), map[string]any{})

			if err != nil {
				return err
			}

			if len(apiErrors) > 0 {
				return fmt.Errorf("failed to create backup: %v", apiErrors)
			}

			data, ok := res["data"].(map[string]any)

			if !ok {
				return fmt.Errorf("invalid data format for database %s", args[0])
			}

			rows := []components.CardRow{
				{
					Key:   "Name",
					Value: fmt.Sprintf("%s/%s", databaseName, branchName),
				},
			}

			if databaseID, ok := data["databaseId"].(string); ok {
				rows = append(rows, components.CardRow{
					Key:   "Database ID",
					Value: databaseID,
				})
			}

			if branchID, ok := data["databaseBranchId"].(string); ok {
				rows = append(rows, components.CardRow{
					Key:   "Branch ID",
					Value: branchID,
				})
			}

			if timestamp, ok := data["timestamp"].(float64); ok {
				rows = append(rows, components.CardRow{
					Key:   "Timestamp",
					Value: fmt.Sprintf("%.0f", timestamp),
				})
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.NewCard(
						components.WithCardTitle("Database Backup"),
						components.WithCardRows(rows),
					).Render(),
				),
			)

			return err
		},
	}
}
