package cmd

import (
	"fmt"
	"log/slog"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBackupListCmd(config *config.CLIConfiguration) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list <database/branch>",
		Short: "List database backups",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/branches/%s/backups", databaseName, branchName))

			if err != nil {
				return err
			}

			if res["data"] == nil {
				_, err := lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No backups found for this database")),
				)

				return err
			}

			rows := [][]string{}

			backups, ok := res["data"].([]any)

			if !ok {
				_, err := lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for access keys")),
				)

				return err
			}

			for _, backup := range backups {
				backupData, ok := backup.(map[string]any)

				if !ok {
					continue
				}

				timestamp := "-"

				if restorePoint, ok := backupData["restorePoint"].(map[string]any); ok {
					if ts, ok := restorePoint["timestamp"].(string); ok {
						timestamp = ts
					}
				}

				rows = append(rows, []string{
					timestamp,
				})
			}

			columns := []string{
				"#",
				"Timestamp",
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							err := accessKeyShow(cmd, config, row[1])

							if err != nil {
								slog.Error("Error showing access key", "error", err, "accessKeyId", row[1])
							}
						}).Render(config.GetInteractive()),
				),
			)

			return err
		},
	}

	return cmd
}
