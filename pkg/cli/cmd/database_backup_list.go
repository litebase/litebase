package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBackupListCmd(config *config.Configuration) *cobra.Command {
	var cmd = &cobra.Command{
		Use:   "list <name>",
		Short: "List database backups",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/%s/backups", databaseName, branchName))

			if err != nil {
				return err
			}

			if res["data"] == nil {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.WarningAlert("No backups found for this database")),
				)

				return nil
			}

			rows := [][]string{}

			backups, ok := res["data"].([]any)

			if !ok {
				lipgloss.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert("Invalid data format for access keys")),
				)

				return nil
			}

			for _, backup := range backups {
				backupData, ok := backup.(map[string]any)

				if !ok {
					continue
				}

				rows = append(rows, []string{
					backupData["restore_point"].(map[string]any)["timestamp"].(string),
				})
			}

			columns := []string{
				"#",
				"Timestamp",
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewTable(columns, rows).
						SetHandler(func(row []string) {
							accessKeyShow(cmd, config, row[1])
						}).Render(config.GetInteractive()),
				),
			)

			return nil
		},
	}

	return cmd
}
