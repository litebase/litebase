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

func NewDatabaseBackupShowCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		//  litebase database backups show test/test
		Use:   "show <name> <timestamp>",
		Short: "Show details of a specific database backup",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			timestamp, err := strconv.ParseInt(args[1], 10, 64)

			if err != nil {
				return fmt.Errorf("invalid timestamp: %w", err)
			}

			res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/%s/backups/%d", databaseName, branchName, timestamp))

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

			rows := []components.CardRow{
				{
					Key:   "Database ID",
					Value: res["data"].(map[string]any)["database_id"].(string),
				},
				{
					Key:   "Database Branch ID",
					Value: res["data"].(map[string]any)["database_branch_id"].(string),
				},
				{
					Key:   "Timestamp",
					Value: res["data"].(map[string]any)["restore_point"].(map[string]any)["timestamp"].(string),
				},
				{
					Key:   "Size",
					Value: fmt.Sprintf("%d bytes", int64(res["data"].(map[string]any)["size"].(float64))),
				},
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.NewCard(
						components.WithCardTitle("Database Backup"),
						components.WithCardRows(rows),
					).Render(),
				),
			)

			return nil
		},
	}
}
