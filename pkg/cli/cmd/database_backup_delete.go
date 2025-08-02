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

func NewDatabaseBackupDeleteCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name> <timestamp>",
		Short: "Delete a database backup",
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

			res, _, err := api.Delete(config, fmt.Sprintf("/v1/databases/%s/%s/backups/%d", databaseName, branchName, timestamp))

			if err != nil {
				return err
			}

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

			return nil
		},
	}
}
