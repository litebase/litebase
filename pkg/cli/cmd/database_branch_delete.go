package cmd

import (
	"fmt"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBranchDeleteCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <database/branch>",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a database branch",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			res, _, err := api.Delete(config, fmt.Sprintf("/v1/databases/%s/branches/%s", databaseName, branchName))

			if err != nil {
				return err
			}

			_, err = lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

			return err
		},
	}
}
