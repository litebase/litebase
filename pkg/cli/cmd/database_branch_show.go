package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseBranchShowCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <database/branch>",
		Args:  cobra.ExactArgs(1),
		Short: "Get a database branch",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/%s", databaseName, branchName))

			if err != nil {
				return err
			}

			_, err = fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseBranchCard(
						res["data"].(map[string]any),
					),
				),
			)

			return err
		},
	}
}
