package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseBranchSettingsShowCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <database/branch>",
		Args:  cobra.ExactArgs(1),
		Short: "Get database branch settings",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/branches/%s/settings", databaseName, branchName))

			if err != nil {
				return err
			}

			message, ok := res["message"].(string)

			if !ok {
				return fmt.Errorf("invalid response from server")
			}

			data := res["data"].(map[string]any)

			if data == nil {
				return fmt.Errorf("no settings found for branch %s/%s", databaseName, branchName)
			}

			_, err = fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(message),
					components.DatabaseBranchSettingsCard(
						data,
					),
				),
			)

			return err
		},
	}
}
