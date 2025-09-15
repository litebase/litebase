package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseSnapshotShowCmd(config *config.CLIConfiguration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <database/branch> <timestamp>",
		Args:  cobra.ExactArgs(2),
		Short: "Get a database snapshot",
		RunE: func(cmd *cobra.Command, args []string) error {
			databaseName, branchName, err := splitDatabasePath(args[0])

			if err != nil {
				return fmt.Errorf("invalid database path: %w", err)
			}

			timestamp := args[1]

			res, err := api.Get(config, fmt.Sprintf("/v1/databases/%s/%s/snapshots/%s", databaseName, branchName, timestamp))

			if err != nil {
				return err
			}

			_, err = fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.DatabaseSnapshotCard(
						res["data"].(map[string]any),
					),
				),
			)

			return err
		},
	}
}
