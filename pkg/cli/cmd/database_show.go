package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseShowCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Get a database",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := api.Get(config, fmt.Sprintf("/resources/databases/%s", args[0]))

			if err != nil {
				return err
			}

			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseCard(
						res["data"].(map[string]any),
					),
				),
			)

			return nil
		},
	}
}
