package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewDatabaseDeleteCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a database",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := api.NewClient(config)

			if err != nil {
				fmt.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert(err.Error())),
				)

				return err
			}

			res, _, err := client.Request("DELETE", fmt.Sprintf("/resources/databases/%s", args[0]), nil)

			if err != nil {
				fmt.Fprint(
					cmd.OutOrStdout(),
					components.Container(components.ErrorAlert(err.Error())),
				)

				return err
			}

			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

			return nil
		},
	}
}
