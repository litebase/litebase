package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

func NewDatabaseShowCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Get a database",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			res, err := api.Get(fmt.Sprintf("/databases/%s", args[0]))

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			fmt.Print(
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseCard(
						res["data"].(map[string]interface{}),
					),
				),
			)
		},
	}

}
