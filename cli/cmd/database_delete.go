package cmd

import (
	"fmt"
	"litebase/cli/api"
	"litebase/cli/components"

	"github.com/spf13/cobra"
)

func NewDatabaseDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <id>",
		Args:  cobra.ExactArgs(1),
		Short: "Delete a database",
		Run: func(cmd *cobra.Command, args []string) {
			client := api.NewClient()

			res, _, err := client.Request("DELETE", fmt.Sprintf("/databases/%s", args[0]), nil)

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			fmt.Print(components.Container(
				components.SuccessAlert(res["message"].(string)),
			))
		},
	}
}
