package cmd

import (
	"fmt"
	"litebase/cli/api"
	"litebase/cli/components"

	"github.com/spf13/cobra"
)

func NewAccessKeyDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <id>",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			res, _, err := api.Delete(fmt.Sprintf("/access-keys/%s", args[0]))

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			fmt.Print(
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

		},
	}
}
