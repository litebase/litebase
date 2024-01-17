package cmd

import (
	"fmt"
	"litebasedb/cli/api"
	"litebasedb/cli/components"
	"log"

	"github.com/spf13/cobra"
)

func NewAccessKeyDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <id>",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			log.Println("Deleting access key...", fmt.Sprintf("/access-keys/%s", args[0]))
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
