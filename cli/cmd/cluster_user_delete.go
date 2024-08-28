package cmd

import (
	"fmt"
	"litebase/cli/api"
	"litebase/cli/components"
	"litebase/cli/styles"

	"github.com/spf13/cobra"
)

func NewClusterUserDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <username>",
		Short: "Delete a user",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			client, err := api.NewClient()

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			_, _, err = client.Request("DELETE", "/users/"+args[0], nil)

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			// if errors != nil {
			// 	log.Println("Error deleting user:", errors)
			// 	fmt.Println(styles.AlertDangerStyle.Render(errors.Error()))
			// 	return
			// }

			fmt.Println(styles.AlertSuccessStyle.Render("User deleted successfully"))
		},
	}
}
