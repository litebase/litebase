package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/litebase/litebase/pkg/cli/styles"

	"github.com/spf13/cobra"
)

func NewClusterUserDeleteCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <username>",
		Short: "Delete a user",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := api.NewClient(config)

			if err != nil {
				return err
			}

			_, _, err = client.Request("DELETE", "/resources/users/"+args[0], nil)

			if err != nil {
				return err
			}

			// if errors != nil {
			// 	log.Println("Error deleting user:", errors)
			// 	fmt.Println(styles.AlertDangerStyle.Render(errors.Error()))
			// 	return
			// }

			fmt.Println(styles.AlertSuccessStyle.Render("User deleted successfully"))

			return nil
		},
	}
}
