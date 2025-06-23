package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

func NewAccessKeyDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:  "delete <id>",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			res, _, err := api.Delete(fmt.Sprintf("/resources/access-keys/%s", args[0]))

			if err != nil {
				return err
			}

			fmt.Print(
				components.Container(
					components.SuccessAlert(res["message"].(string)),
				),
			)

			return nil
		},
	}
}
