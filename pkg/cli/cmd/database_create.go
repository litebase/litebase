package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

func NewDatabaseCreateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Args:  cobra.ExactArgs(1),
		Short: "Create a new database",
		Run: func(cmd *cobra.Command, args []string) {
			res, _, err := api.Post("/databases", map[string]any{"name": args[0]})

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			fmt.Print(
				components.Container(
					components.SuccessAlert(res["message"].(string)),
					components.DatabaseCard(res["data"].(map[string]any)),
				),
			)
		},
	}
}
