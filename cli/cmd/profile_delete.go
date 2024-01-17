package cmd

import (
	"fmt"
	"litebasedb/cli/components"
	"litebasedb/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a profile",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			err := config.DeleteProfile(args[0])

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return
			}

			fmt.Print(components.Container(components.SuccessAlert("Profile deleted successfully")))
		},
	}
}
