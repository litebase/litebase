package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileDeleteCmd(c *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a profile",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := c.DeleteProfile(args[0])

			if err != nil {
				return err
			}

			fmt.Print(components.Container(components.SuccessAlert("Profile deleted successfully")))

			return nil
		},
	}
}
