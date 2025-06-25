package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

func NewProfileDeleteCmd(c *config.Configuration) *cobra.Command {
	return NewCommand("delete <name>", "Delete a profile").
		WithArgs(cobra.ExactArgs(1)).
		WithRunE(func(cmd *cobra.Command, args []string) error {
			err := c.DeleteProfile(args[0])

			if err != nil {
				return err
			}

			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(components.SuccessAlert("Profile deleted successfully")),
			)

			return nil
		}).Build()
}
