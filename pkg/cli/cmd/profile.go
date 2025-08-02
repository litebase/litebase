package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewProfileCmd(c *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage your profiles",
		Long:  `Manage your profiles`,
		Args:  cobra.MinimumNArgs(1),
	}

	cmd.AddCommand(NewProfileCreateCmd(c))
	cmd.AddCommand(NewProfileCurrentCmd(c))
	cmd.AddCommand(NewProfileDeleteCmd(c))
	cmd.AddCommand(NewProfileListCmd(c))
	cmd.AddCommand(NewProfileSwitchCmd(c))

	return cmd
}
