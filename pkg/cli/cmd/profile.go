package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewProfileCmd(c *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "profile",
		Short: "Manage Litebase CLI profiles",
		Long:  `Manage Litebase CLI profiles`,
		Args:  cobra.MinimumNArgs(1),
	}

	cmd.AddCommand(NewProfileCreateCmd(c))
	cmd.AddCommand(NewProfileCurrentCmd(c))
	cmd.AddCommand(NewProfileDeleteCmd(c))
	cmd.AddCommand(NewProfileListCmd(c))
	cmd.AddCommand(NewProfileSwitchCmd(c))

	return cmd
}
