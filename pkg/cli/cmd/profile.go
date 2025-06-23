package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var ProfileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Manage your profiles",
	Long:  `Manage your profiles`,
	Args:  cobra.MinimumNArgs(1),
}

func NewProfileCmd(c *config.Configuration) *cobra.Command {
	ProfileCmd.AddCommand(NewProfileCreateCmd(c))
	ProfileCmd.AddCommand(NewProfileCurrentCmd(c))
	ProfileCmd.AddCommand(NewProfileDeleteCmd(c))
	ProfileCmd.AddCommand(NewProfileListCmd(c))
	ProfileCmd.AddCommand(NewProfileSwitchCmd(c))

	return ProfileCmd
}
