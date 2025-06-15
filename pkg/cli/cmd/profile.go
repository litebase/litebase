package cmd

import "github.com/spf13/cobra"

var ProfileCmd = &cobra.Command{
	Use:   "profile",
	Short: "Manage your profiles",
	Long:  `Manage your profiles`,
	Args:  cobra.MinimumNArgs(1),
}

func NewProfileCmd() *cobra.Command {
	ProfileCmd.AddCommand(NewProfileCreateCmd())
	ProfileCmd.AddCommand(NewProfileCurrentCmd())
	ProfileCmd.AddCommand(NewProfileDeleteCmd())
	ProfileCmd.AddCommand(NewProfileListCmd())
	ProfileCmd.AddCommand(NewProfileSwitchCmd())

	return ProfileCmd
}
