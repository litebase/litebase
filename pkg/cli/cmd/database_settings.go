package cmd

import "github.com/spf13/cobra"

var DatabaseSettingsCmd = &cobra.Command{
	Use:   "settings",
	Short: "Manage database settings",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var DatabaseSettingsUpdateCmd = &cobra.Command{
	Use:   "update <id>",
	Short: "Update database settings",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func NewDatabaseSettingsCmd() *cobra.Command {
	DatabaseSettingsCmd.AddCommand(DatabaseSettingsUpdateCmd)

	return DatabaseSettingsCmd
}
