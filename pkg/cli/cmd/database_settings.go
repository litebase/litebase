package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var DatabaseSettingsCmd = &cobra.Command{
	Use:   "settings",
	Short: "Manage database settings",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := cmd.Help()

		if err != nil {
			return err
		}

		return nil
	},
}

var DatabaseSettingsUpdateCmd = &cobra.Command{
	Use:   "update <id>",
	Short: "Update database settings",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func NewDatabaseSettingsCmd(config *config.Configuration) *cobra.Command {
	DatabaseSettingsCmd.AddCommand(DatabaseSettingsUpdateCmd)

	return DatabaseSettingsCmd
}
