package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBranchSettingsCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "settings",
		Short: "Manage database branch settings",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewDatabaseBranchSettingsShowCmd(config))
	cmd.AddCommand(NewDatabaseBranchSettingsUpdateCmd(config))

	return cmd
}
