package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBranchCmd(config *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "branch",
		Short: "Manage database branches",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewDatabaseBranchCreateCmd(config))
	// cmd.AddCommand(NewDatabaseBranchDeleteCmd(config))
	// cmd.AddCommand(NewDatabaseBranchListCmd(config))
	// cmd.AddCommand(NewDatabaseBranchShowCmd(config))

	return cmd
}
