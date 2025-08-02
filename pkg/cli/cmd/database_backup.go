package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewDatabaseBackupCmd(config *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Manage database backups",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewDatabaseBackupCreateCmd(config))
	cmd.AddCommand(NewDatabaseBackupDeleteCmd(config))
	cmd.AddCommand(NewDatabaseBackupListCmd(config))
	cmd.AddCommand(NewDatabaseBackupShowCmd(config))

	return cmd
}
