package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewAccessKeyCmd(config *config.Configuration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "access-key",
		Short: "Manage access keys",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewAccessKeyListCmd(config))
	cmd.AddCommand(NewAccessKeyCreateCmd(config))
	cmd.AddCommand(NewAccessKeyShowCmd(config))
	cmd.AddCommand(NewAccessKeyDeleteCmd(config))
	cmd.AddCommand(NewAccessKeyUpdateCmd(config))

	return cmd
}
