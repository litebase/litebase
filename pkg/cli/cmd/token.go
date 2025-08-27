package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewTokenCmd(c *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "Manage tokens",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewTokenCreateCmd(c))
	cmd.AddCommand(NewTokenDeleteCmd(c))
	cmd.AddCommand(NewTokenListCmd(c))
	cmd.AddCommand(NewTokenShowCmd(c))
	cmd.AddCommand(NewTokenUpdateCmd(c))

	return cmd
}
