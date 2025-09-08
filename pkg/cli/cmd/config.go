package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewConfigCmd(c *config.CLIConfiguration) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage Litebase Server configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.AddCommand(NewConfigInitCmd(c))
	cmd.AddCommand(NewConfigShowCmd(c))

	hideAuthFlags(cmd)

	return cmd
}
