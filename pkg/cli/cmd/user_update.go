package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewUserUpdateCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "update <username>",
		Short: "Update a user",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return cobra.MinimumNArgs(1)(cmd, args)
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
}
