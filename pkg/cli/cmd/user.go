package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

func NewUserCmd(config *config.Configuration) *cobra.Command {
	UserCmd := &cobra.Command{
		Use:   "user",
		Short: "Manage users",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cmd.Help()

			if err != nil {
				return err
			}

			return nil
		},
	}

	UserCmd.AddCommand(NewUserListCmd(config))
	UserCmd.AddCommand(NewUserCreateCmd(config))
	UserCmd.AddCommand(NewUserDeleteCmd(config))
	UserCmd.AddCommand(NewUserUpdateCmd(config))

	return UserCmd
}
