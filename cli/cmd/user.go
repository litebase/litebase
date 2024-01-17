package cmd

import (
	"github.com/spf13/cobra"
)

var UserCmd = &cobra.Command{
	Use:   "user",
	Short: "Manage users",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var UserUpdateCmd = &cobra.Command{
	Use:   "update <username>",
	Short: "Update a user",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return cobra.MinimumNArgs(1)(cmd, args)
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func NewUserCmd() *cobra.Command {
	UserCmd.AddCommand(NewUserListCmd())
	UserCmd.AddCommand(NewUserCreateCmd())
	UserCmd.AddCommand(NewUserDeleteCmd())
	UserCmd.AddCommand(UserUpdateCmd)

	return UserCmd
}
