package cmd

import "github.com/spf13/cobra"

var UserUpdateCmd = &cobra.Command{
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
