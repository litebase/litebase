package cmd

import (
	"github.com/spf13/cobra"
)

var ClusterUserCmd = &cobra.Command{
	Use:   "cluster-user",
	Short: "Manage cluster users",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := cmd.Help()

		if err != nil {
			panic(err)
		}
	},
}

var ClusterUserUpdateCmd = &cobra.Command{
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
	ClusterUserCmd.AddCommand(NewClusterUserListCmd())
	ClusterUserCmd.AddCommand(NewClusterUserCreateCmd())
	ClusterUserCmd.AddCommand(NewClusterUserDeleteCmd())
	ClusterUserCmd.AddCommand(ClusterUserUpdateCmd)

	return ClusterUserCmd
}
