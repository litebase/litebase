package cmd

import (
	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/spf13/cobra"
)

var ClusterUserCmd = &cobra.Command{
	Use:   "cluster-user",
	Short: "Manage cluster users",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		err := cmd.Help()

		if err != nil {
			return err
		}

		return nil
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
	RunE: func(cmd *cobra.Command, args []string) error {
		return nil
	},
}

func NewUserCmd(config *config.Configuration) *cobra.Command {
	ClusterUserCmd.AddCommand(NewClusterUserListCmd(config))
	ClusterUserCmd.AddCommand(NewClusterUserCreateCmd(config))
	ClusterUserCmd.AddCommand(NewClusterUserDeleteCmd(config))
	ClusterUserCmd.AddCommand(ClusterUserUpdateCmd)

	return ClusterUserCmd
}
