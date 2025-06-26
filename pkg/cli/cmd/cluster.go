package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/config"

	"github.com/spf13/cobra"
)

var ClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Manage Litebase clusters",
	Args:  cobra.MinimumNArgs(1),
}

func NewClusterCreateCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Args:  cobra.ExactArgs(1),
		Short: "Create a new cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: Check if current team is selected
			// TODO: Check if user is logged in
			// TODO: Send request to service to create cluster
			fmt.Println("Cluster created:", args[0])
			return nil
		},
	}
}

func NewClusterCmd(config *config.Configuration) *cobra.Command {
	ClusterCmd.AddCommand(NewClusterCreateCmd(config))

	return ClusterCmd
}
