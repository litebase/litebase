package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"

	"github.com/spf13/cobra"
)

var ClusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Manage Litebase clusters",
	Args:  cobra.MinimumNArgs(1),
}

var ClusterCreateCmd = &cobra.Command{
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

var ClusterStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Get the status of a cluster",
	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := api.Get("status")

		if err != nil {
			fmt.Print(components.Container(components.ErrorAlert(err.Error())))
			return err
		}

		fmt.Println(data)

		return nil
	},
}

func NewClusterCmd() *cobra.Command {
	ClusterCmd.AddCommand(ClusterCreateCmd)
	ClusterCmd.AddCommand(ClusterStatusCmd)

	return ClusterCmd
}
