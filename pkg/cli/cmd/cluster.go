package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/api"
	"github.com/litebase/litebase/pkg/cli/components"
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

func NewClusterStatusCmd(config *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Get the status of a cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := api.Get(config, "status")

			if err != nil {
				fmt.Print(components.Container(components.ErrorAlert(err.Error())))
				return err
			}

			fmt.Println(data)

			return nil
		},
	}
}

func NewClusterCmd(config *config.Configuration) *cobra.Command {
	ClusterCmd.AddCommand(NewClusterCreateCmd(config))
	ClusterCmd.AddCommand(NewClusterStatusCmd(config))

	return ClusterCmd
}
