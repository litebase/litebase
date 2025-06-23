package cmd

import (
	"fmt"
	"os"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewInitCmd() *cobra.Command {
	return NewCommand(
		"init", "Initialize a new Litebase cluster",
	).WithConfig(func(cmd *cobra.Command) {
		err := godotenv.Load(".env")

		if err != nil {
			panic(err)
		}

		clusterId := cmd.Flag("cluster-id").Value.String()

		if clusterId != "" {
			err := os.Setenv("LITEBASE_CLUSTER_ID", clusterId)

			if err != nil {
				panic(err)
			}
		}

		encryptionKey := cmd.Flag("key").Value.String()

		if encryptionKey != "" {
			err := os.Setenv("LITEBASE_ENCRYPTION_KEY", encryptionKey)

			if err != nil {
				panic(err)
			}
		}

		username := cmd.Flag("username").Value.String()

		if username != "" {
			err := os.Setenv("LITEBASE_ROOT_USERNAME", username)

			if err != nil {
				panic(err)
			}
		}

		password := cmd.Flag("password").Value.String()

		if password != "" {
			err := os.Setenv("LITEBASE_ROOT_PASSWORD", password)

			if err != nil {
				panic(err)
			}
		}

	}).WithFlags(func(cmd *cobra.Command) {
		cmd.Flags().String("cluster-id", "", "Provide an ID when initializing new clusters")
		cmd.Flags().String("key", "", "The key (256-bit hash digest) to use when initializing a cluster")
		cmd.Flags().String("username", "", "The username of the initial root user of the cluster")
		cmd.Flags().String("password", "", "The password of the initial root user of the cluster")
	}).WithRun(func(cmd *cobra.Command, args []string) {
		fmt.Println("")
		fmt.Println("Initializing Litebase cluster...")

		// Run the initialization steps
		configInstance := config.NewConfig()

		cluster, err := cluster.NewCluster(configInstance)

		if err != nil {
			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(components.ErrorAlert(
					fmt.Sprintf("[Litebase Error]: %s", err.Error()),
				)),
			)
		}

		authInstance := auth.NewAuth(
			cluster.Config,
			cluster.NetworkFS(),
			cluster.ObjectFS(),
			cluster.TmpFS(),
			cluster.TmpTieredFS(),
		)

		err = cluster.Init(authInstance)

		if err != nil {
			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(components.ErrorAlert(
					fmt.Sprintf("[Litebase Error]: %s", err.Error()),
				)),
			)

			return
		}

		// Initialize the key manager
		err = auth.KeyManagerInit(
			configInstance,
			authInstance.SecretsManager,
		)

		if err != nil {

			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(components.ErrorAlert(
					fmt.Sprintf("[Litebase Error]: %s", err.Error()),
				)),
			)

			return
		}

		err = authInstance.UserManager().Init()

		if err != nil {
			fmt.Fprint(
				cmd.OutOrStdout(),
				components.Container(components.ErrorAlert(
					fmt.Sprintf("[Litebase Error]: %s", err.Error()),
				)),
			)

			return
		}

		fmt.Fprint(
			cmd.OutOrStdout(),
			components.Container(
				components.SuccessAlert("Litebase cluster initialized successfully"),
			),
		)
	}).Build()
}
