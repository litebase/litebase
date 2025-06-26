package cmd

import (
	"log"
	"net/http"
	"os"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewServeCmd() *cobra.Command {
	var app *server.App

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Litebase server locally",
		Run: func(cmd *cobra.Command, args []string) {
			configInstance := config.NewConfig()

			server.NewServer(configInstance).Start(func(s *http.ServeMux) {
				app = server.NewApp(configInstance, s)

				app.Run()

				err := app.Cluster.Node().Start()

				if err != nil {
					log.Fatalf("Node start: %v", err)
				}
			}, func() {
				if app == nil {
					return
				}

				err := app.Cluster.Node().Shutdown()

				if err != nil {
					log.Fatalf("Node shutdown: %v", err)
				}
			})
		},
	}

	// Configuration (setup before command runs)
	cobra.OnInitialize(func() {
		err := godotenv.Load(".env")

		if err != nil {
			panic(err)
		}

		dataPath := cmd.Flag("data-path").Value.String()

		if dataPath != "" {
			err := os.Setenv("LITEBASE_DATA_PATH", dataPath)

			if err != nil {
				panic(err)
			}
		}

		debug := cmd.Flag("debug").Value.String()

		if debug != "" {
			err := os.Setenv("DEBUG", debug)

			if err != nil {
				panic(err)
			}
		}
	})

	// Flags
	cmd.Flags().String("data-path", "./.litebase", "The path to the data directory")
	cmd.Flags().Bool("debug", false, "Run the server in debug mode")
	cmd.Flags().Bool("primary", true, "Run the server as a primary node")
	cmd.Flags().String("port", "8080", "The port to run the server on")
	cmd.Flags().Bool("replica", false, "Run the server as a replica node")
	cmd.Flags().String("key", "", "The key to use for server encryption")
	cmd.Flags().String("tmp-path", "./litebase-tmp", "The directory to use for temporary files")
	cmd.Flags().String("tls-cert", "", "The path to the TLS certificate")
	cmd.Flags().String("tls-key", "", "The path to the TLS key")

	return cmd
}
