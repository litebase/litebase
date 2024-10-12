package cmd

import (
	"litebase/server"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewServeCmd() *cobra.Command {
	var app *server.App

	return NewCommand(
		"serve", "Start the Litebase server locally",
	).WithConfig(func(cmd *cobra.Command) {
		godotenv.Load(".env")

		dataPath := cmd.Flag("data-path").Value.String()

		if dataPath != "" {
			os.Setenv("LITEBASE_DATA_PATH", dataPath)
		}

		debug := cmd.Flag("debug").Value.String()

		if debug != "" {
			os.Setenv("DEBUG", debug)
		}
	}).WithFlags(func(cmd *cobra.Command) {
		cmd.Flags().String("data-path", "./.litebase", "The path to the data directory")
		cmd.Flags().Bool("debug", false, "Run the server in debug mode")
		cmd.Flags().Bool("primary", true, "Run the server as a primary node")
		cmd.Flags().String("port", "8080", "The port to run the server on")
		cmd.Flags().Bool("replica", false, "Run the server as a replica node")
		cmd.Flags().String("signature", "", "The signature to use for server encryption")
		cmd.Flags().String("tmp-path", "./litebase-tmp", "The directory to use for temporary files")
		cmd.Flags().String("tls-cert", "", "The path to the TLS certificate")
		cmd.Flags().String("tls-key", "", "The path to the TLS key")
	}).WithRun(func(cmd *cobra.Command, args []string) {
		server.NewServer().Start(func(s *http.ServeMux) {
			app = server.NewApp(s)

			app.Run()

			err := app.Cluster.Node().Start()

			if err != nil {
				log.Fatalf("Node start: %v", err)
			}
		}, func() {
			if app == nil {
				return
			}

			app.Cluster.Node().Shutdown()
		})
	}).Build()
}
