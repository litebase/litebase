package cmd

import (
	"log"
	"log/slog"
	"net/http"
	"os"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewStartCmd() *cobra.Command {
	var app *server.App

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start Litebase Server",
		Run: func(cmd *cobra.Command, args []string) {
			if err := godotenv.Load(".env"); err != nil {
				slog.Debug("Server .env file not loaded", "error", err)
			}

			configInstance := config.NewConfig()

			server.NewServer(configInstance).
				OnStarted(func() {
					_, err := lipgloss.Fprint(
						cmd.OutOrStdout(),
						components.Container(
							components.NewCard(
								components.WithCardTitle("Litebase Server"),
								components.WithCardRows([]components.CardRow{
									{
										Key:   "Port",
										Value: configInstance.Port,
									},
									{
										Key:   "Cluster ID",
										Value: app.Cluster.ID,
									},
									{
										Key:   "Node ID",
										Value: app.Cluster.Node().ID,
									},
								}),
							).Render(),
						),
					)

					if err != nil {
						log.Fatalf("Error printing server info: %v", err)
					}
				}).
				Start(func(s *http.ServeMux) {
					app = server.NewApp(configInstance, s)

					app.Run()

					<-app.Cluster.Node().Start()
				}, func() {
					if app == nil {
						return
					}

					err := app.Cluster.Node().Shutdown()

					if err != nil {
						log.Fatalf("Node shutdown: %v", err)
					}
				})

			log.Println("FOOOO")
		},
	}

	// Configuration (setup before command runs)
	cobra.OnInitialize(func() {
		err := godotenv.Load(".env")

		if err != nil {
			slog.Debug("Error loading .env file", "error", err)
		}

		dataPath := cmd.Flag("data-path").Value.String()

		if dataPath != "" {
			err := os.Setenv("LITEBASE_DATA_PATH", dataPath)

			if err != nil {
				panic(err)
			}
		}

		port := cmd.Flag("port").Value.String()

		if port != "" {
			if err := os.Setenv("LITEBASE_PORT", port); err != nil {
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
	cmd.Flags().String("key", "", "The key to use for server encryption")
	cmd.Flags().String("tmp-path", "./litebase-tmp", "The directory to use for temporary files")
	cmd.Flags().String("tls-cert", "", "The path to the TLS certificate")
	cmd.Flags().String("tls-key", "", "The path to the TLS key")

	return cmd
}
