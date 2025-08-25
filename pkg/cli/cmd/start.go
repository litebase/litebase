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
		},
	}

	// Configuration (setup before command runs)
	cobra.OnInitialize(func() {
		if debug := cmd.Flag("debug").Value.String(); debug != "" {
			if err := os.Setenv("DEBUG", debug); err != nil {
				panic(err)
			}
		}

		if port := cmd.Flag("port").Value.String(); port != "" {
			if err := os.Setenv("LITEBASE_PORT", port); err != nil {
				panic(err)
			}
		}

		if dataPath := cmd.Flag("storage-path").Value.String(); dataPath != "" {
			if err := os.Setenv("LITEBASE_DATA_PATH", dataPath); err != nil {
				panic(err)
			}
		}

		if networkPath := cmd.Flag("storage-network-path").Value.String(); networkPath != "" {
			if err := os.Setenv("LITEBASE_STORAGE_NETWORK_PATH", networkPath); err != nil {
				panic(err)
			}
		}

		if tmpPath := cmd.Flag("storage-tmp-path").Value.String(); tmpPath != "" {
			if err := os.Setenv("LITEBASE_STORAGE_TMP_PATH", tmpPath); err != nil {
				panic(err)
			}
		}

		if tlsCert := cmd.Flag("tls-cert").Value.String(); tlsCert != "" {
			if err := os.Setenv("LITEBASE_TLS_CERT_PATH", tlsCert); err != nil {
				panic(err)
			}
		}

		if tlsKey := cmd.Flag("tls-key").Value.String(); tlsKey != "" {
			if err := os.Setenv("LITEBASE_TLS_KEY_PATH", tlsKey); err != nil {
				panic(err)
			}
		}
	})

	// Flags
	cmd.Flags().Bool("debug", false, "Run the server in debug mode")
	cmd.Flags().String("port", "8080", "The port to run the server on")
	cmd.Flags().String("key", "", "The key to use for server encryption")
	cmd.Flags().String("storage-path", "", "The path to the data directory")
	cmd.Flags().String("storage-network-path", "", "The path to use for network storage")
	cmd.Flags().String("storage-tmp-path", "", "The path to use for temporary files")
	cmd.Flags().String("tls-cert", "", "The path to the TLS certificate")
	cmd.Flags().String("tls-key", "", "The path to the TLS key")

	hideAuthFlags(cmd)

	return cmd
}
