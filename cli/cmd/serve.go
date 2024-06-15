package cmd

import (
	"litebase/server"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Litebase server locally",
		Run: func(cmd *cobra.Command, args []string) {
			godotenv.Load(".env")

			configureFromFlags(cmd)

			server.NewServer().Start(func(s *server.ServerInstance) {
				server.NewApp(s).Run()
			})
		},
	}

	setFlags(cmd)

	return cmd

}

func configureFromFlags(cmd *cobra.Command) {
	dataPath := cmd.Flag("data-path").Value.String()

	if dataPath != "" {
		os.Setenv("LITEBASEDB_DATA_PATH", dataPath)
	}

	debug := cmd.Flag("debug").Value.String()

	if debug != "" {
		os.Setenv("DEBUG", debug)
	}
}

func setFlags(cmd *cobra.Command) {
	cmd.Flags().String("data-path", "./.litebase", "The path to the data directory")
	cmd.Flags().Bool("debug", false, "Run the server in debug mode")
	cmd.Flags().Bool("primary", true, "Run the server as a primary node")
	cmd.Flags().String("port", "8080", "The port to run the server on")
	cmd.Flags().Bool("replica", false, "Run the server as a replica node")
	cmd.Flags().String("signature", "", "The signature to use for server encryption")
	cmd.Flags().String("tmp-path", "./litebase-tmp", "The directory to use for temporary files")
	cmd.Flags().String("tls-cert", "", "The path to the TLS certificate")
	cmd.Flags().String("tls-key", "", "The path to the TLS key")
}
