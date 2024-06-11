package cmd

import (
	"litebase/server"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the Litebase server locally",
		Run: func(cmd *cobra.Command, args []string) {
			godotenv.Load(".env")

			// TODO: Accept flags to congifure the server

			server.NewServer().Start(func(s *server.ServerInstance) {
				server.NewApp(s).Run()
			})
		},
	}
}
