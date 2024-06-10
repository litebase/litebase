package cmd

import (
	"fmt"
	"litebasedb/server"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
)

func NewServeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the LitebaseDB server",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("serve called")

			godotenv.Load(".env")

			server.NewServer().Start(func(s *server.ServerInstance) {
				server.NewApp(s).Run()
			})
		},
	}
}
