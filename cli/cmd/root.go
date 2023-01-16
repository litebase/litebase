package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

func addCommands(cmd *cobra.Command) {
	cmd.AddCommand(NewDatabaseCmd())
	cmd.AddCommand(LoginCmd)
	cmd.AddCommand(LogoutCmd)
	cmd.AddCommand(SQLCmd)
	cmd.AddCommand(NewSQLCmd())
}

func init() {
	cobra.OnInitialize(initConfig)
}

func initConfig() {
	// Load config file here
}

func NewRoot() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "litebasedb <command> <subcommand> [flags]",
		Short: "LitebaseDB CLI",
		Long:  `Connect with LitebaseDB from the command line.`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("LitebaseDB – use'litebasedb help' for more information.")
		},
	}

	addCommands(cmd)

	return cmd
}
