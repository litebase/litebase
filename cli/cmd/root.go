package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
)

func addCommands(cmd *cobra.Command) {
	cmd.AddCommand(NewDatabaseCmd())
	cmd.AddCommand(LoginCmd)
	cmd.AddCommand(LogoutCmd)
	cmd.AddCommand(NewSQLCmd())
}

func NewRoot() error {
	cmd := &cobra.Command{
		Use:   "litebasedb <command> <subcommand> [flags]",
		Short: "LitebaseDB CLI",
		Long:  `Connect with LitebaseDB from the command line.`,
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("LitebaseDB – use'litebasedb help' for more information.")
		},
	}

	addCommands(cmd)

	return cmd.ExecuteContext(context.Background())
}
