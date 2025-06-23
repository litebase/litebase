package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var LoginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to your Litebase account",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Login successful")
		return nil
	},
}

var LogoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Logout your Litebase account",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Println("Logout successful")
		return nil
	},
}
