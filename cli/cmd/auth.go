package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var LoginCmd = &cobra.Command{
	Use:   "login",
	Short: "Login to your LitebaseDB account",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Login successful")
	},
}

var LogoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Logout of LitebaseDB account",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Logout successful")
	},
}
