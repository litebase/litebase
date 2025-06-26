package cmd

import (
	"os/exec"
	"runtime"

	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/spf13/cobra"
)

func NewAuthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Authentication commands",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(NewLoginCmd())
	cmd.AddCommand(NewLogoutCmd())

	return cmd
}

// TODO: Implement actual login/logout functionality with access keys
// TODO: Start a local server to wait for redirect/response from litebase.com
// TODO: Store a new profile in ~/.litebase/config.json?
func NewLoginCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "login",
		Short: "Login to your Litebase account",
		RunE: func(cmd *cobra.Command, args []string) error {
			// err := openBrowser("https://litebase.com")

			// if err != nil {
			// 	return fmt.Errorf("failed to open browser: %w", err)
			// }

			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.InfoAlert("Login coming soon..."),
				),
			)
			return nil
		},
	}
}

// TODO: remove the stored profile from ~/.litebase/config.json?
func NewLogoutCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Logout your Litebase account",
		RunE: func(cmd *cobra.Command, args []string) error {
			lipgloss.Fprint(
				cmd.OutOrStdout(),
				components.Container(
					components.InfoAlert("Logout coming soon..."),
				),
			)

			return nil
		},
	}
}

// openBrowser opens the specified URL in the default browser
func openBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}

	args = append(args, url)

	return exec.Command(cmd, args...).Start()
}
