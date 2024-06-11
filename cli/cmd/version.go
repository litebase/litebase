package cmd

import (
	"fmt"
	"litebase/cli/components"
	"litebase/cli/styles"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version number of CLI",
	Run: func(cmd *cobra.Command, args []string) {
		style := lipgloss.NewStyle().
			Background(styles.PrimaryBackgroundColor).
			Foreground(styles.PrimaryForegroundColor).
			Padding(1, 2)

		fmt.Print(
			components.Container(style.Render("Litebase CLI -→ v0.0.1")),
		)
	},
}
