package cmd

import (
	"fmt"
	"litebasedb/cli/components"
	"litebasedb/cli/styles"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of LitebaseDB",
	Run: func(cmd *cobra.Command, args []string) {
		style := lipgloss.NewStyle().
			Background(styles.PrimaryBackgroundColor).
			Foreground(styles.PrimaryForegroundColor).
			Padding(1, 2)

		fmt.Print(
			components.Container(style.Render("LitebaseDB CLI -→ v0.0.1")),
		)
	},
}
