package cmd

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/components"
	"github.com/litebase/litebase/pkg/cli/styles"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version number of CLI",
	RunE: func(cmd *cobra.Command, args []string) error {
		style := lipgloss.NewStyle().
			Background(styles.PrimaryBackgroundColor).
			Foreground(styles.PrimaryForegroundColor).
			Padding(1, 2)

		fmt.Print(
			components.Container(style.Render("Litebase CLI -→ v0.0.1")),
		)

		return nil
	},
}
