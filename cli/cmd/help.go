package cmd

import (
	"fmt"
	"litebasedb/cli/components"
	"litebasedb/cli/styles"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func NewHelpCmd() func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		commands := prepareCommands(cmd)
		flags := prepareFlags(cmd)

		fmt.Print(
			components.Container(
				// Usage Section
				lipgloss.NewStyle().
					Background(styles.PrimaryBackgroundColor).
					Foreground(styles.PrimaryForegroundColor).
					Padding(0, 1).Render(cmd.Short),
				lipgloss.NewStyle().Padding(1, 0).Render(cmd.Long),
				lipgloss.NewStyle().
					Padding(0, 1).
					MarginTop(1).
					Background(lipgloss.AdaptiveColor{Light: "#000000", Dark: "#FFFFFF"}).
					Foreground(lipgloss.AdaptiveColor{Light: "#FFFFFF", Dark: "#000000"}).
					Render("Usage"),
				lipgloss.NewStyle().MarginTop(1).PaddingLeft(2).Render(cmd.UseLine()),
				commands,
				flags,
				// "Use \"litebasedb [command] --help\" for more information about a command.",
			),
		)
	}
}

func prepareCommands(cmd *cobra.Command) string {
	var commands string = ""
	var longesteCommandName int = 0

	for _, c := range cmd.Commands() {
		if len(c.Use) > longesteCommandName {
			longesteCommandName = len(c.Use)
		}
	}

	commands += lipgloss.NewStyle().
		Margin(1, 0).
		Padding(0, 1).
		Background(lipgloss.AdaptiveColor{Light: "#000000", Dark: "#FFFFFF"}).
		Foreground(lipgloss.AdaptiveColor{Light: "#FFFFFF", Dark: "#000000"}).
		Render("Commands")

	for _, c := range cmd.Commands() {
		name := c.Use

		for i := 0; i < longesteCommandName-len(c.Use); i++ {
			name += " "
		}

		commands += lipgloss.NewStyle().MarginTop(1).
			PaddingLeft(2).
			Render(fmt.Sprintf("%s\t%s", name, c.Short))
	}

	return commands
}

func prepareFlags(cmd *cobra.Command) string {
	var (
		flags []struct {
			Name  string
			Usage string
		}
		flagString       string = ""
		longesteFlagName int    = 0
	)

	flagString += lipgloss.NewStyle().
		Margin(1, 0).
		Padding(0, 1).
		Background(lipgloss.AdaptiveColor{Light: "#000000", Dark: "#FFFFFF"}).
		Foreground(lipgloss.AdaptiveColor{Light: "#FFFFFF", Dark: "#000000"}).
		Render("Flags")

	flagSet := cmd.LocalFlags()

	var lines []string

	flagSet.VisitAll(func(f *pflag.Flag) {
		name := ""

		if len(f.Shorthand) > 0 {
			name = fmt.Sprintf("-%s, --%s", f.Shorthand, f.Name)
		} else {
			name = fmt.Sprintf("--%s", f.Name)
		}

		flags = append(flags, struct {
			Name  string
			Usage string
		}{
			Name:  name,
			Usage: strings.Replace(f.Usage, "\t", "__", -1),
		})
	})

	for _, f := range flags {
		if len(f.Name) > longesteFlagName {
			longesteFlagName = len(f.Name)
		}
	}

	for fi, f := range flags {
		name := f.Name
		for i := 0; i < longesteFlagName-len(f.Name); i++ {
			name += " "
		}

		flags[fi].Name = name
	}

	for _, f := range flags {
		lines = append(lines, fmt.Sprintf("%s\t%s", f.Name, f.Usage))
	}

	for _, f := range lines {
		flagString += lipgloss.NewStyle().MarginTop(1).MarginLeft(2).Render(f)
	}

	return flagString
}
