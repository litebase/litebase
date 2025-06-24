package styles

import (
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/litebase/litebase/pkg/cli"
)

var PrimaryTextColor = cli.LightDark(cli.White, cli.Black)
var PrimaryBackgroundColor = cli.LightDark(cli.Sky500, cli.Sky300)
var PrimaryForegroundColor = cli.LightDark(cli.White, cli.White)
var TextColor = cli.LightDark(cli.Black, cli.White)

var CursorStyle = lipgloss.NewStyle().Foreground(PrimaryTextColor)

var InputStyle = lipgloss.NewStyle().Foreground(TextColor)
var FocusedInputStyle = InputStyle.Foreground(TextColor)
var PlaceholderStyle = lipgloss.NewStyle().Foreground(
	cli.LightDark(cli.Gray300, cli.Gray500),
)

var PromptStyle = lipgloss.NewStyle()
var FocusedPromptStyle = PromptStyle.Foreground(PrimaryTextColor)

var alertStyle = lipgloss.NewStyle().Bold(true).Padding(0, 1)

var AlertSuccessStyle = alertStyle.
	Background(cli.LightDark(cli.Green700, cli.Green200)).
	Foreground(cli.LightDark(cli.White, cli.Black))

var AlertDangerStyle = alertStyle.
	Background(cli.LightDark(cli.Red700, cli.Red100)).
	Foreground(cli.LightDark(cli.White, cli.Black))

var AlertWarningStyle = alertStyle.
	Background(cli.LightDark(cli.Amber600, cli.Amber100)).
	Foreground(cli.LightDark(cli.White, cli.Black))
