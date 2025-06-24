package components

import (
	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/lipgloss"
	"github.com/litebase/litebase/pkg/cli"
)

func NewForm(groups ...*huh.Group) *huh.Form {
	theme := huh.ThemeBase()

	// Add top margin to the form
	theme.Form.Base = theme.Form.Base.MarginTop(1)
	theme.Focused.Base = theme.Focused.Base.BorderForeground(
		lipgloss.AdaptiveColor{Light: cli.Gray400.Hex(), Dark: cli.Gray500.Hex()},
	)

	theme.Blurred.NoteTitle = lipgloss.NewStyle().
		MarginBottom(1).
		Padding(0, 1).
		Bold(true).
		SetString("►")

	theme.Focused.Title = theme.Group.Title.
		MarginBottom(1).
		Padding(0, 1).
		Bold(true).
		Background(
			lipgloss.AdaptiveColor{Light: cli.Sky700.Hex(), Dark: cli.Sky300.Hex()},
		).
		Foreground(
			lipgloss.AdaptiveColor{Light: cli.White.Hex(), Dark: cli.Black.Hex()},
		)

	theme.Focused.Directory = theme.Focused.Directory.Foreground(lipgloss.AdaptiveColor{Light: cli.Sky700.Hex(), Dark: cli.Sky300.Hex()})
	theme.Focused.Description = theme.Focused.Description.Foreground(lipgloss.AdaptiveColor{Light: "", Dark: "243"})
	theme.Focused.ErrorIndicator = theme.Focused.ErrorIndicator.Foreground(lipgloss.Color(cli.Red700.Hex()))
	theme.Focused.ErrorMessage = theme.Focused.ErrorMessage.Foreground(lipgloss.Color(cli.Red700.Hex()))
	theme.Focused.SelectSelector = theme.Focused.SelectSelector.Foreground(lipgloss.Color(cli.Pink500.Hex()))
	theme.Focused.NextIndicator = theme.Focused.NextIndicator.Foreground(lipgloss.Color(cli.Pink500.Hex()))
	theme.Focused.PrevIndicator = theme.Focused.PrevIndicator.Foreground(lipgloss.Color(cli.Pink500.Hex()))
	theme.Focused.Option = theme.Focused.Option.Foreground(lipgloss.Color(cli.Gray500.Hex()))
	theme.Focused.MultiSelectSelector = theme.Focused.MultiSelectSelector.Foreground(lipgloss.Color(cli.Pink500.Hex()))
	theme.Focused.SelectedOption = theme.Focused.SelectedOption.Foreground(lipgloss.Color(cli.Pink500.Hex()))
	theme.Focused.SelectedPrefix = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "#02CF92", Dark: "#02A877"}).SetString("✓ ")
	theme.Focused.UnselectedPrefix = lipgloss.NewStyle().Foreground(lipgloss.AdaptiveColor{Light: "", Dark: "243"}).SetString("• ")

	theme.Focused.FocusedButton = theme.Focused.FocusedButton.
		Background(
			lipgloss.AdaptiveColor{Light: cli.Sky700.Hex(), Dark: cli.Sky300.Hex()},
		).
		Foreground(
			lipgloss.AdaptiveColor{Light: cli.White.Hex(), Dark: cli.Black.Hex()},
		)
	theme.Focused.Next = theme.Focused.FocusedButton
	theme.Focused.BlurredButton = theme.Focused.BlurredButton.
		Background(
			lipgloss.AdaptiveColor{Light: cli.Gray300.Hex(), Dark: cli.Gray100.Hex()},
		).
		Foreground(
			lipgloss.AdaptiveColor{Light: cli.Gray900.Hex(), Dark: cli.Gray900.Hex()},
		)
	theme.Blurred.FocusedButton = theme.Focused.BlurredButton.
		Background(
			lipgloss.AdaptiveColor{Light: cli.Gray300.Hex(), Dark: cli.Gray500.Hex()},
		).
		Foreground(
			lipgloss.AdaptiveColor{Light: cli.Gray900.Hex(), Dark: cli.Gray100.Hex()},
		)
	theme.Blurred.BlurredButton = theme.Focused.BlurredButton.
		Background(
			lipgloss.AdaptiveColor{Light: cli.Gray300.Hex(), Dark: cli.Gray500.Hex()},
		).
		Foreground(
			lipgloss.AdaptiveColor{Light: cli.Gray900.Hex(), Dark: cli.Gray100.Hex()},
		)

	theme.Focused.TextInput.Cursor = theme.Focused.TextInput.Cursor.Foreground(lipgloss.Color(cli.Pink500.Hex()))
	theme.Focused.TextInput.Prompt = theme.Focused.TextInput.Prompt.Foreground(lipgloss.Color(cli.Pink500.Hex()))

	theme.Blurred.Title = theme.Group.Title.
		MarginBottom(1).
		Padding(0, 1).
		Bold(true).
		Background(
			lipgloss.AdaptiveColor{Light: cli.Gray300.Hex(), Dark: cli.Gray100.Hex()},
		).
		Foreground(
			lipgloss.AdaptiveColor{Light: cli.Gray900.Hex(), Dark: cli.Gray900.Hex()},
		)

	return huh.NewForm(groups...).
		WithHeight(20).
		WithTheme(theme)
}
