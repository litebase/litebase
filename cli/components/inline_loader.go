package components

import (
	"time"

	"github.com/litebase/litebase/cli/styles"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type InlineLoader struct {
	spinnerModel spinner.Model
}

func NewInlineLoader() *InlineLoader {
	s := spinner.New()
	s.Spinner = spinner.Spinner{
		Frames: []string{
			"010010",
			"001100",
			"100101",
			"111010",
			"111101",
			"010111",
			"101011",
			"111000",
			"110011",
			"110101",
		},
		FPS: time.Second / 8,
	}

	s.Style = lipgloss.NewStyle().Foreground(styles.PimaryTextColor)

	return &InlineLoader{
		spinnerModel: s,
	}
}

func (l *InlineLoader) Tick() tea.Cmd {
	return l.spinnerModel.Tick
}

func (l *InlineLoader) Update(msg tea.Msg) (spinner.Model, tea.Cmd) {
	var cmd tea.Cmd

	l.spinnerModel, cmd = l.spinnerModel.Update(msg)

	return l.spinnerModel, cmd
}

func (l *InlineLoader) View() string {
	return lipgloss.NewStyle().Border(
		lipgloss.RoundedBorder(),
	).
		MarginTop(2).
		Padding(0, 1).
		BorderForeground(styles.PimaryTextColor).
		Foreground(styles.PimaryTextColor).
		Render(l.spinnerModel.View())
}
