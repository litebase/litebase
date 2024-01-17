package components

import (
	"fmt"
	"litebasedb/cli/styles"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Button struct {
	focused bool
	text    string
}

func NewButton(text string) *Button {
	return &Button{
		text: text,
	}
}

func (b *Button) Blur() Input {
	b.focused = false

	return b
}

func (b *Button) Errors(errors []string) {
}

func (b *Button) Focus() (Input, tea.Cmd) {
	b.focused = true

	return b, nil
}

func (b *Button) Focused() bool {
	return b.focused
}

func (b *Button) Label(label string) Input {
	return b
}

func (b *Button) Name() string {
	return ""
}

func (b *Button) Placeholder(placeholder string) Input {
	return b
}

func (b *Button) Update(msg tea.Msg) tea.Cmd {
	return nil
}

func (b *Button) Value() interface{} {
	return nil
}

func (b *Button) style() lipgloss.Style {
	return lipgloss.NewStyle().
		Foreground(styles.TextColor).
		Padding(0, 1).
		Border(lipgloss.RoundedBorder())
}

// Render
func (b *Button) View() string {
	if b.focused {
		return b.style().
			Copy().
			BorderForeground(styles.PimaryTextColor).
			Foreground(styles.PimaryTextColor).
			Render(fmt.Sprintf("%s →", b.text))
	}

	return b.style().Render(b.text)
}
