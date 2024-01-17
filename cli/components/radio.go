package components

import (
	"fmt"
	"litebasedb/cli/styles"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Radio struct {
	controlStyle lipgloss.Style
	labelStyle   lipgloss.Style
	checked      bool
	errors       []string
	focused      bool
	name         string
	value        string
}

func (r *Radio) Blur() Input {
	r.controlStyle = styles.PromptStyle
	r.labelStyle = styles.PromptStyle
	r.focused = false

	return r
}

func (r *Radio) Errors(errors []string) {

}

func (r *Radio) Focus() (Input, tea.Cmd) {
	r.controlStyle = styles.FocusedPromptStyle
	r.labelStyle = styles.FocusedPromptStyle
	r.focused = true

	return r, nil
}

func (r *Radio) Focused() bool {
	return r.focused
}

func (r *Radio) Label(label string) Input {
	return r
}

func (r *Radio) Update(msg tea.Msg) tea.Cmd {
	return nil
}

func (r *Radio) Placeholder(s string) Input {
	return r
}

func (r *Radio) Name() string {
	return r.name
}

func (r *Radio) SetChecked(checked bool) {
	r.checked = checked
}

func (r *Radio) Toggle() {
	r.checked = !r.checked
}

func (r *Radio) Value() interface{} {
	return r.value
}

func (r *Radio) View() string {
	check := "○"

	if r.checked {
		check = "●"
	}

	return lipgloss.NewStyle().
		MarginTop(1).
		MarginLeft(2).
		Render(
			fmt.Sprintf(
				"%s %s",
				r.controlStyle.Render(check),
				r.labelStyle.Render(r.name),
			),
		)
}
