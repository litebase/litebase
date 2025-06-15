package components

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/styles"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Checkbox struct {
	controlStyle lipgloss.Style
	labelStyle   lipgloss.Style
	checked      bool
	errors       []string
	focused      bool
	name         string
	value        string
}

func (c *Checkbox) Blur() Input {
	c.controlStyle = styles.PromptStyle
	c.labelStyle = styles.PromptStyle
	c.focused = false

	return c
}

func (c *Checkbox) Errors(errors []string) {

}

func (c *Checkbox) Focus() (Input, tea.Cmd) {
	c.controlStyle = styles.FocusedPromptStyle
	c.labelStyle = styles.FocusedPromptStyle
	c.focused = true

	return c, nil
}

func (c *Checkbox) Focused() bool {
	return c.focused
}

func (c *Checkbox) Label(label string) Input {
	return c
}

func (c *Checkbox) Update(msg tea.Msg) tea.Cmd {
	return nil
}

func (c *Checkbox) Placeholder(s string) Input {
	return c
}

func (c *Checkbox) Name() string {
	return c.name
}

func (c *Checkbox) Toggle() {
	c.checked = !c.checked
}

func (c *Checkbox) Value() interface{} {
	return c.value
}

func (c *Checkbox) View() string {
	check := "☐"

	if c.checked {
		check = "☒"
	}

	return lipgloss.NewStyle().
		MarginTop(1).
		MarginLeft(2).
		Render(
			fmt.Sprintf(
				"%s %s",
				c.controlStyle.Render(check),
				c.labelStyle.Render(c.name),
			),
		)
}
