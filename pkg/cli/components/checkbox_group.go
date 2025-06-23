package components

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/styles"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type CheckboxGroup struct {
	errors      []string
	focused     bool
	focusIndex  int
	label       string
	labelStyle  lipgloss.Style
	name        string
	placeholder string
	options     []*Checkbox
}

func (c *CheckboxGroup) Blur() Input {
	c.labelStyle = styles.PromptStyle

	for _, option := range c.options {
		option.Blur()
	}

	c.focused = false

	return c
}

func (c *CheckboxGroup) Focus() (Input, tea.Cmd) {
	c.labelStyle = styles.FocusedPromptStyle
	c.focusIndex = 0
	c.options[c.focusIndex].Focus()
	c.focused = true

	return c, nil
}

func (c *CheckboxGroup) Focused() bool {
	return c.focused
}

func (c *CheckboxGroup) Errors(errors []string) {
	c.errors = errors
}

func (c *CheckboxGroup) Init() tea.Cmd {
	return nil
}

func (c *CheckboxGroup) Label(s string) Input {
	c.label = fmt.Sprintf("%s: ", s)

	return c
}

func (c *CheckboxGroup) Model() {
	// return nil
}

func (c *CheckboxGroup) Name() string {
	return c.name
}

func (c *CheckboxGroup) Options(options map[string]string) Input {
	for key, value := range options {
		c.options = append(c.options, &Checkbox{
			name:  key,
			value: value,
		})
	}

	return c
}

func (c *CheckboxGroup) Placeholder(s string) Input {
	return c
}

func (c *CheckboxGroup) Update(msg tea.Msg) tea.Cmd {
	cmds := []tea.Cmd{}

	if !c.focused {
		return nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			c.Toggle()

			return nil
		case "tab", "shift+tab", "up", "down":
			if c.focusIndex == -1 {
				c.focusIndex = 0
			}

			keypress := msg.String()

			if keypress == "up" || keypress == "shift+tab" {
				if c.focusIndex == 0 {
					c.Blur()
					return nil
				}

				c.focusIndex = max(0, c.focusIndex-1)

				// Return a cmd to keep focus trapped
				cmds = append(cmds, func() tea.Msg {
					return nil
				})
			} else {
				if c.focusIndex == len(c.options)-1 {
					c.Blur()
					return nil
				}

				cmds = append(cmds, func() tea.Msg {
					return nil
				})

				c.focusIndex = min(len(c.options)-1, c.focusIndex+1)
			}

			for i := range c.options {
				c.options[i].Blur()
			}

			_, cmd := c.options[c.focusIndex].Focus()
			cmds = append(cmds, cmd)
		}
	}

	for _, option := range c.options {
		cmd := option.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	return tea.Batch(cmds...)
}

func (c *CheckboxGroup) Value() any {
	values := []string{}
	for _, option := range c.options {
		if option.checked {
			values = append(values, option.value)
		}
	}

	return values
}

func (c *CheckboxGroup) View() string {
	checkboxes := ""

	for _, option := range c.options {
		checkboxes += option.View()
	}

	return fmt.Sprintf(
		"%s%s%s",
		c.labelStyle.Render(c.label),
		checkboxes,
		InputErrors(c.errors),
	)
}

func NewCheckboxGroup(name string) Input {
	return &CheckboxGroup{
		name:       name,
		focusIndex: 0,
	}
}

func (c *CheckboxGroup) Toggle() {
	c.options[c.focusIndex].Toggle()
}
