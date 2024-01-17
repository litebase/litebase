package components

import (
	"fmt"
	"litebasedb/cli/styles"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type RadioGroup struct {
	errors      []string
	focused     bool
	focusIndex  int
	label       string
	labelStyle  lipgloss.Style
	name        string
	placeholder string
	options     []*Radio
}

func (r *RadioGroup) Blur() Input {
	r.labelStyle = styles.PromptStyle

	for _, option := range r.options {
		option.Blur()
	}

	r.focused = false

	// r.focusIndex = -1

	return r
}

func (r *RadioGroup) Focus() (Input, tea.Cmd) {
	r.labelStyle = styles.FocusedPromptStyle
	r.focusIndex = 0
	r.options[r.focusIndex].Focus()
	r.focused = true

	return r, nil
}

func (r *RadioGroup) Focused() bool {
	return r.focused
}

func (r *RadioGroup) Errors(errors []string) {
	r.errors = errors
}

func (r *RadioGroup) Init() tea.Cmd {
	return nil
}

func (r *RadioGroup) Label(s string) Input {
	r.label = fmt.Sprintf("%s: ", s)

	return r
}

func (r *RadioGroup) Model() {
	// return nil
}

func (r *RadioGroup) Name() string {
	return r.name
}

func (r *RadioGroup) Options(options map[string]string) Input {
	for key, value := range options {
		r.options = append(r.options, &Radio{
			name:  key,
			value: value,
		})
	}

	return r
}

func (r *RadioGroup) Placeholder(s string) Input {
	return r
}

func (r *RadioGroup) Update(msg tea.Msg) tea.Cmd {
	cmds := []tea.Cmd{}

	if !r.focused {
		return nil
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "enter":
			r.Toggle()

			return nil
		case "tab", "shift+tab", "up", "down":
			if r.focusIndex == -1 {
				r.focusIndex = 0
			}

			keypress := msg.String()

			if keypress == "up" || keypress == "shift+tab" {
				if r.focusIndex == 0 {
					r.Blur()
					return nil
				}

				r.focusIndex = max(0, r.focusIndex-1)

				// Return a cmd to keep focus trapped
				cmds = append(cmds, func() tea.Msg {
					return nil
				})
			} else {
				if r.focusIndex == len(r.options)-1 {
					r.Blur()
					return nil
				}

				cmds = append(cmds, func() tea.Msg {
					return nil
				})

				r.focusIndex = min(len(r.options)-1, r.focusIndex+1)
			}

			for i := range r.options {
				r.options[i].Blur()
			}

			_, cmd := r.options[r.focusIndex].Focus()
			cmds = append(cmds, cmd)
		}
	}

	for _, option := range r.options {
		cmd := option.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	return tea.Batch(cmds...)
}

func (r *RadioGroup) Value() interface{} {
	var value string

	for _, option := range r.options {
		if option.checked {
			value = option.value
			break
		}
	}

	return value
}

func (r *RadioGroup) View() string {
	radios := ""

	for _, option := range r.options {
		radios += option.View()
	}

	return fmt.Sprintf(
		"%s%s%s",
		r.labelStyle.Render(r.label),
		radios,
		InputErrors(r.errors),
	)
}

func NewRadioGroup(name string) Input {
	return &RadioGroup{
		name:       name,
		focusIndex: 0,
	}
}

func (r *RadioGroup) Toggle() {
	for i := range r.options {
		r.options[i].SetChecked(false)
	}

	r.options[r.focusIndex].Toggle()
}
