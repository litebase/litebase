package components

import (
	"fmt"

	"github.com/litebase/litebase/pkg/cli/styles"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

type TextInput struct {
	errors  []string
	focused bool
	model   textinput.Model
	name    string
}

func NewTextInput(name string, options ...textinput.EchoMode) Input {
	t := textinput.New()
	t.Cursor.Style = styles.CursorStyle
	t.PlaceholderStyle = styles.PlaceholderStyle
	t.PromptStyle = styles.PromptStyle
	t.TextStyle = styles.InputStyle

	return &TextInput{
		model: t,
		name:  name,
	}
}

func (t *TextInput) Blur() Input {
	t.model.Blur()
	t.focused = false
	t.model.PromptStyle = styles.PromptStyle
	t.model.TextStyle = styles.InputStyle

	return t
}

func (t *TextInput) Errors(errors []string) {
	t.errors = errors
}

func (t *TextInput) Focus() (Input, tea.Cmd) {
	cmd := t.model.Focus()
	t.focused = true
	t.model.PromptStyle = styles.FocusedPromptStyle
	t.model.TextStyle = styles.FocusedInputStyle

	return t, cmd
}

func (t *TextInput) Focused() bool {
	return t.focused
}

func (t *TextInput) CharLimit(limit int) *TextInput {
	t.model.CharLimit = limit

	return t
}

func (t *TextInput) Label(s string) Input {
	t.model.Prompt = fmt.Sprintf("%s: ", s)

	return t
}

func (t *TextInput) Model() textinput.Model {
	return t.model
}

func (t *TextInput) SetModel(model textinput.Model) {
	t.model = model
}

func (t *TextInput) Name() string {
	return t.name
}

func (t *TextInput) Password() *TextInput {
	t.model.EchoMode = textinput.EchoPassword
	t.model.EchoCharacter = '•'

	return t
}

func (t *TextInput) Placeholder(s string) Input {
	t.model.Placeholder = s

	return t
}

func (t *TextInput) Update(msg tea.Msg) tea.Cmd {
	model, cmd := t.Model().Update(msg)

	t.SetModel(model)

	return cmd
}

func (t *TextInput) Value() interface{} {
	return t.model.Value()
}

func (t *TextInput) View() string {
	return fmt.Sprintf("%s%s", t.model.View(), InputErrors(t.errors))
}
