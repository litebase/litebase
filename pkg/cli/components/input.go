package components

import tea "github.com/charmbracelet/bubbletea"

type Input interface {
	Blur() Input
	Errors(errors []string)
	Focus() (Input, tea.Cmd)
	Focused() bool
	Label(label string) Input
	Update(msg tea.Msg) tea.Cmd
	Placeholder(s string) Input
	Name() string
	Value() any
	View() string
}

type InputType string

const (
	CheckboxGroupType InputType = "checkbox-group"
	CheckboxType      InputType = "checkbox"
	RadioGroupType    InputType = "radio-group"
	RadioType         InputType = "radio"
	SelectType        InputType = "select"
	TextType          InputType = "text"
	ButtonType        InputType = "button"
	PasswordType      InputType = "password"
)
