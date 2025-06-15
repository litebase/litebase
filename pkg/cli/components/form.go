package components

import (
	"log"
	"os"

	"github.com/litebase/litebase/pkg/cli/api"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type Form struct {
	action       string
	conditions   [][]Condition
	errorMessage string
	errors       api.Errors
	focusIndex   int
	handler      func(f *Form, requestData interface{}, responseData interface{}, err error) error
	inputs       []Input
	loader       *InlineLoader
	loading      bool
	method       string
	// privilegeOptions []string
	// privileges       []string
	success              bool
	successMessage       string
	title                string
	view                 viewport.Model
	visibleInputsIndices []int
}

type doneMsg struct{}
type errorMsg struct{}

func (f *Form) Init() tea.Cmd {
	return tea.Batch(
		textinput.Blink,
		f.loader.Tick(),
	)
}

func (f *Form) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		f.view.Width = msg.Width
		// f.view.Height = msg.Height
		return f, tea.ClearScreen
	case doneMsg:
		f.loading = false
		return f, tea.Quit
	case errorMsg:
		f.loading = false
		cmds := []tea.Cmd{}
		// Focus the first input with an error
		var firstError bool

		for i, input := range f.inputs {
			if f.errors[input.Name()] != nil {
				input.Errors(f.errors[input.Name()])

				if !firstError {
					firstError = true
					f.focusIndex = i
					input, cmd := input.Focus()
					f.inputs[i] = input
					cmds = append(cmds, cmd)
				}
			}
		}

		return f, tea.Batch(cmds...)
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return f, tea.Quit
			// Set focus to next input
		case "tab", "shift+tab", "enter", "up", "down":
			s := msg.String()

			// Check if the current input has options and if so, handle the
			// keypress appropriately.
			var group Input
			var ok bool

			group, ok = f.inputs[f.focusIndex].(*CheckboxGroup)

			if !ok {
				group, ok = f.inputs[f.focusIndex].(*RadioGroup)
			}

			if ok && group.Focused() {
				cmd := group.Update(msg)
				if s == "up" && cmd == nil {
				} else if s == "down" && cmd == nil {
				} else {
					return f, cmd
				}
			}

			// Did the user press enter while the submit button was focused?
			// If so, exit.
			if s == "enter" && f.focusIndex == len(f.inputs)-1 {
				// Blur the focused input so that when we quit the program it
				// doesn't appear focused.
				f.inputs[f.focusIndex].Blur()

				return f, f.submit()
			}

			// Cycle indexes
			if s == "up" || s == "shift+tab" {
				f.decrementFocusIndex()
			} else {
				f.incrementFocusIndex()
			}

			cmds := make([]tea.Cmd, len(f.inputs))

			for i := range f.inputs {
				if i == f.focusIndex {
					// Set focused state
					_, cmds[i] = f.inputs[i].Focus()

					continue
				}

				// Remove focused state
				f.inputs[i].Blur()
			}

			return f, tea.Batch(cmds...)
		}
	}

	cmds := []tea.Cmd{}

	_, cmd = f.loader.Update(msg)
	cmds = append(cmds, cmd)

	cmd = f.updateInputs(msg)
	cmds = append(cmds, cmd)

	return f, tea.Batch(cmds...)
}

func (f *Form) updateInputs(msg tea.Msg) tea.Cmd {
	cmds := make([]tea.Cmd, len(f.inputs))

	// Only text inputs with Focus() set will respond.
	for i, input := range f.inputs {
		cmds[i] = input.Update(msg)
		// var model textinput.Model
		// model, cmds[i] = f.inputs[i].Model().Update(msg)
		// f.inputs[i].SetModel(model)
	}

	return tea.Batch(cmds...)
}

func (f *Form) incrementFocusIndex() {
	incremented := f.focusIndex + 1
	isVisible := false

	if incremented >= len(f.inputs) {
		f.focusIndex = 0
		return
	}

	for _, i := range f.visibleInputsIndices {
		if i == incremented {
			isVisible = true
			break
		}
	}

	if !isVisible {
		f.focusIndex += 1
		f.incrementFocusIndex()
		return
	}

	f.focusIndex = incremented
}

func (f *Form) decrementFocusIndex() {
	decremented := f.focusIndex - 1
	isVisible := false

	if decremented < 0 {
		f.focusIndex = len(f.inputs) - 1
		return
	}

	for _, i := range f.visibleInputsIndices {
		if i == decremented {
			isVisible = true
			break
		}
	}

	if !isVisible {
		f.focusIndex -= 1
		f.decrementFocusIndex()
		return
	}

	f.focusIndex = decremented
}

func (f *Form) View() string {
	f.visibleInputsIndices = []int{}
	var content []string

	if f.title != "" {
		content = append(content, lipgloss.NewStyle().Bold(true).Render(f.title))
	}

	for i, input := range f.inputs {
		var shouldRender bool = true

		if f.conditions[i] != nil {
			for _, condition := range f.conditions[i] {
				var input Input

				for _, inp := range f.inputs {
					if inp.Name() == condition.FieldName {
						input = inp
						break
					}
				}

				// Use the condition operator to determine if the input should
				// be displayed.
				switch condition.Operator {
				case "=":
					if input.Value() != condition.Value {
						shouldRender = false
						continue
					}
				case "!=":
					if input.Value() == condition.Value {
						shouldRender = false
						continue
					}
				case ">":
					if input.Value().(int) <= condition.Value.(int) {
						shouldRender = false
						continue
					}
				case "<":
					if input.Value().(int) >= condition.Value.(int) {
						shouldRender = false
						continue
					}
				case ">=":
					if input.Value().(int) < condition.Value.(int) {
						shouldRender = false
						continue
					}
				case "<=":
					if input.Value().(int) > condition.Value.(int) {
						shouldRender = false
						continue
					}
				}
			}
		}

		if !shouldRender {
			continue
		} else {
			f.visibleInputsIndices = append(f.visibleInputsIndices, i)
		}

		marginTop := 0

		if i != 0 {
			marginTop = 1
		}

		content = append(content, lipgloss.NewStyle().MarginTop(marginTop).Render(input.View()))
	}

	// log.Println(f.visibleInputsIndices)

	if f.loading {
		content = append(content, f.loader.View())
	} else if f.success {
		content = append(content, lipgloss.NewStyle().MarginTop(1).Render(SuccessAlert(f.successMessage)))
	}

	if f.errorMessage != "" {
		content = append(content, ErrorAlert(f.errorMessage))
	}

	return f.view.Style.Render(Container(content...))
}

func (f *Form) submit() tea.Cmd {
	f.loading = true
	f.errorMessage = ""

	return func() tea.Msg {
		client, err := api.NewClient()

		if err != nil {
			f.errorMessage = err.Error()

			return errorMsg{}
		}

		var errors api.Errors
		var responseData map[string]interface{}
		requestData := make(map[string]interface{})

		for _, input := range f.inputs {
			requestData[input.Name()] = input.Value()
		}

		if f.action != "" && f.method != "" {
			responseData, errors, err = client.Request(f.method, f.action, requestData)

			if err != nil {
				f.errorMessage = err.Error()

				return errorMsg{}
			}

			if errors != nil {
				f.errors = errors

				return errorMsg{}
			}
		}

		if f.handler != nil {
			err := f.handler(f, requestData, responseData, err)

			if err != nil {
				f.errorMessage = "Error submitting form: " + err.Error()
				return errorMsg{}
			}
		}

		f.success = true

		return doneMsg{}
	}
}

func createConditions(fields []FormField) [][]Condition {
	conditions := make([][]Condition, len(fields))

	for i, field := range fields {
		conditions[i] = field.Conditions
	}

	// Create an empty condition for the submit button
	conditions = append(conditions, []Condition{})

	return conditions
}

func createInputs(fields []FormField) []Input {
	inputs := make([]Input, len(fields)+1)

	for i, field := range fields {
		var input Input
		switch field.Type {
		case TextType:
			input = NewTextInput(field.Name).(*TextInput).
				CharLimit(field.CharLimit).
				Label(field.Label).
				Placeholder(field.Placeholder)
		case PasswordType:
			input = NewTextInput(field.Name).(*TextInput).Password().
				CharLimit(field.CharLimit).
				Label(field.Label).
				Placeholder(field.Placeholder)
		case CheckboxGroupType:
			input = NewCheckboxGroup(field.Name).(*CheckboxGroup).
				Options(field.Options).
				Label(field.Label)
		case RadioGroupType:
			input = NewRadioGroup(field.Name).(*RadioGroup).
				Options(field.Options).
				Label(field.Label)
		}

		if i == 0 {
			input, _ = input.Focus()
		}

		inputs[i] = input
	}

	inputs[len(inputs)-1] = NewButton("Submit")

	return inputs
}

func NewForm(fields []FormField) *Form {
	return &Form{
		conditions: createConditions(fields),
		errors:     make(map[string][]string),
		inputs:     createInputs(fields),
		loader:     NewInlineLoader(),
		view: viewport.Model{
			Width: 80,
			// Height: 20,
		},
	}
}

func (f *Form) Render() {
	if _, err := tea.NewProgram(f).Run(); err != nil {
		log.Println("Error running program:", err)
		os.Exit(1)
	}
}

func (f *Form) Action(action string) *Form {
	f.action = action

	return f
}

func (f *Form) Method(method string) *Form {
	f.method = method

	return f
}

func (f *Form) Handler(handler func(f *Form, requestData interface{}, responseData interface{}, err error) error) *Form {
	f.handler = handler

	return f
}

func (f *Form) SuccessMessage(s string) *Form {
	f.successMessage = s

	return f
}

func (f *Form) Title(s string) *Form {
	f.title = s

	return f
}
