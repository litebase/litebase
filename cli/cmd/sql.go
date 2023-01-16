package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

var SQLCmd = &cobra.Command{
	Use:   "sql",
	Short: "Run SQL queries",
	Run: func(cmd *cobra.Command, args []string) {
		tea.NewProgram(initialModel()).Run()
	},
}

type model struct {
	currentValue string
	height       int
	history      []string
	historyIndex int
	loading      bool
	results      []string
	textarea     textarea.Model
	width        int
	err          error
}

func initialModel() model {
	ti := textarea.New()

	ti.ShowLineNumbers = false
	ti.SetHeight(1)
	ti.Focus()
	ti.FocusedStyle.CursorLine = lipgloss.NewStyle()
	ti.KeyMap.InsertNewline.SetEnabled(false)

	ti.SetPromptFunc(0, func(lineIdx int) string {
		if lineIdx > 0 {
			return fmt.Sprintf("%s → ", lipgloss.NewStyle().Bold(true).Render("       ..."))
		}

		return fmt.Sprintf("%s → ", lipgloss.NewStyle().Bold(true).Render("litebasedb"))
	})

	m := model{
		textarea: ti,
		err:      nil,
	}

	return m
}

func (m model) Init() tea.Cmd {
	return textarea.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	var historyIndex int

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.textarea.SetWidth(msg.Width)
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		// On ctrl+c, exit the program
		if msg.Type == tea.KeyCtrlC {
			return m, tea.Quit
		}

		if msg.Type == tea.KeyEsc {
			if len(m.results) > 0 {
				m.textarea.Reset()
				m.currentValue = m.textarea.Value()
				m.results = []string{}
			} else {
				return m, tea.Quit
			}
		}

		if msg.Type == tea.KeyUp {
			// If the history index is at the end, save the current value
			if m.historyIndex == len(m.history) {
				m.currentValue = m.textarea.Value()
			}

			// Set the value to the previous item in the history
			if len(m.history) > 0 {
				if historyIndex >= 0 && m.history[historyIndex] != "" {
					historyIndex = m.historyIndex - 1
				}

				if historyIndex >= 0 {
					m.textarea.SetValue(m.history[historyIndex])
					m.historyIndex = historyIndex
				}
			}
		}

		if msg.Type == tea.KeyDown {
			// Set the value to the next item in the history
			if len(m.history) > 0 {
				historyIndex = m.historyIndex + 1
				if historyIndex < len(m.history) {
					m.textarea.SetValue(m.history[historyIndex])
					m.historyIndex = historyIndex
				} else {
					// If the history index is at the end, resume the text input state with the current value
					m.historyIndex = len(m.history)
					m.textarea.SetValue(m.currentValue)
					m.textarea.CursorEnd()
				}
			}
		}
		// TODO: Need to execute on ctrl+enter or cmd+enter (mac). Enter should add a new line.
		if msg.Type == tea.KeyEnter {
			if msg.Alt {
				// m.textarea.InsertNewline()
				return m, nil
			}
			// Parse the string and execute the query, split by semi-colon
			queries := strings.Split(m.textarea.Value(), ";")
			m.results = []string{}
			loading := make(chan bool)
			m.loading = true

			go func() {
				time.Sleep(1 * time.Second)
				m.loading = false
				loading <- true
			}()

			<-loading

			for range queries {
				jsonString := `[{"column1": "value1","column2": "value2"}]`
				x := make([]map[string]interface{}, 0)
				err := json.Unmarshal([]byte(jsonString), &x)

				if err != nil {
					fmt.Println("error:", err)
				}

				y, err := json.MarshalIndent(x, "", "  ")

				if err != nil {
					fmt.Println("error:", err)
				}

				m.results = append(m.results, string(y))
			}

			if m.textarea.Value() != "" {
				m.history = append(m.history, m.textarea.Value())
				m.historyIndex = len(m.history)
				m.textarea.Reset()
				m.currentValue = m.textarea.Value()
			}
		}
	}

	m.textarea, cmd = m.textarea.Update(msg)
	// update height based on line count
	m.textarea.SetHeight(m.textarea.LineCount())

	return m, cmd
}

func (m model) View() string {
	input := ""
	loading := ""
	results := ""
	query := ""

	if m.loading {
		loading = "loading..."
	}

	if len(m.results) > 0 && m.history[len(m.history)-1] != "" {
		query = m.history[len(m.history)-1]

		query = lipgloss.NewStyle().
			Width(m.width).
			Padding(1, 0, 0, 0).
			Render(query)

		var style = lipgloss.NewStyle().
			Bold(true).
			Padding(0, 1, 0, 1).
			MarginTop(1).
			Width(m.width).
			Background(lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#9333EA"})

		header := style.Render(fmt.Sprintf("Results: (%s)", "24ms"))
		content := ""

		for _, result := range m.results {
			content += result
		}

		content = lipgloss.NewStyle().
			Width(m.width).
			Padding(1, 0, 1, 0).
			Render(content)

		footer := fmt.Sprintf("Results: %d", len(m.results))
		results = fmt.Sprintf("%s\n%s\n%s", header, content, footer)
	}

	input = m.textarea.View()

	i := []string{query, loading, results, input}
	o := []string{}

	// Remove empty strings
	for _, s := range i {
		if s != "" {
			o = append(o, s)
		}
	}

	return strings.Join(o, "\n")
}

func NewSQLCmd() *cobra.Command {
	return SQLCmd
}
