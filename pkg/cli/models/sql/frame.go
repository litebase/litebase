package sql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textarea"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/google/uuid"
)

type Frame struct {
	Completed bool
	content   string
	Id        string
	loading   bool
	query     string
	results   []string
	Textarea  textarea.Model
	err       error
	width     int
	height    int
}

type FrameCompleted struct {
	FrameId string
	Query   string
	Results string
}

type SetFrameQuery struct {
	Query string
}

type RunQuery struct {
	Query string
}

func NewFrame(width int) Frame {
	textarea := textarea.New()
	textarea.ShowLineNumbers = false
	textarea.SetHeight(1)
	textarea.Focus()
	textarea.FocusedStyle.CursorLine = lipgloss.NewStyle()
	textarea.BlurredStyle.CursorLine = lipgloss.NewStyle()
	textarea.KeyMap.InsertNewline.SetEnabled(false)

	textarea.SetPromptFunc(0, func(lineIdx int) string {
		style := lipgloss.NewStyle().Bold(true).Foreground(
			lipgloss.AdaptiveColor{
				Light: "#000000",
				Dark:  "#ffffff",
			},
		)

		if lineIdx > 0 {
			return fmt.Sprintf("%s → ", style.Render("       ..."))
		}

		return fmt.Sprintf("%s → ", style.Render("litebase"))
	})

	return Frame{
		content:  "",
		Id:       uuid.New().String(),
		err:      nil,
		Textarea: textarea,
		width:    width,
		loading:  false,
	}
}

func (f Frame) Init() tea.Cmd {
	return tea.Batch(textarea.Blink)
}

func (f Frame) Update(msg tea.Msg) (Frame, tea.Cmd) {
	var (
		cmds []tea.Cmd
		cmd  tea.Cmd
	)

	f.Textarea, cmd = f.Textarea.Update(msg)
	// update height based on line count
	f.Textarea.SetHeight(f.Textarea.LineCount())
	cmds = append(cmds, cmd)

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		f, _ = updateWindowSize(f, msg)
	case tea.KeyMsg:
		if msg.Type == tea.KeyEsc {
			if len(f.results) > 0 {
				f.Textarea.Reset()
				f.query = f.Textarea.Value()
				f.results = []string{}
			}
		}

		if msg.Type == tea.KeyEnter {
			f, cmd = f.handleEnter(msg)
			cmds = append(cmds, cmd)
		}
	case SetFrameQuery:
		f.Textarea.SetValue(msg.Query)
	}

	return f, tea.Batch(cmds...)
}

func (f Frame) View() string {
	input := ""
	loading := ""
	results := ""

	if len(f.results) > 0 {
		var style = lipgloss.NewStyle().
			Bold(true).
			Padding(0, 1, 0, 1).
			MarginTop(1).
			Width(f.width).
			Background(lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#9333EA"})

		header := style.Render(fmt.Sprintf("Results: (%s)", "24ms"))
		content := ""

		for _, result := range f.results {
			content += result
		}

		content = lipgloss.NewStyle().
			Width(f.width).
			Padding(1, 0, 1, 0).
			Render(content)

		footer := fmt.Sprintf("Results: %d", len(f.results))
		results = fmt.Sprintf("%s\n%s\n%s", header, content, footer)
	}

	input = lipgloss.NewStyle().
		Width(f.width-2).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#9333EA"}).
		Padding(1, 1, 1, 1).
		MarginBottom(1).
		MarginTop(1).
		Render(f.Textarea.View())

	i := []string{input, loading, results}
	o := []string{}

	// Remove empty strings
	for _, s := range i {
		if s != "" {
			o = append(o, s)
		}
	}

	return strings.Join(o, "\n")
}

func (f Frame) handleEnter(msg tea.KeyMsg) (Frame, tea.Cmd) {
	var cmds []tea.Cmd
	value := f.Textarea.Value()

	if value == "" || !f.Textarea.Focused() {
		return f, nil
	}

	if value[len(value)-1:] != ";" {
		f.Textarea.InsertRune('\n')
		f.Textarea.SetHeight(f.Textarea.LineCount())
		return f, nil
	}

	if value[len(value)-1:] == ";" {
		f.loading = true
		cmds = append(cmds, func() tea.Msg {
			return RunQuery{Query: value}
		})
	}

	return f, tea.Batch(cmds...)
}

func (f Frame) RunQuery(query string) (Frame, tea.Cmd) {
	var cmds []tea.Cmd

	fmt.Println("Running query")
	httpClient := &http.Client{
		Timeout: time.Second * 10,
	}

	res, err := httpClient.Get("http://example.com")

	if err != nil {
		f.err = err
		return f, nil
	}

	defer res.Body.Close()

	_, err = ioutil.ReadAll(res.Body)

	if err != nil {
		f.err = err
		return f, nil
	}

	// req, err := http.NewRequest("POST", "http://localhost:8080/query", strings.NewReader(query))

	// Parse the string and execute the query, split by semi-colon
	query = strings.Trim(query, " ")
	queries := strings.Split(query, ";")
	q := []string{}

	for _, query := range queries {
		query = strings.Trim(query, " ")
		query = strings.Trim(query, "\n")

		if query == "" {
			continue
		}

		q = append(q, query)
	}

	f.results = []string{}
	var results []string

	for range q {
		jsonString := `[{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"},{"column1": "value1","column2": "value2"}]`
		x := make([]map[string]any, 0)
		err := json.Unmarshal([]byte(jsonString), &x)

		if err != nil {
			log.Fatalln("error:", err)
		}

		result, err := json.MarshalIndent(x, "", "  ")

		if err != nil {
			fmt.Println("error:", err)
		}

		results = append(results, string(result))
	}

	f.results = results
	f.loading = false
	if len(f.results) > 0 {
		f.Textarea.Blur()
		f.query = f.Textarea.Value()
		f.Completed = true

		cmds = append(cmds, tea.Tick(time.Millisecond, func(time time.Time) tea.Msg {
			return FrameCompleted{
				FrameId: f.Id,
				Query:   f.query,
				Results: f.View(),
			}
		}))
	}

	return f, tea.Batch(cmds...)
}

func updateWindowSize(f Frame, msg tea.WindowSizeMsg) (Frame, tea.Cmd) {
	f.width = msg.Width
	f.height = msg.Height
	f.Textarea.SetWidth(msg.Width)

	return f, nil
}
