package cmd

import (
	"strings"

	"github.com/litebase/litebase/pkg/cli/config"
	"github.com/litebase/litebase/pkg/cli/models/sql"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/spf13/cobra"
)

type Model struct {
	activeFrame  int
	content      string
	currentValue string
	// err          error
	frames       []sql.Frame
	history      []string
	historyIndex int
	width        int
}

type Init struct{}

func createFrame(m Model) sql.Frame {
	return sql.NewFrame(m.width)
}

func initialModel() tea.Model {
	return Model{
		activeFrame:  -1,
		historyIndex: 0,
	}
}

func (m Model) Init() tea.Cmd {
	return nil
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd, _ tea.Cmd
	var frame sql.Frame

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width

		if len(m.frames) <= 0 {
			m.frames = append(m.frames, createFrame(m))
			m.activeFrame = 0
			cmds = append(cmds, m.frames[m.activeFrame].Init())
		}

		if len(m.frames) > 0 {
			for i, frame := range m.frames {
				frame, cmd = frame.Update(msg)
				m.frames[i] = frame
				cmds = append(cmds, cmd)
			}
		}
	case sql.RunQuery:
		if m.activeFrame >= 0 {
			frame, cmd = m.frames[m.activeFrame].RunQuery(msg.Query)
			m.frames[m.activeFrame] = frame
			cmds = append(cmds, cmd)
		}
	case sql.SetFrameQuery:
		if m.activeFrame >= 0 {
			frame, cmd = m.frames[m.activeFrame].Update(msg)
			m.frames[m.activeFrame] = frame
			cmds = append(cmds, cmd)
		}
	case sql.FrameCompleted:
		newFrame := createFrame(m)
		m.frames = append(m.frames, newFrame)
		m.activeFrame = len(m.frames) - 1
		cmds = append(cmds, m.frames[m.activeFrame].Init())
		m.history = append(m.history, msg.Query)
		m.historyIndex = len(m.history)

		cmd = tea.Println(msg.Results)
		cmds = append(cmds, cmd)
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit
		case tea.KeyEsc:
			if len(m.frames) > 0 && m.activeFrame < 0 {
				frame, cmd = m.frames[m.activeFrame].Update(msg)
				cmds = append(cmds, cmd)
				m.frames[m.activeFrame] = frame
				m.activeFrame = -1
			} else {
				return m, tea.Quit
			}
		case tea.KeyUp:
			m, cmd = handleKeyUp(m)
			cmds = append(cmds, cmd)
			// return m, tea.Batch(cmds...)
		case tea.KeyDown:
			m, cmd = handleKeyDown(m)
			cmds = append(cmds, cmd)
			// return m, tea.Batch(cmds...)
		}
	}

	frame, cmd = m.frames[m.activeFrame].Update(msg)
	cmds = append(cmds, cmd)
	m.frames[m.activeFrame] = frame

	if len(m.frames) > 0 {
		frameStrings := []string{}

		for _, frame := range m.frames {
			if frame.Completed {
				continue
			}
			frameStrings = append(frameStrings, frame.View())
		}

		m.content = strings.Join(frameStrings, "\n")
	} else {
		m.content = ""
	}

	return m, tea.Batch(cmds...)
}

func (m Model) View() string {
	return m.content
}

func handleKeyDown(m Model) (Model, tea.Cmd) {
	var cmds []tea.Cmd
	var cmd func() tea.Msg
	var historyIndex int

	// Set the value to the next item in the history
	if len(m.history) > 0 {
		historyIndex = m.historyIndex + 1
		if historyIndex < len(m.history) {
			cmd = func() tea.Msg {
				return sql.SetFrameQuery{
					Query: m.history[historyIndex],
				}
			}

			if historyIndex <= len(m.history) {
				m.historyIndex = historyIndex
			}
		} else {
			m.historyIndex = len(m.history)
			cmd = func() tea.Msg {
				return sql.SetFrameQuery{
					Query: m.currentValue,
				}
			}
		}

		cmds = append(cmds, cmd)
	}

	return m, tea.Batch(cmds...)
}

func handleKeyUp(m Model) (Model, tea.Cmd) {
	var cmds []tea.Cmd

	if m.historyIndex == len(m.history) {
		m.currentValue = m.frames[m.activeFrame].Textarea.Value()
	}

	// Set the value to the previous item in the history
	if len(m.history) > 0 {
		if m.historyIndex >= 0 {
			cmd := func() tea.Msg {
				return sql.SetFrameQuery{
					Query: m.history[m.historyIndex],
				}
			}

			cmds = append(cmds, cmd)
		}

		if m.historyIndex >= 1 {
			m.historyIndex = m.historyIndex - 1
		}
	}

	return m, tea.Batch(cmds...)
}

func NewSQLCmd(c *config.Configuration) *cobra.Command {
	return &cobra.Command{
		Use:   "sql",
		Short: "Run SQL queries",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := tea.NewProgram(initialModel()).Run()

			if err != nil {
				return err
			}

			return nil
		},
	}
}
