package components

import (
	"fmt"
	"os"

	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var baseStyle = lipgloss.NewStyle().
	BorderStyle(lipgloss.NormalBorder()).
	BorderForeground(lipgloss.Color("240")).
	Margin(1, 0, 2, 0)

type Table struct {
	handler func(row []string)
	table   table.Model
	width   int
	height  int
}

func (t *Table) Init() tea.Cmd { return nil }

func (t *Table) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	if t.handler == nil {
		return t, tea.Quit
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		t.width = msg.Width
		t.height = msg.Height
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if t.table.Focused() {
				t.table.Blur()
			} else {
				t.table.Focus()
			}
		case "q", "ctrl+c":
			return t, tea.Quit
		case "enter":
			// return t, tea.Quit

			// TODO: Handle custom enter handlers
			if t.handler != nil {
				t.handler(t.table.SelectedRow())
			}
			// return m, tea.Batch(
			// 	tea.Println(t.table.SelectedRow()),
			// )
		}
	}

	t.table, cmd = t.table.Update(msg)

	return t, cmd
}

func (t *Table) View() string {
	return baseStyle.Render(t.table.View())
}

func NewTable(
	columns []string,
	rows [][]string,
) *Table {

	columnWidths := make([]int, len(columns))

	// First set the column widths based on the column titles
	for i, title := range columns {
		if (len(title) + 4) > columnWidths[i] {
			columnWidths[i] = len(title)
		}
	}

	// Expand the column widths based on the values of the rows
	for _, column := range rows {
		for i, cell := range column {
			if (len(cell) + 4) > columnWidths[i] {
				columnWidths[i] = len(cell) + 4
			}
		}
	}

	tableColumns := make([]table.Column, len(columns))
	tableRows := make([]table.Row, len(rows))

	for i, column := range columns {
		tableColumns[i] = table.Column{
			Title: column,
			Width: columnWidths[i],
		}
	}

	for i, row := range rows {
		tableRows[i] = row
	}

	height := 11

	if len(rows) < height {
		height = len(rows) + 1
	}

	tbl := table.New(
		table.WithColumns(tableColumns),
		table.WithRows(tableRows),
		table.WithHeight(height),
		table.WithFocused(false),
	)

	tbl.Blur()

	s := table.DefaultStyles()

	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		Bold(false)

	s.Selected = s.Selected.
		// Foreground(lipgloss.Color("229")).
		Foreground(lipgloss.Color("0")).
		// Background(lipgloss.Color("57")).
		// Background(lipgloss.Color("240")).
		Bold(false)

	tbl.SetStyles(s)

	t := &Table{table: tbl, width: 0, height: 0}

	return t
}

func (t *Table) SetHandler(handler func(row []string)) *Table {
	t.handler = handler

	return t
}

func (t *Table) Render() {
	if _, err := tea.NewProgram(t).Run(); err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}
}

func (t *Table) WitFocus() *Table {
	table.WithFocused(true)(&t.table)

	return t
}
