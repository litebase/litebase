package components

import (
	"fmt"
	"os"

	"github.com/charmbracelet/bubbles/v2/table"
	tea "github.com/charmbracelet/bubbletea/v2"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/charmbracelet/x/term"
	"github.com/litebase/litebase/pkg/cli"
)

type Table struct {
	handler     func(row []string)
	table       table.Model
	width       int
	height      int
	columns     []string
	rows        [][]string
	columnPercs []float64 // Percentage widths for each column
	selectedRow []string  // Store selected row for handler
}

func (t *Table) Init() tea.Cmd { return nil }

func (m *Table) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = min(14, msg.Height-4) // Reserve space for help view
		m.table.SetWidth(m.width)
		m.table.SetHeight(m.height)

		// Recalculate and update column widths based on new window size
		newColumns := m.calculateColumnWidths(m.width)
		m.table.SetColumns(newColumns)
	case tea.KeyMsg:
		switch msg.String() {
		case "esc":
			if m.table.Focused() {
				m.table.Blur()
			} else {
				m.table.Focus()
			}
		case "q", "ctrl+c":
			return m, tea.Quit
		case "enter":
			if m.handler != nil {
				// Store the selected row for the handler to use after quit
				m.selectedRow = m.table.SelectedRow()
			}
			return m, tea.Quit
		}
	}

	m.table, cmd = m.table.Update(msg)

	return m, cmd
}

func (m *Table) View(displayHelp bool) string {
	helpView := ""

	if displayHelp {
		helpView = m.table.HelpView()
	}

	return lipgloss.JoinVertical(
		lipgloss.Left,
		m.table.View(),
		"\n",
		helpView,
	)
}

func NewTable(
	columns []string,
	rows [][]string,
) *Table {
	// Calculate intelligent percentage widths based on column types and content
	columnPercs := make([]float64, len(columns))
	totalWeight := 0.0

	// Assign weights based on column types and content
	weights := make([]float64, len(columns))
	for i, title := range columns {
		switch title {
		case "#", "No", "Index":
			weights[i] = 1.0 // Small weight for number columns
		case "Access Key Id", "ID", "Key":
			weights[i] = 3.0 // Medium weight for IDs
		case "Description", "Desc", "Name":
			weights[i] = 5.0 // Large weight for descriptions
		default:
			weights[i] = 2.5 // Default medium weight
		}

		// Adjust weight based on content length
		maxContentLength := len(title)

		for _, row := range rows {
			if i < len(row) && len(row[i]) > maxContentLength {
				maxContentLength = len(row[i])
			}
		}

		// Scale weight based on content length
		if maxContentLength > 30 {
			weights[i] *= 1.5
		} else if maxContentLength < 10 {
			weights[i] *= 0.7
		}

		totalWeight += weights[i]
	}

	// Convert weights to percentages
	for i := range columnPercs {
		columnPercs[i] = weights[i] / totalWeight
	}

	// Create initial table with percentage-based widths
	tableColumns := make([]table.Column, len(columns))
	initialWidth := 120 // Default width for initial render
	usableWidth := initialWidth - (len(columns) - 1) - 2

	for i, title := range columns {
		width := int(float64(usableWidth) * columnPercs[i])
		minWidth := max(3, len(title))
		if width < minWidth {
			width = minWidth
		}

		tableColumns[i] = table.Column{
			Title: title,
			Width: width,
		}
	}

	tableRows := make([]table.Row, len(rows))

	for i, row := range rows {
		tableRows[i] = row
	}

	var height int

	if term.IsTerminal(os.Stdout.Fd()) {
		height = min(10, len(rows))
	} else {
		height = len(rows) + 1 // Reserve space for headers
	}

	t := table.New(
		table.WithColumns(tableColumns),
		table.WithRows(tableRows),
		table.WithFocused(true),
		table.WithHeight(height),
	)

	s := table.DefaultStyles()

	s.Header = s.Header.
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(lipgloss.Color("240")).
		BorderBottom(true).
		MarginTop(1).
		Bold(true)

	s.Selected = s.Selected.
		Foreground(cli.LightDark(cli.White, cli.Black)).
		Background(cli.LightDark(cli.Sky700, cli.Sky300)).
		Bold(false)

	t.SetStyles(s)

	return &Table{
		handler:     nil,
		table:       t,
		columns:     columns,
		rows:        rows,
		columnPercs: columnPercs,
		width:       initialWidth,
		height:      height,
	}
}

func (t *Table) Render(interactive bool) string {
	if !term.IsTerminal(os.Stdout.Fd()) || !interactive {
		// In non-TTY environments (like tests), just return the table view
		// without interactive functionality
		return t.View(false)
	}

	p := tea.NewProgram(t, tea.WithAltScreen())

	// Run the program
	model, err := p.Run()

	if err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}

	// After the program exits and alternate screen is restored,
	// check if we have a handler to run
	if finalTable, ok := model.(*Table); ok && finalTable.handler != nil {
		// Use the stored selected row and run the handler
		if len(finalTable.selectedRow) > 0 {
			finalTable.handler(finalTable.selectedRow)
		}
	}

	return ""
}

func (t *Table) SetHandler(handler func(row []string)) *Table {
	t.handler = handler

	return t
}

// calculateColumnWidths calculates column widths based on percentages and available width
func (m *Table) calculateColumnWidths(availableWidth int) []table.Column {
	if len(m.columnPercs) == 0 || len(m.columns) == 0 {
		return m.table.Columns()
	}

	// Reserve space for borders and padding (3 chars per column separator + 2 for outer borders)
	usableWidth := availableWidth - (len(m.columns) - 1) - 2
	if usableWidth < 10 {
		usableWidth = 10
	}

	tableColumns := make([]table.Column, len(m.columns))

	for i, title := range m.columns {
		var width int
		if i < len(m.columnPercs) {
			width = int(float64(usableWidth) * m.columnPercs[i])
		} else {
			// Fallback to equal distribution if percentage not specified
			width = usableWidth / len(m.columns)
		}

		// Ensure minimum width
		minWidth := max(3, len(title))
		if width < minWidth {
			width = minWidth
		}

		tableColumns[i] = table.Column{
			Title: title,
			Width: width,
		}
	}

	return tableColumns
}
