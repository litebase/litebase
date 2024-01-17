package styles

import "github.com/charmbracelet/lipgloss"

var PimaryTextColor = lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#0284c7"}
var PrimaryBackgroundColor = lipgloss.AdaptiveColor{Light: "#2563eb", Dark: "#0284c7"}
var PrimaryForegroundColor = lipgloss.AdaptiveColor{Light: "#FFFFFF", Dark: "#FFFFFF"}
var TextColor = lipgloss.AdaptiveColor{Light: "#000000", Dark: "#FFFFFF"}

var CursorStyle = lipgloss.NewStyle().Foreground(PimaryTextColor)

var InputStyle = lipgloss.NewStyle().Foreground(TextColor)
var FocusedInputStyle = InputStyle.Copy().Foreground(TextColor)
var PlaceholderStyle = lipgloss.NewStyle().Foreground(
	lipgloss.AdaptiveColor{Light: "#8c8c8c", Dark: "#DDDDDD"},
)

var PromptStyle = lipgloss.NewStyle()
var FocusedPromptStyle = PromptStyle.Copy().Foreground(PimaryTextColor)

var alertStyle = lipgloss.NewStyle().Padding(1, 2)

var AlertSuccessStyle = alertStyle.Copy().
	Background(lipgloss.CompleteAdaptiveColor{
		Light: lipgloss.CompleteColor{ //bg-green-200
			TrueColor: "#bbf7d0",
			ANSI256:   "194",
			ANSI:      "37",
		},
		Dark: lipgloss.CompleteColor{ //bg-green-200
			TrueColor: "#bbf7d0",
			ANSI256:   "194",
			ANSI:      "37",
		},
	}).
	Foreground(lipgloss.CompleteAdaptiveColor{
		Light: lipgloss.CompleteColor{ //text-green-900
			TrueColor: "#14532d",
			ANSI256:   "29",
			ANSI:      "30",
		},
		Dark: lipgloss.CompleteColor{
			TrueColor: "#166534", //text-green-800
			ANSI256:   "29",
			ANSI:      "30",
		},
	})

var AlertDangerStyle = alertStyle.Copy().
	Background(lipgloss.CompleteAdaptiveColor{
		Light: lipgloss.CompleteColor{ //bg-red-200
			TrueColor: "#fecaca",
			ANSI256:   "224",
			ANSI:      "97",
		},
		Dark: lipgloss.CompleteColor{ //bg-red-200
			TrueColor: "#fecaca",
			ANSI256:   "224",
			ANSI:      "97",
		},
	}).
	Foreground(lipgloss.CompleteAdaptiveColor{
		Light: lipgloss.CompleteColor{ //text-red-900
			TrueColor: "#7f1d1d",
			ANSI256:   "95",
			ANSI:      "30",
		},
		Dark: lipgloss.CompleteColor{
			TrueColor: "#991b1b", //text-red-800
			ANSI256:   "132",
			ANSI:      "31",
		},
	})

var AlertWarningStyle = alertStyle.Copy().
	Background(lipgloss.AdaptiveColor{
		Light: "#fde68a", // bg-amber-200
		Dark:  "#fde68a", // bg-amber-200
	}).
	Foreground(lipgloss.AdaptiveColor{
		Light: "#78350f", // text-amber-900
		Dark:  "#92400e", // text-amber-800
	})
