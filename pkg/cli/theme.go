package cli

import (
	"image/color"
	"os"

	"github.com/charmbracelet/fang"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/charmbracelet/x/term"
)

func IsDarkMode() bool {
	if term.IsTerminal(os.Stdout.Fd()) {
		return lipgloss.HasDarkBackground(os.Stdin, os.Stdout)
	}

	return false
}

func LightDark(light color.Color, dark color.Color) color.Color {
	var isDark bool

	if term.IsTerminal(os.Stdout.Fd()) {
		isDark = lipgloss.HasDarkBackground(os.Stdin, os.Stdout)
	}

	return lipgloss.LightDark(isDark)(light, dark)
}

func ColorScheme(lightDark lipgloss.LightDarkFunc) fang.ColorScheme {
	return fang.ColorScheme{
		Base:           lightDark(Black, White),
		Title:          lightDark(Black, White),
		Codeblock:      lightDark(Gray100, Gray900),
		Program:        lightDark(Sky700, Sky500),
		Command:        lightDark(Sky700, Sky500),
		DimmedArgument: lightDark(Gray500, Sky300),
		Comment:        lightDark(Gray500, Gray300),
		Flag:           lightDark(Green700, Green200),
		Argument:       lightDark(Green700, Gray900),
		Description:    lightDark(Black, White),
		FlagDefault:    lightDark(Gray500, Gray500),
		QuotedString:   lightDark(Gray900, Gray100),
		ErrorHeader: [2]color.Color{
			Red100,
			Red700,
		},
	}
}
