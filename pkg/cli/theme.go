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

func ColorScheme() fang.ColorScheme {
	return fang.ColorScheme{
		Base:           LightDark(Black, White),
		Title:          LightDark(Black, White),
		Codeblock:      LightDark(Gray100, Gray900),
		Program:        LightDark(Sky500, Sky500),
		Command:        LightDark(Sky500, Sky500),
		DimmedArgument: LightDark(Gray300, Sky300),
		Comment:        LightDark(Gray500, Gray300),
		Flag:           LightDark(Green, Green),
		Argument:       LightDark(Green, Gray900),
		Description:    LightDark(Black, White),
		FlagDefault:    LightDark(Gray500, Gray500),
		QuotedString:   LightDark(Gray100, Gray900),
		ErrorHeader: [2]color.Color{
			Red100,
			Red700,
		},
	}
}
