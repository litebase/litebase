package cli

import (
	"os"

	"github.com/charmbracelet/fang"
	"github.com/charmbracelet/lipgloss/v2"
	"github.com/charmbracelet/x/term"
)

func ColorScheme() fang.ColorScheme {
	var isDark bool

	if term.IsTerminal(os.Stdout.Fd()) {
		isDark = lipgloss.HasDarkBackground(os.Stdin, os.Stdout)
	}

	c := lipgloss.LightDark(isDark)

	return fang.ColorScheme{
		Base:           c(Black, White),
		Title:          c(Black, White),
		Codeblock:      c(Gray100, Gray900),
		Program:        c(Sky500, Sky500),
		Command:        c(Sky500, Sky500),
		DimmedArgument: c(Gray300, Sky300),
		Comment:        c(Gray500, Gray300),
		Flag:           c(Green, Green),
		Argument:       c(Green, Gray900),
		Description:    c(Black, White),
		FlagDefault:    c(Gray500, Gray500),
		QuotedString:   c(Gray100, Gray900),
	}
}
