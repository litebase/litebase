package cli

import (
	"fmt"

	"github.com/lucasb-eyer/go-colorful"
)

type ColorKey int

// Colors
const (
	Black ColorKey = iota
	Blue
	Gray100
	Gray300
	Gray500
	Gray900
	Green
	Pink
	Pink500
	Red
	Sky300
	Sky500
	Sky700
	White
)

func (ck ColorKey) Hex() string {
	return map[ColorKey]string{
		Black:   "#000000",
		Blue:    "#5BBAFF",
		Gray100: "#F5F5F5",
		Gray300: "#F5F5F5",
		Gray500: "#737373",
		Gray900: "#1C1C1C",
		Green:   "#77FF74",
		Pink:    "#FF94E0",
		Pink500: "#f6339a",
		Red:     "#FF0000",
		Sky300:  "#74d4ff",
		Sky500:  "#00a6f4",
		Sky700:  "#0069a8",
		White:   "#FFFFFF",
	}[ck]
}

func (ck ColorKey) RGBA() (r, g, b, a uint32) {
	c, err := colorful.Hex(ck.Hex())

	if err != nil {
		panic(fmt.Sprintf("invalid color key %d: %s: %v", ck, ck.String(), err))
	}
	return c.RGBA()
}

func (ck ColorKey) String() string {
	return map[ColorKey]string{
		Black: "black",
		Blue:  "blue",
		Green: "green",
		Pink:  "pink",
		Red:   "red",
		White: "white",
	}[ck]
}
