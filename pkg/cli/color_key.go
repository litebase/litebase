package cli

import (
	"fmt"

	"github.com/lucasb-eyer/go-colorful"
)

type ColorKey int

// Colors
const (
	Amber100 ColorKey = iota
	Amber600
	Black
	Blue
	Gray100
	Gray300
	Gray400
	Gray500
	Gray900
	Green
	Green200
	Green500
	Green700
	Pink
	Pink500
	Pink600
	Red100
	Red700
	Sky300
	Sky500
	Sky700
	White
)

func (ck ColorKey) Hex() string {
	return map[ColorKey]string{
		Amber100: "#fef3c6",
		Amber600: "#e17100",
		Black:    "#000000",
		Blue:     "#5BBAFF",
		Gray100:  "#F5F5F5",
		Gray300:  "#F5F5F5",
		Gray400:  "#d4d4d4",
		Gray500:  "#737373",
		Gray900:  "#1C1C1C",
		Green:    "#77FF74",
		Green200: "#00c951",
		Green500: "#22C55E",
		Green700: "#008236",
		Pink:     "#FF94E0",
		Pink500:  "#f6339a",
		Pink600:  "#e60076",
		Red100:   "#ffe2e2",
		Red700:   "#c10007",
		Sky300:   "#74d4ff",
		Sky500:   "#00a6f4",
		Sky700:   "#0069a8",
		White:    "#FFFFFF",
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
		Amber100: "amber100",
		Amber600: "amber600",
		Black:    "black",
		Blue:     "blue",
		Gray100:  "gray100",
		Gray300:  "gray300",
		Gray400:  "gray400",
		Gray500:  "gray500",
		Gray900:  "gray900",
		Green:    "green",
		Green200: "green200",
		Green500: "green500",
		Green700: "green700",
		Pink:     "pink",
		Pink500:  "pink500",
		Pink600:  "pink600",
		Red100:   "red100",
		Red700:   "red700",
		Sky300:   "sky300",
		Sky500:   "sky500",
		Sky700:   "sky700",
		White:    "white",
	}[ck]
}
