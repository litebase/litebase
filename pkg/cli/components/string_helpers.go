package components

import "strings"

// truncateString truncates a string to maxLength and adds "..." if truncated
func truncateString(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}

	if maxLength <= 3 {
		return s[:maxLength]
	}

	return s[:maxLength-3] + "..."
}

// wordWrap wraps text to the specified width, breaking at word boundaries
func wordWrap(text string, width int) string {
	if width <= 0 {
		return text
	}

	words := strings.Fields(text)
	if len(words) == 0 {
		return text
	}

	var lines []string
	var currentLine []string
	currentLength := 0

	for _, word := range words {
		wordLength := len(word)

		// If adding this word would exceed the width, start a new line
		if currentLength+wordLength+len(currentLine) > width && len(currentLine) > 0 {
			lines = append(lines, strings.Join(currentLine, " "))
			currentLine = []string{word}
			currentLength = wordLength
		} else {
			currentLine = append(currentLine, word)
			currentLength += wordLength
		}
	}

	// Add the last line if it has content
	if len(currentLine) > 0 {
		lines = append(lines, strings.Join(currentLine, " "))
	}

	return strings.Join(lines, "\n")
}
