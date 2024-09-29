package output

import "C"
import (
	"fmt"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
	"os"
	"strings"
	"unicode/utf8"
)

type Table struct {
	headers   []string
	rows      [][]string
	colWidths []int
	maxWidth  int
}

func NewTable(headers []string) *Table {
	width, err := getTerminalWidth()
	width = width - 5
	if err != nil {
		fmt.Println("Error getting terminal size:", err)
		width = 80
	}

	colWidth := width/len(headers) - 3
	colWidths := make([]int, len(headers))
	for i := range colWidths {
		colWidths[i] = colWidth
	}

	for i, header := range headers {
		headers[i] = wrapText(header, colWidths[i])
	}

	return &Table{
		headers:   headers,
		colWidths: colWidths,
		maxWidth:  width,
	}
}

func (t *Table) AddRow(row []string) {
	if len(row) != len(t.headers) {
		fmt.Println("Error: row length does not match header length")
		return
	}

	for i, col := range row {
		row[i] = wrapText(col, t.colWidths[i])
	}

	t.rows = append(t.rows, row)
}

func getTerminalWidth() (int, error) {
	if width, _, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
		return width, nil
	}

	ws, err := unix.IoctlGetWinsize(int(os.Stdout.Fd()), unix.TIOCGWINSZ)
	if err != nil {
		return 0, os.NewSyscallError("GetWinsize", err)
	}

	return int(ws.Col), nil
}

func wrapText(text string, width int) string {
	if utf8.RuneCountInString(text) <= width {
		return text
	}

	var wrapped strings.Builder
	for len(text) > width {
		wrapped.WriteString(text[:width])
		wrapped.WriteString("\n")
		text = text[width:]
	}
	wrapped.WriteString(text)

	return wrapped.String()
}

func (t *Table) Render() {
	t.optimizeColumnWidths()
	t.rewrapContent()

	separator := t.createSeparator()

	fmt.Println(separator)
	t.printRow(t.headers)
	fmt.Println(separator)

	for _, row := range t.rows {
		t.printRow(row)
	}

	fmt.Println(separator)
}

func (t *Table) optimizeColumnWidths() {
	usedWidths := make([]int, len(t.colWidths))
	totalWonSpace := 0
	var shrunkCols []int

	for i, colWidth := range t.colWidths {
		maxUsedWidth := 0
		for _, content := range append([]string{t.headers[i]}, t.getColumnContent(i)...) {
			for _, line := range strings.Split(content, "\n") {
				if len(line) > maxUsedWidth {
					maxUsedWidth = len(line)
				}
			}
		}

		if maxUsedWidth < colWidth {
			wonSpace := colWidth - maxUsedWidth
			totalWonSpace += wonSpace
			t.colWidths[i] = maxUsedWidth
			shrunkCols = append(shrunkCols, i)
		}
		usedWidths[i] = maxUsedWidth
	}

	if totalWonSpace > 0 {
		columnsNeedingSpace := 0
		for i, colWidth := range t.colWidths {
			if colWidth == usedWidths[i] && !contains(shrunkCols, i) {
				columnsNeedingSpace++
			}
		}

		if columnsNeedingSpace > 0 {
			extraSpacePerColumn := totalWonSpace / columnsNeedingSpace
			for i := range t.colWidths {
				if t.colWidths[i] == usedWidths[i] && !contains(shrunkCols, i) {
					t.colWidths[i] += extraSpacePerColumn
				}
			}
		}
	}
}

func contains(arr []int, target int) bool {
	for _, value := range arr {
		if value == target {
			return true
		}
	}
	return false
}

func (t *Table) rewrapContent() {
	for i, header := range t.headers {
		unwrappedHeader := unwrapText(header)
		t.headers[i] = wrapText(unwrappedHeader, t.colWidths[i])
	}

	for i, row := range t.rows {
		for j, col := range row {
			unwrappedCol := unwrapText(col)
			t.rows[i][j] = wrapText(unwrappedCol, t.colWidths[j])
		}
	}
}

func unwrapText(text string) string {
	return strings.ReplaceAll(text, "\n", "")
}

func (t *Table) getColumnContent(columnIndex int) []string {
	content := make([]string, len(t.rows))
	for i, row := range t.rows {
		content[i] = row[columnIndex]
	}
	return content
}

func (t *Table) createSeparator() string {
	parts := make([]string, len(t.headers))
	for i, width := range t.colWidths {
		parts[i] = strings.Repeat("-", width+2)
	}
	return "+" + strings.Join(parts, "+") + "+"
}

func (t *Table) printRow(row []string) {
	lines := make([][]string, len(row))
	maxLines := 1
	for i, col := range row {
		lines[i] = strings.Split(col, "\n")
		if len(lines[i]) > maxLines {
			maxLines = len(lines[i])
		}
	}

	for i := 0; i < maxLines; i++ {
		parts := make([]string, len(row))
		for j, colLines := range lines {
			if i < len(colLines) {
				parts[j] = fmt.Sprintf(" %-*s ", t.colWidths[j], colLines[i])
			} else {
				parts[j] = fmt.Sprintf(" %-*s ", t.colWidths[j], "")
			}
		}
		fmt.Println("|" + strings.Join(parts, "|") + "|")
	}
}
