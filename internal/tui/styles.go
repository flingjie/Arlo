package tui

import (
	"github.com/charmbracelet/lipgloss"
)

// Semantic palette — keep to ~6 roles (see TUI redesign spec).
var (
	Green  = lipgloss.Color("42")  // RUNNING / success
	Yellow = lipgloss.Color("226") // BLOCKED / warn
	Red    = lipgloss.Color("196") // FAILED / error
	Blue   = lipgloss.Color("39")  // selection background accent
	Gray   = lipgloss.Color("244") // secondary / unfocused
	White  = lipgloss.Color("255")
	DarkBg = lipgloss.Color("236")
	Cyan   = lipgloss.Color("86") // titles / focused border

	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(Cyan)

	// PanelStyle is the unfocused single-line panel chrome.
	PanelStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(Gray).
			Padding(0, 1)

	StatusBarStyle = lipgloss.NewStyle().
			Background(DarkBg).
			Foreground(lipgloss.Color("252"))

	CommandPromptStyle = lipgloss.NewStyle().
				Foreground(Yellow).
				Bold(true)

	SelectedStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("57")).
			Foreground(White)

	NormalStyle = lipgloss.NewStyle()

	GreenStyle  = lipgloss.NewStyle().Foreground(Green)
	YellowStyle = lipgloss.NewStyle().Foreground(Yellow)
	RedStyle    = lipgloss.NewStyle().Foreground(Red)
	GrayStyle   = lipgloss.NewStyle().Foreground(Gray)
	WhiteStyle  = lipgloss.NewStyle().Foreground(White)
	CyanStyle   = lipgloss.NewStyle().Foreground(Cyan)

	DimStyle     = lipgloss.NewStyle().Foreground(Gray).Faint(true)
	RunningStyle = lipgloss.NewStyle().Foreground(Green).Bold(true)
	FailedStyle  = lipgloss.NewStyle().Foreground(Red)

	ProgressFull  = lipgloss.NewStyle().Foreground(Green)
	ProgressEmpty = lipgloss.NewStyle().Foreground(lipgloss.Color("238"))
)

// PanelChrome returns single-line panel border style; focused borders are Cyan.
func PanelChrome(focused bool) lipgloss.Style {
	fg := Gray
	if focused {
		fg = Cyan
	}
	return lipgloss.NewStyle().
		BorderStyle(lipgloss.NormalBorder()).
		BorderForeground(fg).
		Padding(0, 1)
}

// SelectionCursor returns the row prefix for the selected node (not a status glyph).
// Uses ▸ (narrow) rather than ▶ (ambiguous): on CJK terminals ▶ often renders
// double-width while the unselected placeholder is a single space, shifting icons.
func SelectionCursor(selected bool) string {
	if selected {
		return CyanStyle.Render("▸")
	}
	return " "
}

// StatusIcon returns a dual-coded glyph for a display/raw status string.
func StatusIcon(status string) string {
	switch status {
	case "COMPLETED":
		return GreenStyle.Render("✓")
	case "RUNNING", "STARTING":
		return GreenStyle.Render("●")
	case "FAILED", "CANCELLED":
		return RedStyle.Render("✗")
	case "BLOCKED":
		return YellowStyle.Render("■")
	case "WAITING", "PENDING":
		return GrayStyle.Render("○")
	case "READY":
		return CyanStyle.Render("↻")
	default:
		return GrayStyle.Render("○")
	}
}

// nodeLineStyle returns the style for a node's main line based on its status.
func nodeLineStyle(status string, isSelected bool) lipgloss.Style {
	if isSelected {
		return SelectedStyle
	}
	switch status {
	case "RUNNING", "STARTING":
		return RunningStyle
	case "COMPLETED":
		return DimStyle
	case "FAILED", "CANCELLED":
		return FailedStyle
	case "WAITING", "PENDING", "BLOCKED":
		return GrayStyle
	default:
		return DimStyle
	}
}

// statusTextStyle returns the style for the status label on the right side.
func statusTextStyle(status string, isSelected bool) lipgloss.Style {
	if isSelected {
		return SelectedStyle
	}
	switch status {
	case "RUNNING", "STARTING":
		return GreenStyle.Bold(true)
	case "COMPLETED":
		return GreenStyle
	case "FAILED", "CANCELLED":
		return RedStyle
	case "BLOCKED":
		return YellowStyle.Bold(true)
	case "WAITING", "PENDING":
		return GrayStyle
	case "READY":
		return CyanStyle
	default:
		return GrayStyle
	}
}

// ProgressBar renders a progress bar.
func ProgressBar(completed, total int, width int) string {
	if total == 0 {
		return ProgressEmpty.Render("[") + repeatStr(" ", width) + ProgressEmpty.Render("]")
	}
	ratio := float64(completed) / float64(total)
	filled := int(ratio * float64(width))
	if filled > width {
		filled = width
	}
	empty := width - filled
	return ProgressFull.Render("[") +
		ProgressFull.Render(repeatStr("█", filled)) +
		ProgressEmpty.Render(repeatStr("░", empty)) +
		ProgressFull.Render("]")
}

func repeatStr(s string, n int) string {
	b := make([]byte, 0, n*len(s))
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return string(b)
}
