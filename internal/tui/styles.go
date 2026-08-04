package tui

import (
	"github.com/charmbracelet/lipgloss"
)

var (
	// Colors
	Green  = lipgloss.Color("42")
	Yellow = lipgloss.Color("226")
	Red    = lipgloss.Color("196")
	Blue   = lipgloss.Color("39")
	Gray   = lipgloss.Color("244")
	White  = lipgloss.Color("255")
	DarkBg = lipgloss.Color("236")
	Cyan   = lipgloss.Color("86")
	Purple = lipgloss.Color("129")
	Orange = lipgloss.Color("214")

	// Styles
	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(Cyan).
			MarginBottom(1)

	PanelStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(Blue).
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

	GreenStyle   = lipgloss.NewStyle().Foreground(Green)
	YellowStyle  = lipgloss.NewStyle().Foreground(Yellow)
	RedStyle     = lipgloss.NewStyle().Foreground(Red)
	GrayStyle    = lipgloss.NewStyle().Foreground(Gray)
	WhiteStyle   = lipgloss.NewStyle().Foreground(White)
	CyanStyle    = lipgloss.NewStyle().Foreground(Cyan)
	PurpleStyle  = lipgloss.NewStyle().Foreground(Purple)
	OrangeStyle  = lipgloss.NewStyle().Foreground(Orange)

	// Dim style for completed/terminal nodes.
	DimStyle = lipgloss.NewStyle().Foreground(Gray).Faint(true)

	// Running style for the currently executing node.
	RunningStyle = lipgloss.NewStyle().Foreground(White).Bold(true)

	// Failed style for failed/cancelled nodes — stands out in red.
	FailedStyle = lipgloss.NewStyle().Foreground(Red)

	// Progress bar styles.
	ProgressFull  = lipgloss.NewStyle().Foreground(Green)
	ProgressEmpty = lipgloss.NewStyle().Foreground(lipgloss.Color("238"))
)

// StatusIcon returns a descriptive icon for a node status.
func StatusIcon(status string) string {
	switch status {
	case "COMPLETED":
		return GreenStyle.Render("✓")
	case "RUNNING":
		// ▶ is the running focus marker — the only bright icon on screen.
		return YellowStyle.Render("▶")
	case "FAILED":
		return RedStyle.Render("✗")
	case "WAITING":
		return PurpleStyle.Render("⏸")
	case "READY":
		return CyanStyle.Render("↻")
	default: // PENDING
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
	case "WAITING":
		return PurpleStyle
	default:
		return DimStyle
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
