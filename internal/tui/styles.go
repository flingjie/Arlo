package tui

import "github.com/charmbracelet/lipgloss"

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
			Foreground(lipgloss.Color("252")).
			Padding(0, 1)

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
	PurpleStyle = lipgloss.NewStyle().Foreground(Purple)
)

// StatusIcon returns a colored dot for a node status.
func StatusIcon(status string) string {
	switch status {
	case "COMPLETED":
		return GreenStyle.Render("●")
	case "RUNNING", "STARTING":
		return YellowStyle.Render("●")
	case "FAILED", "CANCELLED":
		return RedStyle.Render("●")
	case "WAITING":
		return PurpleStyle.Render("●")
	case "READY":
		return CyanStyle.Render("●")
	default:
		return GrayStyle.Render("○")
	}
}

// ProgressBar renders a progress bar.
func ProgressBar(completed, total int, width int) string {
	if total == 0 {
		return GrayStyle.Render("[" + repeatStr(" ", width) + "]")
	}
	ratio := float64(completed) / float64(total)
	filled := int(ratio * float64(width))
	if filled > width {
		filled = width
	}
	empty := width - filled
	return lipgloss.NewStyle().Foreground(Green).Render("[" +
		repeatStr("█", filled) +
		repeatStr("░", empty) +
		"]")
}

func repeatStr(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
