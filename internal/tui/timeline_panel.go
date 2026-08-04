package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/lipgloss"
)

// TimelinePanel renders the event timeline.
type TimelinePanel struct {
	items      []TimelineItem
	Filter     FilterState
	focused    bool
	viewport   viewport.Model
	dispatcher *Dispatcher
}

// NewTimelinePanel creates a new timeline panel.
func NewTimelinePanel(dispatcher *Dispatcher) *TimelinePanel {
	vp := viewport.New(80, 20)
	vp.Style = lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return &TimelinePanel{
		Filter:     DefaultFilter(),
		viewport:   vp,
		dispatcher: dispatcher,
	}
}

// Init is a no-op; the Model owns the single dispatcher subscription.
func (p *TimelinePanel) Init() tea.Cmd { return nil }

// Update handles messages.
func (p *TimelinePanel) Update(msg tea.Msg) (tea.Cmd, bool) {
	switch msg := msg.(type) {
	case EventAppendedEvent:
		if p.filterItem(msg.Item) {
			p.items = append(p.items, msg.Item)
		}
		return nil, true

	case tea.KeyMsg:
		if !p.focused {
			return nil, false
		}
		switch msg.String() {
		case "up", "k":
			p.viewport.LineUp(1)
		case "down", "j":
			p.viewport.LineDown(1)
		case "pgup":
			p.viewport.PageUp()
		case "pgdown":
			p.viewport.PageDown()
		default:
			return nil, false
		}
		return nil, true

	case tea.WindowSizeMsg:
		p.viewport.Width = msg.Width - 4
		p.viewport.Height = msg.Height - 4
		return nil, true
	}

	return nil, false
}

// SetFocus sets keyboard focus on this panel.
func (p *TimelinePanel) SetFocus(focused bool) {
	p.focused = focused
}

func (p *TimelinePanel) filterItem(item TimelineItem) bool {
	switch item.Level() {
	case ERROR:
		return p.Filter.Errors
	case WARN:
		return p.Filter.NodeEvents || p.Filter.WorkflowEvents
	case DEBUG:
		return p.Filter.ToolCalls
	default:
		return p.Filter.WorkflowEvents || p.Filter.NodeEvents
	}
}

// View renders the timeline content.
func (p *TimelinePanel) View(width, height int) string {
	p.viewport.Width = width - 4
	p.viewport.Height = height - 4

	var sb strings.Builder
	sb.WriteString(HeaderStyle.Render("TIMELINE"))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	if len(p.items) == 0 {
		sb.WriteString(GrayStyle.Render("  waiting for events...\n"))
	} else {
		start := 0
		if len(p.items) > p.viewport.Height {
			start = len(p.items) - p.viewport.Height
		}
		for _, item := range p.items[start:] {
			timeStr := item.Time().Local().Format("15:04:05")
			levelColor := lipgloss.NewStyle().Foreground(lipgloss.Color(item.Level().Color()))
			sb.WriteString(fmt.Sprintf("  %s  %s  %s\n",
				GrayStyle.Render(timeStr),
				levelColor.Render(item.Level().String()),
				item.Render(),
			))
		}
	}

	p.viewport.SetContent(sb.String())
	return PanelStyle.Width(width).Render(p.viewport.View())
}
