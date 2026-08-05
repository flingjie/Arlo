package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// TimelinePanel renders the event timeline.
type TimelinePanel struct {
	items      []TimelineItem
	Filter     FilterState
	focused    bool
	viewport   viewport.Model
	dispatcher *Dispatcher

	Follow  bool // auto-scroll to latest; paused on manual scroll
	Compact bool // critical events only
	cursor  int  // index into visibleItems()
	expand  bool // show full text for cursor line
}

// NewTimelinePanel creates a new timeline panel.
func NewTimelinePanel(dispatcher *Dispatcher) *TimelinePanel {
	vp := viewport.New(80, 20)
	vp.Style = lipgloss.NewStyle()

	return &TimelinePanel{
		Filter:     DefaultFilter(),
		viewport:   vp,
		dispatcher: dispatcher,
		Follow:     true,
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
			if p.Follow {
				vis := p.visibleItems()
				if len(vis) > 0 {
					p.cursor = len(vis) - 1
				}
			}
		}
		return nil, true

	case tea.KeyMsg:
		if !p.focused {
			return nil, false
		}
		switch msg.String() {
		case "up", "k":
			p.Follow = false
			if p.cursor > 0 {
				p.cursor--
			}
			p.viewport.LineUp(1)
		case "down", "j":
			p.Follow = false
			vis := p.visibleItems()
			if p.cursor < len(vis)-1 {
				p.cursor++
			}
			p.viewport.LineDown(1)
		case "pgup":
			p.Follow = false
			p.viewport.PageUp()
		case "pgdown":
			p.Follow = false
			p.viewport.PageDown()
		case "right", "l":
			p.expand = !p.expand
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

// ResumeFollow re-enables auto-follow and jumps to the latest event.
func (p *TimelinePanel) ResumeFollow() {
	p.Follow = true
	vis := p.visibleItems()
	if len(vis) > 0 {
		p.cursor = len(vis) - 1
	}
	p.expand = false
}

// ToggleCompact flips compact mode (critical events only).
func (p *TimelinePanel) ToggleCompact() {
	p.Compact = !p.Compact
	vis := p.visibleItems()
	if p.cursor >= len(vis) {
		if len(vis) == 0 {
			p.cursor = 0
		} else {
			p.cursor = len(vis) - 1
		}
	}
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

// isCriticalTimelineItem is the compact-mode allowlist:
// failed, waiting, annotated, completed (plus task-level failed/completed).
func isCriticalTimelineItem(item TimelineItem) bool {
	switch item.(type) {
	case NodeFailedItem, NodeWaitingItem, NodeAnnotatedItem, NodeCompletedItem,
		TaskFailedItem, TaskCompletedItem:
		return true
	default:
		return false
	}
}

func (p *TimelinePanel) visibleItems() []TimelineItem {
	out := make([]TimelineItem, 0, len(p.items))
	for _, it := range p.items {
		if p.Compact && !isCriticalTimelineItem(it) {
			continue
		}
		out = append(out, it)
	}
	return out
}

func levelShort(l Level) string {
	switch l {
	case ERROR:
		return "ERRO"
	case DEBUG:
		return "DEBG"
	default:
		s := l.String()
		if len(s) > 4 {
			return s[:4]
		}
		return s
	}
}

// View renders the timeline content.
func (p *TimelinePanel) View(width, height int) string {
	p.viewport.Width = width - 4
	p.viewport.Height = height - 4

	var sb strings.Builder
	title := "TIMELINE"
	if p.focused {
		title = "TIMELINE *"
	}
	sb.WriteString(HeaderStyle.Render(title))
	if p.Compact {
		sb.WriteString(GrayStyle.Render(" [compact]"))
	}
	if !p.Follow {
		sb.WriteString(YellowStyle.Render(" [paused]"))
	}
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", max(width-2, 2)))
	sb.WriteString("\n\n")

	vis := p.visibleItems()
	if len(vis) == 0 {
		sb.WriteString(GrayStyle.Render("  waiting for events...\n"))
	} else {
		if p.Follow {
			p.cursor = len(vis) - 1
		}
		if p.cursor < 0 {
			p.cursor = 0
		}
		if p.cursor >= len(vis) {
			p.cursor = len(vis) - 1
		}

		msgWidth := width - 18
		if msgWidth < 10 {
			msgWidth = 10
		}

		start := 0
		if len(vis) > p.viewport.Height {
			if p.Follow {
				start = len(vis) - p.viewport.Height
			} else {
				start = p.cursor - p.viewport.Height/2
				if start < 0 {
					start = 0
				}
				if start > len(vis)-p.viewport.Height {
					start = len(vis) - p.viewport.Height
				}
			}
		}
		end := start + p.viewport.Height
		if end > len(vis) {
			end = len(vis)
		}

		for i := start; i < end; i++ {
			item := vis[i]
			timeStr := item.Time().Local().Format("15:04:05")
			lvl := levelShort(item.Level())
			levelColor := lipgloss.NewStyle().Foreground(lipgloss.Color(item.Level().Color()))
			msg := item.Render()
			if !(p.expand && i == p.cursor) {
				msg = truncateRunes(msg, msgWidth)
			}
			prefix := SelectionCursor(i == p.cursor && p.focused) + " "
			sb.WriteString(fmt.Sprintf("%s%s  %s  %s\n",
				prefix,
				GrayStyle.Render(timeStr),
				levelColor.Render(fmt.Sprintf("%-4s", lvl)),
				msg,
			))
		}
	}

	p.viewport.SetContent(sb.String())
	return PanelChrome(p.focused).Width(width).Render(p.viewport.View())
}

func truncateRunes(s string, maxW int) string {
	if maxW <= 1 {
		return s
	}
	if lipgloss.Width(s) <= maxW {
		return s
	}
	// Walk runes until visible width fits with ellipsis.
	var b strings.Builder
	w := 0
	for _, r := range s {
		rw := lipgloss.Width(string(r))
		if w+rw >= maxW-1 {
			b.WriteRune('…')
			return b.String()
		}
		b.WriteRune(r)
		w += rw
	}
	return b.String()
}
