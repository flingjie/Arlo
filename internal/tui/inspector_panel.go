package tui

import (
	"fmt"
	"strings"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// InspectorPanel renders the detailed view of a selected node.
type InspectorPanel struct {
	node    *arlov1.NodeState
	tab     InspectorTab
	focused bool

	// Event log buffer: nodeID → log lines for the Logs tab.
	nodeEvents map[string][]TimelineItem

	// Dispatcher subscription.
	dispatcher *Dispatcher
	sub        Subscriber
}

// NewInspectorPanel creates a new inspector panel.
func NewInspectorPanel(dispatcher *Dispatcher) *InspectorPanel {
	return &InspectorPanel{
		tab:        TabSummary,
		dispatcher: dispatcher,
		nodeEvents: make(map[string][]TimelineItem),
	}
}

// Init subscribes to the dispatcher for event log collection.
func (p *InspectorPanel) Init() tea.Cmd {
	if p.dispatcher != nil {
		p.sub = p.dispatcher.Subscribe()
		return p.listenDispatcher
	}
	return nil
}

func (p *InspectorPanel) listenDispatcher() tea.Msg {
	event := <-p.sub
	return event
}

// Update handles internal events for log collection.
func (p *InspectorPanel) Update(msg tea.Msg) (tea.Cmd, bool) {
	switch msg := msg.(type) {
	case EventAppendedEvent:
		nodeID := extractNodeIDFromItem(msg.Item)
		if nodeID != "" {
			p.nodeEvents[nodeID] = append(p.nodeEvents[nodeID], msg.Item)
			// Keep last 200 entries per node.
			if len(p.nodeEvents[nodeID]) > 200 {
				p.nodeEvents[nodeID] = p.nodeEvents[nodeID][len(p.nodeEvents[nodeID])-200:]
			}
		}
		return nil, true
	case InternalEvent:
		return p.listenDispatcher, true
	}
	return nil, false
}

// extractNodeIDFromItem extracts the node ID from a timeline item.
func extractNodeIDFromItem(item TimelineItem) string {
	switch it := item.(type) {
	case NodeCreatedItem:
		return it.NodeID
	case NodeStartedItem:
		return it.NodeID
	case NodeCompletedItem:
		return it.NodeID
	case NodeFailedItem:
		return it.NodeID
	case NodeWaitingItem:
		return it.NodeID
	case NodeAnnotatedItem:
		return it.NodeID
	case NodeHeartbeatItem:
		return it.NodeID
	case MetricsSnapshotItem:
		return it.NodeID
	}
	return ""
}

// SetNode sets the node to inspect.
func (p *InspectorPanel) SetNode(n *arlov1.NodeState) {
	p.node = n
}

// Node returns the currently inspected node.
func (p *InspectorPanel) Node() *arlov1.NodeState {
	return p.node
}

// SetTab sets the active tab.
func (p *InspectorPanel) SetTab(t InspectorTab) {
	p.tab = t
}

// SetFocus sets keyboard focus.
func (p *InspectorPanel) SetFocus(focused bool) { p.focused = focused }

// View renders the inspector.
func (p *InspectorPanel) View(width, height int) string {
	if p.node == nil {
		return PanelStyle.Width(width).Height(height).Render(
			GrayStyle.Render("  select a node to inspect (Enter)"),
		)
	}

	var sb strings.Builder

	tabs := []InspectorTab{TabSummary, TabLogs, TabPrompt, TabArtifacts, TabMetrics}
	tabBar := ""
	for _, t := range tabs {
		if t == p.tab {
			tabBar += SelectedStyle.Render(fmt.Sprintf("[%s]", t.String()))
		} else {
			tabBar += GrayStyle.Render(fmt.Sprintf(" %s ", t.String()))
		}
		tabBar += " "
	}
	sb.WriteString(HeaderStyle.Render("NODE INSPECTOR"))
	sb.WriteString("  ")
	sb.WriteString(tabBar)
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	switch p.tab {
	case TabSummary:
		p.renderSummary(&sb)
	case TabLogs:
		p.renderLogs(&sb)
	case TabPrompt:
		p.renderPrompt(&sb)
	case TabArtifacts:
		p.renderArtifacts(&sb)
	case TabMetrics:
		p.renderMetrics(&sb)
	}

	return PanelStyle.Width(width).Height(height).Render(sb.String())
}

// ── Summary Tab ───────────────────────────────────

func (p *InspectorPanel) renderSummary(sb *strings.Builder) {
	n := p.node

	// ── Status ──
	sb.WriteString("  ")
	sb.WriteString(CyanStyle.Render("── Status"))
	sb.WriteString(strings.Repeat("─", 40))
	sb.WriteString("\n")

	icon := StatusIcon(n.Status)
	statusStyle := statusTextStyle(n.Status, false)
	sb.WriteString(fmt.Sprintf("  %s  %s\n", icon, statusStyle.Render(n.Status)))

	if n.StartedAt != "" {
		sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Started"), WhiteStyle.Render(relativeTime(n.StartedAt))))
	}
	if n.CompletedAt != "" {
		sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Completed"), WhiteStyle.Render(relativeTime(n.CompletedAt))))
	}
	if n.RetryCount > 0 {
		sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Retries"), YellowStyle.Render(fmt.Sprintf("%d", n.RetryCount))))
	}

	sb.WriteString("\n")

	// ── Configuration ──
	sb.WriteString("  ")
	sb.WriteString(CyanStyle.Render("── Configuration"))
	sb.WriteString(strings.Repeat("─", 34))
	sb.WriteString("\n")

	gate := n.Gate
	if gate == "" || gate == "none" {
		gate = "—"
	}
	sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Gate"), WhiteStyle.Render(gate)))

	deps := strings.Join(n.DependsOn, ", ")
	if deps == "" {
		deps = "—"
	}
	sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Depends On"), WhiteStyle.Render(deps)))

	children := strings.Join(n.Children, ", ")
	if children == "" {
		children = "—"
	}
	sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Children"), WhiteStyle.Render(children)))

	sb.WriteString("\n")

	// ── Session ──
	sb.WriteString("  ")
	sb.WriteString(CyanStyle.Render("── Session"))
	sb.WriteString(strings.Repeat("─", 38))
	sb.WriteString("\n")

	sess := n.SessionId
	if sess == "" {
		sess = "—"
	}
	sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Session"), WhiteStyle.Render(sess)))

	rt := n.RuntimeId
	if rt == "" {
		rt = "—"
	}
	sb.WriteString(fmt.Sprintf("  %-12s  %s\n", GrayStyle.Render("Runtime"), WhiteStyle.Render(rt)))

	sb.WriteString("\n")

	// ── Commands ──
	sb.WriteString("  ")
	sb.WriteString(CyanStyle.Render("── Commands"))
	sb.WriteString(strings.Repeat("─", 36))
	sb.WriteString("\n")
	sb.WriteString("  ")
	sb.WriteString(GrayStyle.Render(":attach :approve :reject :retry :logs"))
	sb.WriteString("\n")
}

// ── Logs Tab ──────────────────────────────────────

func (p *InspectorPanel) renderLogs(sb *strings.Builder) {
	events := p.nodeEvents[p.node.NodeId]
	if len(events) == 0 {
		sb.WriteString(GrayStyle.Render("  No log entries yet — events will appear as the node runs.\n"))
		return
	}

	// Show most recent first (reversed chronological).
	for i := len(events) - 1; i >= 0; i-- {
		item := events[i]
		timeStr := item.Time().Local().Format("15:04:05")
		levelColor := lipgloss.NewStyle().Foreground(lipgloss.Color(item.Level().Color()))
		sb.WriteString(fmt.Sprintf("  %s  %s  %s\n",
			GrayStyle.Render(timeStr),
			levelColor.Render(item.Level().String()),
			item.Render(),
		))
	}
}

// ── Stub Tabs ─────────────────────────────────────

func (p *InspectorPanel) renderPrompt(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Prompt tab — coming in v1.1"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderArtifacts(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Artifacts tab — coming in v1.1"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderMetrics(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Metrics tab — coming in v1.1"))
	sb.WriteString("\n")
}

// ── Helpers ───────────────────────────────────────

// relativeTime converts an RFC3339 time string to a human-readable relative duration.
func relativeTime(rfc3339 string) string {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		return rfc3339
	}
	d := time.Since(t)
	if d < 0 {
		d = -d
	}
	switch {
	case d < time.Second:
		return "just now"
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
}
