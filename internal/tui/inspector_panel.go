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

	// Latest metrics snapshot per node (for the Metrics tab).
	latestMetrics map[string]MetricsSnapshotItem

	// Dispatcher is retained for command Dispatch helpers; Model owns Subscribe.
	dispatcher *Dispatcher
}

// NewInspectorPanel creates a new inspector panel.
func NewInspectorPanel(dispatcher *Dispatcher) *InspectorPanel {
	return &InspectorPanel{
		tab:           TabSummary,
		dispatcher:    dispatcher,
		nodeEvents:    make(map[string][]TimelineItem),
		latestMetrics: make(map[string]MetricsSnapshotItem),
	}
}

// Init is a no-op; the Model owns the single dispatcher subscription.
func (p *InspectorPanel) Init() tea.Cmd { return nil }

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
		// Cache latest metrics snapshot per node.
		if m, ok := msg.Item.(MetricsSnapshotItem); ok {
			p.latestMetrics[m.NodeID] = m
		}
		return nil, true
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
	case ArtifactCreatedItem:
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

	innerW := width - 4 // account for panel border + padding
	if innerW < 20 {
		innerW = 20
	}

	var sb strings.Builder

	// Title on its own line — HeaderStyle.MarginBottom used to scramble the tab bar.
	sb.WriteString(CyanStyle.Bold(true).Render("NODE INSPECTOR"))
	sb.WriteString("\n")
	sb.WriteString(p.renderTabBar())
	sb.WriteString("\n")
	sb.WriteString(GrayStyle.Render(strings.Repeat("─", innerW)))
	sb.WriteString("\n")

	switch p.tab {
	case TabSummary:
		p.renderSummary(&sb, innerW)
	case TabLogs:
		p.renderLogs(&sb)
	case TabPrompt:
		p.renderPrompt(&sb, innerW)
	case TabArtifacts:
		p.renderArtifacts(&sb)
	case TabMetrics:
		p.renderMetrics(&sb)
	}

	return PanelStyle.Width(width).Height(height).Render(sb.String())
}

func (p *InspectorPanel) renderTabBar() string {
	tabs := []InspectorTab{TabSummary, TabLogs, TabPrompt, TabArtifacts, TabMetrics}
	parts := make([]string, 0, len(tabs))
	for i, t := range tabs {
		label := fmt.Sprintf("%d:%s", i+1, t.String())
		if t == p.tab {
			parts = append(parts, SelectedStyle.Render(" "+label+" "))
		} else {
			parts = append(parts, GrayStyle.Render(" "+label+" "))
		}
	}
	return strings.Join(parts, "")
}

// ── Summary Tab ───────────────────────────────────

func (p *InspectorPanel) renderSummary(sb *strings.Builder, width int) {
	n := p.node

	// Hero: icon + name left, status right.
	icon := StatusIcon(n.Status)
	statusStyle := statusTextStyle(n.Status, false)
	name := WhiteStyle.Bold(true).Render(n.NodeId)
	status := statusStyle.Render(n.Status)
	left := icon + " " + name
	gap := width - lipgloss.Width(left) - lipgloss.Width(status) - 2
	if gap < 1 {
		gap = 1
	}
	sb.WriteString("  " + left + strings.Repeat(" ", gap) + status + "\n")

	var meta []string
	if n.RetryCount > 0 {
		meta = append(meta, YellowStyle.Render(fmt.Sprintf("retry %d", n.RetryCount)))
	}
	if n.StartedAt != "" {
		meta = append(meta, GrayStyle.Render("started "+relativeTime(n.StartedAt)))
	}
	if n.CompletedAt != "" {
		meta = append(meta, GrayStyle.Render("done "+relativeTime(n.CompletedAt)))
	}
	if len(meta) > 0 {
		sb.WriteString("  " + strings.Join(meta, GrayStyle.Render(" · ")) + "\n")
	}

	sb.WriteString("\n")
	sectionHeader(sb, "Configuration", width)
	kvLine(sb, "gate", emptyDash(n.Gate, "none"), WhiteStyle)
	kvLine(sb, "depends on", emptyDash(strings.Join(n.DependsOn, ", "), ""), WhiteStyle)
	kvLine(sb, "children", emptyDash(strings.Join(n.Children, ", "), ""), WhiteStyle)

	sb.WriteString("\n")
	sectionHeader(sb, "Session", width)
	kvLine(sb, "session", emptyDash(n.SessionId, ""), WhiteStyle)
	kvLine(sb, "runtime", emptyDash(n.RuntimeId, ""), WhiteStyle)

	sb.WriteString("\n")
	sectionHeader(sb, "Commands", width)
	sb.WriteString("  ")
	cmds := []string{":attach", ":approve", ":reject", ":retry", ":logs"}
	styled := make([]string, len(cmds))
	for i, c := range cmds {
		styled[i] = CyanStyle.Render(c)
	}
	sb.WriteString(strings.Join(styled, "  "))
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

// ── Metrics Tab ──────────────────────────────────

func (p *InspectorPanel) renderMetrics(sb *strings.Builder) {
	m, ok := p.latestMetrics[p.node.NodeId]
	if !ok {
		sb.WriteString(GrayStyle.Render("  No metrics yet — data will appear when the node runs.\n"))
		return
	}

	maxTokens := max(m.TokensIn, m.TokensOut)
	if maxTokens < 100 {
		maxTokens = 100
	}
	barW := 20

	kvLine(sb, "tokens in", fmt.Sprintf("%d", m.TokensIn), WhiteStyle)
	sb.WriteString("              " + GrayStyle.Render("│") + " " + tokenBar(m.TokensIn, maxTokens, barW) + "\n")
	kvLine(sb, "tokens out", fmt.Sprintf("%d", m.TokensOut), WhiteStyle)
	sb.WriteString("              " + GrayStyle.Render("│") + " " + tokenBar(m.TokensOut, maxTokens, barW) + "\n")

	if m.TokensIn+m.TokensOut > 0 {
		total := m.TokensIn + m.TokensOut
		cost := float64(m.TokensIn)/1_000_000*3 + float64(m.TokensOut)/1_000_000*15
		kvLine(sb, "total", fmt.Sprintf("%d tokens", total), WhiteStyle)
		kvLine(sb, "est. cost", fmt.Sprintf("$%.4f", cost), YellowStyle)
	}

	sb.WriteString("\n")
	kvLine(sb, "tool calls", fmt.Sprintf("%d", m.ToolCalls), WhiteStyle)
	kvLine(sb, "duration", formatDur(m.DurationMs), WhiteStyle)

	sb.WriteString("\n")
	sb.WriteString(GrayStyle.Render("  pricing · $3/M in · $15/M out"))
	sb.WriteString("\n")
}

func tokenBar(value, maxValue int64, width int) string {
	if maxValue <= 0 {
		return ""
	}
	filled := int(float64(value) / float64(maxValue) * float64(width))
	if filled > width {
		filled = width
	}
	return GreenStyle.Render(strings.Repeat("█", filled)) +
		ProgressEmpty.Render(strings.Repeat("░", width-filled))
}

// ── Artifacts Tab ────────────────────────────────

func (p *InspectorPanel) renderArtifacts(sb *strings.Builder) {
	events := p.nodeEvents[p.node.NodeId]
	var artifacts []ArtifactCreatedItem
	for _, e := range events {
		if a, ok := e.(ArtifactCreatedItem); ok {
			artifacts = append(artifacts, a)
		}
	}

	if len(artifacts) == 0 {
		sb.WriteString(GrayStyle.Render("  No artifacts yet.\n"))
		sb.WriteString(GrayStyle.Render("  Created when a node finishes with output files.\n"))
		sb.WriteString(GrayStyle.Render("  Try: arlo artifacts <task-id>\n"))
		return
	}

	for _, a := range artifacts {
		timeStr := a.Timestamp.Local().Format("15:04:05")
		sb.WriteString(fmt.Sprintf("  %s  %s\n",
			GrayStyle.Render(timeStr),
			WhiteStyle.Render(fmt.Sprintf("%s  →  %s", a.Name, a.ArtifactID)),
		))
	}
}

// ── Prompt Tab ────────────────────────────────────

func (p *InspectorPanel) renderPrompt(sb *strings.Builder, width int) {
	n := p.node

	var skill string
	for _, e := range p.nodeEvents[n.NodeId] {
		if nc, ok := e.(NodeCreatedItem); ok {
			skill = nc.Skill
			break
		}
	}

	sectionHeader(sb, "Agent Configuration", width)
	kvLine(sb, "skill", emptyDash(skill, ""), WhiteStyle)
	kvLine(sb, "runtime", emptyDash(n.RuntimeId, ""), WhiteStyle)

	sb.WriteString("\n")
	sectionHeader(sb, "Prompt Context", width)
	kvLine(sb, "node", n.NodeId, WhiteStyle)
	kvLine(sb, "gate", emptyDash(n.Gate, "none"), WhiteStyle)
	kvLine(sb, "retries", fmt.Sprintf("%d", n.RetryCount), WhiteStyle)
	kvLine(sb, "depends on", emptyDash(strings.Join(n.DependsOn, ", "), ""), WhiteStyle)

	sb.WriteString("\n")
	sb.WriteString(GrayStyle.Render("  Prompt is assembled at runtime from skill + context + workspace."))
	sb.WriteString("\n")
}

// ── Helpers ───────────────────────────────────────

const kvLabelWidth = 12

// sectionHeader writes a compact cyan section title with a trailing rule to width.
func sectionHeader(sb *strings.Builder, title string, width int) {
	prefix := "── " + title + " "
	fill := width - 2 - lipgloss.Width(prefix)
	if fill < 2 {
		fill = 2
	}
	sb.WriteString("  ")
	sb.WriteString(CyanStyle.Render(prefix + strings.Repeat("─", fill)))
	sb.WriteString("\n")
}

// kvLine writes a label/value row. Labels are padded by visible width so ANSI
// color codes do not break column alignment.
func kvLine(sb *strings.Builder, label string, value string, valueStyle lipgloss.Style) {
	styledLabel := GrayStyle.Render(label)
	pad := kvLabelWidth - lipgloss.Width(label)
	if pad < 0 {
		pad = 0
	}
	sb.WriteString("  ")
	sb.WriteString(styledLabel)
	sb.WriteString(strings.Repeat(" ", pad))
	sb.WriteString("  ")
	sb.WriteString(valueStyle.Render(value))
	sb.WriteString("\n")
}

// emptyDash returns "—" if value is empty or equals the exclude string.
func emptyDash(value, exclude string) string {
	if value == "" || (exclude != "" && value == exclude) {
		return "—"
	}
	return value
}

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
