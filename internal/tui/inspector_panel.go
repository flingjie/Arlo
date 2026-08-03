package tui

import (
	"fmt"
	"strings"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// InspectorPanel renders the detailed view of a selected node.
type InspectorPanel struct {
	node *arlov1.NodeState
	tab  InspectorTab
}

// NewInspectorPanel creates a new inspector panel.
func NewInspectorPanel() *InspectorPanel {
	return &InspectorPanel{tab: TabSummary}
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

func (p *InspectorPanel) renderSummary(sb *strings.Builder) {
	n := p.node
	lines := []struct{ label, value string }{
		{"Node", n.NodeId},
		{"Status", n.Status},
		{"Session", n.SessionId},
		{"Runtime", n.RuntimeId},
		{"Retry", fmt.Sprintf("%d", n.RetryCount)},
		{"Gate", n.Gate},
		{"Depends On", strings.Join(n.DependsOn, ", ")},
		{"Children", strings.Join(n.Children, ", ")},
	}

	for _, l := range lines {
		if l.value == "" {
			l.value = "—"
		}
		sb.WriteString(fmt.Sprintf("  %-12s  %s\n",
			GrayStyle.Render(l.label),
			WhiteStyle.Render(l.value),
		))
	}

	sb.WriteString("\n")
	sb.WriteString(GrayStyle.Render("  :attach workspace    :retry    :logs"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderLogs(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Logs tab — coming in v1.1"))
	sb.WriteString("\n")
}

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
