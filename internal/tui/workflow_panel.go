package tui

import (
	"fmt"
	"strings"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	tea "github.com/charmbracelet/bubbletea"
)

// WorkflowPanel renders the workflow node tree.
type WorkflowPanel struct {
	nodes      []*arlov1.NodeState
	selected   int
	collapsed  map[string]bool
	focused    bool
	sub        Subscriber
	dispatcher *Dispatcher
}

// NewWorkflowPanel creates a new workflow panel.
func NewWorkflowPanel(dispatcher *Dispatcher) *WorkflowPanel {
	return &WorkflowPanel{
		collapsed:  make(map[string]bool),
		dispatcher: dispatcher,
	}
}

// Init subscribes to the dispatcher.
func (p *WorkflowPanel) Init() tea.Cmd {
	p.sub = p.dispatcher.Subscribe()
	return p.listenDispatcher
}

func (p *WorkflowPanel) listenDispatcher() tea.Msg {
	event := <-p.sub
	return event
}

// Update handles messages and dispatcher events.
func (p *WorkflowPanel) Update(msg tea.Msg) (tea.Cmd, bool) {
	switch msg := msg.(type) {
	case WorkflowUpdatedEvent:
		p.nodes = msg.Nodes
		return nil, true

	case tea.KeyMsg:
		if !p.focused {
			return nil, false
		}
		switch msg.String() {
		case "up", "k":
			if p.selected > 0 {
				p.selected--
			}
		case "down", "j":
			if p.selected < len(p.nodes)-1 {
				p.selected++
			}
		case "left", "h":
			if p.selected < len(p.nodes) {
				p.collapsed[p.nodes[p.selected].NodeId] = true
			}
		case "right", "l":
			if p.selected < len(p.nodes) {
				p.collapsed[p.nodes[p.selected].NodeId] = false
			}
		case "enter":
			return nil, true
		default:
			return nil, false
		}
		return nil, true

	case InternalEvent:
		return p.listenDispatcher, true
	}

	return nil, false
}

// SetFocus sets keyboard focus on this panel.
func (p *WorkflowPanel) SetFocus(focused bool) {
	p.focused = focused
}

// View renders the workflow tree.
func (p *WorkflowPanel) View(width int) string {
	var sb strings.Builder

	// Header with progress and status counts.
	done, failed, total := p.countTerminal()
	sb.WriteString(HeaderStyle.Render("WORKFLOW"))
	sb.WriteString("  ")
	sb.WriteString(GrayStyle.Render(fmt.Sprintf("%d/%d", done, total)))
	if failed > 0 {
		sb.WriteString("  ")
		sb.WriteString(RedStyle.Render(fmt.Sprintf("✗%d", failed)))
	}
	sb.WriteString("\n")
	sb.WriteString(ProgressBar(done, total, width-2))
	sb.WriteString("\n\n")

	if len(p.nodes) == 0 {
		sb.WriteString(GrayStyle.Render("  no nodes yet...\n"))
		return PanelStyle.Width(width).Render(sb.String())
	}

	// Roots: nodes with no dependencies.
	for _, n := range p.nodes {
		if len(n.DependsOn) == 0 {
			p.renderNode(&sb, n)
		}
	}

	return PanelStyle.Width(width).Render(sb.String())
}

// renderNode renders a single node and its subtree in dependency order.
// Children are rendered at the same visual level — no indentation.
func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState) {
	icon := StatusIcon(n.Status)

	dur := formatNodeDuration(n)
	if dur != "" {
		if n.Status == "RUNNING" {
			dur = "  " + WhiteStyle.Render(dur)
		} else {
			dur = "  " + GrayStyle.Render(dur)
		}
	}

	isSelected := p.focused && p.selected == p.flatIndex(n)
	lineStyle := nodeLineStyle(n.Status, isSelected)

	sb.WriteString(fmt.Sprintf("%s %s%s\n",
		icon, lineStyle.Render(n.NodeId), dur))

	meta := formatMeta(n)
	if meta != "" {
		sb.WriteString(fmt.Sprintf("   %s\n", GrayStyle.Render(meta)))
	}

	// Render children at same level.
	if !p.collapsed[n.NodeId] && len(n.Children) > 0 {
		for _, childID := range n.Children {
			for _, cn := range p.nodes {
				if cn.NodeId == childID {
					p.renderNode(sb, cn)
					break
				}
			}
		}
	}
}

// countTerminal returns (done, failed, total) where done includes all terminal
// states (COMPLETED, FAILED, CANCELLED). Used for the progress bar and header.
func (p *WorkflowPanel) countTerminal() (int, int, int) {
	done, failed := 0, 0
	for _, n := range p.nodes {
		switch n.Status {
		case "COMPLETED":
			done++
		case "FAILED", "CANCELLED":
			done++
			failed++
		}
	}
	return done, failed, len(p.nodes)
}

// flatIndex finds the flat visual index of a node for selection tracking.
func (p *WorkflowPanel) flatIndex(target *arlov1.NodeState) int {
	idx := 0
	var walk func(n *arlov1.NodeState) bool
	walk = func(n *arlov1.NodeState) bool {
		if n.NodeId == target.NodeId {
			return true
		}
		idx++
		if !p.collapsed[n.NodeId] {
			for _, childID := range n.Children {
				for _, cn := range p.nodes {
					if cn.NodeId == childID {
						if walk(cn) {
							return true
						}
						break
					}
				}
			}
		}
		return false
	}
	for _, n := range p.nodes {
		if len(n.DependsOn) == 0 {
			if walk(n) {
				return idx
			}
		}
	}
	return idx
}

// GetSelectedNode returns the node ID of the currently selected node.
func (p *WorkflowPanel) GetSelectedNode() string {
	if p.selected >= 0 && p.selected < len(p.nodes) {
		return p.nodes[p.selected].NodeId
	}
	return ""
}

// ── Helpers ──────────────────────────────────────

func formatMeta(n *arlov1.NodeState) string {
	parts := []string{}
	if n.RetryCount > 0 {
		parts = append(parts, fmt.Sprintf("retry:%d", n.RetryCount))
	}
	if n.Gate != "" && n.Gate != "none" {
		parts = append(parts, fmt.Sprintf("gate:%s", n.Gate))
	}
	if n.SessionId != "" {
		parts = append(parts, n.SessionId)
	}
	return strings.Join(parts, "  ")
}

func formatNodeDuration(n *arlov1.NodeState) string {
	if n.StartedAt == "" {
		return ""
	}
	start, err := time.Parse(time.RFC3339, n.StartedAt)
	if err != nil {
		return ""
	}

	var end time.Time
	if n.CompletedAt != "" {
		end, err = time.Parse(time.RFC3339, n.CompletedAt)
		if err != nil {
			return ""
		}
	} else {
		end = time.Now()
	}

	d := end.Sub(start)
	if d < time.Second {
		return ""
	}

	switch {
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds", m, s)
	default:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh%dm", h, m)
	}
}
