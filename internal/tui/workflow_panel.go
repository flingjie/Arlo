package tui

import (
	"fmt"
	"strings"

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

	// Build a root set: nodes with no dependencies are roots.
	for i, n := range p.nodes {
		if len(n.DependsOn) == 0 {
			p.renderNode(&sb, n, 0, i)
		}
	}

	return PanelStyle.Width(width).Render(sb.String())
}

func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState, depth int, idx int) {
	indent := strings.Repeat("  ", depth)
	expandIcon := "▼"
	if p.collapsed[n.NodeId] {
		expandIcon = "▶"
	}

	lineStyle := nodeLineStyle(n.Status, p.focused && p.selected == idx)

	icon := StatusIcon(n.Status)
	sb.WriteString(lineStyle.Render(fmt.Sprintf("%s%s %s %s", indent, expandIcon, icon, n.NodeId)))
	sb.WriteString("\n")

	meta := []string{}
	meta = append(meta, GrayStyle.Render(n.Status))
	if n.SessionId != "" {
		meta = append(meta, CyanStyle.Render(n.SessionId))
	}
	if n.RetryCount > 0 {
		meta = append(meta, YellowStyle.Render(fmt.Sprintf("retry:%d", n.RetryCount)))
	}
	if n.Gate != "" && n.Gate != "none" {
		meta = append(meta, PurpleStyle.Render(fmt.Sprintf("gate:%s", n.Gate)))
	}
	sb.WriteString(fmt.Sprintf("%s  %s\n", indent, strings.Join(meta, "  ")))

	if !p.collapsed[n.NodeId] {
		for _, childID := range n.Children {
			for _, cn := range p.nodes {
				if cn.NodeId == childID {
					sb.WriteString(fmt.Sprintf("%s  ↳\n", indent))
					p.renderNode(sb, cn, depth+1, idx)
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

// GetSelectedNode returns the node ID of the currently selected node.
func (p *WorkflowPanel) GetSelectedNode() string {
	if p.selected >= 0 && p.selected < len(p.nodes) {
		return p.nodes[p.selected].NodeId
	}
	return ""
}
