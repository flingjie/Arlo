package tui

import (
	"fmt"
	"strings"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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

	// Header.
	completed, _ := p.countStatus()
	total := len(p.nodes)
	sb.WriteString(HeaderStyle.Render(fmt.Sprintf("WORKFLOW   %d/%d", completed, total)))
	sb.WriteString("\n")
	sb.WriteString(ProgressBar(completed, total, width-2))
	sb.WriteString("\n\n")

	if len(p.nodes) == 0 {
		sb.WriteString(GrayStyle.Render("  no nodes yet...\n"))
		return PanelStyle.Width(width).Render(sb.String())
	}

	// Roots: nodes with no dependencies.
	for _, n := range p.nodes {
		if len(n.DependsOn) == 0 {
			p.renderNode(&sb, n, width)
		}
	}

	return PanelStyle.Width(width).Render(sb.String())
}

// renderNode renders a single node and its subtree in dependency order.
// Children are rendered at the same visual level — no indentation.
func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState, width int) {
	icon := StatusIcon(n.Status)
	label := displayStatus(n.Status)
	statusStyle := statusTextStyle(n.Status, p.focused && p.selected == p.flatIndex(n))

	// Pad the name so status text aligns roughly.
	namePart := fmt.Sprintf("%s %s", icon, n.NodeId)
	nameWidth := lipgloss.Width(namePart)
	statusWidth := lipgloss.Width(statusStyle.Render(label))
	gap := width - nameWidth - statusWidth - 10 // 10 for panel border + padding
	if gap < 1 {
		gap = 1
	}

	sb.WriteString(fmt.Sprintf("%s%s%s\n",
		namePart,
		strings.Repeat(" ", gap),
		statusStyle.Render(label),
	))

	// Show gate info on a sub-line.
	if n.Gate != "" && n.Gate != "none" {
		sb.WriteString(fmt.Sprintf("   %s\n",
			GrayStyle.Render(fmt.Sprintf("gate:%s", n.Gate))))
	}

	// Render children at same level.
	if !p.collapsed[n.NodeId] && len(n.Children) > 0 {
		for _, childID := range n.Children {
			for _, cn := range p.nodes {
				if cn.NodeId == childID {
					p.renderNode(sb, cn, width)
					break
				}
			}
		}
	}
}

// countStatus returns (completed, total) nodes for the progress bar.
func (p *WorkflowPanel) countStatus() (int, int) {
	completed := 0
	for _, n := range p.nodes {
		if n.Status == "COMPLETED" {
			completed++
		}
	}
	return completed, len(p.nodes)
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
