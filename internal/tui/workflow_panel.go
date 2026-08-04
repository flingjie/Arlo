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

	// Workflow-level info for the panel header.
	wfID     string
	wfStatus string
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
		p.wfID = msg.WorkflowID
		p.wfStatus = msg.Status
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

// View renders the workflow tree in a manual box.
func (p *WorkflowPanel) View(width int) string {
	inner := width - 2 // space inside borders
	if inner < 10 {
		inner = 10
	}

	var sb strings.Builder

	labelStyle := lipgloss.NewStyle().Foreground(Cyan).Bold(true)
	keyStyle := lipgloss.NewStyle().Foreground(Gray)

	// Top border with title.
	sb.WriteString(fmt.Sprintf("┌─ %s %s┐\n",
		labelStyle.Render("WORKFLOW"),
		strings.Repeat("─", inner-10)))

	// Task info line.
	sb.WriteString(fmt.Sprintf("│ %s %s%s│\n",
		keyStyle.Render("Task:"),
		WhiteStyle.Render(p.wfID),
		strings.Repeat(" ", inner-6-len(p.wfID))))

	// Status line.
	completed, _ := p.countStatus()
	statusLine := fmt.Sprintf("Status: %s       Nodes: %d/%d",
		p.wfStatus, completed, len(p.nodes))
	sb.WriteString(fmt.Sprintf("│ %s%s│\n",
		statusLine,
		strings.Repeat(" ", inner-1-len(statusLine))))

	// Separator.
	sb.WriteString(fmt.Sprintf("├%s┤\n", strings.Repeat("─", inner)))

	// Separator between header and nodes.
	sb.WriteString(fmt.Sprintf("│%s│\n", strings.Repeat(" ", inner)))

	if len(p.nodes) == 0 {
		sb.WriteString(fmt.Sprintf("│%s│\n",
			GrayStyle.Render("  no nodes yet...")+
				strings.Repeat(" ", inner-16)))
	} else {
		for _, n := range p.nodes {
			if len(n.DependsOn) == 0 {
				p.renderNode(&sb, n, inner)
			}
		}
		// Fill remaining space.
		sb.WriteString(fmt.Sprintf("│%s│\n", strings.Repeat(" ", inner)))
	}

	// Bottom border.
	sb.WriteString(fmt.Sprintf("└%s┘", strings.Repeat("─", inner)))

	return sb.String()
}

// displayStatus maps internal status to user-facing label.
// Nodes waiting on deps show "WAITING"; nodes blocked by a gate show "BLOCKED".
func displayStatus(n *arlov1.NodeState) string {
	if n.Status == "PENDING" || n.Status == "WAITING" {
		if n.Gate != "" && n.Gate != "none" {
			return "BLOCKED"
		}
		return "WAITING"
	}
	return n.Status
}

// renderNode renders a single node and its subtree in dependency order.
func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState, inner int) {
	icon := StatusIcon(n.Status)
	label := displayStatus(n)
	statusStyle := statusTextStyle(n.Status, p.focused && p.selected == p.flatIndex(n))

	// Build the main line: │ icon name              STATUS │
	prefix := icon + " " + n.NodeId
	gap := inner - lipgloss.Width(prefix) - lipgloss.Width(label) - 1
	if gap < 1 {
		gap = 1
	}
	sb.WriteString(fmt.Sprintf("│ %s%s%s │\n",
		prefix,
		strings.Repeat(" ", gap),
		statusStyle.Render(label),
	))

	// Sub-line: session ID for running nodes.
	if n.Status == "RUNNING" && n.SessionId != "" {
		sub := GrayStyle.Render(fmt.Sprintf("└─ %s", n.SessionId))
		subGap := inner - lipgloss.Width(sub) - 1
		if subGap < 1 {
			subGap = 1
		}
		sb.WriteString(fmt.Sprintf("│   %s%s │\n",
			sub, strings.Repeat(" ", subGap)))
	}

	// Sub-line: gate info for WAITING nodes.
	if isBlocked(n) {
		sub := PurpleStyle.Render(fmt.Sprintf("└─ gate: human_approval"))
		subGap := inner - lipgloss.Width(sub) - 1
		if subGap < 1 {
			subGap = 1
		}
		sb.WriteString(fmt.Sprintf("│   %s%s │\n",
			sub, strings.Repeat(" ", subGap)))
	}

	// Blank line between nodes.
	if len(n.Children) == 0 || p.collapsed[n.NodeId] {
		sb.WriteString(fmt.Sprintf("│%s│\n", strings.Repeat(" ", inner)))
	}

	// Render children at same level.
	if !p.collapsed[n.NodeId] && len(n.Children) > 0 {
		for _, childID := range n.Children {
			for _, cn := range p.nodes {
				if cn.NodeId == childID {
					p.renderNode(sb, cn, inner)
					break
				}
			}
		}
	}
}

// isBlocked returns true if a node is in WAITING status due to a human approval gate.
func isBlocked(n *arlov1.NodeState) bool {
	return n.Gate != "" && n.Gate != "none" &&
		(n.Status == "WAITING" || n.Status == "PENDING")
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
