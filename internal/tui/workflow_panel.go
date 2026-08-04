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

// View renders the workflow tree in a manual box, filling to height.
func (p *WorkflowPanel) View(width, height int) string {
	inner := width - 2
	if inner < 20 {
		inner = 20
	}

	var sb strings.Builder
	labelStyle := lipgloss.NewStyle().Foreground(Cyan).Bold(true)
	keyStyle := lipgloss.NewStyle().Foreground(Gray)

	// ┌─ WORKFLOW ─────────────┐
	title := labelStyle.Render("WORKFLOW")
	titleLine := "┌─ " + title
	pad := inner - lipgloss.Width(titleLine) - 1
	if pad < 1 {
		pad = 1
	}
	sb.WriteString(titleLine + strings.Repeat("─", pad) + "┐\n")

	// │ Task: wf-xxx          │
	taskLine := "│ " + keyStyle.Render("Task:") + " " + WhiteStyle.Render(p.wfID)
	taskPad := inner - lipgloss.Width(taskLine) + 1
	if taskPad < 1 {
		taskPad = 1
	}
	sb.WriteString(taskLine + strings.Repeat(" ", taskPad) + "│\n")

	// │ Status: ACTIVE  Nodes: 0/3 │
	completed, _ := p.countStatus()
	statusText := fmt.Sprintf("Status: %s       Nodes: %d/%d", p.wfStatus, completed, len(p.nodes))
	statusLine := "│ " + statusText
	statusPad := inner - len(statusText)
	if statusPad < 1 {
		statusPad = 1
	}
	sb.WriteString(statusLine + strings.Repeat(" ", statusPad) + "│\n")

	// ├────────────────────────┤
	sb.WriteString("├" + strings.Repeat("─", inner) + "┤\n")
	sb.WriteString("│" + strings.Repeat(" ", inner) + "│\n")

	if len(p.nodes) == 0 {
		sb.WriteString("│" + GrayStyle.Render("  no nodes yet...") + strings.Repeat(" ", inner-16) + "│\n")
	} else {
		for _, n := range p.nodes {
			if len(n.DependsOn) == 0 {
				p.renderNode(&sb, n, inner)
			}
		}
	}

	// Pad to height.
	lineCount := strings.Count(sb.String(), "\n")
	for lineCount < height-1 {
		sb.WriteString("│" + strings.Repeat(" ", inner) + "│\n")
		lineCount++
	}
	sb.WriteString("└" + strings.Repeat("─", inner) + "┘")

	return sb.String()
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

func displayStatus(n *arlov1.NodeState) string {
	if n.Status == "PENDING" || n.Status == "WAITING" {
		if n.Gate != "" && n.Gate != "none" {
			return "BLOCKED"
		}
		return "WAITING"
	}
	return n.Status
}

func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState, inner int) {
	icon := StatusIcon(n.Status)
	label := displayStatus(n)
	statusStyle := statusTextStyle(n.Status, p.focused && p.selected == p.flatIndex(n))

	// Main line.
	prefix := icon + " " + n.NodeId
	styledLabel := statusStyle.Render(label)
	gap := inner - lipgloss.Width(prefix) - lipgloss.Width(styledLabel) - 1
	if gap < 1 {
		gap = 1
	}
	sb.WriteString("│ " + prefix + strings.Repeat(" ", gap) + styledLabel + " │\n")

	// Sub-lines.
	if n.SessionId != "" {
		sub := GrayStyle.Render("sess:" + n.SessionId)
		subGap := inner - lipgloss.Width(sub) - 1
		if subGap < 1 {
			subGap = 1
		}
		sb.WriteString("│   " + sub + strings.Repeat(" ", subGap) + " │\n")
	}
	if n.RetryCount > 0 {
		sub := YellowStyle.Render(fmt.Sprintf("retry:%d", n.RetryCount))
		subGap := inner - lipgloss.Width(sub) - 1
		if subGap < 1 {
			subGap = 1
		}
		sb.WriteString("│   " + sub + strings.Repeat(" ", subGap) + " │\n")
	}
	if isBlocked(n) {
		sub := PurpleStyle.Render("gate: human_approval")
		subGap := inner - lipgloss.Width(sub) - 1
		if subGap < 1 {
			subGap = 1
		}
		sb.WriteString("│   " + sub + strings.Repeat(" ", subGap) + " │\n")
	}

	if len(n.Children) == 0 || p.collapsed[n.NodeId] {
		sb.WriteString("│" + strings.Repeat(" ", inner) + "│\n")
	}

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

func isBlocked(n *arlov1.NodeState) bool {
	return n.Gate != "" && n.Gate != "none" &&
		(n.Status == "WAITING" || n.Status == "PENDING")
}

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

func (p *WorkflowPanel) GetSelectedNode() string {
	idx := -1
	var result string
	var walk func(n *arlov1.NodeState) bool
	walk = func(n *arlov1.NodeState) bool {
		idx++
		if idx == p.selected {
			result = n.NodeId
			return true
		}
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
				return result
			}
		}
	}
	return ""
}
