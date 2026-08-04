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
	collapsed  map[string]bool // children tree collapsed
	detailOpen map[string]bool // session/gate sublines visible
	narrow     bool            // column collapsed to icon+name
	focused    bool
	dispatcher *Dispatcher

	wfID      string
	wfStatus  string
	wfVersion uint64 // last applied snapshot version; rejects stale updates
}

// NewWorkflowPanel creates a new workflow panel.
func NewWorkflowPanel(dispatcher *Dispatcher) *WorkflowPanel {
	return &WorkflowPanel{
		collapsed:  make(map[string]bool),
		detailOpen: make(map[string]bool),
		dispatcher: dispatcher,
	}
}

func (p *WorkflowPanel) Init() tea.Cmd { return nil }

func (p *WorkflowPanel) Update(msg tea.Msg) (tea.Cmd, bool) {
	switch msg := msg.(type) {
	case WorkflowUpdatedEvent:
		// Ignore out-of-order snapshots so a delayed older event cannot
		// overwrite sess-N+1 / retry state with a stale sess-N view.
		if msg.Version > 0 && msg.Version < p.wfVersion {
			return nil, true
		}
		p.nodes = msg.Nodes
		p.wfID = msg.WorkflowID
		p.wfStatus = msg.Status
		if msg.Version > p.wfVersion {
			p.wfVersion = msg.Version
		}
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
		case " ":
			id := p.GetSelectedNode()
			if id != "" {
				p.detailOpen[id] = !p.detailOpen[id]
			}
		case "enter":
			return nil, true
		}
		return nil, true
	}
	return nil, false
}

func (p *WorkflowPanel) SetFocus(focused bool) { p.focused = focused }

// SetNarrow collapses the column to icon + short name (no status/sublines).
func (p *WorkflowPanel) SetNarrow(narrow bool) { p.narrow = narrow }

// Narrow reports whether the column is collapsed.
func (p *WorkflowPanel) Narrow() bool { return p.narrow }

// View renders the workflow box. Every line is padded to exactly `width` visible chars.
func (p *WorkflowPanel) View(width, height int) string {
	w := width
	minW := 30
	if p.narrow {
		minW = 14
	}
	if w < minW {
		w = minW
	}
	contentW := w - 2 // space between │ borders

	var sb strings.Builder
	gray := lipgloss.NewStyle().Foreground(Gray)

	// ┌─ WORKFLOW ──────────────────┐  (* when focused)
	title := "WORKFLOW"
	titleStyle := GrayStyle.Bold(true)
	if p.focused {
		title = "WORKFLOW *"
		titleStyle = CyanStyle.Bold(true)
	}
	if p.narrow {
		title = "WF"
		if p.focused {
			title = "WF *"
		}
	}
	sb.WriteString(boxLine("┌─ "+titleStyle.Render(title), "─", "┐", w))

	if !p.narrow {
		sb.WriteString(boxLine("│ "+gray.Render("Task:")+" "+WhiteStyle.Render(p.wfID), " ", "│", w))
		completed, _ := p.countStatus()
		status := fmt.Sprintf("Status: %s       Nodes: %d/%d", p.wfStatus, completed, len(p.nodes))
		sb.WriteString(boxLine("│ "+status, " ", "│", w))
		sb.WriteString(boxLine("├", "─", "┤", w))
		sb.WriteString(boxLine("│", " ", "│", w))
	}

	// Nodes.
	if len(p.nodes) == 0 {
		msg := "Launching…"
		if p.wfStatus != "" && p.wfStatus != "LOADING" {
			msg = "no nodes yet…"
		}
		sb.WriteString(boxLine("│ "+GrayStyle.Render(msg), " ", "│", w))
	} else {
		for _, n := range p.nodes {
			if len(n.DependsOn) == 0 {
				p.renderNode(&sb, n, w, contentW)
			}
		}
	}

	// Pad to height.
	lineCount := strings.Count(sb.String(), "\n")
	for lineCount < height-1 {
		sb.WriteString(boxLine("│", " ", "│", w))
		lineCount++
	}
	sb.WriteString("└" + strings.Repeat("─", w-2) + "┘")

	return sb.String()
}

// boxLine builds a line padded to exactly `targetW` visible characters.
// left is the prefix (already styled), fill is the padding char, right is the suffix.
func boxLine(left, fill, right string, targetW int) string {
	visibleW := lipgloss.Width(left) + lipgloss.Width(right)
	pad := targetW - visibleW
	if pad < 0 {
		pad = 0
	}
	return left + strings.Repeat(fill, pad) + right + "\n"
}

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

func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState, w, contentW int) {
	selected := p.focused && p.selected == p.flatIndex(n)
	cursor := SelectionCursor(selected)
	label := displayStatus(n)
	icon := StatusIcon(label)

	if p.narrow {
		name := n.NodeId
		maxName := contentW - 4
		if maxName < 2 {
			maxName = 2
		}
		if lipgloss.Width(name) > maxName {
			name = truncateRunes(name, maxName)
		}
		sb.WriteString(boxLine("│ "+cursor+" "+icon+" "+name, " ", "│", w))
	} else {
		statusStyle := statusTextStyle(label, selected)
		styledLabel := statusStyle.Render(label)
		prefix := cursor + " " + icon + " " + n.NodeId
		sb.WriteString(boxLine("│ "+prefix+gapTo(styledLabel, contentW-lipgloss.Width(prefix)-1)+styledLabel, " ", "│", w))

		// Sub-lines only when detail is open for this node.
		if p.detailOpen[n.NodeId] {
			if n.SessionId != "" {
				sb.WriteString(boxLine("│   "+GrayStyle.Render("sess:"+n.SessionId), " ", "│", w))
			}
			if n.RetryCount > 0 {
				sb.WriteString(boxLine("│   "+YellowStyle.Render(fmt.Sprintf("retry:%d", n.RetryCount)), " ", "│", w))
			}
			if isBlocked(n) {
				sb.WriteString(boxLine("│   "+YellowStyle.Render("gate: human_approval"), " ", "│", w))
			}
		}
	}

	// Blank separator.
	if len(n.Children) == 0 || p.collapsed[n.NodeId] {
		sb.WriteString(boxLine("│", " ", "│", w))
	}

	if !p.collapsed[n.NodeId] && len(n.Children) > 0 {
		for _, childID := range n.Children {
			for _, cn := range p.nodes {
				if cn.NodeId == childID {
					p.renderNode(sb, cn, w, contentW)
					break
				}
			}
		}
	}
}

// gapTo returns a string of spaces that, when placed before `target`, makes the
// combined visual width equal to `totalW`.
func gapTo(target string, totalW int) string {
	n := totalW - lipgloss.Width(target)
	if n < 0 {
		n = 0
	}
	return strings.Repeat(" ", n)
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
