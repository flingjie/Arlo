package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model is the top-level Bubble Tea model.
type Model struct {
	client   *Client
	socket   string
	workflow string

	// State
	ui UIState
	wf WorkflowState

	// Panels
	workflowPanel  *WorkflowPanel
	timelinePanel  *TimelinePanel
	inspectorPanel *InspectorPanel

	// Infrastructure
	dispatcher *Dispatcher
	commands   *CommandRegistry

	// Internal
	ready    bool
	quitting bool
	err      error
	cmdBuf   string
	cmdMsg   string
}

// New creates a new TUI model.
func New(socket, workflow string) *Model {
	d := NewDispatcher()
	return &Model{
		client:    NewClient(socket),
		socket:    socket,
		workflow:  workflow,
		dispatcher: d,
		commands:  NewCommandRegistry(),
		ui: UIState{
			Focus: FocusWorkflow,
		},
		wf: WorkflowState{
			ID: workflow,
		},
		workflowPanel:  NewWorkflowPanel(d),
		timelinePanel:  NewTimelinePanel(d),
		inspectorPanel: NewInspectorPanel(),
	}
}

// Init connects to arlod and starts the event stream.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		m.connectAndStart,
		m.workflowPanel.Init(),
		m.timelinePanel.Init(),
	)
}

type connectedMsg struct {
	err error
}

func (m *Model) connectAndStart() tea.Msg {
	if err := m.client.Connect(); err != nil {
		return connectedMsg{err: err}
	}
	return connectedMsg{}
}

// Update handles all messages.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Command mode input.
		if m.ui.CommandMode {
			switch msg.String() {
			case "esc":
				m.ui.CommandMode = false
				m.cmdBuf = ""
				return m, nil
			case "enter":
				ctx := &AppContext{
					Socket:     m.socket,
					WorkflowID: m.workflow,
					Client:     m.client,
					UIState:    &m.ui,
					Workflow:   &m.wf,
					Dispatch:   m.dispatcher.Emit,
				}
				cmd := m.commands.Execute(m.cmdBuf, ctx)
				m.ui.CommandMode = false
				m.cmdBuf = ""
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
				break
			case "backspace":
				if len(m.cmdBuf) > 0 {
					m.cmdBuf = m.cmdBuf[:len(m.cmdBuf)-1]
				}
			default:
				if len(msg.String()) == 1 {
					m.cmdBuf += msg.String()
				}
			}
			return m, tea.Batch(cmds...)
		}

		// Global keys (non-command-mode).
		switch msg.String() {
		case "ctrl+c":
			m.quitting = true
			return m, tea.Quit

		case "esc":
			if m.ui.FilterOpen {
				m.ui.FilterOpen = false
				return m, nil
			}
			m.quitting = true
			return m, tea.Quit

		case "q":
			m.quitting = true
			return m, tea.Quit

		case "tab":
			// Cycle focus: workflow → timeline → inspector → workflow
			switch m.ui.Focus {
			case FocusWorkflow:
				m.ui.Focus = FocusTimeline
				m.workflowPanel.SetFocus(false)
				m.timelinePanel.SetFocus(true)
			case FocusTimeline:
				m.ui.Focus = FocusInspector
				m.timelinePanel.SetFocus(false)
			case FocusInspector:
				m.ui.Focus = FocusWorkflow
				m.workflowPanel.SetFocus(true)
			}

		case "up", "k":
			if m.ui.Focus == FocusWorkflow {
				m.workflowPanel.Update(msg)
			} else if m.ui.Focus == FocusTimeline {
				m.timelinePanel.Update(msg)
			}
			m.syncInspectorToSelection()
			return m, nil

		case "down", "j":
			if m.ui.Focus == FocusWorkflow {
				m.workflowPanel.Update(msg)
			} else if m.ui.Focus == FocusTimeline {
				m.timelinePanel.Update(msg)
			}
			m.syncInspectorToSelection()
			return m, nil

		case "enter":
			m.syncInspectorToSelection()

		case ":":
			m.ui.CommandMode = true
			m.cmdBuf = ""
			return m, nil

		case "1", "2", "3", "4", "5":
			switch msg.String() {
			case "1":
				m.inspectorPanel.SetTab(TabSummary)
			case "2":
				m.inspectorPanel.SetTab(TabLogs)
			case "3":
				m.inspectorPanel.SetTab(TabPrompt)
			case "4":
				m.inspectorPanel.SetTab(TabArtifacts)
			case "5":
				m.inspectorPanel.SetTab(TabMetrics)
			}
			return m, nil

		case "f":
			m.ui.FilterOpen = !m.ui.FilterOpen
			return m, nil
		}

	case tea.WindowSizeMsg:
		m.ui.Width = msg.Width
		m.ui.Height = msg.Height

	case connectedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.ready = true
		cmds = append(cmds,
			m.client.StartEventStream(m.workflow),
			m.client.GetSnapshot(m.workflow),
		)

	case snapshotMsg:
		if msg.err == nil {
			m.wf.Status = msg.status
			m.wf.Version = msg.version
			m.wf.Nodes = msg.nodes
			if msg.startedAt != "" {
				m.wf.StartedAt, _ = time.Parse(time.RFC3339, msg.startedAt)
			}

			// Auto-select first root node for inspector.
			if m.ui.SelectedNode == "" {
				for _, n := range msg.nodes {
					if len(n.DependsOn) == 0 {
						m.ui.SelectedNode = n.NodeId
						break
					}
				}
			}

			m.dispatcher.Emit(WorkflowUpdatedEvent{
				WorkflowID: m.workflow,
				Status:     msg.status,
				Version:    msg.version,
				Nodes:      msg.nodes,
			})
			m.syncInspectorToSelection()
		}

	case streamReadyMsg:
		cmds = append(cmds, m.client.RecvEvent())

	case eventMsg:
		if msg.event != nil {
			item := EventToItem(msg.event)
			m.dispatcher.Emit(EventAppendedEvent{Item: item})
		}
		cmds = append(cmds, m.client.RecvEvent())

	case streamErrMsg:
		cmds = append(cmds, m.client.GetSnapshot(m.workflow))
		m.dispatcher.Emit(ReconnectedEvent{})

	case streamEndMsg:
		cmds = append(cmds, m.client.GetSnapshot(m.workflow))

	case commandMsg:
		m.cmdMsg = msg.output

	case commandResultMsg:
		if msg.err != nil {
			m.cmdMsg = fmt.Sprintf("command failed: %v", msg.err)
		} else if msg.success {
			m.cmdMsg = msg.message
			cmds = append(cmds, m.client.GetSnapshot(m.workflow))
		} else {
			m.cmdMsg = fmt.Sprintf("command rejected: %s", msg.message)
		}

	case attachMsg:
		m.cmdMsg = fmt.Sprintf("attach to session %s (node %s) — use: arlo attach %s",
			msg.sessionID, msg.nodeID, msg.sessionID)
	}

	// Route to focused panel.
	switch m.ui.Focus {
	case FocusWorkflow:
		cmd, _ := m.workflowPanel.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	case FocusTimeline:
		cmd, _ := m.timelinePanel.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	// Always route dispatcher events to all panels.
	switch msg.(type) {
	case InternalEvent:
		cmd, _ := m.workflowPanel.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
		cmd, _ = m.timelinePanel.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

// syncInspectorToSelection updates the inspector with the currently selected node.
func (m *Model) syncInspectorToSelection() {
	nodeID := m.workflowPanel.GetSelectedNode()
	if nodeID == "" && len(m.wf.Nodes) > 0 {
		nodeID = m.wf.Nodes[0].NodeId
	}
	if nodeID == "" {
		return
	}
	m.ui.SelectedNode = nodeID
	for _, n := range m.wf.Nodes {
		if n.NodeId == nodeID {
			m.inspectorPanel.SetNode(n)
			break
		}
	}
}

// View renders the full TUI.
func (m *Model) View() string {
	if m.err != nil {
		return fmt.Sprintf("Error: %v\n", m.err)
	}
	if m.quitting {
		return "Goodbye.\n"
	}
	if !m.ready {
		return fmt.Sprintf("Connecting to arlod on %s...\n", m.socket)
	}

	return m.renderDashboard()
}

func (m *Model) renderDashboard() string {
	w := m.ui.Width
	if w < 80 {
		w = 80
	}
	h := m.ui.Height
	if h < 15 {
		h = 24
	}

	overview := m.renderOverview(w)

	// 25% | 50% | 25% split.
	leftW := w * 25 / 100
	if leftW < 25 {
		leftW = 25
	}
	rightW := w * 25 / 100
	if rightW < 25 {
		rightW = 25
	}
	midW := w - leftW - rightW

	panelH := h - 4 // minus overview + command bar + spacing

	left := m.workflowPanel.View(leftW, panelH)
	mid := m.timelinePanel.View(midW, panelH)
	right := m.inspectorPanel.View(rightW, panelH)

	panels := lipgloss.JoinHorizontal(lipgloss.Top, left, mid, right)

	status := m.renderCommandBar(w)

	var overlay string
	if m.ui.FilterOpen {
		overlay = m.renderFilterOverlay()
	}

	return lipgloss.JoinVertical(lipgloss.Left, overview, panels, status, overlay)
}

func (m *Model) renderOverview(width int) string {
	status := m.wf.Status
	if status == "" {
		status = "LOADING"
	}

	active, completed, failed := 0, 0, 0
	for _, n := range m.wf.Nodes {
		switch n.Status {
		case "COMPLETED":
			completed++
		case "FAILED", "CANCELLED":
			failed++
		case "RUNNING":
			active++
		}
	}
	total := len(m.wf.Nodes)

	elapsed := ""
	if !m.wf.StartedAt.IsZero() {
		d := time.Since(m.wf.StartedAt).Round(time.Second)
		if d < time.Minute {
			elapsed = fmt.Sprintf("%ds", int(d.Seconds()))
		} else if d < time.Hour {
			elapsed = fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
		} else {
			elapsed = fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
		}
	}

	// Status icon.
	statusIcon := "●"
	statusColor := YellowStyle
	switch status {
	case "COMPLETED":
		statusIcon = "✓"
		statusColor = GreenStyle
	case "FAILED":
		statusIcon = "✗"
		statusColor = RedStyle
	case "ACTIVE":
		statusIcon = "●"
		statusColor = YellowStyle
	}

	bar := ProgressBar(completed+failed, total, 10)

	parts := []string{
		CyanStyle.Render(m.workflow),
		statusColor.Render(fmt.Sprintf("%s %s", statusIcon, status)),
	}
	if elapsed != "" {
		parts = append(parts, WhiteStyle.Render(elapsed))
	}
	parts = append(parts, bar)
	parts = append(parts, GrayStyle.Render(fmt.Sprintf("nodes %d", total)))
	if active > 0 {
		parts = append(parts, YellowStyle.Render(fmt.Sprintf("active %d", active)))
	}
	if failed > 0 {
		parts = append(parts, RedStyle.Render(fmt.Sprintf("failed %d", failed)))
	}

	left := strings.Join(parts, "  ")

	return lipgloss.NewStyle().
		Background(DarkBg).
		Foreground(lipgloss.Color("252")).
		Width(width).
		Padding(0, 1).
		Render(left)
}

func (m *Model) renderCommandBar(width int) string {
	if m.ui.CommandMode {
		return StatusBarStyle.Width(width).Render(
			fmt.Sprintf(":%s", m.cmdBuf),
		)
	}

	msg := m.cmdMsg
	if msg != "" {
		m.cmdMsg = ""
		return StatusBarStyle.Width(width).Render(WhiteStyle.Render(msg))
	}

	// NORMAL mode commands.
	cmds := []string{":a", ":ap", ":rj", ":f", ":rf", ":h", ":q"}
	cmdText := strings.Join(cmds, " ")
	return StatusBarStyle.Width(width).Render(GrayStyle.Render(cmdText))
}

func (m *Model) renderFilterOverlay() string {
	lines := []string{
		"┌── Filter ──────────────────────────┐",
		"│                                    │",
		fmt.Sprintf("│  [%s] workflow events               │", checkMark(m.timelinePanel.Filter.WorkflowEvents)),
		fmt.Sprintf("│  [%s] node events                   │", checkMark(m.timelinePanel.Filter.NodeEvents)),
		fmt.Sprintf("│  [%s] tool calls                    │", checkMark(m.timelinePanel.Filter.ToolCalls)),
		fmt.Sprintf("│  [%s] errors                        │", checkMark(m.timelinePanel.Filter.Errors)),
		"│                                    │",
		"│  Press 1-4 to toggle, Esc to close │",
		"└────────────────────────────────────┘",
	}
	return lipgloss.NewStyle().
		Background(lipgloss.Color("235")).
		Foreground(White).
		Padding(1).
		Render(strings.Join(lines, "\n"))
}

func checkMark(b bool) string {
	if b {
		return "x"
	}
	return " "
}

// Run starts the TUI and blocks until the user quits.
func Run(socket, workflow string) error {
	m := New(socket, workflow)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
