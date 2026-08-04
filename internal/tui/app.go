package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/lingjiefan/arlo/internal/store"
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
	// Single dispatcher subscription — panels must not Subscribe themselves,
	// otherwise Emit fans out N ways and routeInternalEvent appends N times.
	sub Subscriber

	// Internal
	ready    bool
	quitting bool
	err      error
	cmdBuf   string
	cmdMsg   string

	// Dedup: track event IDs we've already seen.
	seenEvents map[string]bool
}

// New creates a new TUI model.
func New(socket, workflow string) *Model {
	d := NewDispatcher()
	m := &Model{
		client:     NewClient(socket),
		socket:     socket,
		workflow:   workflow,
		dispatcher: d,
		commands:   NewCommandRegistry(),
		ui: UIState{
			Focus: FocusWorkflow,
		},
		wf: WorkflowState{
			ID: workflow,
		},
		workflowPanel:  NewWorkflowPanel(d),
		timelinePanel:  NewTimelinePanel(d),
		inspectorPanel: NewInspectorPanel(d),
		seenEvents:     make(map[string]bool),
	}
	m.workflowPanel.SetFocus(true)
	return m
}

// Init connects to arlod and starts the event stream.
func (m *Model) Init() tea.Cmd {
	m.sub = m.dispatcher.Subscribe()
	return tea.Batch(
		m.connectAndStart,
		m.listenDispatcher,
	)
}

func (m *Model) listenDispatcher() tea.Msg {
	return <-m.sub
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

// ── Update ──────────────────────────────────────────────────────────

// Update handles all messages.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKeyMsg(msg)
	case tea.WindowSizeMsg:
		m.ui.Width = msg.Width
		m.ui.Height = msg.Height
	case connectedMsg:
		cmds = m.handleConnected(msg)
	case snapshotMsg:
		m.handleSnapshot(msg)
	case streamReadyMsg:
		cmds = append(cmds, m.client.RecvEvent())
	case eventMsg:
		cmds = m.handleEvent(msg)
	case streamErrMsg:
		cmds = append(cmds, m.client.GetSnapshot(m.workflow))
		m.dispatcher.Emit(ReconnectedEvent{})
	case streamEndMsg:
		cmds = append(cmds, m.client.GetSnapshot(m.workflow))
	case commandMsg:
		m.cmdMsg = msg.output
	case commandResultMsg:
		cmds = m.handleCommandResult(msg)
	case attachMsg:
		m.cmdMsg = fmt.Sprintf("attach to session %s (node %s) — use: arlo attach %s",
			msg.sessionID, msg.nodeID, msg.sessionID)
	}

	// Dispatcher events: route once to all panels, then re-arm the single listener.
	// Do not also routePanelUpdate — that would double-apply on the focused panel.
	if _, isInternal := msg.(InternalEvent); isInternal {
		cmds = m.routeInternalEvent(msg, cmds)
		cmds = append(cmds, m.listenDispatcher)
		return m, tea.Batch(cmds...)
	}

	// Route to focused panel (except keystrokes, which handle themselves).
	if _, isKey := msg.(tea.KeyMsg); !isKey {
		cmds = m.routePanelUpdate(msg, cmds)
	}

	return m, tea.Batch(cmds...)
}

// ── Key handling ────────────────────────────────────────────────────

var tabKeys = map[string]InspectorTab{
	"1": TabSummary,
	"2": TabLogs,
	"3": TabPrompt,
	"4": TabArtifacts,
	"5": TabMetrics,
}

func (m *Model) handleKeyMsg(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// Command mode input.
	if m.ui.CommandMode {
		return m.handleCommandInput(msg)
	}

	// Global keys (non-command-mode).
	switch msg.String() {
	case "ctrl+c":
		m.quitting = true
		return m, tea.Quit

	case "q":
		if m.ui.HelpOpen {
			m.ui.HelpOpen = false
			return m, nil
		}
		m.quitting = true
		return m, tea.Quit

	case "esc":
		if m.ui.HelpOpen {
			m.ui.HelpOpen = false
			return m, nil
		}
		if m.ui.InspectorOverlay {
			m.ui.InspectorOverlay = false
			return m, nil
		}
		if m.ui.FilterOpen {
			m.ui.FilterOpen = false
			return m, nil
		}
		m.quitting = true
		return m, tea.Quit

	case "tab":
		m.cycleFocus()

	case "up", "k":
		m.navigatePanel(msg)
		m.syncInspectorToSelection()
		return m, nil

	case "down", "j":
		m.navigatePanel(msg)
		m.syncInspectorToSelection()
		return m, nil

	case "enter":
		m.syncInspectorToSelection()

	case ":":
		m.ui.CommandMode = true
		m.cmdBuf = ""
		return m, nil

	case "f":
		m.ui.FilterOpen = !m.ui.FilterOpen
		return m, nil

	case "a":
		return m, m.runRegistryCommand("attach")
	case "p":
		return m, m.runRegistryCommand("approve")
	case "r":
		return m, m.runRegistryCommand("reject")
	case "R":
		return m, m.runRegistryCommand("retry")
	case "?":
		m.ui.HelpOpen = !m.ui.HelpOpen
		return m, nil

	case " ":
		if m.ui.Focus == FocusWorkflow {
			m.workflowPanel.Update(msg)
			return m, nil
		}

	case "-":
		m.ui.WorkflowNarrow = true
		m.workflowPanel.SetNarrow(true)
		return m, nil
	case "+":
		m.ui.WorkflowNarrow = false
		m.workflowPanel.SetNarrow(false)
		return m, nil

	case "s":
		m.timelinePanel.ResumeFollow()
		return m, nil
	case "c":
		m.timelinePanel.ToggleCompact()
		return m, nil

	case "i":
		mode := layoutModeForWidth(m.ui.Width)
		if mode != LayoutFull {
			m.ui.InspectorOverlay = !m.ui.InspectorOverlay
			return m, nil
		}

	case "right", "l":
		if m.ui.Focus == FocusTimeline {
			m.timelinePanel.Update(msg)
			return m, nil
		}
		if m.ui.Focus == FocusWorkflow {
			m.workflowPanel.Update(msg)
			return m, nil
		}

	case "left", "h":
		if m.ui.Focus == FocusWorkflow {
			m.workflowPanel.Update(msg)
			return m, nil
		}

	default:
		if tab, ok := tabKeys[msg.String()]; ok {
			m.inspectorPanel.SetTab(tab)
			return m, nil
		}
	}

	return m, nil
}

func (m *Model) appContext() *AppContext {
	return &AppContext{
		Socket:     m.socket,
		WorkflowID: m.workflow,
		Client:     m.client,
		UIState:    &m.ui,
		Workflow:   &m.wf,
		Dispatch:   m.dispatcher.Emit,
	}
}

func (m *Model) runRegistryCommand(name string) tea.Cmd {
	return m.commands.Execute(name, m.appContext())
}

func (m *Model) handleCommandInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "esc":
		m.ui.CommandMode = false
		m.cmdBuf = ""
		return m, nil
	case "enter":
		cmd := m.commands.Execute(m.cmdBuf, m.appContext())
		m.ui.CommandMode = false
		m.cmdBuf = ""
		if cmd != nil {
			return m, cmd
		}
		return m, nil
	case "backspace":
		if len(m.cmdBuf) > 0 {
			m.cmdBuf = m.cmdBuf[:len(m.cmdBuf)-1]
		}
	default:
		if len(msg.String()) == 1 {
			m.cmdBuf += msg.String()
		}
	}
	return m, nil
}

func (m *Model) cycleFocus() {
	mode := layoutModeForWidth(m.ui.Width)
	switch mode {
	case LayoutSingle:
		m.cycleFocusSingle()
		return
	case LayoutNoInspector:
		m.cycleFocusNoInspector()
		return
	}

	switch m.ui.Focus {
	case FocusWorkflow:
		m.ui.Focus = FocusTimeline
		m.workflowPanel.SetFocus(false)
		m.timelinePanel.SetFocus(true)
		m.inspectorPanel.SetFocus(false)
	case FocusTimeline:
		m.ui.Focus = FocusInspector
		m.timelinePanel.SetFocus(false)
		m.inspectorPanel.SetFocus(true)
	case FocusInspector:
		m.ui.Focus = FocusWorkflow
		m.inspectorPanel.SetFocus(false)
		m.workflowPanel.SetFocus(true)
	}
}

func (m *Model) cycleFocusNoInspector() {
	m.inspectorPanel.SetFocus(false)
	if m.ui.Focus == FocusWorkflow {
		m.ui.Focus = FocusTimeline
		m.workflowPanel.SetFocus(false)
		m.timelinePanel.SetFocus(true)
		return
	}
	m.ui.Focus = FocusWorkflow
	m.timelinePanel.SetFocus(false)
	m.workflowPanel.SetFocus(true)
}

func (m *Model) cycleFocusSingle() {
	m.workflowPanel.SetFocus(false)
	m.timelinePanel.SetFocus(false)
	m.inspectorPanel.SetFocus(false)
	switch m.ui.Focus {
	case FocusWorkflow:
		m.ui.Focus = FocusTimeline
		m.timelinePanel.SetFocus(true)
	case FocusTimeline:
		m.ui.Focus = FocusInspector
		m.inspectorPanel.SetFocus(true)
	default:
		m.ui.Focus = FocusWorkflow
		m.workflowPanel.SetFocus(true)
	}
}

func (m *Model) navigatePanel(msg tea.KeyMsg) {
	switch m.ui.Focus {
	case FocusWorkflow:
		m.workflowPanel.Update(msg)
	case FocusTimeline:
		m.timelinePanel.Update(msg)
	}
}

// ── Message handlers ────────────────────────────────────────────────

func (m *Model) handleConnected(msg connectedMsg) []tea.Cmd {
	if msg.err != nil {
		m.err = msg.err
		return nil
	}
	m.ready = true
	return []tea.Cmd{
		m.client.StartEventStream(m.workflow),
		m.client.GetSnapshot(m.workflow),
	}
}

func (m *Model) handleSnapshot(msg snapshotMsg) {
	if msg.err != nil {
		return
	}
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

	ev := WorkflowUpdatedEvent{
		WorkflowID: m.workflow,
		Status:     msg.status,
		Version:    msg.version,
		Nodes:      msg.nodes,
	}
	// Apply synchronously so the tree cannot lag behind the inspector, and so a
	// delayed tea.Msg from an older snapshot cannot win a race before Emit lands.
	m.workflowPanel.Update(ev)
	m.dispatcher.Emit(ev)
	m.syncInspectorToSelection()
}

func (m *Model) handleEvent(msg eventMsg) []tea.Cmd {
	if msg.event == nil {
		return []tea.Cmd{m.client.RecvEvent()}
	}
	cmds := []tea.Cmd{m.client.RecvEvent()}
	// Dedup by event ID — prevents duplicates from reconnect or replay.
	if !m.seenEvents[msg.event.EventId] {
		m.seenEvents[msg.event.EventId] = true
		item := EventToItem(msg.event)
		m.dispatcher.Emit(EventAppendedEvent{Item: item})
		// For state-changing events, refresh the snapshot so the
		// Workflow panel and Inspector stay in sync with the projection.
		if isStateChangeEvent(msg.event.Type) {
			cmds = append(cmds, m.client.GetSnapshot(m.workflow))
		}
	}
	// Prune seenEvents map if it grows large.
	if len(m.seenEvents) > pruneThreshold {
		m.seenEvents = make(map[string]bool)
	}
	return cmds
}

// isStateChangeEvent reports whether an event type mutates node/workflow state
// that is visible in the Workflow panel and Inspector. These events trigger a
// GetSnapshot to keep the TUI panels in sync with server-side projections.
func isStateChangeEvent(eventType string) bool {
	switch eventType {
	case string(store.EventNodeStarted), string(store.EventNodeCompleted), string(store.EventNodeFailed),
		string(store.EventNodeWaiting), string(store.EventTaskCompleted), string(store.EventTaskFailed):
		return true
	}
	return false
}

func (m *Model) handleCommandResult(msg commandResultMsg) []tea.Cmd {
	if msg.err != nil {
		m.cmdMsg = fmt.Sprintf("command failed: %v", msg.err)
		return nil
	}
	if msg.success {
		m.cmdMsg = msg.message
		return []tea.Cmd{m.client.GetSnapshot(m.workflow)}
	}
	m.cmdMsg = fmt.Sprintf("command rejected: %s", msg.message)
	return nil
}

// pruneThreshold is the size at which the seenEvents dedup map is reset.
const pruneThreshold = 2000

// ── Panel routing ───────────────────────────────────────────────────

func (m *Model) routePanelUpdate(msg tea.Msg, cmds []tea.Cmd) []tea.Cmd {
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
	return cmds
}

func (m *Model) routeInternalEvent(msg tea.Msg, cmds []tea.Cmd) []tea.Cmd {
	cmd, _ := m.workflowPanel.Update(msg)
	if cmd != nil {
		cmds = append(cmds, cmd)
	}
	cmd, _ = m.timelinePanel.Update(msg)
	if cmd != nil {
		cmds = append(cmds, cmd)
	}
	cmd, _ = m.inspectorPanel.Update(msg)
	if cmd != nil {
		cmds = append(cmds, cmd)
	}
	return cmds
}

// ── Inspector sync ──────────────────────────────────────────────────

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

// ── View ────────────────────────────────────────────────────────────

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

// ── Dashboard layout ────────────────────────────────────────────────

// dashboardMinWidth and dashboardMinHeight are minimum panel sizes.
const (
	dashboardMinWidth  = 80
	dashboardMinHeight = 15
	overviewHeight     = 4
)

func (m *Model) renderDashboard() string {
	w := max(m.ui.Width, dashboardMinWidth)
	h := max(m.ui.Height, dashboardMinHeight)
	mode := layoutModeForWidth(m.ui.Width)

	// Use actual width for layout decisions when terminal is narrow.
	if m.ui.Width > 0 {
		w = m.ui.Width
	}

	overview := m.renderOverview(w)
	panelH := h - overviewHeight

	var panels string
	switch mode {
	case LayoutSingle:
		panels = m.renderSinglePane(w, panelH)
	case LayoutNoInspector:
		panels = m.renderTwoPane(w, panelH)
	default:
		panels = m.renderThreePane(w, panelH)
	}

	status := m.renderCommandBar(w)

	var overlay string
	if m.ui.HelpOpen {
		overlay = m.renderHelpOverlay()
	} else if m.ui.FilterOpen {
		overlay = m.renderFilterOverlay()
	} else if m.ui.InspectorOverlay && mode != LayoutFull {
		overlay = m.inspectorPanel.View(w, panelH)
	}

	return lipgloss.JoinVertical(lipgloss.Left, overview, panels, status, overlay)
}

func (m *Model) renderThreePane(w, panelH int) string {
	leftW := max(w*25/100, 25)
	if m.ui.WorkflowNarrow {
		leftW = 16
	}
	rightW := max(w*25/100, 25)
	midW := w - leftW - rightW
	if midW < 20 {
		midW = 20
	}
	left := m.workflowPanel.View(leftW, panelH)
	mid := m.timelinePanel.View(midW, panelH)
	right := m.inspectorPanel.View(rightW, panelH)
	return lipgloss.JoinHorizontal(lipgloss.Top, left, mid, right)
}

func (m *Model) renderTwoPane(w, panelH int) string {
	leftW := max(w*35/100, 20)
	if m.ui.WorkflowNarrow {
		leftW = 16
	}
	midW := w - leftW
	left := m.workflowPanel.View(leftW, panelH)
	mid := m.timelinePanel.View(midW, panelH)
	return lipgloss.JoinHorizontal(lipgloss.Top, left, mid)
}

func (m *Model) renderSinglePane(w, panelH int) string {
	switch m.ui.Focus {
	case FocusTimeline:
		return m.timelinePanel.View(w, panelH)
	case FocusInspector:
		return m.inspectorPanel.View(w, panelH)
	default:
		return m.workflowPanel.View(w, panelH)
	}
}

// ── Overview bar ────────────────────────────────────────────────────

func (m *Model) renderOverview(width int) string {
	status := m.wf.Status
	if status == "" {
		status = "LOADING"
	}

	active, completed, failed := m.countNodeStatuses()

	elapsed := ""
	if !m.wf.StartedAt.IsZero() {
		elapsed = formatElapsed(time.Since(m.wf.StartedAt))
	}

	statusIcon, statusColor := overviewStatus(status)

	bar := ProgressBar(completed+failed, len(m.wf.Nodes), 10)

	parts := []string{
		CyanStyle.Render(m.workflow),
		statusColor.Render(fmt.Sprintf("%s %s", statusIcon, status)),
	}
	if elapsed != "" {
		parts = append(parts, WhiteStyle.Render(elapsed))
	}
	parts = append(parts,
		bar,
		GrayStyle.Render(fmt.Sprintf("nodes %d", len(m.wf.Nodes))))
	if active > 0 {
		parts = append(parts, YellowStyle.Render(fmt.Sprintf("active %d", active)))
	}
	if failed > 0 {
		parts = append(parts, RedStyle.Render(fmt.Sprintf("failed %d", failed)))
	}

	return lipgloss.NewStyle().
		Background(DarkBg).
		Foreground(lipgloss.Color("252")).
		Width(width).
		Padding(0, 1).
		Render(strings.Join(parts, "  "))
}

func (m *Model) countNodeStatuses() (active, completed, failed int) {
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
	return
}

func overviewStatus(status string) (string, lipgloss.Style) {
	switch status {
	case "COMPLETED":
		return "✓", GreenStyle
	case "FAILED":
		return "✗", RedStyle
	case "ACTIVE":
		return "●", YellowStyle
	default:
		return "●", YellowStyle
	}
}

func formatElapsed(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

// ── Command bar ─────────────────────────────────────────────────────

func (m *Model) renderCommandBar(width int) string {
	if m.ui.CommandMode {
		return StatusBarStyle.Width(width).Render(fmt.Sprintf(":%s", m.cmdBuf))
	}

	if m.cmdMsg != "" {
		msg := m.cmdMsg
		m.cmdMsg = ""
		return StatusBarStyle.Width(width).Render(WhiteStyle.Render(msg))
	}

	mode := focusLabel(m.ui.Focus)
	hints := m.renderKeyHints()
	nav := GrayStyle.Render("↑↓ Tab Enter")

	pad := max(width-lipgloss.Width(mode)-lipgloss.Width(hints)-lipgloss.Width(nav)-4, 1)
	line := mode + "  │  " + hints + strings.Repeat(" ", pad) + "│  " + nav
	return StatusBarStyle.Width(width).Render(line)
}

func (m *Model) renderKeyHints() string {
	blocked := m.selectedNodeBlocked()
	attach := GrayStyle.Render("a:attach")
	approve := GrayStyle.Render("p:approve")
	reject := GrayStyle.Render("r:reject")
	if blocked {
		approve = YellowStyle.Bold(true).Render("p:approve")
		reject = YellowStyle.Bold(true).Render("r:reject")
	}
	rest := GrayStyle.Render("R:retry  f:filter  s:follow  c:compact  ?:help  q:quit")
	return attach + "  " + approve + "  " + reject + "  " + rest
}

func (m *Model) selectedNodeBlocked() bool {
	for _, n := range m.wf.Nodes {
		if n.NodeId == m.ui.SelectedNode {
			return isBlocked(n)
		}
	}
	return false
}

func focusLabel(f FocusTarget) string {
	switch f {
	case FocusWorkflow:
		return GrayStyle.Render("NORMAL") + " " + WhiteStyle.Render("workflow")
	case FocusTimeline:
		return GrayStyle.Render("NORMAL") + " " + WhiteStyle.Render("timeline")
	case FocusInspector:
		return GrayStyle.Render("NORMAL") + " " + WhiteStyle.Render("inspector")
	}
	return ""
}

// ── Help overlay ────────────────────────────────────────────────────

func (m *Model) renderHelpOverlay() string {
	lines := []string{
		"┌── Keys ────────────────────────────────┐",
		"│  j/k ↑↓   navigate                     │",
		"│  Tab      cycle panels                 │",
		"│  Enter    inspect selection            │",
		"│  Space    toggle node detail           │",
		"│  - / +    collapse / expand workflow   │",
		"│  a        attach                       │",
		"│  p / r    approve / reject             │",
		"│  R        retry                        │",
		"│  f        filter                       │",
		"│  s        resume timeline follow       │",
		"│  c        compact timeline             │",
		"│  →        expand timeline line         │",
		"│  i        inspector overlay (narrow)   │",
		"│  ?        toggle this help             │",
		"│  :        command mode                 │",
		"│  q / Esc  quit (closes overlay first)  │",
		"└────────────────────────────────────────┘",
	}
	return strings.Join(lines, "\n")
}

// ── Filter overlay ──────────────────────────────────────────────────

func (m *Model) renderFilterOverlay() string {
	f := m.timelinePanel.Filter
	lines := []string{
		"┌── Filter ──────────────────────────┐",
		"│                                    │",
		fmt.Sprintf("│  [%s] workflow events               │", checkMark(f.WorkflowEvents)),
		fmt.Sprintf("│  [%s] node events                   │", checkMark(f.NodeEvents)),
		fmt.Sprintf("│  [%s] tool calls                    │", checkMark(f.ToolCalls)),
		fmt.Sprintf("│  [%s] errors                        │", checkMark(f.Errors)),
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

// ── Run ─────────────────────────────────────────────────────────────

// Run starts the TUI and blocks until the user quits.
func Run(socket, workflow string) error {
	m := New(socket, workflow)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
