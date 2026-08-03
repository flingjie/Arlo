// Package tui provides the Bubble Tea terminal interface for Arlo.
// It renders a three-panel dashboard: DAG viewer, Event Timeline, Status bar.
package tui

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Model is the top-level TUI state.
type Model struct {
	client   arlov1.ArloServiceClient
	socket   string
	workflow string
	conn     *grpc.ClientConn

	// Panels
	dag       *dagPanel
	timeline  *timelinePanel
	statusBar *statusBar

	// State
	nodes     []*arlov1.NodeState
	events    []eventEntry
	ready     bool
	quitting  bool
	err       error
	spinner   spinner.Model
	width     int
	height    int
}

type eventEntry struct {
	Time string
	Type string
	Node string
}

type dagPanel struct {
	nodes []*arlov1.NodeState
	width int
}

type timelinePanel struct {
	viewport viewport.Model
	events   []eventEntry
}

type statusBar struct {
	workflow string
	status   string
	ready    bool
}

// New creates a new TUI model.
func New(socket, workflow string) *Model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	vp := viewport.New(80, 10)
	vp.Style = lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return &Model{
		socket:   socket,
		workflow: workflow,
		spinner:  s,
		dag:      &dagPanel{},
		timeline: &timelinePanel{viewport: vp},
		statusBar: &statusBar{
			workflow: workflow,
			ready:    false,
		},
	}
}

// Init connects to arlod and subscribes to events.
func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		m.connectAndSubscribe,
	)
}

type connectMsg struct {
	conn   *grpc.ClientConn
	client arlov1.ArloServiceClient
	err    error
}

type eventMsg struct {
	event *arlov1.Event
	err   error
}

type nodesMsg struct {
	nodes  []*arlov1.NodeState
	status string
	err    error
}

func (m *Model) connectAndSubscribe() tea.Msg {
	conn, err := grpc.NewClient(
		"unix://"+m.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return connectMsg{err: err}
	}

	client := arlov1.NewArloServiceClient(conn)

	// Get current workflow state.
	ctx := context.Background()
	_, gErr := client.GetWorkflow(ctx, &arlov1.GetWorkflowRequest{WorkflowId: m.workflow})
	if gErr != nil {
		return connectMsg{conn: conn, client: client, err: gErr}
	}

	return connectMsg{conn: conn, client: client, err: nil}
}

func (m *Model) fetchNodes() tea.Msg {
	ctx := context.Background()
	resp, err := m.client.GetWorkflow(ctx, &arlov1.GetWorkflowRequest{WorkflowId: m.workflow})
	if err != nil {
		return nodesMsg{err: err}
	}
	return nodesMsg{nodes: resp.Nodes, status: resp.Status}
}

func (m *Model) streamEvents() tea.Cmd {
	return func() tea.Msg {
		stream, err := m.client.SubscribeEvents(context.Background(), &arlov1.SubscribeEventsRequest{
			WorkflowId:  m.workflow,
			FromPosition: 0,
		})
		if err != nil {
			return eventMsg{err: err}
		}

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return eventMsg{err: err}
			}
			// Send event to the update loop.
			// In Bubble Tea, we return messages that get processed.
			// This is a continuous stream — we use tea.Tick or custom cmds.
			_ = event
		}
	}
}

// Update handles messages.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			m.quitting = true
			return m, tea.Quit
		case "r":
			return m, m.fetchNodes
		case "tab":
			// Switch focus between panels (future).
		}

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.timeline.viewport.Width = msg.Width - 4
		m.timeline.viewport.Height = msg.Height - 12
		return m, nil

	case connectMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.conn = msg.conn
		m.client = msg.client
		m.ready = true
		m.statusBar.ready = true
		return m, tea.Batch(
			m.fetchNodes,
			tickCmd(),
		)

	case nodesMsg:
		if msg.err == nil {
			m.nodes = msg.nodes
			m.statusBar.status = msg.status
		}

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case tickMsg:
		if m.ready {
			return m, tea.Batch(
				m.fetchNodes,
				tickCmd(),
			)
		}
	}

	return m, nil
}

type tickMsg struct{}

func tickCmd() tea.Cmd {
	return tea.Tick(2*time.Second, func(t time.Time) tea.Msg {
		return tickMsg{}
	})
}

// View renders the TUI.
func (m *Model) View() string {
	if m.err != nil {
		return fmt.Sprintf("Error: %v\n", m.err)
	}

	if m.quitting {
		return "Goodbye.\n"
	}

	if !m.ready {
		return fmt.Sprintf("%s Connecting to arlod on %s...\n", m.spinner.View(), m.socket)
	}

	return m.renderDashboard()
}

var (
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("86")).
			MarginBottom(1)

	panelStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("62")).
			Padding(0, 1)

	green  = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	yellow = lipgloss.NewStyle().Foreground(lipgloss.Color("226"))
	red    = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	gray   = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))
	white  = lipgloss.NewStyle().Foreground(lipgloss.Color("255"))
)

func (m *Model) renderDashboard() string {
	// Header
	header := headerStyle.Render(fmt.Sprintf("Arlo — %s [%s]", m.workflow, m.statusBar.status))

	// DAG Panel
	dagWidth := m.width/2 - 2
	dag := m.renderDAG(dagWidth)

	// Timeline Panel
	timelineWidth := m.width/2 - 2
	timeline := m.renderTimeline(timelineWidth)

	// Layout: header + two panels side by side + status bar
	panels := lipgloss.JoinHorizontal(lipgloss.Top, dag, timeline)

	// Status bar at bottom
	status := m.renderStatusBar()

	return lipgloss.JoinVertical(lipgloss.Left,
		header,
		panels,
		status,
	)
}

func (m *Model) renderDAG(width int) string {
	var sb strings.Builder
	sb.WriteString("NODES\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	// Build a simple DAG view from node states.
	nodeMap := make(map[string]*arlov1.NodeState)
	for _, n := range m.nodes {
		nodeMap[n.NodeId] = n
	}

	// Render nodes with status indicators.
	for _, n := range m.nodes {
		indicator := statusIcon(n.Status)
		sb.WriteString(fmt.Sprintf("  %s %s", indicator, n.NodeId))
		if n.SessionId != "" {
			sb.WriteString(fmt.Sprintf("  [%s]", n.SessionId))
		}
		sb.WriteString("\n")
		sb.WriteString(fmt.Sprintf("    %s\n", gray.Render(n.Status)))
		sb.WriteString("\n")
	}

	return panelStyle.Width(width).Render(sb.String())
}

func statusIcon(status string) string {
	switch status {
	case "COMPLETED":
		return green.Render("●")
	case "RUNNING", "STARTING":
		return yellow.Render("●")
	case "FAILED", "CANCELLED":
		return red.Render("●")
	default:
		return gray.Render("○")
	}
}

func (m *Model) renderTimeline(width int) string {
	var sb strings.Builder
	sb.WriteString("EVENTS\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	if len(m.events) == 0 {
		sb.WriteString(gray.Render("  waiting for events...\n"))
	} else {
		for _, e := range m.events {
			sb.WriteString(fmt.Sprintf("  %s  %s", gray.Render(e.Time), e.Type))
			if e.Node != "" {
				sb.WriteString(fmt.Sprintf("  %s", white.Render(e.Node)))
			}
			sb.WriteString("\n")
		}
	}

	return panelStyle.Width(width).Render(sb.String())
}

func (m *Model) renderStatusBar() string {
	s := fmt.Sprintf(" [q]uit  [r]efresh  [tab] switch  nodes: %d", len(m.nodes))
	return lipgloss.NewStyle().
		Background(lipgloss.Color("236")).
		Foreground(lipgloss.Color("252")).
		Width(m.width).
		Padding(0, 1).
		Render(s)
}

// Run starts the TUI and blocks until the user quits.
func Run(socket, workflow string) error {
	m := New(socket, workflow)
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}
