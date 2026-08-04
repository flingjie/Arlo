package tui

import (
	"fmt"
	"strings"
	"testing"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// ── RetryCommand ──────────────────────────────────

func TestRetryCommandInterface(t *testing.T) {
	c := &RetryCommand{}
	if c.Name() != "retry" {
		t.Errorf("Name() = %q, want %q", c.Name(), "retry")
	}
	if c.Aliases() != nil {
		t.Errorf("Aliases() = %v, want nil", c.Aliases())
	}
	if c.Description() != "Retry a failed or cancelled node" {
		t.Errorf("Description() = %q", c.Description())
	}
	if c.Usage() != ":retry [<node-id>]" {
		t.Errorf("Usage() = %q", c.Usage())
	}
}

func TestRetryCommandRegistered(t *testing.T) {
	r := NewCommandRegistry()
	cmd := r.commands["retry"]
	if cmd == nil {
		t.Fatal("retry command not registered")
	}
	if cmd.Name() != "retry" {
		t.Errorf("registered command Name() = %q", cmd.Name())
	}
}

func TestRetryCommandExecuteWithArg(t *testing.T) {
	ctx := &AppContext{
		Client:  &Client{},
		UIState: &UIState{},
	}
	c := &RetryCommand{}
	cmd := c.Execute([]string{"my-node"}, ctx)
	// With an unconnected client, the returned Cmd will fail at runtime,
	// but the command resolution and argument passing should work.
	if cmd == nil {
		t.Fatal("Execute returned nil")
	}
}

func TestRetryCommandExecuteFallbackToSelected(t *testing.T) {
	ctx := &AppContext{
		Client:  &Client{},
		UIState: &UIState{SelectedNode: "selected-node"},
	}
	c := &RetryCommand{}
	cmd := c.Execute(nil, ctx)
	if cmd == nil {
		t.Fatal("Execute returned nil")
	}
}

func TestRetryCommandExecuteNoNode(t *testing.T) {
	ctx := &AppContext{
		Client:  &Client{},
		UIState: &UIState{},
	}
	c := &RetryCommand{}
	cmd := c.Execute(nil, ctx)
	msg := cmd()
	cm, ok := msg.(commandMsg)
	if !ok {
		t.Fatalf("expected commandMsg, got %T", msg)
	}
	if !strings.Contains(cm.output, "no node selected") {
		t.Errorf("output = %q, want 'no node selected'", cm.output)
	}
}

// ── ApproveCommand / RejectCommand interface ──────

func TestApproveRejectCommands(t *testing.T) {
	for _, c := range []Command{&ApproveCommand{}, &RejectCommand{}} {
		if c.Name() == "" {
			t.Errorf("%T has empty name", c)
		}
		if c.Description() == "" {
			t.Errorf("%T has empty description", c)
		}
	}
}

func TestApproveRejectNoNode(t *testing.T) {
	ctx := &AppContext{Client: &Client{}, UIState: &UIState{}}
	for _, c := range []Command{&ApproveCommand{}, &RejectCommand{}} {
		cmd := c.Execute(nil, ctx)
		msg := cmd()
		cm := msg.(commandMsg)
		if !strings.Contains(cm.output, "no node selected") {
			t.Errorf("%T: got %q", c, cm.output)
		}
	}
}

// ── AttachCommand ──────────────────────────────────

func TestAttachCommandNoArgs(t *testing.T) {
	ctx := &AppContext{Client: &Client{}, UIState: &UIState{}}
	c := &AttachCommand{}
	cmd := c.Execute(nil, ctx)
	msg := cmd()
	cm := msg.(commandMsg)
	if !strings.Contains(cm.output, "usage:") {
		t.Errorf("got %q", cm.output)
	}
}

func TestAttachCommandNodeNotFound(t *testing.T) {
	ctx := &AppContext{
		Client:   &Client{},
		Workflow: &WorkflowState{Nodes: []*arlov1.NodeState{}},
	}
	c := &AttachCommand{}
	cmd := c.Execute([]string{"missing-node"}, ctx)
	msg := cmd()
	cm := msg.(commandMsg)
	if !strings.Contains(cm.output, "no session found") {
		t.Errorf("got %q", cm.output)
	}
}

func TestAttachCommandFindsSession(t *testing.T) {
	ctx := &AppContext{
		Client: &Client{},
		Workflow: &WorkflowState{Nodes: []*arlov1.NodeState{
			{NodeId: "n1", SessionId: "sess-1"},
		}},
	}
	c := &AttachCommand{}
	cmd := c.Execute([]string{"n1"}, ctx)
	msg := cmd()
	am, ok := msg.(attachMsg)
	if !ok {
		t.Fatalf("expected attachMsg, got %T", msg)
	}
	if am.nodeID != "n1" || am.sessionID != "sess-1" {
		t.Errorf("attachMsg: node=%q session=%q", am.nodeID, am.sessionID)
	}
}

// ── Command Registry ──────────────────────────────

func TestCommandRegistryAllCommandsRegistered(t *testing.T) {
	r := NewCommandRegistry()
	expected := []string{
		"quit", "q",
		"help", "h",
		"filter", "f",
		"refresh", "rf",
		"attach", "a",
		"approve", "ap",
		"reject", "rj",
		"retry",
	}
	for _, name := range expected {
		if r.commands[name] == nil {
			t.Errorf("command %q not registered", name)
		}
	}
}

func TestCommandRegistryHelpIncludesRetry(t *testing.T) {
	ctx := &AppContext{Client: &Client{}}
	cmd := &HelpCommand{}
	teaCmd := cmd.Execute(nil, ctx)
	msg := teaCmd()
	cm, ok := msg.(commandMsg)
	if !ok {
		t.Fatalf("expected commandMsg, got %T", msg)
	}
	if !strings.Contains(cm.output, ":retry") {
		t.Errorf("help output doesn't contain :retry: %s", cm.output)
	}
}

func TestCommandRegistryUnknownCommand(t *testing.T) {
	r := NewCommandRegistry()
	cmd := r.Execute("nonexistent", &AppContext{})
	msg := cmd()
	cm := msg.(commandMsg)
	if !strings.Contains(cm.output, "unknown command") {
		t.Errorf("got %q, want 'unknown command'", cm.output)
	}
}

// ── resolveNodeID ──────────────────────────────────

func TestResolveNodeIDFromArgs(t *testing.T) {
	ctx := &AppContext{UIState: &UIState{SelectedNode: "selected"}}
	got := resolveNodeID([]string{"explicit"}, ctx)
	if got != "explicit" {
		t.Errorf("got %q, want %q", got, "explicit")
	}
}

func TestResolveNodeIDFallback(t *testing.T) {
	ctx := &AppContext{UIState: &UIState{SelectedNode: "selected"}}
	got := resolveNodeID(nil, ctx)
	if got != "selected" {
		t.Errorf("got %q, want %q", got, "selected")
	}
}

func TestResolveNodeIDEmpty(t *testing.T) {
	ctx := &AppContext{UIState: &UIState{}}
	got := resolveNodeID(nil, ctx)
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// ── InspectorPanel ────────────────────────────────

func TestNewInspectorPanel(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	if p.tab != TabSummary {
		t.Errorf("default tab = %v, want TabSummary", p.tab)
	}
	if p.dispatcher != d {
		t.Error("dispatcher not set")
	}
	if p.nodeEvents == nil {
		t.Error("nodeEvents map is nil")
	}
	if p.latestMetrics == nil {
		t.Error("latestMetrics map is nil")
	}
}

func TestInspectorPanelSetNode(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "test-node", Status: "RUNNING"}
	p.SetNode(n)
	if p.Node().NodeId != "test-node" {
		t.Errorf("Node().NodeId = %q", p.Node().NodeId)
	}
}

func TestInspectorPanelSetTab(t *testing.T) {
	p := NewInspectorPanel(nil)
	p.SetTab(TabLogs)
	if p.tab != TabLogs {
		t.Errorf("tab = %v, want TabLogs", p.tab)
	}
}

func TestInspectorPanelViewNoNode(t *testing.T) {
	p := NewInspectorPanel(nil)
	view := p.View(60, 20)
	if !strings.Contains(view, "select a node") {
		t.Errorf("view without node: %s", view)
	}
}

func TestInspectorPanelViewWithNode(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{
		NodeId: "hello", Status: "COMPLETED",
		SessionId: "sess-1", RuntimeId: "claude-code",
	}
	p.SetNode(n)
	view := p.View(80, 20)
	for _, want := range []string{
		"NODE INSPECTOR", "COMPLETED", "sess-1",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in view", want)
		}
	}
	// Node ID may be rendered with icon prefix, check that view contains it somehow.
	if !strings.Contains(stripAnsi(view), "hello") {
		t.Errorf("view doesn't contain node ID 'hello': %s", view)
	}
}

func TestInspectorPanelTabsAllPresent(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "n1", Status: "READY"}
	p.SetNode(n)

	for _, tab := range []InspectorTab{TabSummary, TabLogs, TabPrompt, TabArtifacts, TabMetrics} {
		p.SetTab(tab)
		view := p.View(60, 30)
		// Each tab should show itself as selected (highlighted with key hint).
		want := fmt.Sprintf("%d:%s", int(tab)+1, tab.String())
		if !strings.Contains(stripAnsi(view), want) {
			t.Errorf("tab %s not shown as selected (want %q)", tab.String(), want)
		}
	}
}

func TestInspectorPanelSummarySections(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{
		NodeId: "n1", Status: "RUNNING",
		SessionId: "sess-1", RuntimeId: "claude-code",
		Gate: "human_approval", DependsOn: []string{"init"},
		Children: []string{"deploy"}, RetryCount: 2,
		StartedAt: time.Now().Add(-30 * time.Second).Format(time.RFC3339),
	}
	p.SetNode(n)
	view := p.View(60, 20)

	for _, section := range []string{"Configuration", "Session"} {
		if !strings.Contains(view, section) {
			t.Errorf("missing section %q", section)
		}
	}
	if !strings.Contains(stripAnsi(view), "RUNNING") {
		t.Error("missing RUNNING status")
	}
	if !strings.Contains(view, "retry") {
		t.Error("missing retry hint")
	}
	if !strings.Contains(view, "human_approval") {
		t.Error("missing gate")
	}
}

func TestInspectorPanelSummaryEmptyFields(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "n1", Status: "PENDING"}
	p.SetNode(n)
	view := p.View(60, 20)
	// Empty values should show em-dash.
	if !strings.Contains(view, "—") {
		t.Error("empty fields should display '—'")
	}
}

func TestInspectorPanelEventCollection(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)

	now := time.Now()
	item := NodeStartedItem{Timestamp: now, NodeID: "n1", SessionID: "s1"}
	event := EventAppendedEvent{Item: item}

	_, _ = p.Update(event)

	events := p.nodeEvents["n1"]
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if _, ok := events[0].(NodeStartedItem); !ok {
		t.Errorf("wrong item type: %T", events[0])
	}
}

func TestInspectorPanelEventCollectionMultipleNodes(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: NodeStartedItem{Timestamp: now, NodeID: "n1"}})
	_, _ = p.Update(EventAppendedEvent{Item: NodeCompletedItem{Timestamp: now, NodeID: "n1"}})
	_, _ = p.Update(EventAppendedEvent{Item: NodeStartedItem{Timestamp: now, NodeID: "n2"}})

	if len(p.nodeEvents["n1"]) != 2 {
		t.Errorf("n1: got %d events, want 2", len(p.nodeEvents["n1"]))
	}
	if len(p.nodeEvents["n2"]) != 1 {
		t.Errorf("n2: got %d events, want 1", len(p.nodeEvents["n2"]))
	}
}

func TestInspectorPanelEventBufferLimit(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)

	now := time.Now()
	for i := 0; i < 201; i++ {
		_, _ = p.Update(EventAppendedEvent{Item: NodeHeartbeatItem{Timestamp: now, NodeID: "n1"}})
	}

	if len(p.nodeEvents["n1"]) > 200 {
		t.Errorf("buffer not capped: got %d events", len(p.nodeEvents["n1"]))
	}
}

func TestInspectorPanelMetricsCaching(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)

	now := time.Now()
	m1 := MetricsSnapshotItem{Timestamp: now, NodeID: "n1", TokensIn: 100, TokensOut: 50, ToolCalls: 3, DurationMs: 5000}
	_, _ = p.Update(EventAppendedEvent{Item: m1})

	cached, ok := p.latestMetrics["n1"]
	if !ok {
		t.Fatal("metrics not cached")
	}
	if cached.TokensIn != 100 || cached.ToolCalls != 3 {
		t.Errorf("cached metrics: tokensIn=%d, toolCalls=%d", cached.TokensIn, cached.ToolCalls)
	}

	// Update with newer metrics.
	m2 := MetricsSnapshotItem{Timestamp: now, NodeID: "n1", TokensIn: 200, TokensOut: 100, ToolCalls: 5, DurationMs: 8000}
	_, _ = p.Update(EventAppendedEvent{Item: m2})

	cached, _ = p.latestMetrics["n1"]
	if cached.TokensIn != 200 {
		t.Errorf("metrics not updated: tokensIn=%d, want 200", cached.TokensIn)
	}
}

func TestInspectorPanelMetricsTabEmpty(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING"}
	p.SetNode(n)
	p.SetTab(TabMetrics)
	view := p.View(60, 20)
	if !strings.Contains(view, "No metrics yet") {
		t.Error("should show empty state for metrics tab")
	}
}

func TestInspectorPanelMetricsTabWithData(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING"}
	p.SetNode(n)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: MetricsSnapshotItem{
		Timestamp: now, NodeID: "n1",
		TokensIn: 500, TokensOut: 200, ToolCalls: 4, DurationMs: 12000,
	}})

	p.SetTab(TabMetrics)
	view := p.View(70, 20)
	for _, want := range []string{"500", "200", "4", "12.0s"} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in metrics view", want)
		}
	}
}

func TestInspectorPanelLogsTabEmpty(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING"}
	p.SetNode(n)
	p.SetTab(TabLogs)
	view := p.View(60, 20)
	if !strings.Contains(view, "No log entries yet") {
		t.Error("should show empty state for logs tab")
	}
}

func TestInspectorPanelLogsTabWithEvents(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING", SessionId: "s1"}
	p.SetNode(n)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: NodeCreatedItem{Timestamp: now, NodeID: "n1", Skill: "root-cause"}})
	_, _ = p.Update(EventAppendedEvent{Item: NodeStartedItem{Timestamp: now, NodeID: "n1", SessionID: "s1"}})

	p.SetTab(TabLogs)
	view := stripAnsi(p.View(80, 24))
	if !strings.Contains(view, "created  skill=root-cause") {
		t.Error("should show diagnostic created line")
	}
	if !strings.Contains(view, "started  session=s1") {
		t.Error("should show diagnostic started line")
	}
	if !strings.Contains(view, "session") || !strings.Contains(view, "s1") {
		t.Error("context header should show session")
	}
}

func TestInspectorPanelLogsTabChronological(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING"}
	p.SetNode(n)

	now := time.Now()
	// Insert oldest first so the buffer is time-ordered.
	// renderLogs iterates oldest → newest (causal order for debugging).
	_, _ = p.Update(EventAppendedEvent{Item: NodeCreatedItem{Timestamp: now, NodeID: "n1", Skill: "s"}})
	_, _ = p.Update(EventAppendedEvent{Item: NodeStartedItem{Timestamp: now.Add(1 * time.Second), NodeID: "n1"}})

	p.SetTab(TabLogs)
	view := stripAnsi(p.View(80, 20))
	createdIdx := strings.Index(view, "created")
	startedIdx := strings.Index(view, "started")
	if startedIdx == -1 || createdIdx == -1 {
		t.Fatalf("missing events in view: %q", view)
	}
	if createdIdx > startedIdx {
		t.Error("events should be in causal order (oldest first)")
	}
}

func TestFormatLogLine(t *testing.T) {
	tests := []struct {
		name string
		item TimelineItem
		want string
	}{
		{"created", NodeCreatedItem{NodeID: "n", Skill: "root-cause"}, "created  skill=root-cause"},
		{"started", NodeStartedItem{NodeID: "n", SessionID: "sess-1"}, "started  session=sess-1"},
		{"failed", NodeFailedItem{NodeID: "n", Reason: "exit code -1 (killed by signal)"}, "FAILED   exit code -1 (killed by signal)"},
		{"metrics", MetricsSnapshotItem{TokensIn: 10, TokensOut: 5, ToolCalls: 2, DurationMs: 1500}, "metrics  10↑/5↓ tokens · 2 tools · 1.5s"},
		{"annotated", NodeAnnotatedItem{Key: "runtime.launch", Value: "id=rt-n-1"}, "runtime.launch = id=rt-n-1"},
		{"completed", NodeCompletedItem{NodeID: "n"}, "completed ✓"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatLogLine(tt.item)
			if got != tt.want {
				t.Errorf("formatLogLine() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInspectorPanelLogsHidesHeartbeats(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING"}
	p.SetNode(n)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: NodeStartedItem{Timestamp: now, NodeID: "n1", SessionID: "s1"}})
	_, _ = p.Update(EventAppendedEvent{Item: NodeHeartbeatItem{Timestamp: now, NodeID: "n1"}})
	_, _ = p.Update(EventAppendedEvent{Item: NodeHeartbeatItem{Timestamp: now, NodeID: "n1"}})

	p.SetTab(TabLogs)
	view := stripAnsi(p.View(80, 20))
	if strings.Contains(view, "heartbeat\n") || strings.Count(view, "heartbeat") > 1 {
		// footer may mention "heartbeat(s) hidden"
	}
	if !strings.Contains(view, "2 heartbeat") {
		t.Errorf("expected heartbeat hidden footer, got: %s", view)
	}
	if strings.Contains(view, "DEBUG") {
		t.Error("heartbeat DEBUG lines should be hidden from Logs tab")
	}
}

func TestInspectorPanelLogsShowsLastError(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{NodeId: "analyze", Status: "FAILED", RetryCount: 1}
	p.SetNode(n)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: NodeFailedItem{
		Timestamp: now, NodeID: "analyze", Reason: "exit code -1 (killed by signal)",
	}})

	p.SetTab(TabLogs)
	view := stripAnsi(p.View(80, 20))
	if !strings.Contains(view, "last err") {
		t.Error("context should show last err label")
	}
	if !strings.Contains(view, "killed by signal") {
		t.Error("context should surface failure reason")
	}
	if !strings.Contains(view, "retry=1") {
		t.Error("context should show retry count")
	}
}

func TestInspectorPanelArtifactsTabEmpty(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "n1", Status: "RUNNING"}
	p.SetNode(n)
	p.SetTab(TabArtifacts)
	view := p.View(60, 20)
	if !strings.Contains(view, "No artifacts yet") {
		t.Error("should show empty state for artifacts tab")
	}
}

func TestInspectorPanelArtifactsTabWithArtifacts(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{NodeId: "n1", Status: "COMPLETED"}
	p.SetNode(n)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: ArtifactCreatedItem{
		Timestamp: now, NodeID: "n1", ArtifactID: "art-abc123", Name: "plan.md",
	}})

	p.SetTab(TabArtifacts)
	view := p.View(80, 20)
	if !strings.Contains(view, "plan.md") {
		t.Error("should show artifact name")
	}
	if !strings.Contains(view, "art-abc123") {
		t.Error("should show artifact ID")
	}
}

func TestInspectorPanelPromptTab(t *testing.T) {
	d := NewDispatcher()
	p := NewInspectorPanel(d)
	n := &arlov1.NodeState{
		NodeId: "n1", Status: "RUNNING",
		RuntimeId: "claude-code", Gate: "human_approval",
		DependsOn: []string{"init"}, RetryCount: 0,
	}
	p.SetNode(n)

	now := time.Now()
	_, _ = p.Update(EventAppendedEvent{Item: NodeCreatedItem{Timestamp: now, NodeID: "n1", Skill: "root-cause"}})

	p.SetTab(TabPrompt)
	view := p.View(80, 24)
	for _, want := range []string{
		"root-cause", "claude-code", "Agent Configuration",
		"Prompt Context", "human_approval", "init",
	} {
		if !strings.Contains(view, want) {
			t.Errorf("missing %q in prompt view", want)
		}
	}
}

func TestInspectorPanelPromptTabNoSkill(t *testing.T) {
	p := NewInspectorPanel(nil)
	n := &arlov1.NodeState{NodeId: "n1", Status: "PENDING"}
	p.SetNode(n)
	p.SetTab(TabPrompt)
	view := p.View(80, 24)
	// Should still render without crashing.
	if !strings.Contains(view, "Agent Configuration") {
		t.Error("missing Agent Configuration section")
	}
	if !strings.Contains(view, "Prompt Context") {
		t.Error("missing Prompt Context section")
	}
}

func TestInspectorPanelSetFocus(t *testing.T) {
	p := NewInspectorPanel(nil)
	p.SetFocus(true)
	if !p.focused {
		t.Error("focus not set")
	}
	p.SetFocus(false)
	if p.focused {
		t.Error("focus not cleared")
	}
}

// ── extractNodeIDFromItem ──────────────────────────

func TestExtractNodeIDFromItem_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		item     TimelineItem
		expected string
	}{
		{"NodeCreatedItem", NodeCreatedItem{NodeID: "n1"}, "n1"},
		{"NodeStartedItem", NodeStartedItem{NodeID: "n2"}, "n2"},
		{"NodeCompletedItem", NodeCompletedItem{NodeID: "n3"}, "n3"},
		{"NodeFailedItem", NodeFailedItem{NodeID: "n4"}, "n4"},
		{"NodeWaitingItem", NodeWaitingItem{NodeID: "n5"}, "n5"},
		{"NodeAnnotatedItem", NodeAnnotatedItem{NodeID: "n6"}, "n6"},
		{"NodeHeartbeatItem", NodeHeartbeatItem{NodeID: "n7"}, "n7"},
		{"MetricsSnapshotItem", MetricsSnapshotItem{NodeID: "n8"}, "n8"},
		{"ArtifactCreatedItem", ArtifactCreatedItem{NodeID: "n9"}, "n9"},
		{"TaskCreatedItem", TaskCreatedItem{Title: "t1"}, ""},
		{"GenericEventItem", GenericEventItem{EventType: "UNKNOWN"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractNodeIDFromItem(tt.item)
			if got != tt.expected {
				t.Errorf("extractNodeIDFromItem(%T) = %q, want %q", tt.item, got, tt.expected)
			}
		})
	}
}

// ── ArtifactCreatedItem ────────────────────────────

func TestArtifactCreatedItemInterface(t *testing.T) {
	now := time.Now()
	var item TimelineItem = ArtifactCreatedItem{
		Timestamp: now, NodeID: "n1",
		ArtifactID: "art-long-id-12345", Name: "output.md",
	}

	if item.Time() != now {
		t.Error("Time() mismatch")
	}
	if item.Level() != INFO {
		t.Errorf("Level() = %v, want INFO", item.Level())
	}
	rendered := item.Render()
	if !strings.Contains(rendered, "n1") {
		t.Error("Render() missing node ID")
	}
	if !strings.Contains(rendered, "output.md") {
		t.Error("Render() missing name")
	}
}

func TestArtifactCreatedItemWithoutName(t *testing.T) {
	item := ArtifactCreatedItem{
		Timestamp: time.Now(), NodeID: "n1",
		ArtifactID: "art-xyz", Name: "",
	}
	rendered := item.Render()
	if !strings.Contains(rendered, "art-xyz") {
		t.Error("should show artifact ID")
	}
}

// ── relativeTime ───────────────────────────────────

func TestRelativeTime(t *testing.T) {
	tests := []struct {
		name   string
		ago    time.Duration
		expect string
	}{
		{"just now", 50 * time.Millisecond, "just now"},
		{"seconds", 45 * time.Second, "45s ago"},
		{"one minute", 90 * time.Second, "1m ago"},
		{"minutes", 5 * time.Minute, "5m ago"},
		{"one hour", 90 * time.Minute, "1h ago"},
		{"hours", 3 * time.Hour, "3h ago"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := time.Now().Add(-tt.ago).Format(time.RFC3339)
			got := relativeTime(ts)
			if got != tt.expect {
				t.Errorf("relativeTime(%q) = %q, want %q", ts, got, tt.expect)
			}
		})
	}
}

func TestRelativeTimeInvalidFormat(t *testing.T) {
	got := relativeTime("not-a-time")
	if got != "not-a-time" {
		t.Errorf("expected fallback to original, got %q", got)
	}
}

// ── tokenBar ───────────────────────────────────────

func TestTokenBar(t *testing.T) {
	tests := []struct {
		name  string
		value int64
		max   int64
		width int
	}{
		{"zero", 0, 100, 10},
		{"half", 50, 100, 10},
		{"full", 100, 100, 10},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tokenBar(tt.value, tt.max, tt.width)
			clean := stripAnsi(got)
			total := len([]rune(clean))
			if total != tt.width {
				t.Errorf("bar width = %d, want %d (got: %q)", total, tt.width, clean)
			}
		})
	}
}

func TestTokenBarOverflow(t *testing.T) {
	got := tokenBar(200, 100, 10)
	clean := stripAnsi(got)
	// Full bar, no empty segments.
	if strings.Count(clean, "░") != 0 {
		t.Errorf("overflow bar should be full: %q", clean)
	}
}

func TestTokenBarZeroMax(t *testing.T) {
	got := tokenBar(50, 0, 10)
	if got != "" {
		t.Errorf("zero max should return empty: got %q", got)
	}
}

// ── truncateID ─────────────────────────────────────

func TestTruncateID(t *testing.T) {
	if got := truncateID("abc"); got != "abc" {
		t.Errorf("short ID: got %q, want %q", got, "abc")
	}
	longID := "art-12345678901234567"
	got := truncateID(longID)
	if len(got) > 15 {
		t.Errorf("long ID not truncated: %q (%d chars)", got, len(got))
	}
	if !strings.HasSuffix(got, "...") {
		t.Errorf("truncated ID should end with '...': %q", got)
	}
}

// ── Timeline Item Levels ───────────────────────────

func TestTimelineItemLevels(t *testing.T) {
	tests := []struct {
		name     string
		item     TimelineItem
		expected Level
	}{
		{"TaskCreatedItem", TaskCreatedItem{}, INFO},
		{"TaskCompletedItem", TaskCompletedItem{}, INFO},
		{"TaskFailedItem", TaskFailedItem{}, ERROR},
		{"NodeStartedItem", NodeStartedItem{}, INFO},
		{"NodeCompletedItem", NodeCompletedItem{}, INFO},
		{"NodeFailedItem", NodeFailedItem{}, ERROR},
		{"NodeWaitingItem", NodeWaitingItem{}, WARN},
		{"NodeHeartbeatItem", NodeHeartbeatItem{}, DEBUG},
	}
	for _, tt := range tests {
		if tt.item.Level() != tt.expected {
			t.Errorf("%s Level() = %v, want %v",
				tt.name, tt.item.Level(), tt.expected)
		}
	}
}

func TestGenericEventItemLevels(t *testing.T) {
	if g := (GenericEventItem{EventType: "NODE_FAILED"}).Level(); g != ERROR {
		t.Errorf("NODE_FAILED level = %v", g)
	}
	if g := (GenericEventItem{EventType: "NODE_WAITING"}).Level(); g != WARN {
		t.Errorf("NODE_WAITING level = %v", g)
	}
	if g := (GenericEventItem{EventType: "NODE_HEARTBEAT"}).Level(); g != DEBUG {
		t.Errorf("NODE_HEARTBEAT level = %v", g)
	}
	if g := (GenericEventItem{EventType: "SOMETHING_ELSE"}).Level(); g != INFO {
		t.Errorf("unknown type level = %v", g)
	}
}

// ── Focus cycling ──────────────────────────────────

func TestFocusCycling(t *testing.T) {
	if FocusWorkflow != 0 || FocusTimeline != 1 || FocusInspector != 2 {
		t.Error("FocusTarget enum values changed — update tab cycling logic")
	}
}

// ── Tab string representation ──────────────────────

func TestInspectorTabStrings(t *testing.T) {
	tests := map[InspectorTab]string{
		TabSummary:   "Summary",
		TabLogs:      "Logs",
		TabPrompt:    "Prompt",
		TabArtifacts: "Artifacts",
		TabMetrics:   "Metrics",
	}
	for tab, expected := range tests {
		if tab.String() != expected {
			t.Errorf("%v.String() = %q, want %q", tab, tab.String(), expected)
		}
	}
}

// ── ProgressBar ────────────────────────────────────

func TestProgressBar(t *testing.T) {
	bar := ProgressBar(5, 10, 10)
	clean := stripAnsi(bar)
	if strings.Count(clean, "█") != 5 || strings.Count(clean, "░") != 5 {
		t.Errorf("5/10 bar: got %q", clean)
	}
}

func TestProgressBarZero(t *testing.T) {
	bar := ProgressBar(0, 0, 10)
	if !strings.Contains(bar, "[") || !strings.Contains(bar, "]") {
		t.Error("empty bar should still have brackets")
	}
}

// ── StatusIcon ─────────────────────────────────────

func TestStatusIconAllStatuses(t *testing.T) {
	tests := map[string]string{
		"COMPLETED": "✓",
		"RUNNING":   "▶",
		"FAILED":    "✗",
		"WAITING":   "⏸",
		"READY":     "↻",
		"PENDING":   "○",
		"UNKNOWN":   "○",
	}
	for status, expected := range tests {
		icon := StatusIcon(status)
		if !strings.Contains(icon, expected) {
			t.Errorf("StatusIcon(%q) expected to contain %q, got %q", status, expected, icon)
		}
	}
}

// ── formatDur ──────────────────────────────────────

func TestFormatDur(t *testing.T) {
	tests := []struct {
		ms     int64
		expect string
	}{
		{500, "500ms"},
		{1500, "1.5s"},
		{90000, "1m30s"},
	}
	for _, tt := range tests {
		got := formatDur(tt.ms)
		if got != tt.expect {
			t.Errorf("formatDur(%d) = %q, want %q", tt.ms, got, tt.expect)
		}
	}
}

// ── MetricsSnapshotItem render ─────────────────────

func TestMetricsSnapshotItemRender(t *testing.T) {
	item := MetricsSnapshotItem{
		NodeID: "n1", TokensIn: 1000, TokensOut: 500,
		ToolCalls: 3, DurationMs: 3500,
	}
	rendered := item.Render()
	for _, want := range []string{"n1", "1000", "500", "3", "3.5s"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("missing %q in MetricsSnapshotItem render: %s", want, rendered)
		}
	}
}

// ── Dispatcher ─────────────────────────────────────

func TestDispatcherSubscribeUnsubscribe(t *testing.T) {
	d := NewDispatcher()
	sub := d.Subscribe()
	if sub == nil {
		t.Fatal("Subscribe returned nil")
	}
	if len(d.subscribers) != 1 {
		t.Errorf("subscribers = %d, want 1", len(d.subscribers))
	}

	d.Unsubscribe(sub)
	if len(d.subscribers) != 0 {
		t.Errorf("subscribers after unsubscribe = %d, want 0", len(d.subscribers))
	}
}

func TestDispatcherEmit(t *testing.T) {
	d := NewDispatcher()
	sub := d.Subscribe()
	d.Emit(NodeChangedEvent{})
	select {
	case _, ok := <-sub:
		if !ok {
			t.Error("channel closed unexpectedly")
		}
	default:
		t.Error("event not received")
	}
}

func TestDispatcherEmitNonBlocking(t *testing.T) {
	d := NewDispatcher()
	// Fill up a subscriber with a tiny buffer.
	sub := make(Subscriber) // no buffer, not via Subscribe
	d.mu.Lock()
	d.subscribers[sub] = struct{}{}
	d.mu.Unlock()

	// Emit should not block even if subscriber cannot receive.
	d.Emit(NodeChangedEvent{})
	// If we reach here without deadlock, the test passes.
}

// ── FilterState ────────────────────────────────────

func TestDefaultFilter(t *testing.T) {
	f := DefaultFilter()
	if !f.WorkflowEvents || !f.NodeEvents || !f.ToolCalls || !f.Errors {
		t.Error("default filter should have all categories enabled except token stream")
	}
	if f.TokenStream {
		t.Error("token stream should be disabled by default")
	}
}

// ── isStateChangeEvent ───────────────────────────────

func TestIsStateChangeEventNodeFailed(t *testing.T) {
	if !isStateChangeEvent("NODE_FAILED") {
		t.Error("NODE_FAILED should be a state-change event")
	}
}

func TestIsStateChangeEventNodeCompleted(t *testing.T) {
	if !isStateChangeEvent("NODE_COMPLETED") {
		t.Error("NODE_COMPLETED should be a state-change event")
	}
}

func TestIsStateChangeEventNodeStarted(t *testing.T) {
	if !isStateChangeEvent("NODE_STARTED") {
		t.Error("NODE_STARTED should be a state-change event")
	}
}

func TestIsStateChangeEventTaskCompleted(t *testing.T) {
	if !isStateChangeEvent("TASK_COMPLETED") {
		t.Error("TASK_COMPLETED should be a state-change event")
	}
}

func TestIsStateChangeEventNonStateChange(t *testing.T) {
	nonState := []string{"METRICS_SNAPSHOT", "NODE_HEARTBEAT", "NODE_ANNOTATED",
		"NODE_CREATED", "TASK_CREATED", "ARTIFACT_CREATED", "NODE_UNKNOWN_EVENT"}
	for _, typ := range nonState {
		if isStateChangeEvent(typ) {
			t.Errorf("%s should NOT be a state-change event", typ)
		}
	}
}

// ── handleEvent schedules GetSnapshot ────────────────

func TestHandleEventSchedulesGetSnapshotForNodeFailed(t *testing.T) {
	m := New("socket", "wf-1")
	evt := &arlov1.Event{EventId: "evt-1", Type: "NODE_FAILED"}
	cmds := m.handleEvent(eventMsg{event: evt})
	if len(cmds) != 2 {
		t.Errorf("handleEvent(NODE_FAILED) returned %d commands, want 2 (RecvEvent + GetSnapshot)", len(cmds))
	}
}

func TestHandleEventSchedulesGetSnapshotForNodeCompleted(t *testing.T) {
	m := New("socket", "wf-1")
	evt := &arlov1.Event{EventId: "evt-1", Type: "NODE_COMPLETED"}
	cmds := m.handleEvent(eventMsg{event: evt})
	if len(cmds) != 2 {
		t.Errorf("handleEvent(NODE_COMPLETED) returned %d commands, want 2 (RecvEvent + GetSnapshot)", len(cmds))
	}
}

func TestHandleEventNoGetSnapshotForMetricsSnapshot(t *testing.T) {
	m := New("socket", "wf-1")
	evt := &arlov1.Event{EventId: "evt-1", Type: "METRICS_SNAPSHOT"}
	cmds := m.handleEvent(eventMsg{event: evt})
	if len(cmds) != 1 {
		t.Errorf("handleEvent(METRICS_SNAPSHOT) returned %d commands, want 1 (RecvEvent only)", len(cmds))
	}
}

func TestHandleEventNoGetSnapshotForDuplicateEvent(t *testing.T) {
	m := New("socket", "wf-1")
	evt := &arlov1.Event{EventId: "evt-1", Type: "NODE_FAILED"}
	// First time should schedule GetSnapshot.
	_ = m.handleEvent(eventMsg{event: evt})
	// Duplicate should NOT schedule GetSnapshot.
	cmds := m.handleEvent(eventMsg{event: evt})
	if len(cmds) != 1 {
		t.Errorf("handleEvent(duplicate NODE_FAILED) returned %d commands, want 1 (RecvEvent only)", len(cmds))
	}
}

func TestHandleEventNilEvent(t *testing.T) {
	m := New("socket", "wf-1")
	cmds := m.handleEvent(eventMsg{event: nil})
	if len(cmds) != 1 {
		t.Errorf("handleEvent(nil) returned %d commands, want 1 (RecvEvent only)", len(cmds))
	}
}

// ── Helpers ────────────────────────────────────────

// stripAnsi removes ANSI escape sequences for test assertions.
func stripAnsi(s string) string {
	var result strings.Builder
	inEscape := false
	for i := 0; i < len(s); i++ {
		if s[i] == '\033' {
			inEscape = true
			continue
		}
		if inEscape {
			if s[i] == 'm' {
				inEscape = false
			}
			continue
		}
		result.WriteByte(s[i])
	}
	return result.String()
}
