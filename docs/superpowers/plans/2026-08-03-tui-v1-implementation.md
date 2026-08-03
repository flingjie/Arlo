# Arlo Bubble Tea TUI — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a three-panel Bubble Tea TUI that observes and controls workflows via gRPC.

**Architecture:** CLI creates tasks (CreateTask), TUI observes and controls (SubscribeEvents + GetWorkflowSnapshot). Internal Event Dispatcher decouples gRPC from panels. Command Registry enables extensible commands.

**Tech Stack:** Go, Bubble Tea (v1.3.10), Lipgloss (v1.1.0), Bubbles (v1.0.0), gRPC over Unix socket, protobuf (buf generate).

## Global Constraints

- TUI is a View, not a Runtime — arlod owns all state.
- Event Stream is SSOT for real-time updates. Snapshot API only on reconnect/version gap.
- Completion doesn't exit TUI — users stay to inspect results.
- Command bar: `:command` pattern like vim/lazygit.
- CreateTask lives in CLI layer, not in TUI.
- Extensible internals: Command Registry, Event Dispatcher, TimelineItem interface.

---

## Phase 1: Backend Prep (Proto, Domain, Projection, Service)

### Task 1: Add DependsOn and Gate to domain.NodeCreated event payload

**Files:**
- Modify: `internal/domain/events.go:48-54`

**Interfaces:**
- Produces: `domain.NodeCreated{NodeID, WorkflowID, SkillName, Runtime, DependsOn []string, Gate string}`

- [ ] **Step 1: Add fields to NodeCreated**

```go
// NodeCreated is the payload for NODE_CREATED events.
type NodeCreated struct {
	NodeID     string   `json:"node_id"`
	WorkflowID string   `json:"workflow_id"`
	SkillName  string   `json:"skill_name"`
	Runtime    string   `json:"runtime"`
	DependsOn  []string `json:"depends_on,omitempty"`
	Gate       string   `json:"gate,omitempty"`
}
```

- [ ] **Step 2: Run tests to verify compilation**

Run: `go build ./internal/domain/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/domain/events.go
git commit -m "feat: add DependsOn and Gate to NodeCreated event payload"
```

### Task 2: Add DependsOn, Children, Gate to domain.NodeState

**Files:**
- Modify: `internal/domain/node.go:22-31`

**Interfaces:**
- Consumes: (none)
- Produces: `domain.NodeState{NodeID, Status, SessionID, RuntimeID, StartedAt, CompletedAt, RetryCount, Output, DependsOn []string, Children []string, Gate string}`

- [ ] **Step 1: Add fields to NodeState**

```go
type NodeState struct {
	NodeID      string            `json:"node_id"`
	Status      NodeStatus        `json:"status"`
	SessionID   string            `json:"session_id,omitempty"`
	RuntimeID   string            `json:"runtime_id,omitempty"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	RetryCount  int               `json:"retry_count"`
	Output      map[string]string `json:"output,omitempty"`
	DependsOn   []string          `json:"depends_on,omitempty"`
	Children    []string          `json:"children,omitempty"`
	Gate        string            `json:"gate,omitempty"`
}
```

- [ ] **Step 2: Run tests to verify compilation**

Run: `go build ./internal/domain/... ./internal/state/... ./internal/service/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/domain/node.go
git commit -m "feat: add DependsOn, Children, Gate to domain.NodeState"
```

### Task 3: Update proto NodeState and add GetWorkflowSnapshot RPC

**Files:**
- Modify: `api/proto/arlo/v1/service.proto`

**Interfaces:**
- Produces: `arlo.v1.NodeState` with fields 6-8; `arlo.v1.GetWorkflowSnapshot` RPC (request/response messages)

- [ ] **Step 1: Add depends_on, children, gate to NodeState message**

Find the `NodeState` message (around line 82-89) and add three new fields:

```proto
message NodeState {
  string node_id = 1;
  string status = 2;
  string session_id = 3;
  string runtime_id = 4;
  int32 retry_count = 5;
  repeated string depends_on = 6;
  repeated string children = 7;
  string gate = 8;
}
```

- [ ] **Step 2: Add GetWorkflowSnapshot RPC to the service definition**

Add inside `service ArloService { ... }`:

```proto
// Snapshot (for TUI reconciliation after reconnect)
rpc GetWorkflowSnapshot(GetWorkflowSnapshotRequest) returns (GetWorkflowSnapshotResponse);
```

- [ ] **Step 3: Add GetWorkflowSnapshot request/response messages**

Add after the existing `GetWorkflowResponse` message block (before `// ── Session`):

```proto
message GetWorkflowSnapshotRequest {
  string workflow_id = 1;
}

message GetWorkflowSnapshotResponse {
  string workflow_id = 1;
  string status = 2;
  uint64 version = 3;
  repeated NodeState nodes = 4;
  string started_at = 5;  // RFC 3339 timestamp
}
```

- [ ] **Step 4: Regenerate protobuf**

Run: `make proto`
Expected: regenerates `api/gen/arlo/v1/service.pb.go` and `api/gen/arlo/v1/service_grpc.pb.go`

- [ ] **Step 5: Verify compilation**

Run: `go build ./...`
Expected: builds without error

- [ ] **Step 6: Commit**

```bash
git add api/proto/arlo/v1/service.proto api/gen/
git commit -m "feat: add depends_on/children/gate to NodeState; add GetWorkflowSnapshot RPC"
```

### Task 4: Populate DependsOn and Gate in CreateTask service handler

**Files:**
- Modify: `internal/service/arlo_service.go:97-107`

**Interfaces:**
- Consumes: `domain.NodeCreated` with new fields, `graph.Nodes` (each has `.DependsOn []string` and `.Gate domain.ApprovalGate`)
- Produces: Correctly populated `DependsOn` and `Gate` in NODE_CREATED events

- [ ] **Step 1: Update the seed node event loop to include DependsOn and Gate**

```go
// Create node events.
for _, n := range graph.Nodes {
	if _, err := s.eventStore.Append(ctx, "node-"+n.ID, []store.Event{{
		ID:   fmt.Sprintf("evt-node-%s-%s", n.ID, wfID),
		Type: store.EventNodeCreated,
		Payload: marshalJSON(domain.NodeCreated{
			NodeID:     n.ID,
			WorkflowID: wfID,
			SkillName:  n.SkillRef.Name,
			Runtime:    n.Runtime.Provider,
			DependsOn:  n.DependsOn,
			Gate:       string(n.Gate),
		}),
	}}); err != nil {
		return nil, status.Errorf(codes.Internal, "seed node event: %v", err)
	}
}
```

- [ ] **Step 2: Run service package tests**

Run: `go build ./internal/service/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/service/arlo_service.go
git commit -m "feat: populate DependsOn and Gate in NodeCreated seed events"
```

### Task 5: Update WorkflowProjection to store DependsOn and Gate, build Children

**Files:**
- Modify: `internal/state/workflow_projection.go:288-295` (applyNodeCreated)
- Modify: `internal/state/statestore.go:170-171` (Rebuild, after replay loop)

**Interfaces:**
- Consumes: `domain.NodeCreated` with `DependsOn` and `Gate` fields
- Produces: `domain.NodeState` with `DependsOn`, `Gate` populated; `Children` computed via `rebuildTree()`

- [ ] **Step 1: Update applyNodeCreated to store DependsOn and Gate**

```go
func (p *workflowProjection) applyNodeCreated(event store.Event) error {
	var payload domain.NodeCreated
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeCreated: %w", err)
	}
	ns := &domain.NodeState{
		NodeID:    payload.NodeID,
		Status:    domain.NodeStatusPending,
		DependsOn: payload.DependsOn,
		Gate:      payload.Gate,
	}
	if ns.DependsOn == nil {
		ns.DependsOn = []string{}
	}

	wf := p.store.upsertWorkflow(payload.WorkflowID)
	wf.Nodes[payload.NodeID] = *ns
	p.store.nodeIndex[payload.NodeID] = payload.WorkflowID
	return nil
}
```

Note: This replaces the old `applyNodeCreated` which called `p.store.upsertNode(payload.WorkflowID, payload.NodeID)`. The new version directly sets NodeState with DependsOn and Gate instead of using the `upsertNode` helper.

- [ ] **Step 2: Add rebuildTree method to InMemoryStateStore**

Add this method inside `InMemoryStateStore`:

```go
// rebuildTree computes Children from DependsOn for all nodes in all workflows.
// Must be called with the write lock held.
func (ss *InMemoryStateStore) rebuildTree() {
	for _, wf := range ss.workflows {
		for nodeID, ns := range wf.Nodes {
			for _, dep := range ns.DependsOn {
				if depNS, ok := wf.Nodes[dep]; ok {
					// Avoid duplicates if rebuildTree is called multiple times.
					found := false
					for _, child := range depNS.Children {
						if child == nodeID {
							found = true
							break
						}
					}
					if !found {
						depNS.Children = append(depNS.Children, nodeID)
						wf.Nodes[dep] = depNS
					}
				}
			}
		}
	}
}
```

- [ ] **Step 3: Call rebuildTree at the end of Rebuild**

In `Rebuild()`, after the replay loop and position tracking, add:

```go
// Compute workflow tree (Children from DependsOn).
ss.mu.Lock()
ss.rebuildTree()
ss.mu.Unlock()
```

Add this right before `return nil` in `Rebuild()`, after `ss.lastRebuiltPos = currentPos`.

- [ ] **Step 4: Run state tests**

Run: `go test -race ./internal/state/...`
Expected: all tests pass

- [ ] **Step 5: Commit**

```bash
git add internal/state/workflow_projection.go internal/state/statestore.go
git commit -m "feat: store DependsOn and Gate in projection; compute Children via rebuildTree"
```

### Task 6: Implement GetWorkflowSnapshot service handler

**Files:**
- Modify: `internal/service/arlo_service.go` (after GetWorkflow handler, around line 182)
- Modify: `internal/state/statestore.go` (add Version field to WorkflowState)

**Interfaces:**
- Consumes: `stateStore.GetWorkflow()` returning `*domain.WorkflowState`
- Produces: `GetWorkflowSnapshot` gRPC handler returning `GetWorkflowSnapshotResponse` with version

- [ ] **Step 1: Add Version field to domain.WorkflowState**

```go
type WorkflowState struct {
	ID        string              `json:"id"`
	Status    WorkflowStatus      `json:"status"`
	Version   uint64              `json:"version"`  // NEW
	Nodes     map[string]NodeState `json:"nodes"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}
```

- [ ] **Step 2: Track version in the projection**

The `version` should be the last event position processed for this workflow. Since the event store has a global position counter, we can use `LastPosition()` from the event store.

In `InMemoryStateStore`, add a field and track it during Rebuild:

Actually, a simpler approach: compute version as the max event position for events belonging to this workflow's streams. But that's complex. 

Simplest correct approach: use the event store's global `LastPosition()` as the version. When `GetWorkflowSnapshot` is called, query the event store for the current maximum position. This gives a monotonic counter the TUI can compare.

- [ ] **Step 3: Implement GetWorkflowSnapshot handler**

Add to `internal/service/arlo_service.go`:

```go
// GetWorkflowSnapshot returns the current workflow state plus a monotonic version
// for gap detection after stream reconnect.
func (s *ArloService) GetWorkflowSnapshot(ctx context.Context, req *arlov1.GetWorkflowSnapshotRequest) (*arlov1.GetWorkflowSnapshotResponse, error) {
	wf, err := s.stateStore.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}

	var nodes []*arlov1.NodeState
	for _, ns := range wf.Nodes {
		nodes = append(nodes, &arlov1.NodeState{
			NodeId:     ns.NodeID,
			Status:     string(ns.Status),
			SessionId:  ns.SessionID,
			RuntimeId:  ns.RuntimeID,
			RetryCount: int32(ns.RetryCount),
			DependsOn:  ns.DependsOn,
			Children:   ns.Children,
			Gate:       ns.Gate,
		})
	}

	version := s.eventStore.LastPosition()

	return &arlov1.GetWorkflowSnapshotResponse{
		WorkflowId: wf.ID,
		Status:     string(wf.Status),
		Version:    uint64(version),
		Nodes:      nodes,
		StartedAt:  wf.CreatedAt.Format(time.RFC3339),
	}, nil
}
```

- [ ] **Step 4: Also update GetWorkflow to include new fields for backward compat**

```go
nodes = append(nodes, &arlov1.NodeState{
	NodeId:     ns.NodeID,
	Status:     string(ns.Status),
	SessionId:  ns.SessionID,
	RuntimeId:  ns.RuntimeID,
	RetryCount: int32(ns.RetryCount),
	DependsOn:  ns.DependsOn,
	Children:   ns.Children,
	Gate:       ns.Gate,
})
```

- [ ] **Step 5: Run tests**

Run: `go test -race ./internal/service/... ./internal/state/...`
Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add internal/service/arlo_service.go internal/state/statestore.go internal/domain/workflow.go
git commit -m "feat: implement GetWorkflowSnapshot with version tracking"
```

---

## Phase 2: TUI Architecture

### Task 7: Create UI state and workflow state types

**Files:**
- Create: `internal/tui/state.go`

**Interfaces:**
- Produces: `tui.UIState` struct, `tui.WorkflowState` struct, `tui.FocusTarget` type, `tui.InspectorTab` type

- [ ] **Step 1: Write state.go**

```go
package tui

import (
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// FocusTarget represents which panel has keyboard focus.
type FocusTarget int

const (
	FocusWorkflow  FocusTarget = iota
	FocusTimeline
	FocusCommand
)

// InspectorTab represents the active tab in the Node Inspector.
type InspectorTab int

const (
	TabSummary   InspectorTab = iota
	TabLogs
	TabPrompt
	TabArtifacts
	TabMetrics
)

func (t InspectorTab) String() string {
	switch t {
	case TabSummary:
		return "Summary"
	case TabLogs:
		return "Logs"
	case TabPrompt:
		return "Prompt"
	case TabArtifacts:
		return "Artifacts"
	case TabMetrics:
		return "Metrics"
	default:
		return "Summary"
	}
}

// UIState holds all UI-only state, separate from workflow data.
type UIState struct {
	Focus         FocusTarget
	SelectedNode  string
	InspectorOpen bool
	InspectorTab  InspectorTab
	FilterOpen    bool
	FilterState   FilterState
	CommandMode   bool
	CommandInput  string
	Width         int
	Height        int
}

// WorkflowState holds the current snapshot of workflow data.
type WorkflowState struct {
	ID        string
	Status    string
	Version   uint64
	Nodes     []*arlov1.NodeState
	StartedAt time.Time
}

// FilterState controls which event categories are visible.
type FilterState struct {
	WorkflowEvents bool
	NodeEvents     bool
	ToolCalls      bool
	Errors         bool
	TokenStream    bool
}

// DefaultFilter returns the default filter (all categories on).
func DefaultFilter() FilterState {
	return FilterState{
		WorkflowEvents: true,
		NodeEvents:     true,
		ToolCalls:      true,
		Errors:         true,
		TokenStream:    false,
	}
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/state.go
git commit -m "feat: add UIState, WorkflowState, FilterState types"
```

### Task 8: Create Event Dispatcher

**Files:**
- Create: `internal/tui/dispatcher.go`

**Interfaces:**
- Produces: `tui.Dispatcher` struct with `Subscribe()`, `Emit()` methods; `tui.InternalEvent` interface; concrete event types `NodeChangedEvent`, `EventAppendedEvent`, `WorkflowUpdatedEvent`, `ReconnectedEvent`

- [ ] **Step 1: Write dispatcher.go**

```go
package tui

import (
	"sync"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// InternalEvent is a marker interface for events dispatched within the TUI.
type InternalEvent interface {
	internalEventMarker()
}

// NodeChangedEvent is emitted when a node's state changes.
type NodeChangedEvent struct {
	NodeID    string
	NewStatus string
}

func (NodeChangedEvent) internalEventMarker() {}

// EventAppendedEvent is emitted when a new timeline item arrives from gRPC.
type EventAppendedEvent struct {
	Item TimelineItem
}

func (EventAppendedEvent) internalEventMarker() {}

// WorkflowUpdatedEvent is emitted after a snapshot reconciliation.
type WorkflowUpdatedEvent struct {
	Status  string
	Version uint64
	Nodes   []*arlov1.NodeState
}

func (WorkflowUpdatedEvent) internalEventMarker() {}

// ReconnectedEvent is emitted when the gRPC stream reconnects.
type ReconnectedEvent struct{}

func (ReconnectedEvent) internalEventMarker() {}

// Subscriber is a channel that receives InternalEvents.
type Subscriber chan InternalEvent

// Dispatcher is an internal pub/sub bus for TUI panels.
type Dispatcher struct {
	mu          sync.RWMutex
	subscribers map[Subscriber]struct{}
}

// NewDispatcher creates a new event dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		subscribers: make(map[Subscriber]struct{}),
	}
}

// Subscribe registers a new subscriber channel.
func (d *Dispatcher) Subscribe() Subscriber {
	d.mu.Lock()
	defer d.mu.Unlock()
	ch := make(Subscriber, 32)
	d.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a subscriber channel.
func (d *Dispatcher) Unsubscribe(ch Subscriber) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.subscribers, ch)
	close(ch)
}

// Emit sends an event to all subscribers. Non-blocking — slow subscribers are skipped.
func (d *Dispatcher) Emit(event InternalEvent) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for ch := range d.subscribers {
		select {
		case ch <- event:
		default:
			// Drop event if subscriber buffer is full (non-blocking).
		}
	}
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/dispatcher.go
git commit -m "feat: add Event Dispatcher with pub/sub for panel decoupling"
```

### Task 9: Create gRPC client layer

**Files:**
- Create: `internal/tui/client.go`

**Interfaces:**
- Produces: `tui.Client` struct with `Connect()`, `SubscribeEvents()`, `GetSnapshot()`, `Close()` methods; returns tea.Cmd compatible messages

- [ ] **Step 1: Write client.go**

```go
package tui

import (
	"context"
	"fmt"
	"io"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the gRPC connection to arlod.
type Client struct {
	socket string
	conn   *grpc.ClientConn
	api    arlov1.ArloServiceClient
}

// NewClient creates a new gRPC client for the given Unix socket.
func NewClient(socket string) *Client {
	return &Client{socket: socket}
}

// Connect establishes the gRPC connection.
func (c *Client) Connect() error {
	conn, err := grpc.NewClient(
		"unix://"+c.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	c.conn = conn
	c.api = arlov1.NewArloServiceClient(conn)
	return nil
}

// Close shuts down the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetSnapshot fetches the current workflow snapshot.
func (c *Client) GetSnapshot(workflowID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := c.api.GetWorkflowSnapshot(ctx, &arlov1.GetWorkflowSnapshotRequest{
			WorkflowId: workflowID,
		})
		if err != nil {
			return snapshotMsg{err: err}
		}
		return snapshotMsg{
			workflowID: resp.WorkflowId,
			status:     resp.Status,
			version:    resp.Version,
			nodes:      resp.Nodes,
			startedAt:  resp.StartedAt,
		}
	}
}

type snapshotMsg struct {
	workflowID string
	status     string
	version    uint64
	nodes      []*arlov1.NodeState
	startedAt  string
	err        error
}

// SubscribeEvents starts an event stream and returns events as messages.
func (c *Client) SubscribeEvents(workflowID string) tea.Cmd {
	return func() tea.Msg {
		ctx := context.Background()
		stream, err := c.api.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
			WorkflowId:  workflowID,
			FromPosition: 0,
		})
		if err != nil {
			return streamErrMsg{err: fmt.Errorf("subscribe: %w", err)}
		}

		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return streamEndMsg{}
			}
			if err != nil {
				return streamErrMsg{err: fmt.Errorf("stream recv: %w", err)}
			}
			// We can't return from inside a goroutine in Bubble Tea.
			// We send the event back. Bubble Tea will call this cmd again.
			return eventMsg{event: event}
		}
	}
}

type eventMsg struct {
	event *arlov1.Event
}

type streamErrMsg struct {
	err error
}

type streamEndMsg struct{}
```

- [ ] **Step 2: Add stream reconnection logic**

The event stream needs to loop. In the Bubble Tea model update, when we receive an `eventMsg`, we re-issue `SubscribeEvents` to get the next event. When we receive `streamErrMsg`, we reconnect the stream and fetch a snapshot.

- [ ] **Step 3: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 4: Commit**

```bash
git add internal/tui/client.go
git commit -m "feat: add gRPC client layer with snapshot and event stream"
```

### Task 10: Create styles

**Files:**
- Create: `internal/tui/styles.go`

**Interfaces:**
- Produces: Exported lipgloss styles used by all panels

- [ ] **Step 1: Write styles.go**

```go
package tui

import "github.com/charmbracelet/lipgloss"

var (
	// Colors
	Green  = lipgloss.Color("42")
	Yellow = lipgloss.Color("226")
	Red    = lipgloss.Color("196")
	Blue   = lipgloss.Color("39")
	Gray   = lipgloss.Color("244")
	White  = lipgloss.Color("255")
	DarkBg = lipgloss.Color("236")
	Cyan   = lipgloss.Color("86")
	Purple = lipgloss.Color("129")

	// Styles
	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(Cyan).
			MarginBottom(1)

	PanelStyle = lipgloss.NewStyle().
			BorderStyle(lipgloss.RoundedBorder()).
			BorderForeground(Blue).
			Padding(0, 1)

	StatusBarStyle = lipgloss.NewStyle().
			Background(DarkBg).
			Foreground(lipgloss.Color("252")).
			Padding(0, 1)

	CommandPromptStyle = lipgloss.NewStyle().
				Foreground(Yellow).
				Bold(true)

	SelectedStyle = lipgloss.NewStyle().
			Background(lipgloss.Color("57")).
			Foreground(White)

	NormalStyle = lipgloss.NewStyle()

	GreenStyle  = lipgloss.NewStyle().Foreground(Green)
	YellowStyle = lipgloss.NewStyle().Foreground(Yellow)
	RedStyle    = lipgloss.NewStyle().Foreground(Red)
	GrayStyle   = lipgloss.NewStyle().Foreground(Gray)
	WhiteStyle  = lipgloss.NewStyle().Foreground(White)
	CyanStyle   = lipgloss.NewStyle().Foreground(Cyan)
	PurpleStyle = lipgloss.NewStyle().Foreground(Purple)
)

// StatusIcon returns a colored dot for a node status.
func StatusIcon(status string) string {
	switch status {
	case "COMPLETED":
		return GreenStyle.Render("●")
	case "RUNNING", "STARTING":
		return YellowStyle.Render("●")
	case "FAILED", "CANCELLED":
		return RedStyle.Render("●")
	case "WAITING":
		return PurpleStyle.Render("●")
	case "READY":
		return CyanStyle.Render("●")
	default:
		return GrayStyle.Render("○")
	}
}

// ProgressBar renders a progress bar.
func ProgressBar(completed, total int, width int) string {
	if total == 0 {
		return GrayStyle.Render("[" + repeatStr(" ", width) + "]")
	}
	ratio := float64(completed) / float64(total)
	filled := int(ratio * float64(width))
	if filled > width {
		filled = width
	}
	empty := width - filled
	return lipgloss.NewStyle().Foreground(Green).Render("[" +
		repeatStr("█", filled) +
		repeatStr("░", empty) +
		"]")
}

func repeatStr(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/styles.go
git commit -m "feat: add lipgloss styles and theme"
```

### Task 11: Create TimelineItem types

**Files:**
- Create: `internal/tui/timeline_items.go`

**Interfaces:**
- Produces: `tui.TimelineItem` interface; concrete types `NodeStartedItem`, `NodeCompletedItem`, `NodeFailedItem`, `NodeWaitingItem`, `GenericEventItem`

- [ ] **Step 1: Write timeline_items.go**

```go
package tui

import (
	"fmt"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// Level represents the severity of a timeline item.
type Level int

const (
	INFO  Level = iota
	WARN
	ERROR
	DEBUG
)

func (l Level) String() string {
	switch l {
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case DEBUG:
		return "DEBUG"
	default:
		return "INFO"
	}
}

func (l Level) Color() string {
	switch l {
	case INFO:
		return "42"
	case WARN:
		return "226"
	case ERROR:
		return "196"
	case DEBUG:
		return "244"
	default:
		return "42"
	}
}

// TimelineItem is the interface for items displayed in the timeline panel.
type TimelineItem interface {
	Time() time.Time
	Level() Level
	Render() string
}

// NodeStartedItem represents a NODE_STARTED event.
type NodeStartedItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeStartedItem) Time() time.Time { return i.Timestamp }
func (i NodeStartedItem) Level() Level    { return INFO }
func (i NodeStartedItem) Render() string {
	return fmt.Sprintf("%s started", i.NodeID)
}

// NodeCompletedItem represents a NODE_COMPLETED event.
type NodeCompletedItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeCompletedItem) Time() time.Time { return i.Timestamp }
func (i NodeCompletedItem) Level() Level    { return INFO }
func (i NodeCompletedItem) Render() string {
	return fmt.Sprintf("%s completed", i.NodeID)
}

// NodeFailedItem represents a NODE_FAILED event.
type NodeFailedItem struct {
	Timestamp time.Time
	NodeID    string
	Reason    string
}

func (i NodeFailedItem) Time() time.Time { return i.Timestamp }
func (i NodeFailedItem) Level() Level    { return ERROR }
func (i NodeFailedItem) Render() string {
	return fmt.Sprintf("%s failed: %s", i.NodeID, i.Reason)
}

// NodeWaitingItem represents a NODE_WAITING event.
type NodeWaitingItem struct {
	Timestamp time.Time
	NodeID    string
	Reason    string
}

func (i NodeWaitingItem) Time() time.Time { return i.Timestamp }
func (i NodeWaitingItem) Level() Level    { return WARN }
func (i NodeWaitingItem) Render() string {
	return fmt.Sprintf("%s waiting: %s", i.NodeID, i.Reason)
}

// GenericEventItem wraps a gRPC event into a timeline item.
type GenericEventItem struct {
	Timestamp time.Time
	EventType string
}

func (i GenericEventItem) Time() time.Time { return i.Timestamp }
func (i GenericEventItem) Level() Level {
	// Errors are ERROR level.
	switch i.EventType {
	case "NODE_FAILED", "TASK_FAILED":
		return ERROR
	case "NODE_WAITING":
		return WARN
	default:
		return INFO
	}
}
func (i GenericEventItem) Render() string {
	return i.EventType
}

// EventToItem converts a gRPC event to a more specific TimelineItem when possible.
func EventToItem(event *arlov1.Event) TimelineItem {
	t, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		t = time.Now()
	}

	switch event.Type {
	case "NODE_STARTED":
		return NodeStartedItem{Timestamp: t}
	case "NODE_COMPLETED":
		return NodeCompletedItem{Timestamp: t}
	case "NODE_FAILED":
		return NodeFailedItem{Timestamp: t}
	case "NODE_WAITING":
		return NodeWaitingItem{Timestamp: t}
	default:
		return GenericEventItem{Timestamp: t, EventType: event.Type}
	}
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/timeline_items.go
git commit -m "feat: add TimelineItem interface and built-in item types"
```

### Task 12: Create Workflow panel

**Files:**
- Create: `internal/tui/workflow_panel.go`

**Interfaces:**
- Consumes: `[]*arlov1.NodeState` from state; `UIState` for selected node; `Dispatcher` subscriber channel
- Produces: `tui.WorkflowPanel` with `View(width int) string` and `Update(msg tea.Msg)` returning commands

- [ ] **Step 1: Write workflow_panel.go**

```go
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
			// Collapse selected node.
			if p.selected < len(p.nodes) {
				p.collapsed[p.nodes[p.selected].NodeId] = true
			}
		case "right", "l":
			// Expand selected node.
			if p.selected < len(p.nodes) {
				p.collapsed[p.nodes[p.selected].NodeId] = false
			}
		case "enter":
			// Selected node is handled by app model for Inspector.
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
	sb.WriteString(HeaderStyle.Render("WORKFLOW"))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	if len(p.nodes) == 0 {
		sb.WriteString(GrayStyle.Render("  no nodes yet...\n"))
		return PanelStyle.Width(width).Render(sb.String())
	}

	// Build a root set: nodes with no dependencies.
	hasDep := make(map[string]bool)
	for _, n := range p.nodes {
		for _, dep := range n.DependsOn {
			hasDep[dep] = true
		}
	}

	// Render tree starting from roots.
	for i, n := range p.nodes {
		if hasDep[n.NodeId] {
			continue // Will be rendered as child.
		}
		p.renderNode(&sb, n, 0, i)
	}

	return PanelStyle.Width(width).Render(sb.String())
}

func (p *WorkflowPanel) renderNode(sb *strings.Builder, n *arlov1.NodeState, depth int, idx int) {
	indent := strings.Repeat("  ", depth)
	expandIcon := "▼"
	if p.collapsed[n.NodeId] {
		expandIcon = "▶"
	}

	lineStyle := NormalStyle
	if p.focused && p.selected == idx {
		lineStyle = SelectedStyle
	}

	// First line: icon + node ID + status.
	icon := StatusIcon(n.Status)
	sb.WriteString(lineStyle.Render(fmt.Sprintf("%s%s %s %s", indent, expandIcon, icon, n.NodeId)))
	sb.WriteString("\n")

	// Second line: status text + metadata.
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

	// Render children (if not collapsed).
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

// GetSelectedNode returns the node ID of the currently selected node.
func (p *WorkflowPanel) GetSelectedNode() string {
	if p.selected >= 0 && p.selected < len(p.nodes) {
		return p.nodes[p.selected].NodeId
	}
	return ""
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/workflow_panel.go
git commit -m "feat: add Workflow panel with tree rendering"
```

### Task 13: Create Timeline panel

**Files:**
- Create: `internal/tui/timeline_panel.go`

**Interfaces:**
- Consumes: `TimelineItem` interface; `FilterState`; `Dispatcher` subscriber channel
- Produces: `tui.TimelinePanel` with `View(width, height int) string` and `Update(msg tea.Msg)`

- [ ] **Step 1: Write timeline_panel.go**

```go
package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/viewport"
	"github.com/charmbracelet/lipgloss"
)

// TimelinePanel renders the event timeline.
type TimelinePanel struct {
	items      []TimelineItem
	filter     FilterState
	focused    bool
	viewport   viewport.Model
	sub        Subscriber
	dispatcher *Dispatcher
}

// NewTimelinePanel creates a new timeline panel.
func NewTimelinePanel(dispatcher *Dispatcher) *TimelinePanel {
	vp := viewport.New(80, 20)
	vp.Style = lipgloss.NewStyle().
		BorderStyle(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62"))

	return &TimelinePanel{
		filter:     DefaultFilter(),
		viewport:   vp,
		dispatcher: dispatcher,
	}
}

// Init subscribes to the dispatcher.
func (p *TimelinePanel) Init() tea.Cmd {
	p.sub = p.dispatcher.Subscribe()
	return p.listenDispatcher
}

func (p *TimelinePanel) listenDispatcher() tea.Msg {
	event := <-p.sub
	return event
}

// Update handles messages.
func (p *TimelinePanel) Update(msg tea.Msg) (tea.Cmd, bool) {
	switch msg := msg.(type) {
	case EventAppendedEvent:
		if p.filterItem(msg.Item) {
			p.items = append(p.items, msg.Item)
		}
		return p.listenDispatcher, true

	case tea.KeyMsg:
		if !p.focused {
			return nil, false
		}
		switch msg.String() {
		case "up", "k":
			p.viewport.LineUp(1)
		case "down", "j":
			p.viewport.LineDown(1)
		case "pgup":
			p.viewport.PageUp()
		case "pgdown":
			p.viewport.PageDown()
		default:
			return nil, false
		}
		return nil, true

	case InternalEvent:
		return p.listenDispatcher, true

	case tea.WindowSizeMsg:
		p.viewport.Width = msg.Width - 4
		p.viewport.Height = msg.Height - 4
		return nil, true
	}

	return nil, false
}

// SetFocus sets keyboard focus on this panel.
func (p *TimelinePanel) SetFocus(focused bool) {
	p.focused = focused
}

// SetFilter updates the event filter.
func (p *TimelinePanel) SetFilter(f FilterState) {
	p.filter = f
}

func (p *TimelinePanel) filterItem(item TimelineItem) bool {
	switch item.Level() {
	case ERROR:
		return p.filter.Errors
	case WARN:
		return p.filter.NodeEvents
	case DEBUG:
		return p.filter.ToolCalls
	default:
		return p.filter.WorkflowEvents || p.filter.NodeEvents
	}
}

// View renders the timeline content.
func (p *TimelinePanel) View(width, height int) string {
	p.viewport.Width = width - 4
	p.viewport.Height = height - 4

	var sb strings.Builder
	sb.WriteString(HeaderStyle.Render("TIMELINE"))
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	if len(p.items) == 0 {
		sb.WriteString(GrayStyle.Render("  waiting for events...\n"))
	} else {
		// Show last N items that fit in the viewport.
		start := 0
		if len(p.items) > p.viewport.Height {
			start = len(p.items) - p.viewport.Height
		}
		for _, item := range p.items[start:] {
			timeStr := item.Time().Format("15:04:05")
			levelColor := lipgloss.NewStyle().Foreground(lipgloss.Color(item.Level().Color()))
			sb.WriteString(fmt.Sprintf("  %s  %s  %s\n",
				GrayStyle.Render(timeStr),
				levelColor.Render(item.Level().String()),
				item.Render(),
			))
		}
	}

	p.viewport.SetContent(sb.String())
	return PanelStyle.Width(width).Render(p.viewport.View())
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/timeline_panel.go
git commit -m "feat: add Timeline panel with filter and scrolling"
```

### Task 14: Create Inspector panel

**Files:**
- Create: `internal/tui/inspector_panel.go`

**Interfaces:**
- Consumes: `*arlov1.NodeState` for selected node; `UIState` for tab selection
- Produces: `tui.InspectorPanel` with `View(width, height int) string`

- [ ] **Step 1: Write inspector_panel.go**

```go
package tui

import (
	"fmt"
	"strings"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// InspectorPanel renders the detailed view of a selected node.
type InspectorPanel struct {
	node *arlov1.NodeState
	tab  InspectorTab
}

// NewInspectorPanel creates a new inspector panel.
func NewInspectorPanel() *InspectorPanel {
	return &InspectorPanel{tab: TabSummary}
}

// SetNode sets the node to inspect.
func (p *InspectorPanel) SetNode(n *arlov1.NodeState) {
	p.node = n
}

// SetTab sets the active tab.
func (p *InspectorPanel) SetTab(t InspectorTab) {
	p.tab = t
}

// View renders the inspector.
func (p *InspectorPanel) View(width, height int) string {
	if p.node == nil {
		return PanelStyle.Width(width).Height(height).Render(
			GrayStyle.Render("  select a node to inspect (Enter)"),
		)
	}

	var sb strings.Builder

	// Tab bar.
	tabs := []InspectorTab{TabSummary, TabLogs, TabPrompt, TabArtifacts, TabMetrics}
	tabBar := ""
	for _, t := range tabs {
		if t == p.tab {
			tabBar += SelectedStyle.Render(fmt.Sprintf("[%s]", t.String()))
		} else {
			tabBar += GrayStyle.Render(fmt.Sprintf(" %s ", t.String()))
		}
		tabBar += " "
	}
	sb.WriteString(HeaderStyle.Render("NODE INSPECTOR"))
	sb.WriteString("  ")
	sb.WriteString(tabBar)
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", width-2))
	sb.WriteString("\n\n")

	// Tab content.
	switch p.tab {
	case TabSummary:
		p.renderSummary(&sb)
	case TabLogs:
		p.renderLogs(&sb)
	case TabPrompt:
		p.renderPrompt(&sb)
	case TabArtifacts:
		p.renderArtifacts(&sb)
	case TabMetrics:
		p.renderMetrics(&sb)
	}

	return PanelStyle.Width(width).Height(height).Render(sb.String())
}

func (p *InspectorPanel) renderSummary(sb *strings.Builder) {
	n := p.node
	lines := []struct{ label, value string }{
		{"Node", n.NodeId},
		{"Status", n.Status},
		{"Session", n.SessionId},
		{"Runtime", n.RuntimeId},
		{"Retry", fmt.Sprintf("%d", n.RetryCount)},
		{"Gate", n.Gate},
		{"Depends On", strings.Join(n.DependsOn, ", ")},
		{"Children", strings.Join(n.Children, ", ")},
	}

	for _, l := range lines {
		if l.value == "" {
			l.value = "—"
		}
		sb.WriteString(fmt.Sprintf("  %-12s  %s\n",
			GrayStyle.Render(l.label),
			WhiteStyle.Render(l.value),
		))
	}

	sb.WriteString("\n")
	sb.WriteString(GrayStyle.Render("  :attach workspace    :retry    :logs"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderLogs(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Logs tab — coming in v1.1"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderPrompt(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Prompt tab — coming in v1.1"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderArtifacts(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Artifacts tab — coming in v1.1"))
	sb.WriteString("\n")
}

func (p *InspectorPanel) renderMetrics(sb *strings.Builder) {
	sb.WriteString(GrayStyle.Render("  Metrics tab — coming in v1.1"))
	sb.WriteString("\n")
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/inspector_panel.go
git commit -m "feat: add tabbed Node Inspector panel"
```

### Task 15: Create Command Registry

**Files:**
- Create: `internal/tui/command.go`

**Interfaces:**
- Produces: `tui.Command` interface; `tui.CommandRegistry` struct; built-in commands: `AttachCommand`, `QuitCommand`, `HelpCommand`, `FilterCommand`, `RefreshCommand`

- [ ] **Step 1: Write command.go**

```go
package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// AppContext provides commands access to the TUI state.
type AppContext struct {
	Socket     string
	WorkflowID string
	Client     *Client
	UIState    *UIState
	Workflow   *WorkflowState
	Dispatch   func(InternalEvent)
}

// Command is an executable command in the command bar.
type Command interface {
	Name() string
	Aliases() []string
	Description() string
	Usage() string
	Execute(args []string, ctx *AppContext) tea.Cmd
}

// CommandRegistry holds all registered commands.
type CommandRegistry struct {
	commands map[string]Command
}

// NewCommandRegistry creates a new registry and registers built-in commands.
func NewCommandRegistry() *CommandRegistry {
	r := &CommandRegistry{commands: make(map[string]Command)}
	r.Register(&QuitCommand{})
	r.Register(&HelpCommand{})
	r.Register(&FilterCommand{})
	r.Register(&RefreshCommand{})
	r.Register(&AttachCommand{})
	return r
}

// Register adds a command.
func (r *CommandRegistry) Register(cmd Command) {
	r.commands[cmd.Name()] = cmd
	for _, alias := range cmd.Aliases() {
		r.commands[alias] = cmd
	}
}

// Execute finds a command by name and executes it.
func (r *CommandRegistry) Execute(input string, ctx *AppContext) tea.Cmd {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil
	}
	name := parts[0]
	args := parts[1:]

	cmd, ok := r.commands[name]
	if !ok {
		return func() tea.Msg { return commandMsg{output: fmt.Sprintf("unknown command: %s", name)} }
	}
	return cmd.Execute(args, ctx)
}

// Complete returns tab-completion suggestions for a partial input.
func (r *CommandRegistry) Complete(partial string) []string {
	var suggestions []string
	for name, cmd := range r.commands {
		if strings.HasPrefix(name, partial) && name == cmd.Name() { // Only primary names.
			suggestions = append(suggestions, name)
		}
	}
	return suggestions
}

type commandMsg struct {
	output string
}

// ── QuitCommand ────────────────────────────────────

type QuitCommand struct{}

func (c *QuitCommand) Name() string        { return "quit" }
func (c *QuitCommand) Aliases() []string   { return []string{"q"} }
func (c *QuitCommand) Description() string { return "Exit the TUI" }
func (c *QuitCommand) Usage() string       { return ":quit" }
func (c *QuitCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return tea.Quit
}

// ── HelpCommand ────────────────────────────────────

type HelpCommand struct{}

func (c *HelpCommand) Name() string        { return "help" }
func (c *HelpCommand) Aliases() []string   { return []string{"h"} }
func (c *HelpCommand) Description() string { return "Show available commands" }
func (c *HelpCommand) Usage() string       { return ":help" }

func (c *HelpCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return func() tea.Msg {
		lines := []string{
			"Available commands:",
			"  :quit, :q        Exit the TUI",
			"  :help, :h        Show this help",
			"  :filter, :f      Toggle event filter",
			"  :refresh, :rf    Reconnect stream and fetch snapshot",
			"  :attach, :a      Attach to a node's workspace session (usage: :attach <node-id>)",
		}
		return commandMsg{output: strings.Join(lines, "\n")}
	}
}

// ── FilterCommand ──────────────────────────────────

type FilterCommand struct{}

func (c *FilterCommand) Name() string        { return "filter" }
func (c *FilterCommand) Aliases() []string   { return []string{"f"} }
func (c *FilterCommand) Description() string { return "Toggle event filter overlay" }
func (c *FilterCommand) Usage() string       { return ":filter" }

func (c *FilterCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return func() tea.Msg {
		ctx.UIState.FilterOpen = !ctx.UIState.FilterOpen
		return nil
	}
}

// ── RefreshCommand ─────────────────────────────────

type RefreshCommand struct{}

func (c *RefreshCommand) Name() string        { return "refresh" }
func (c *RefreshCommand) Aliases() []string   { return []string{"rf"} }
func (c *RefreshCommand) Description() string { return "Reconnect stream and fetch snapshot" }
func (c *RefreshCommand) Usage() string       { return ":refresh" }

func (c *RefreshCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return tea.Batch(
		ctx.Client.SubscribeEvents(ctx.WorkflowID),
		ctx.Client.GetSnapshot(ctx.WorkflowID),
	)
}

// ── AttachCommand ──────────────────────────────────

type AttachCommand struct{}

func (c *AttachCommand) Name() string        { return "attach" }
func (c *AttachCommand) Aliases() []string   { return []string{"a"} }
func (c *AttachCommand) Description() string { return "Attach to a node's workspace session" }
func (c *AttachCommand) Usage() string       { return ":attach <node-id>" }

func (c *AttachCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	if len(args) < 1 {
		return func() tea.Msg { return commandMsg{output: "usage: :attach <node-id>"} }
	}
	nodeID := args[0]
	return func() tea.Msg {
		// Find the session for this node.
		for _, n := range ctx.Workflow.Nodes {
			if n.NodeId == nodeID && n.SessionId != "" {
				return attachMsg{nodeID: nodeID, sessionID: n.SessionId}
			}
		}
		return commandMsg{output: fmt.Sprintf("no session found for node %s", nodeID)}
	}
}

type attachMsg struct {
	nodeID    string
	sessionID string
}
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/command.go
git commit -m "feat: add Command Registry with built-in commands"
```

### Task 16: Create App Model (wire everything)

**Files:**
- Create: `internal/tui/app.go`

**Interfaces:**
- Consumes: All panel types, Client, Dispatcher, CommandRegistry, state types
- Produces: `tui.Model` implementing `tea.Model`; `tui.Run(socket, workflowID string) error` entry point

- [ ] **Step 1: Write app.go**

```go
package tui

import (
	"fmt"
	"strings"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Model is the top-level Bubble Tea model.
type Model struct {
	client    *Client
	socket    string
	workflow  string

	// State
	ui       UIState
	wf       WorkflowState

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
	cmdMsg   string // Feedback from command execution.
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

	// Fetch initial snapshot.
	snapshot := m.client.GetSnapshot(m.workflow)
	// Subscribe to events (will be re-issued on each event).
	subscribe := m.client.SubscribeEvents(m.workflow)

	// Execute both and combine results (tea.Sequence handles sequential).
	_ = snapshot
	_ = subscribe
	return connectedMsg{}
}

// Update handles all messages.
func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Global keys.
		switch msg.String() {
		case "ctrl+c":
			m.quitting = true
			return m, tea.Quit

		case "esc":
			if m.ui.CommandMode {
				m.ui.CommandMode = false
				m.cmdBuf = ""
				return m, nil
			}
			if m.ui.InspectorOpen {
				m.ui.InspectorOpen = false
				m.ui.Focus = FocusWorkflow
				return m, nil
			}
			if m.ui.FilterOpen {
				m.ui.FilterOpen = false
				return m, nil
			}
			m.quitting = true
			return m, tea.Quit

		case "q":
			if !m.ui.CommandMode {
				m.quitting = true
				return m, tea.Quit
			}

		case "tab":
			if m.ui.CommandMode {
				break
			}
			if m.ui.Focus == FocusWorkflow {
				m.ui.Focus = FocusTimeline
				m.workflowPanel.SetFocus(false)
				m.timelinePanel.SetFocus(true)
			} else if m.ui.Focus == FocusTimeline {
				m.ui.Focus = FocusWorkflow
				m.timelinePanel.SetFocus(false)
				m.workflowPanel.SetFocus(true)
			}

		case "enter":
			if m.ui.CommandMode {
				// Execute command.
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
			}
			// Enter on focused node → open inspector.
			if m.ui.Focus == FocusWorkflow {
				nodeID := m.workflowPanel.GetSelectedNode()
				if nodeID != "" {
					for _, n := range m.wf.Nodes {
						if n.NodeId == nodeID {
							m.inspectorPanel.SetNode(n)
							break
						}
					}
					m.ui.InspectorOpen = true
					m.ui.SelectedNode = nodeID
					m.ui.Focus = FocusTimeline // Right panel gets focus.
				}
			}

		case ":":
			m.ui.CommandMode = true
			m.cmdBuf = ""
			return m, nil

		case "1", "2", "3", "4", "5":
			if m.ui.InspectorOpen {
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
			}

		case "f":
			m.ui.FilterOpen = !m.ui.FilterOpen
			return m, nil

		default:
			if m.ui.CommandMode {
				// Handle command input.
				switch msg.String() {
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
		}

	case tea.WindowSizeMsg:
		m.ui.Width = msg.Width
		m.ui.Height = msg.Height
		cmds = append(cmds, func() tea.Msg { return msg })

	case connectedMsg:
		if msg.err != nil {
			m.err = msg.err
			return m, tea.Quit
		}
		m.ready = true
		cmds = append(cmds,
			m.client.SubscribeEvents(m.workflow),
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
			m.dispatcher.Emit(WorkflowUpdatedEvent{
				Status:  msg.status,
				Version: msg.version,
				Nodes:   msg.nodes,
			})
		}
		// Continue streaming.
		cmds = append(cmds, m.client.SubscribeEvents(m.workflow))

	case eventMsg:
		if msg.event != nil {
			item := EventToItem(msg.event)
			m.dispatcher.Emit(EventAppendedEvent{Item: item})
		}
		// Continue streaming.
		cmds = append(cmds, m.client.SubscribeEvents(m.workflow))

	case streamErrMsg:
		// Reconnect: fetch snapshot to sync state, then resubscribe.
		cmds = append(cmds, m.client.GetSnapshot(m.workflow))
		m.dispatcher.Emit(ReconnectedEvent{})

	case streamEndMsg:
		// Stream ended normally (workflow completed).
		cmds = append(cmds, m.client.GetSnapshot(m.workflow))

	case commandMsg:
		m.cmdMsg = msg.output

	case attachMsg:
		// Attach to session — for now, print info.
		m.cmdMsg = fmt.Sprintf("attach to session %s (node %s) — attach via CLI: arlo attach %s",
			msg.sessionID, msg.nodeID, msg.sessionID)
	}

	// Route to focused panel.
	if m.ui.Focus == FocusWorkflow {
		cmd, _ := m.workflowPanel.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	} else if m.ui.Focus == FocusTimeline {
		cmd, _ := m.timelinePanel.Update(msg)
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	// Always route dispatcher events to panels that didn't get focus.
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
	if w < 40 {
		w = 80
	}
	h := m.ui.Height
	if h < 10 {
		h = 24
	}

	// Overview bar.
	overview := m.renderOverview(w)

	// Left panel (Workflow).
	leftWidth := w / 2
	left := m.workflowPanel.View(leftWidth)

	// Right panel (Timeline or Inspector).
	rightWidth := w - leftWidth - 2
	var right string
	if m.ui.InspectorOpen {
		right = m.inspectorPanel.View(rightWidth, h-5)
	} else {
		right = m.timelinePanel.View(rightWidth, h-5)
	}

	panels := lipgloss.JoinHorizontal(lipgloss.Top, left, right)

	// Command bar / status bar.
	status := m.renderCommandBar(w)

	// Filter overlay.
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

	completed := 0
	for _, n := range m.wf.Nodes {
		if n.Status == "COMPLETED" {
			completed++
		}
	}
	total := len(m.wf.Nodes)

	elapsed := ""
	if !m.wf.StartedAt.IsZero() {
		elapsed = time.Since(m.wf.StartedAt).Round(time.Second).String()
	}

	bar := ProgressBar(completed, total, 10)

	left := fmt.Sprintf("%s %s  %s  %s  %d nodes",
		CyanStyle.Render("Arlo"),
		WhiteStyle.Render(m.workflow),
		YellowStyle.Render(status),
		elapsed,
		total,
	)
	right := bar

	leftLen := lipgloss.Width(left)
	rightLen := lipgloss.Width(right)
	padding := width - leftLen - rightLen - 2
	if padding < 1 {
		padding = 1
	}

	return lipgloss.NewStyle().
		Background(DarkBg).
		Foreground(lipgloss.Color("252")).
		Width(width).
		Padding(0, 1).
		Render(left + strings.Repeat(" ", padding) + right)
}

func (m *Model) renderCommandBar(width int) string {
	if m.ui.CommandMode {
		return StatusBarStyle.Width(width).Render(
			fmt.Sprintf(":%s", m.cmdBuf),
		)
	}

	msg := m.cmdMsg
	if msg != "" {
		m.cmdMsg = "" // Clear after display.
	}

	left := GrayStyle.Render(":attach :retry :logs :filter :help :quit")
	right := ""
	if msg != "" {
		right = WhiteStyle.Render(msg)
	}

	return StatusBarStyle.Width(width).Render(
		left + "  " + right,
	)
}

func (m *Model) renderFilterOverlay() string {
	// Simple text-based filter overlay.
	lines := []string{
		"┌── Filter ──────────────────────────┐",
		"│                                    │",
		fmt.Sprintf("│  [%s] workflow events               │", checkMark(m.timelinePanel.filter.WorkflowEvents)),
		fmt.Sprintf("│  [%s] node events                   │", checkMark(m.timelinePanel.filter.NodeEvents)),
		fmt.Sprintf("│  [%s] tool calls                    │", checkMark(m.timelinePanel.filter.ToolCalls)),
		fmt.Sprintf("│  [%s] errors                        │", checkMark(m.timelinePanel.filter.Errors)),
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
```

- [ ] **Step 2: Verify compilation**

Run: `go build ./internal/tui/...`
Expected: builds without error

- [ ] **Step 3: Commit**

```bash
git add internal/tui/app.go
git commit -m "feat: add App Model wiring all panels, commands, and gRPC client"
```

### Task 17: Update cmd/arlo/main.go — CLI creates task, TUI observes

**Files:**
- Modify: `cmd/arlo/main.go`
- Delete: `internal/tui/tui.go` (old skeleton, replaced by app.go + panels)

**Interfaces:**
- Consumes: `tui.Run(socket, workflowID)` entry point
- Produces: `arlo run <yaml>` = CreateTask → TUI

- [ ] **Step 1: Refactor run command**

Update the `run` function in `cmd/arlo/main.go`:

```go
func run(args []string) {
	if len(args) < 1 {
		log.Fatal("usage: arlo run <workflow.yaml>")
	}

	mustCreateDir()

	yamlPath := args[0]
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		log.Fatalf("failed to read workflow file: %v", err)
	}

	conn, client, ctx := dial()
	defer conn.Close()

	resp, err := client.CreateTask(ctx, &arlov1.CreateTaskRequest{
		Title:          yamlPath,
		WorkflowSource: string(data),
	})
	if err != nil {
		log.Fatalf("create task: %v", err)
	}

	// Clean up gRPC connection before launching TUI (TUI creates its own).
	conn.Close()

	fmt.Printf("Task created: %s\n", resp.TaskId)
	fmt.Printf("Workflow:    %s\n", resp.WorkflowId)
	fmt.Println("Launching TUI...")

	time.Sleep(500 * time.Millisecond) // Brief pause for daemon to process.

	if err := tui.Run(socketPath, resp.WorkflowId); err != nil {
		log.Fatalf("tui: %v", err)
	}
}
```

Add `"time"` to imports.

- [ ] **Step 2: Remove old tui command**

Remove the `tuiCmd` function and the `"tui"` case from the switch block. Keep the `tui` import since it's now used by `run`.

- [ ] **Step 3: Delete old tui.go**

```bash
rm internal/tui/tui.go
```

- [ ] **Step 4: Verify compilation and test**

Run: `go build ./cmd/arlo/...`
Expected: builds without error

Run: `go build ./...`
Expected: entire project builds

- [ ] **Step 5: Commit**

```bash
git add cmd/arlo/main.go internal/tui/
git rm internal/tui/tui.go 2>/dev/null || true
git commit -m "feat: refactor CLI run command to create task then launch TUI"
```

---

## Phase 3: Verification

### Task 18: End-to-end verification

- [ ] **Step 1: Build everything**

Run: `make build`
Expected: both binaries built without error

- [ ] **Step 2: Run all tests**

Run: `make test`
Expected: all tests pass

- [ ] **Step 3: Manual smoke test**

```bash
# Start daemon.
./scripts/arlo.sh start

# Run a workflow (launches TUI).
./scripts/arlo.sh run graphs/bugfix.yaml

# Expected: TUI shows three panels: Overview, Workflow (left), Timeline (right).
# Press Enter on a node → Inspector opens.
# Press Esc from Inspector → back to Timeline.
# Press : → command bar opens.
# Type :quit → exits.
```

- [ ] **Step 4: Commit any final fixes**
