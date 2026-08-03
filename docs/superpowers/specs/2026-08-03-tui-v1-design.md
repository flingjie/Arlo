# Arlo Bubble Tea TUI — v1 Design Spec

> **Status:** Approved (v1.1 — Architecture Update)
> **Date:** 2026-08-03
> **Related:** `docs/architecture.md` Layer 2 (gRPC API), Layer 1 (Workflow + DAG Engine)

## Motivation

The current `arlo run` command streams raw text events — it's an Event Viewer, not a Workflow Controller. Arlo is designed as an AgentOS: users need to observe, understand, control, and debug workflows, not just watch events scroll by.

The TUI is Arlo's primary user interface. It should feel like `k9s` or `lazygit` — a full-screen interactive controller with inspect, filter, and command capabilities.

## Design Principles

1. **TUI is a View, not a Runtime** — arlod owns all state; the TUI is a read-only view + control surface over gRPC.
2. **Event Stream is SSOT for real-time updates** — `SubscribeEvents` drives all state updates.
3. **Snapshot API for reconciliation, not polling** — `GetWorkflowSnapshot` is called only on stream reconnect, version gap detection, or explicit user request. Under normal operation, the event stream alone keeps the UI current.
4. **Completion doesn't exit** — users stay in the TUI to inspect results, artifacts, and logs after the workflow finishes.
5. **Command bar, not key soup** — `:command` pattern like vim/lazygit for actions beyond navigation.
6. **Separation of concerns** — CLI layer creates tasks (CreateTask), TUI layer observes and controls (Subscribe + Commands). The TUI never creates tasks.
7. **Extensible internals** — Command Registry, Event Dispatcher, TimelineItem interface. Panels subscribe to internal events, not gRPC directly.

## Internal Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Bubble Tea App                     │
│                                                     │
│  ┌─────────┐  ┌──────────┐  ┌───────────────────┐  │
│  │ App     │  │ UI       │  │ Workflow          │  │
│  │ Model   │  │ State    │  │ State             │  │
│  └────┬────┘  └────┬─────┘  └────────┬──────────┘  │
│       │            │                 │              │
│  ┌────┴────────────┴─────────────────┴──────────┐  │
│  │              Event Dispatcher                 │  │
│  │  (internal pub/sub — decouples panels)       │  │
│  └────┬────────────┬──────────────┬─────────────┘  │
│       │            │              │                │
│  ┌────┴────┐ ┌─────┴──────┐ ┌────┴──────────┐    │
│  │Workflow │ │ Timeline   │ │ Inspector     │    │
│  │ Panel   │ │ Panel      │ │ Panel         │    │
│  └─────────┘ └────────────┘ └───────────────┘    │
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │           Command Registry                    │  │
│  │  attach | retry | kill | logs | filter | ... │  │
│  └──────────────────────────────────────────────┘  │
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │           gRPC Client Layer                   │  │
│  │  SubscribeEvents | GetWorkflowSnapshot       │  │
│  │  ExecuteCommand | AttachWorkspace            │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Role |
|-----------|------|
| **App Model** | Bubble Tea `Model`. Owns UIState, WorkflowState, gRPC connection. Delegates to panels for View. |
| **UI State** | `struct { Focus Panel; SelectedNode string; InspectorOpen bool; InspectorTab string; Filter FilterState; CommandMode bool; CommandInput string }` — pure UI concerns. |
| **Workflow State** | `struct { ID string; Status string; Version uint64; Nodes []NodeState; StartedAt time.Time }` — snapshot of workflow data, updated by event stream + reconciliation. |
| **Event Dispatcher** | Internal pub/sub. Panels subscribe to typed events: `NodeChanged`, `EventAppended`, `WorkflowUpdated`, `Reconnected`. Decouples gRPC from rendering. |
| **Command Registry** | `type Command interface { Name() string; Aliases() []string; Execute(args []string) tea.Cmd; Complete(input string) []string }`. Plugins register commands at init. |
| **gRPC Client** | Connects to arlod. Runs SubscribeEvents loop, calls GetWorkflowSnapshot on reconnect/version gap. Emits internal events to Dispatcher. |

## Data Flow

```
CLI (cmd/arlo)
  │
  │ 1. CreateTask(yaml) → workflowID
  │
  └──► TUI.Run(socket, workflowID)
         │
         │ 2. SubscribeEvents(workflowID) ──► real-time event stream
         │ 3. GetWorkflowSnapshot(workflowID) ──► initial state + version
         │
         ▼
    Event Dispatcher
         │
    ┌────┼────┬──────────┐
    ▼    ▼    ▼          ▼
  WF   TL   Insp    Overview
```

### Reconnect / Version Gap

```
Stream breaks
  │
  ├──► Reconnect SubscribeEvents
  │
  ├──► GetWorkflowSnapshot(workflowID)
  │       returns { version: 41, nodes: [...] }
  │
  ├──► Compare: local version = 35, remote = 41
  │       → gap detected, full state replacement
  │
  └──► Emit WorkflowUpdated to Dispatcher
```

No fixed-interval polling. The only time `GetWorkflowSnapshot` is called:
- On initial TUI startup (to get baseline state)
- On stream disconnect/reconnect (to detect gaps)
- On explicit user refresh (`:refresh`)

## Layout

```
┌──────────────────────────────────────────────────────────────────┐
│ Arlo wf-123  RUNNING  02:14  ██████░░ 66%  3 nodes  1 retry  1 gate │  ← Overview
├──────────────────────────┬───────────────────────────────────────┤
│ WORKFLOW                 │ TIMELINE / INSPECTOR (toggle)         │  ← 50/50 split
│                          │                                       │
│ ▼ analyze                │  15:27  analyze started               │
│   ● RUNNING              │  15:28  tool: grep                    │
│   sess-abc  retry:1      │  15:29  tool: git diff               │
│                          │  15:30  error: tool timeout           │
│ ○ implement              │  15:31  retry                         │
│   PENDING                │  15:32  completed                     │
│   depends: analyze       │                                       │
│                          │                                       │
│ ○ review                 │                                       │
│   PENDING                │                                       │
│   gate: human_approval   │                                       │
│                          │                                       │
├──────────────────────────┴───────────────────────────────────────┤
│ :attach :retry :logs :filter :help :quit                           │  ← Command Bar
└──────────────────────────────────────────────────────────────────┘
```

### Three Panels

| Panel | Content | Data Source |
|-------|---------|-------------|
| **Overview Bar** | Workflow ID + status badge + elapsed time + progress bar + node/retry/gate/worker counts | Event Dispatcher (`WorkflowUpdated`) |
| **Workflow (left)** | Tree of nodes with status icons, session IDs, retry counts, dependency arrows, gate markers | Event Dispatcher (`NodeChanged`) |
| **Timeline/Inspector (right)** | TimelineItem stream (events + execution items) with filter; toggles to Tabbed Node Inspector on `Enter` | Event Dispatcher (`EventAppended`) |

## Interaction Model

| Key | Action |
|-----|--------|
| `q` / `Esc` / `Ctrl+C` | Quit TUI (stays open after workflow completes) |
| `tab` | Switch focus between Workflow panel and Timeline/Inspector |
| `↑` `↓` | Navigate node tree (left) or scroll timeline (right) |
| `PgUp` `PgDn` | Page scroll in Timeline |
| `Enter` | On focused node → right panel switches to **Node Inspector** |
| `Esc` | From Inspector → back to Timeline; or close overlay |
| `f` | Open event type filter overlay |
| `:` | Enter command mode |
| `←` `→` | Collapse/expand workflow tree nodes |
| `1`-`5` | Switch Inspector tabs (Summary, Logs, Prompt, Artifacts, Metrics) |

## Timeline Items (not raw Events)

```go
type TimelineItem interface {
    Time()    time.Time
    Level()   Level   // INFO, WARN, ERROR, DEBUG
    Render()  string  // single-line representation
}

type Level int
const (
    INFO  Level = iota
    WARN
    ERROR
    DEBUG
)
```

Built-in item types:

| Item | Source | Level |
|------|--------|-------|
| `NodeStartedItem` | `NODE_STARTED` event | INFO |
| `NodeCompletedItem` | `NODE_COMPLETED` event | INFO |
| `NodeFailedItem` | `NODE_FAILED` event | ERROR |
| `NodeWaitingItem` | `NODE_WAITING` event | WARN |
| `RetryItem` | Replay analysis | WARN |
| `ToolCallItem` | Runtime instrumentation (future) | DEBUG |
| `ErrorItem` | Any error event | ERROR |

The Timeline panel renders `TimelineItem` interface, not proto Events directly. New item types are added without changing the panel.

## Node Inspector (tabbed)

When `Enter` is pressed on a node, the right panel shows a tabbed inspector:

```
┌── Node Inspector ─── [Summary] [Logs] [Prompt] [Artifacts] [Metrics] ──┐
│                                                                         │
│  Node        implement                                                  │
│  Status      RUNNING                                                    │
│  Session     sess-abc123                                                │
│  Started     15:27:42                                                   │
│  Duration    3m12s                                                      │
│  Retry       2                                                          │
│  Depends     analyze                                                    │
│  Worker      runtime-3                                                  │
│  Skill       coder                                                      │
│  Gate        —                                                          │
│                                                                         │
│  :attach workspace    :retry    :logs                                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

Tabs:
- **Summary** (1) — node metadata, status, actions
- **Logs** (2) — filtered timeline items for this node
- **Prompt** (3) — skill prompt + input (future)
- **Artifacts** (4) — output artifacts list (future)
- **Metrics** (5) — token usage, duration breakdown (future)

## Command Registry

```go
type Command interface {
    Name()        string              // "attach"
    Aliases()     []string            // ["a"]
    Description() string              // "Attach to a node's workspace session"
    Usage()       string              // ":attach <node-id>"
    Execute(args  []string, ctx *AppContext) tea.Cmd
    Complete(input string) []string   // tab completion suggestions
}
```

Registry is a map, not a switch. Commands register at init:

```go
func init() {
    registry.Register(&AttachCommand{})
    registry.Register(&RetryCommand{})
    registry.Register(&KillCommand{})
    registry.Register(&LogsCommand{})
    registry.Register(&FilterCommand{})
    registry.Register(&QuitCommand{})
    registry.Register(&HelpCommand{})
}
```

| Command | Alias | Status |
|---------|-------|--------|
| `:attach <node>` | `:a` | v1.0 |
| `:quit` | `:q` | v1.0 |
| `:help` | `:h` | v1.0 |
| `:filter` | `:f` | v1.0 |
| `:refresh` | `:rf` | v1.0 |
| `:retry <node>` | `:r` | future (needs gRPC) |
| `:kill <node>` | `:k` | future |
| `:logs <node>` | `:l` | future |
| `:pause` | `:p` | future |
| `:continue` | `:c` | future |

## Data Model Changes

### Proto: `NodeState` — add `depends_on`, `children`, `gate`

```proto
message NodeState {
  string node_id = 1;
  string status = 2;
  string session_id = 3;
  string runtime_id = 4;
  int32 retry_count = 5;
  repeated string depends_on = 6;  // NEW: upstream dependencies
  repeated string children = 7;     // NEW: downstream dependents (computed by projection)
  string gate = 8;                  // NEW: human gate condition
}
```

### Proto: `GetWorkflowSnapshot` — add `version`

```proto
message GetWorkflowSnapshotRequest {
  string workflow_id = 1;
}

message GetWorkflowSnapshotResponse {
  string workflow_id = 1;
  string status = 2;
  uint64 version = 3;               // NEW: monotonic version for gap detection
  google.protobuf.Timestamp started_at = 4;  // NEW
  repeated NodeState nodes = 5;
}
```

(Keep existing `GetWorkflow` for backward compat; add `GetWorkflowSnapshot` as the TUI-facing RPC.)

### Domain: `NodeState` — add `DependsOn`, `Children`, `Gate`

```go
type NodeState struct {
    NodeID      string
    Status      NodeStatus
    SessionID   string
    RuntimeID   string
    StartedAt   *time.Time
    CompletedAt *time.Time
    RetryCount  int
    Output      map[string]string
    DependsOn   []string  // NEW
    Children    []string  // NEW (computed by projection)
    Gate        string    // NEW
}
```

### Event payload: `NodeCreated` — include `DependsOn`, `Gate`

```go
type NodeCreated struct {
    NodeID     string   `json:"node_id"`
    WorkflowID string   `json:"workflow_id"`
    SkillName  string   `json:"skill_name"`
    Runtime    string   `json:"runtime"`
    DependsOn  []string `json:"depends_on,omitempty"`
    Gate       string   `json:"gate,omitempty"`
}
```

### Projection: Build Workflow Tree

WorkflowProjection computes `Children` from `DependsOn` during rebuild:

```
For each node n with DependsOn [A, B]:
    A.Children = append(A.Children, n.NodeID)
    B.Children = append(B.Children, n.NodeID)
```

This way the TUI receives a pre-built tree — no DAG construction at render time.

## Implementation Scope

### Phase 1: Proto + Domain Changes (backend prep)
1. Add `depends_on`, `children`, `gate` to `NodeState` proto
2. Add `GetWorkflowSnapshot` RPC with `version` + `started_at`
3. Add `DependsOn`, `Children`, `Gate` to `domain.NodeState`
4. Add `DependsOn`, `Gate` to `domain.NodeCreated` event payload
5. Populate in `CreateTask` service handler
6. Build Children in WorkflowProjection
7. Implement `GetWorkflowSnapshot` service handler
8. Regenerate protobuf

### Phase 2: TUI Architecture
1. `internal/tui/` package restructured:
   - `app.go` — App Model (Bubble Tea Model + Init + Update + View)
   - `state.go` — UIState + WorkflowState structs
   - `dispatcher.go` — Event Dispatcher (internal pub/sub)
   - `client.go` — gRPC client: SubscribeEvents loop + snapshot on reconnect
   - `workflow_panel.go` — Workflow tree panel
   - `timeline_panel.go` — Timeline panel (renders TimelineItem interface)
   - `inspector_panel.go` — Tabbed Node Inspector
   - `command.go` — Command Registry + individual commands
   - `styles.go` — Lipgloss styles + theme
   - `timeline_items.go` — Built-in TimelineItem types
2. `cmd/arlo/main.go` — CLI does CreateTask, then calls `tui.Run(socket, workflowID)`

### Phase 3: Interactive Features
1. Command bar with `:attach`, `:quit`, `:help`, `:filter`, `:refresh`
2. Node Inspector with tab navigation (Summary tab full; other tabs stubbed)
3. Event type filter overlay
4. Workflow tree collapse/expand
5. Progress bar in overview
6. Workspace attach (via `:attach <node>` — delegates to gRPC AttachWorkspace)

### Out of Scope (v1+)
- `:retry`, `:kill`, `:logs`, `:pause`, `:continue` — need command RPCs
- Inspector tabs beyond Summary (Logs, Prompt, Artifacts, Metrics)
- Execution-level TimelineItems (TOOL_CALL, MODEL_CALL, TOKEN_STREAM)
- Soft/human dependency types in proto
- PTY inline rendering in TUI
- Event Bus persistence / session restore

## Files to Change

| File | Change |
|------|--------|
| `api/proto/arlo/v1/service.proto` | Add fields to NodeState; add GetWorkflowSnapshot RPC |
| `internal/domain/node.go` | Add DependsOn, Children, Gate to NodeState and NodeCreated |
| `internal/state/workflow_projection.go` | Build Children from DependsOn during projection |
| `internal/service/arlo_service.go` | Populate new fields; implement GetWorkflowSnapshot |
| `cmd/arlo/main.go` | Refactor: CLI creates task, TUI observes |
| `internal/tui/app.go` | NEW — App Model, Init/Update/View |
| `internal/tui/state.go` | NEW — UIState, WorkflowState |
| `internal/tui/dispatcher.go` | NEW — Event Dispatcher |
| `internal/tui/client.go` | NEW — gRPC client + event stream loop |
| `internal/tui/workflow_panel.go` | NEW — Workflow tree panel |
| `internal/tui/timeline_panel.go` | NEW — Timeline panel |
| `internal/tui/inspector_panel.go` | NEW — Tabbed Node Inspector |
| `internal/tui/command.go` | NEW — Command Registry |
| `internal/tui/styles.go` | NEW — Styles + theme |
| `internal/tui/timeline_items.go` | NEW — TimelineItem types |
| `internal/tui/tui.go` | DELETE (replaced by app.go + panels) |

## Testing

- **Unit tests** for each panel's render output
- **Unit tests** for Command Registry (registration, execution, completion)
- **Unit tests** for Event Dispatcher (subscribe, emit, unsubscribe)
- **Integration test** with mock gRPC server simulating event stream + reconnect
- **Manual test** against running arlod with `graphs/bugfix.yaml`

## References

- `docs/architecture.md` — 12-layer North Star design
- `CLAUDE.md` — Bubble Tea conventions, gRPC conventions
- `api/proto/arlo/v1/service.proto` — current gRPC contract
