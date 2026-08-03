# Arlo Bubble Tea TUI — v1 Design Spec

> **Status:** Approved
> **Date:** 2026-08-03
> **Related:** `docs/architecture.md` Layer 2 (gRPC API), Layer 1 (Workflow + DAG Engine)

## Motivation

The current `arlo run` command streams raw text events — it's an Event Viewer, not a Workflow Controller. Arlo is designed as an AgentOS: users need to observe, understand, control, and debug workflows, not just watch events scroll by.

The TUI is Arlo's primary user interface. It should feel like `k9s` or `lazygit` — a full-screen interactive controller with inspect, filter, and command capabilities.

## Design Principles

1. **TUI is a View, not a Runtime** — arlod owns all state; the TUI is a read-only view + control surface over gRPC.
2. **Event Stream is SSOT for real-time updates** — `SubscribeEvents` drives the Timeline.
3. **Polling for eventual consistency** — `GetWorkflow` polled every 2s to reconcile state.
4. **Completion doesn't exit** — users stay in the TUI to inspect results, artifacts, and logs after the workflow finishes.
5. **Command bar, not key soup** — `:command` pattern like vim/lazygit for actions beyond navigation.

## Layout

```
┌──────────────────────────────────────────────────────────────────┐
│ Arlo wf-123  RUNNING  02:14  ██████░░ 66%  3 nodes  1 retry  1 gate │  ← Overview Bar
├──────────────────────────┬───────────────────────────────────────┤
│ WORKFLOW                 │ TIMELINE / INSPECTOR (toggle)         │  ← Panels (50/50 split)
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
| **Overview Bar** | Workflow ID + status badge + elapsed time + progress bar + node/retry/gate/worker counts | `GetWorkflow` (2s poll) |
| **Workflow (left)** | Tree of nodes with status icons, session IDs, retry counts, dependency arrows, gate markers | `GetWorkflow` (2s poll) |
| **Timeline/Inspector (right)** | Real-time event stream with filter support; toggles to Node Inspector on `Enter` | `SubscribeEvents` (real-time stream) + `GetWorkflow` |

### Command Bar

`:q` / `:quit` — exit
`:a <node>` / `:attach <node>` — attach PTY session
`:r <node>` / `:retry <node>` — retry failed node (future)
`:k <node>` / `:kill <node>` — kill running node (future)
`:f` / `:filter` — toggle event filter
`:logs <node>` — view node logs (future)
`:help` — show help

Also supports `Ctrl+C` and `Esc` for quick quit.

## Interaction Model

| Key | Action |
|-----|--------|
| `q` / `Esc` / `Ctrl+C` | Quit TUI (stays open after workflow completes) |
| `tab` | Switch focus between Workflow panel and Timeline/Inspector |
| `↑` `↓` | Navigate node list (left) or scroll events (right) |
| `PgUp` `PgDn` | Page scroll in Timeline |
| `Enter` | On focused node → right panel switches to **Node Inspector** |
| `Esc` | From Inspector → back to Timeline |
| `f` | Open event type filter overlay |
| `:` | Enter command mode |
| `←` `→` | Collapse/expand workflow tree nodes |

## Node Inspector (right panel toggle)

When `Enter` is pressed on a node, the right panel shows:

```
┌── Node Inspector ──────────────────┐
│                                    │
│  Node        implement             │
│  Status      RUNNING               │
│  Session     sess-abc123           │
│  Started     15:27:42              │
│  Duration    3m12s                 │
│  Retry       2                     │
│  Depends     analyze               │
│  Worker      runtime-3             │
│  Skill       coder                 │
│  Gate        —                     │
│                                    │
│  [Attach Session]  (press a)       │
│  [Retry Node]      (press r)       │
│  [View Logs]       (press l)       │
│                                    │
└────────────────────────────────────┘
```

## Event Timeline Filter

```
┌── Filter ──────────────────────────┐
│                                    │
│  [x] workflow events               │
│  [x] node events                   │
│  [ ] tool calls                    │
│  [x] errors                        │
│  [ ] token stream                  │
│                                    │
│  Press Enter to apply              │
│                                    │
└────────────────────────────────────┘
```

## Data Model Changes

### Proto: `NodeState` — add `depends_on`

```proto
message NodeState {
  string node_id = 1;
  string status = 2;
  string session_id = 3;
  string runtime_id = 4;
  int32 retry_count = 5;
  repeated string depends_on = 6;  // NEW: node IDs this node depends on
  string gate = 7;                  // NEW: human gate condition (e.g. "human_approval")
}
```

### Domain: `NodeState` — add `DependsOn` and `Gate`

Add `DependsOn []string` and `Gate string` to `domain.NodeState` (internal/domain/node.go).

### Projection: Populate dependencies

During `NODE_CREATED` event handling in the WorkflowProjection, read the node's `DependsOn` from the graph definition (or embed it in the event payload) and store it in the projection.

### Event payload: `NodeCreated` — include `DependsOn` and `Gate`

```go
type NodeCreated struct {
    NodeID     string   `json:"node_id"`
    WorkflowID string   `json:"workflow_id"`
    SkillName  string   `json:"skill_name"`
    Runtime    string   `json:"runtime"`
    DependsOn  []string `json:"depends_on,omitempty"`  // NEW
    Gate       string   `json:"gate,omitempty"`         // NEW
}
```

## Implementation Scope

### Phase 1: Proto + Domain Changes (backend prep)
1. Add `depends_on` + `gate` to `NodeState` proto
2. Add `DependsOn` + `Gate` to `domain.NodeState`
3. Add `DependsOn` + `Gate` to `domain.NodeCreated` event payload
4. Populate in `CreateTask` service handler
5. Populate in WorkflowProjection
6. Pass through in `GetWorkflow` service handler
7. Regenerate protobuf

### Phase 2: TUI Rewrite
1. Rewrite `internal/tui/tui.go` — new Model with three-panel layout
2. Split into files: `tui.go` (model+update), `workflow.go` (left panel), `timeline.go` (right panel), `inspector.go`, `command.go`, `styles.go`
3. gRPC integration: `CreateTask` → `SubscribeEvents` + `GetWorkflow` poll loop
4. Merge `arlo run` and `arlo tui` into single TUI entry flow

### Phase 3: Interactive Features
1. Command bar with `:attach`, `:quit`, `:help`
2. Node Inspector (Enter/Esc toggle)
3. Event filter overlay
4. Workflow tree collapse/expand
5. Progress bar in overview

### Out of Scope (v1+)
- Execution-level events (TOOL_CALL, MODEL_CALL, TOKEN_STREAM) — needs runtime instrumentation
- `:retry`, `:kill`, `:logs`, `:pause`, `:continue` — needs command RPCs
- Soft/human dependency types in proto
- PTY inline rendering in TUI

## Files to Change

| File | Change |
|------|--------|
| `api/proto/arlo/v1/service.proto` | Add `depends_on`, `gate` to `NodeState` |
| `internal/domain/node.go` | Add `DependsOn`, `Gate` to `NodeState` and `NodeCreated` |
| `internal/domain/workflow.go` | Verify `Node` struct has required fields |
| `internal/state/workflow_projection.go` | Populate `DependsOn` + `Gate` during projection |
| `internal/service/arlo_service.go` | Pass `DependsOn` + `Gate` in `CreateTask` seed + `GetWorkflow` response |
| `cmd/arlo/main.go` | Merge `run` + `tui` commands |
| `internal/tui/tui.go` | Rewrite — new Model, three-panel layout, gRPC integration |
| `internal/tui/workflow.go` | NEW — Workflow tree panel |
| `internal/tui/timeline.go` | NEW — Event Timeline panel |
| `internal/tui/inspector.go` | NEW — Node Inspector panel |
| `internal/tui/command.go` | NEW — Command bar + mode |
| `internal/tui/styles.go` | NEW — Lipgloss styles |

## Testing

- **Unit tests** for each panel's render output (snapshot or golden file)
- **Integration test** with a mock gRPC server streaming events
- **Manual test** against running arlod with `graphs/bugfix.yaml`

## References

- `docs/architecture.md` — 12-layer North Star design
- `CLAUDE.md` — Bubble Tea conventions, gRPC conventions
- `api/proto/arlo/v1/service.proto` — current gRPC contract
