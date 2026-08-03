# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Identity

**Arlo** is an AgentOS — a system conceptually similar to Kubernetes + Temporal + HumanLayer + Claude Code Runtime, written in Go.

| Concept | Role in Arlo |
|---------|-------------|
| `arlod` | The daemon: a long-lived gRPC server. The AgentOS runtime. |
| `arlo`  | The CLI/TUI client: a Bubble Tea terminal app that talks to `arlod`. |
| Event Sourcing | The persistence model. All state changes are events. The event log is the source of truth. |
| gRPC | Communication protocol between `arlo` ↔ `arlod` (and future agents). |

## Essential Commands

```bash
# Build everything
make build

# Run tests with race detection
make test

# Run linters
make lint

# Generate protobuf from .proto files (requires buf CLI)
make proto

# Run daemon in development
make run-daemon

# Run CLI in development
make run-cli
```

If you need to run a single test or package:

```bash
go test -race -run TestName ./internal/eventsourcing/...
go test -race ./internal/runtime/...
```

## Architecture

### Process Model

```
┌──────────────┐     gRPC      ┌──────────────────────────────┐
│   arlo CLI   │◄────────────►│         arlod daemon          │
│ (Bubble Tea) │               │                              │
└──────────────┘               │  ┌────────────────────────┐  │
                               │  │   gRPC Service Layer   │  │
                               │  │   (internal/service)   │  │
                               │  └───────────┬────────────┘  │
                               │              │               │
                               │  ┌───────────▼────────────┐  │
                               │  │   Agent Runtime         │  │
                               │  │   (internal/runtime)    │  │
                               │  └───────────┬────────────┘  │
                               │              │               │
                               │  ┌───────────▼────────────┐  │
                               │  │   Event Sourcing Core   │  │
                               │  │   (internal/eventsourcing)│  │
                               │  └───────────┬────────────┘  │
                               │              │               │
                               │  ┌───────────▼────────────┐  │
                               │  │   Event Store           │  │
                               │  │   (internal/store)      │  │
                               │  └────────────────────────┘  │
                               └──────────────────────────────┘
```

### Package Responsibilities

| Package | Responsibility | Has Tests? |
|---------|---------------|------------|
| `cmd/arlod/` | Daemon binary entrypoint. Wires dependencies, starts gRPC server. | No (thin main) |
| `cmd/arlo/` | CLI binary entrypoint. Starts Bubble Tea app, connects to daemon. | No (thin main) |
| `api/proto/` | Protobuf service and message definitions. Single source of truth for the gRPC contract. | N/A |
| `internal/eventsourcing/` | **Core domain.** Command, Event, Aggregate, Projection base types. The event sourcing engine. | **Yes** |
| `internal/runtime/` | Agent lifecycle: scheduling, execution, state machines. Depends on `eventsourcing`. | **Yes** |
| `internal/store/` | Event store implementation. Append-only log, snapshots, replay. | **Yes** |
| `internal/service/` | gRPC service handlers. Thin layer that delegates to `runtime` and `eventsourcing`. | **Yes** |
| `pkg/` | Packages safe to import externally. Avoid unless the abstraction is truly stable. | **Yes** |

### Dependency Direction

```
cmd → internal/service → internal/runtime → internal/eventsourcing → internal/store
```

Each layer depends only on the layer directly beneath it. Never invert this. If `runtime` needs something from `service`, extract it into a shared interface in `eventsourcing` or a new `internal/` package.

## Event Sourcing Conventions

All domain state changes flow through this pipeline:

```
Command → Validate → Apply to Aggregate → Produce Events → Append to Store → Update Projections
```

### Naming

- **Commands**: imperative verb, e.g. `StartAgent`, `CancelRun`, `AssignWorkflow`
- **Events**: past-tense verb, e.g. `AgentStarted`, `RunCancelled`, `WorkflowAssigned`
- **Aggregates**: the noun, e.g. `Agent`, `Run`, `Workflow`
- **Projections**: the noun + `Projection`, e.g. `AgentStateProjection`, `ActiveRunProjection`

### Aggregate Pattern

```go
// An aggregate is reconstructed by replaying its event stream.
type Agent struct {
    ID     string
    Status AgentStatus
    // ... mutable state
}

// Apply applies a single event, mutating the aggregate.
// Never contains side effects — it's pure state transition.
func (a *Agent) Apply(event Event) error { ... }

// Handle validates a command and returns new events.
// This is the only place where business logic decides what happens.
func (a *Agent) Handle(cmd Command) ([]Event, error) { ... }
```

### Event Store

- Events are **immutable** once written. Never update or delete events.
- The event store is **append-only**.
- Use **snapshots** for read performance — but the event log remains authoritative.
- Each event carries: `AggregateID`, `SequenceNumber`, `Timestamp`, `Type`, `Payload`.

## gRPC Conventions

- Protobuf definitions live in `api/proto/`.
- Service names are nouns: `AgentService`, `RunService`, `WorkflowService`.
- RPC names use Standard Methods where possible: `CreateAgent`, `GetAgent`, `ListAgents`, `CancelRun`.
- Always define request/response messages as top-level types, never inline.
- Use `buf` for proto generation (see `make proto`).
- Generated code goes into `api/gen/` (gitignored; generated at build time).

## Bubble Tea Conventions (CLI)

The TUI client in `cmd/arlo/` uses the [Bubble Tea](https://github.com/charmbracelet/bubbletea) Elm-like architecture:

```go
type Model struct { ... }           // Application state
func (m Model) Init() Cmd           // Initial command
func (m Model) Update(msg Msg) (Model, Cmd)  // Message handler
func (m Model) View() string        // Render
```

Keep the Bubble Tea model in `cmd/arlo/` itself. Extract reusable UI components into a `internal/tui/` package if they grow complex. gRPC calls are made from `Update` via Bubble Tea `Cmd` (never block the render loop).

## Code Conventions

### Go Idioms

- **Error handling**: always wrap errors with context: `fmt.Errorf("doing X: %w", err)`. Use `errors.Is` / `errors.As` for inspection.
- **Interfaces**: define interfaces where they are consumed, not where they are implemented. Prefer small, focused interfaces (1-3 methods).
- **Concurrency**: prefer `context.Context` propagation over channel-based cancellation. Use `errgroup` for grouped goroutines.
- **Testing**: table-driven tests. Test event sourcing with: given a history of events, when I handle this command, then I expect these events. Use `testing.T` helpers from the standard library; add `testify` only if the team agrees.

### Linter Philosophy

`.golangci.yml` is configured for this architecture:
- Complexity checks are slightly relaxed (`gocyclo: 20`, `gocognit: 25`) because event sourcing handlers can be long.
- `exhaustruct` is disabled for now — enable it once domain types stabilize.
- `funlen` is disabled — event sourcing `Handle` methods are naturally longer.

### What NOT to Do

- **Don't call external services from aggregates.** Aggregates are pure state machines. Side effects belong in command handlers or sagas.
- **Don't mutate events.** Events are immutable. If you need different data, create a new event type.
- **Don't block the Bubble Tea render loop.** All I/O happens via Tea commands.
- **Don't import `internal/` packages from `cmd/` directly.** Wire dependencies in `cmd/` and pass interfaces.
