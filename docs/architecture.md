# Arlo Architecture

> **Arlo — Agent Runtime & Loop Orchestration System**
>
> An operating system for autonomous coding agents.
>
> **Status:** North Star (v1.0 target). v0.1 scope is [defined below](#v01-mvp-scope).

---

## Table of Contents

1. [Project Identity](#project-identity)
2. [Core Principles](#core-principles)
3. [System Topology](#system-topology)
4. [Layer Architecture](#layer-architecture)
   - [Layer 1: Workflow + DAG Engine](#layer-1-workflow--dag-engine)
   - [Layer 2: gRPC API](#layer-2-grpc-api)
   - [Layer 3: Runtime Adapter](#layer-3-runtime-adapter)
   - [Layer 4: Workspace Provider](#layer-4-workspace-provider)
   - [Layer 5: Scheduler + Reconciler](#layer-5-scheduler--reconciler)
   - [Layer 6: Skill Engine](#layer-6-skill-engine)
   - [Layer 7: Artifact Store + Context Management](#layer-7-artifact-store--context-management)
   - [Layer 8: Memory + Learning + Replay](#layer-8-memory--learning--replay)
   - [Layer 9: Event Store + State Store + Reconciliation Loop](#layer-9-event-store--state-store--reconciliation-loop)
   - [Layer 10: Multi-Agent Worker Architecture](#layer-10-multi-agent-worker-architecture)
   - [Layer 11: Security / Permission / Sandbox](#layer-11-security--permission--sandbox)
   - [Layer 12: Plugin / Extension System](#layer-12-plugin--extension-system)
5. [v0.1 MVP Scope](#v01-mvp-scope)
6. [v0.1 Implementation Path](#v01-implementation-path)
7. [Non-Goals](#non-goals)
8. [Design Decisions Log](#design-decisions-log)

---

## Project Identity

| Concept | Role in Arlo |
|---------|-------------|
| `arlod` | The daemon: AgentOS Control Plane. Manages workflows, schedules agents, persists events. |
| `arlo` | The CLI client. Sends commands to arlod over Unix socket. |
| `arlo-worker` | Remote execution node (v1+). Runs agent processes on other machines. |
| Event Sourcing | The persistence model. All state changes are events. The event log is the source of truth. |
| gRPC | Communication protocol between `arlo` ↔ `arlod` (and future workers). |
| Reconciler | The control loop. Reads desired state vs actual state, drives the system toward desired. |
| Tmux | The v0.1 workspace provider. Terminal multiplexer for multi-agent window management. |

**Arlo is not a Claude Code wrapper.** It is a platform that manages agent lifecycles, workflows, contexts, artifacts, and learning — with Claude Code as one of many possible runtimes.

---

## Core Principles

These invariants hold across all layers:

1. **Dual Channel** — Control Plane (Events) and Human Plane (PTY) are strictly separated. Control state never derives from stdout parsing. PTY output is for human consumption only.

2. **Event Sourcing** — Event Store is the single source of truth. All state changes flow through: `Command → Validate → Produce Events → Append to Store → Update Projections`. Events are immutable and append-only.

3. **Reconciliation Loop** — The system does not trust individual events. It periodically reads current state vs desired state, computes the delta, and acts. This is what makes crash recovery and retry correct by construction.

4. **Interface Isolation** — Every component depends on the smallest interface it needs. Runtime doesn't know about Workspace. Workspace doesn't know about Agent logic. The Scheduler doesn't call LLMs directly.

5. **Capability Gate** — All dangerous operations (filesystem writes, network calls, tool execution) flow through a Policy Enforcement Point. Nothing bypasses it.

6. **v0.1 Interfaces, Trivial Implementations** — Every interface is real and designed for the long term. v0.1 implementations are the simplest thing that works. The gap is intentional and documented.

---

## System Topology

### Full Architecture (v1.0 North Star)

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              User / Developer                               │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                         ┌─────────┴─────────┐
                         │    arlo CLI       │
                         │    (control       │
                         │     client)       │
                         └─────────┬─────────┘
                                   │
                          gRPC over Unix Socket
                          (~/.arlo/arlo.sock)
                                   │
┌──────────────────────────────────┴──────────────────────────────────────────┐
│                              arlod — AgentOS Control Plane                   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        API Layer (gRPC)                              │   │
│  │  Task │ Workflow │ Runtime │ Session │ Event Stream │ PTY │ Command  │   │
│  └──────────────────────────────────┬──────────────────────────────────┘   │
│                                     │                                       │
│  ┌──────────────────────────────────┴──────────────────────────────────┐   │
│  │                      Orchestration Layer                             │   │
│  │                                                                      │   │
│  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐     │   │
│  │  │  Workflow    │   │  Reconciler  │   │  Placement Engine    │     │   │
│  │  │  Engine      │   │  (control    │   │  (which worker)      │     │   │
│  │  │  DAG+Skills  │   │   loop)      │   │                      │     │   │
│  │  └──────┬───────┘   └──────┬───────┘   └──────────┬───────────┘     │   │
│  │         │                  │                       │                  │   │
│  │         └──────────────────┼───────────────────────┘                  │   │
│  │                            │                                          │   │
│  └────────────────────────────┼──────────────────────────────────────────┘   │
│                               │                                               │
│  ┌────────────────────────────┼──────────────────────────────────────────┐   │
│  │                      Execution Layer                                   │   │
│  │                            │                                           │   │
│  │  ┌──────────────┐  ┌──────┴───────┐  ┌──────────────────────┐        │   │
│  │  │  Runtime     │  │  Workspace   │  │  Context Builder     │        │   │
│  │  │  Manager     │  │  Manager     │  │  (what agent sees)   │        │   │
│  │  │  (who runs)  │  │  (where)     │  │                      │        │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘        │   │
│  │         │                 │                      │                     │   │
│  └─────────┼─────────────────┼──────────────────────┼─────────────────────┘   │
│            │                 │                      │                          │
│  ┌─────────┼─────────────────┼──────────────────────┼─────────────────────┐   │
│  │         ▼                 ▼                      ▼                      │   │
│  │                      Security Layer                                      │   │
│  │  Identity │ Permission (PEP) │ Sandbox │ Secret Injection │ Audit       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                     │                                       │
│  ┌──────────────────────────────────┴──────────────────────────────────┐   │
│  │                       Persistence Layer                              │   │
│  │                                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │   │
│  │  │  Event Store │  │  State Store │  │  Artifact    │              │   │
│  │  │  (SSOT,      │  │  (projection,│  │  Store       │              │   │
│  │  │   append-only│  │   read-only) │  │  (content)   │              │   │
│  │  │   SQLite)    │  │              │  │              │              │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │   │
│  │         │                 │                  │                       │   │
│  │  ┌──────┴─────────────────┴──────────────────┴───────┐              │   │
│  │  │              Trace Store + Memory System           │              │   │
│  │  │  Episodic │ Semantic │ Procedural │ Pattern Lib   │              │   │
│  │  └──────────────────────────────────────────────────┘              │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                     │                                       │
│  ┌──────────────────────────────────┴──────────────────────────────────┐   │
│  │                       Extension Layer                                │   │
│  │  Runtime Adapter │ Workspace Provider │ Skill │ Policy │ Tool │ Hook │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
               ┌────┴────┐   ┌────┴────┐   ┌─────┴────┐
               │Worker-1 │   │Worker-2 │   │Worker-N  │
               │(local)  │   │(remote) │   │(remote)  │
               │         │   │         │   │          │
               │ Claude  │   │ Claude  │   │ Codex    │
               │ tmux    │   │ Docker  │   │ GPU      │
               └─────────┘   └─────────┘   └──────────┘
```

### v0.1 Topology

```text
                         User Terminal
                              │
                      ┌───────┴───────┐
                      │   arlo CLI    │
                      │ (run, status, │
                      │  attach)      │
                      └───────┬───────┘
                              │
                       Unix Socket
                       gRPC
                              │
                      ┌───────┴──────────────────────────┐
                      │              arlod                │
                      │                                  │
                      │  ┌──────────────────────────┐   │
                      │  │     gRPC Service Layer    │   │
                      │  └────────────┬─────────────┘   │
                      │               │                  │
                      │  ┌────────────▼─────────────┐   │
                      │  │       Reconciler          │   │
                      │  │     (simple tick loop)    │   │
                      │  └────────────┬─────────────┘   │
                      │               │                  │
                      │  ┌────────────▼─────────────┐   │
                      │  │     Workflow Engine       │   │
                      │  │    (static DAG + YAML)    │   │
                      │  └────────────┬─────────────┘   │
                      │               │                  │
                      │  ┌────────────┼─────────────┐   │
                      │  │            │              │   │
                      │  │  ┌─────────▼───────┐     │   │
                      │  │  │ Runtime Manager │     │   │
                      │  │  │ (Claude only)   │     │   │
                      │  │  └─────────┬───────┘     │   │
                      │  │            │              │   │
                      │  │  ┌─────────▼───────┐     │   │
                      │  │  │Workspace Manager│     │   │
                      │  │  │ (tmux+worktree) │     │   │
                      │  │  └─────────┬───────┘     │   │
                      │  └────────────┼─────────────┘   │
                      │               │                  │
                      │  ┌────────────▼─────────────┐   │
                      │  │   Event Store (SQLite)    │   │
                      │  │   State Projection        │   │
                      │  └──────────────────────────┘   │
                      └──────────────────────────────────┘
                              │
                      ┌───────┴───────┐
                      │  Tmux Session │
                      │ "arlo-<task>" │
                      │               │
                      │ Win1: Planner │
                      │ Win2: Coder   │
                      │ Win3: Reviewer│
                      └───────┬───────┘
                              │
                      ┌───────┴───────┐
                      │  Claude Code  │
                      │  (headless)   │
                      └───────────────┘
```

---

## Layer Architecture

### Layer 1: Workflow + DAG Engine

**Question:** What should happen, in what order?

**Responsibility:** Define, compile, validate, and evaluate workflows as static DAGs with conditional transitions.

**Key Interfaces:**

```go
type WorkflowEngine interface {
    Compile(ctx context.Context, source []byte) (*ExecutableGraph, error)
    Validate(ctx context.Context, graph *ExecutableGraph) error
    Instantiate(ctx context.Context, taskID string, graph *ExecutableGraph) (*WorkflowInstance, error)
    Evaluate(ctx context.Context, workflowID string, state WorkflowState) ([]Decision, error)
}
```

**Key Types:**

```go
type ExecutableNode struct {
    ID         string
    SkillRef   SkillRef       // reference to skill registry, not embedded
    Runtime    RuntimeRef     // which runtime executes this node
    DependsOn  []string
    Gate       ApprovalGate   // "none", "human_approval"
    Retry      RetryPolicy
    Transition []Transition   // conditional edges: from review→iterate when verdict != APPROVED
}

type Transition struct {
    From string
    To   string
    When string              // expression: "verdict == CHANGES_REQUESTED"
}

type Decision struct {
    Action string            // "START_NODE", "STOP_NODE", "COMPLETE_WORKFLOW"
    NodeID string
    Reason string
}
```

**Key Constraints:**
- DAG is static at compile time. Dynamic mutation is a deferred advanced feature.
- Conditional loops (review → iterate → review) are modeled as Transitions, not mutation.
- Skills are platform-level assets referenced by SkillRef, not embedded in the graph.

**Workflow YAML (v0.1 format):**

```yaml
name: bugfix
version: 1

nodes:
  - id: analyze
    skill: root-cause
    runtime:
      provider: claude-code
    retry:
      max_retries: 1

  - id: implement
    skill: implement-fix
    runtime:
      provider: claude-code
    depends_on:
      - analyze
    retry:
      max_retries: 2

  - id: review
    skill: code-review
    runtime:
      provider: claude-code
    depends_on:
      - implement
    gate: human_approval

policy:
  max_concurrent_nodes: 1
```

---

### Layer 2: gRPC API

**Question:** How does the CLI talk to the daemon?

**Responsibility:** Define the control plane protocol between arlo CLI and arlod.

**Service Definition:**

```protobuf
service ArloService {
  // Task
  rpc CreateTask(CreateTaskRequest) returns (CreateTaskResponse);
  rpc GetTask(GetTaskRequest) returns (GetTaskResponse);
  rpc ListTasks(ListTasksRequest) returns (ListTasksResponse);
  rpc CancelTask(CancelTaskRequest) returns (CancelTaskResponse);

  // Workflow
  rpc GetWorkflowSnapshot(GetWorkflowSnapshotRequest) returns (WorkflowSnapshot);

  // Runtime
  rpc GetRuntime(GetRuntimeRequest) returns (GetRuntimeResponse);

  // Session
  rpc GetSession(GetSessionRequest) returns (GetSessionResponse);
  rpc ListSessions(ListSessionsRequest) returns (ListSessionsResponse);

  // Event Stream (TUI heartbeat)
  rpc SubscribeEvents(SubscribeEventsRequest) returns (stream Event);

  // PTY (Human Interaction Plane)
  rpc AttachPTY(AttachPTYRequest) returns (stream PTYFrame);
  rpc SendPTYInput(SendPTYInputRequest) returns (SendPTYInputResponse);

  // Command
  rpc ExecuteCommand(CommandRequest) returns (CommandResponse);
}
```

**Key Constraints:**
- Transport: gRPC over Unix socket (`~/.arlo/arlo.sock`) in v0.1.
- Future: same proto, swap transport to TCP for remote access.
- Event stream is the TUI's sole state source — no polling.

---

### Layer 3: Runtime Adapter

**Question:** Who executes the agent?

**Responsibility:** Manage agent process lifecycle. Adapters translate between arlod's runtime model and specific agent CLIs (Claude Code, Codex, etc.).

**Key Interfaces:**

```go
type RuntimeAdapter interface {
    Prepare(ctx context.Context, inst RuntimeInstance) error
    Start(ctx context.Context, inst RuntimeInstance) error
    Stop(ctx context.Context, id string) error
    Destroy(ctx context.Context, id string) error
    SendInstruction(ctx context.Context, id string, instruction Instruction) error
    Status(ctx context.Context, id string) RuntimeStatus
    Events() <-chan RuntimeEvent   // buffered, never blocks Runtime
}

type InteractiveRuntime interface {
    Attach(ctx context.Context, id string) (<-chan PTYFrame, io.Writer, error)
}
```

**Key Types:**

```go
type RuntimeInstance struct {
    ID          string
    Type        string           // "claude-code"
    Config      RuntimeConfig
    WorkspaceID string
    SlotID      string
    SessionID   string
    WorkDir     string
    Prompt      string
}

type RuntimeConfig struct {
    Model          string
    Capabilities   []string     // ["filesystem", "git"]
    PermissionMode string       // "auto", "manual"
}

type RuntimeStatus struct {
    ID      string
    State   RuntimeState
    Metrics RuntimeMetrics      // CPU, memory, tokens, duration — no PID
}
```

**Key Constraints:**
- PID is never exposed in the abstraction. It's an implementation detail.
- `Events()` channel is buffered — slow consumers don't block agent execution.
- `InteractiveRuntime` is a separate interface. Not all runtimes have PTYs.
- Prepare → Start → Stop → Destroy lifecycle, similar to Kubernetes Pod phases.

**v0.1 Implementation:** `ClaudeCodeAdapter` only. Spawns `claude --headless` via `creack/pty`.

---

### Layer 4: Workspace Provider

**Question:** Where does the agent run?

**Responsibility:** Create and manage execution environments. Tmux is the first provider; Docker and Kubernetes are future providers.

**Key Interfaces:**

```go
type WorkspaceProvider interface {
    Create(ctx context.Context, spec WorkspaceSpec) (*Workspace, error)
    Destroy(ctx context.Context, wsID string) error
    CreateSlot(ctx context.Context, wsID string, spec SlotSpec) (*ExecutionSlot, error)
    DeleteSlot(ctx context.Context, slotID string) error
    BindRuntime(ctx context.Context, slotID string, runtimeID string) error
    Attach(ctx context.Context, slotID string) (<-chan PTYFrame, io.Writer, error)
    Status(ctx context.Context, wsID string) (WorkspaceStatus, error)
}
```

**Key Types:**

```go
type ExecutionSlot struct {
    ID        string
    Name      string
    RuntimeID string    // "" if not bound
}

// Provider mapping:
// TmuxProvider:  Workspace = tmux session, ExecutionSlot = tmux window
// DockerProvider: Workspace = container,   ExecutionSlot = exec session
// K8sProvider:    Workspace = namespace,   ExecutionSlot = pod
```

**Key Constraints:**
- Not "Window" — "ExecutionSlot". Provider-agnostic terminology.
- `BindRuntime` only records the association. RuntimeManager.Start() actually launches the process.
- Workspace is a logical resource. It's scheduled onto a physical Worker (v1+).

---

### Layer 5: Scheduler + Reconciler

**Question:** When does each node run?

**Responsibility:** Drive the system from current state toward desired state. This is the heart of arlod — the control loop that makes the system self-healing.

**Key Interfaces:**

```go
type Reconciler interface {
    Start(ctx context.Context) error
    Reconcile(ctx context.Context, workflowID string) error
}
```

**Core Algorithm:**

```go
func (r *reconciler) Reconcile(ctx context.Context, workflowID string) error {
    // 1. READ: get current state from StateStore (projection of events)
    state, _ := r.stateStore.GetWorkflow(ctx, workflowID)

    // 2. COMPUTE: what should happen?
    decisions, _ := r.workflowEng.Evaluate(ctx, workflowID, *state)

    // 3. ACT: execute decisions (idempotently)
    for _, d := range decisions {
        switch d.Action {
        case "START_NODE":
            r.executeStartNode(ctx, workflowID, d.NodeID)
        case "COMPLETE_WORKFLOW":
            r.executeCompleteWorkflow(ctx, workflowID)
        }
    }
    return nil
}
```

**Node State Machine:**

```text
PENDING ──► READY ──► STARTING ──► RUNNING ──► COMPLETED
                        │              │
                        │              ├──► WAITING (human gate)
                        │              │        │
                        │              │        ▼
                        │              │    RUNNING (resumed)
                        │              │
                        │              └──► STOPPING ──► FAILED
                        │                               │
                        └───────────────────────────────┘
                                CANCELLED
```

**Key Constraints:**
- Reconciler does NOT consume events directly. It reads StateStore, which is a projection of events.
- Reconciliation is idempotent. Reconciling the same workflow 100 times produces no duplicate actions.
- Crash recovery: on startup, rebuild all projections from Event Store, then reconcile all active workflows.

**v0.1 Implementation:** Simple tick loop (5s interval) + event-triggered wakeup. No Intent/Controller split. Single local worker only.

---

### Layer 6: Skill Engine

**Question:** How does the agent know what to do?

**Responsibility:** Define and resolve agent capabilities as versioned, platform-level assets.

**Key Interfaces:**

```go
type SkillRegistry interface {
    Resolve(ref SkillRef) (*Skill, error)
    List() ([]Skill, error)
    Register(skill Skill) error
}

type SkillEngine interface {
    Expand(skill *Skill, ctx SkillContext) (string, error)
}
```

**Key Types:**

```go
type SkillRef struct {
    Name    string
    Version string
}

type Skill struct {
    Name        string
    Version     string
    Description string
    Prompt      string
    Output      []string        // expected output patterns
    ContextPolicy ContextPolicy // what context this skill needs
}
```

**Key Constraints:**
- Skills are platform assets, not embedded in workflows.
- SkillRef provides indirection: workflows reference skills, skills are versioned independently.
- Battle mode (multi-agent debate) is a Skill Engine pattern, not a DAG concern.

**v0.1 Implementation:** Builtin skills embedded in workflow YAML. No separate skill registry yet.

---

### Layer 7: Artifact Store + Context Management

**Question:** What does the next agent see?

**Responsibility:** Store agent outputs as versioned artifacts. Build optimized context windows for each node, respecting token budgets.

**Key Interfaces:**

```go
type ArtifactStore interface {
    Save(ctx context.Context, artifact Artifact, content io.Reader) error
    Get(ctx context.Context, id string) (*Artifact, io.ReadCloser, error)
    ListByNode(ctx context.Context, nodeID string) ([]Artifact, error)
    Version(ctx context.Context, id string, content io.Reader) (*Artifact, error)
}

type ContextBuilder interface {
    Build(ctx context.Context, spec ContextSpec) (*Context, error)
}

type ContextOptimizer interface {
    Fit(ctx context.Context, context *Context, budget int) (*AssembledPrompt, error)
}
```

**Key Types:**

```go
type Artifact struct {
    ID          string
    Name        string            // "plan.md"
    Type        string            // "markdown", "diff", "log"
    NodeID      string
    ParentID    string            // lineage support
    Version     int
    ContentHash string
    Metadata    map[string]string // semantic tags for retrieval
}

type ContextSpec struct {
    NodeID    string
    DependsOn []string
    Policy    ContextPolicy
    MaxTokens int
}
```

**Three-Tier Degradation:**
1. **Keep** — P0 (system prompt, direct upstream output)
2. **Compress** — P1-P2 (summarize large files)
3. **Reference** — P3 (mention artifact ID, don't include content)

**Key Constraints:**
- Context never includes secret values. They're injected via filesystem, not prompt.
- Artifact Store and Event Store are separate. Events record "what happened"; artifacts store "what was produced."
- ContextBuilder checks read permissions before including files (security boundary).

**v0.1 Implementation:** Local filesystem ArtifactStore. Basic ContextBuilder that includes upstream artifacts only. No compression — if it fits, it's included.

---

### Layer 8: Memory + Learning + Replay

**Question:** How does the system get better over time?

**Responsibility:** Record execution traces, extract patterns, evolve skills. The system learns from every session.

**Three Memory Tiers:**

| Tier | What | Example |
|------|------|---------|
| **Episodic** | What happened | Trace: OAuth bugfix session #42 failed because missing race test |
| **Semantic** | What we learned | Pattern: OAuth token refresh needs concurrency tests |
| **Procedural** | What we changed | Skill update: golang-oauth-review now checks mutex usage |

**Key Interfaces:**

```go
type TraceStore interface {
    Save(ctx context.Context, trace *Trace) error
    GetBySession(ctx context.Context, sessionID string) (*Trace, error)
}

type SessionAnalyzer interface {
    Analyze(ctx context.Context, trace *Trace) (*SessionAnalysis, error)
}

type PatternLibrary interface {
    Ingest(ctx context.Context, analysis *SessionAnalysis) error
    AntiPatterns(ctx context.Context, domain string) ([]AntiPattern, error)
    BestPractices(ctx context.Context, taskType string) ([]BestPractice, error)
}

type SkillEvolver interface {
    ProposeChanges(ctx context.Context, skill SkillRef) ([]SkillChange, error)
    ValidateChange(ctx context.Context, change SkillChange, traces []*Trace) (*ValidationResult, error)
}

type ReplayEngine interface {
    Replay(ctx context.Context, spec ReplaySpec) (*ReplayResult, error)
}
```

**Replay Three Levels:**
1. **Context Replay** (zero cost) — recompute what context WOULD have been assembled
2. **Model Replay** — re-run with different model/temperature, same context
3. **Full Replay** — re-execute everything including tool calls

**Key Constraints:**
- Skill changes are always human-gated. The system proposes, humans approve.
- Pattern Library is queried during context building — past lessons influence current sessions.
- Trace is per execution attempt, not per session (retries produce separate traces).

**v0.1 Status:** Deferred entirely. Interfaces defined but not implemented. Trace collection begins as raw event log queries.

---

### Layer 9: Event Store + State Store + Reconciliation Loop

**Question:** What is the system's source of truth?

**Responsibility:** Persist all state changes as an immutable event log. Build query-optimized projections from that log.

**Three-Component Model:**

```text
Event Store (SSOT)          State Store (Projection)      Reconciler (Control Loop)
─────────────────          ─────────────────────────      ────────────────────────
Immutable facts             Materialized views            Control loop
"coder started"             "coder is running"            "coder should be running
"coder failed"              "coder is failed"              → start it"
Append only                 Rebuilt from events           Reads desired vs actual
Never delete                Query optimized               Acts on delta
```

**Key Interfaces:**

```go
type EventStore interface {
    Append(ctx context.Context, streamID string, events []Event) error
    Read(ctx context.Context, streamID string, fromVersion int) ([]Event, error)
    Subscribe(ctx context.Context, fromPosition int64) (<-chan Event, error)
}

type StateStore interface {
    GetWorkflow(ctx context.Context, workflowID string) (*WorkflowState, error)
    ListActiveWorkflows(ctx context.Context) ([]WorkflowState, error)
    GetNodeState(ctx context.Context, nodeID string) (*NodeState, error)
}
```

**Event Schema:**

```go
type Event struct {
    ID        string
    StreamID  string          // aggregate ID
    Version   int             // monotonic within stream
    Position  int64           // global position
    Type      EventType
    Payload   json.RawMessage
    Timestamp time.Time
}

// Aggregate streams:
//   workflow-{id}  — Workflow-level events (WORKFLOW_CREATED, NODE_CREATED, ...)
//   node-{id}      — Node-level events (NODE_STARTED, NODE_COMPLETED, ...)
//   runtime-{id}   — Runtime-level events (RUNTIME_STARTED, RUNTIME_EXITED, ...)
//   workspace-{id} — Workspace-level events (WORKSPACE_CREATED, SLOT_CREATED, ...)
```

**Key Constraints:**
- Event Store is append-only. Events are never modified or deleted.
- State Store is read-only from the application's perspective. Only projections write to it.
- Event Store ≠ Event Bus. Store guarantees durability; Bus provides real-time notifications.
- State Store is disposable — it can be fully rebuilt by replaying all events.

**v0.1 Implementation:** SQLite with WAL mode. Single events table. In-memory projection rebuilt on startup.

```sql
CREATE TABLE events (
    position   INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_id  TEXT NOT NULL,
    version    INTEGER NOT NULL,
    event_id   TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    payload    BLOB NOT NULL,
    timestamp  TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(stream_id, version)
);
```

---

### Layer 10: Multi-Agent Worker Architecture

**Question:** Which machine runs the agent?

**Responsibility:** Register execution nodes, match RuntimeInstances to Workers by capability and resource availability.

**Key Interfaces:**

```go
type WorkerRegistry interface {
    Register(ctx context.Context, worker Worker) error
    Heartbeat(ctx context.Context, workerID string, load WorkerLoad) error
    Get(ctx context.Context, workerID string) (*Worker, error)
    List(ctx context.Context, filter WorkerFilter) ([]Worker, error)
}

type PlacementEngine interface {
    Select(ctx context.Context, spec RuntimeSpec, workers []Worker) (*Worker, error)
}
```

**Key Types:**

```go
type Worker struct {
    ID           string
    Status       WorkerStatus    // REGISTERING → READY → SUSPECT → OFFLINE
    Capabilities []Capability
    Labels       map[string]string
    Lease        WorkerLease     // heartbeat + TTL
}

type WorkerLease struct {
    HeartbeatInterval time.Duration
    LastHeartbeat     time.Time
    TTL               time.Duration
}
```

**Worker Lifecycle:**

```text
OFFLINE → REGISTERING → READY ↔ BUSY
                ↓           ↓
              (fail)    DRAINING
                            ↓
                         OFFLINE
```

**Key Constraints:**
- Worker is a mini control plane (like kubelet), not just an RPC server.
- Workspace is a logical resource scheduled onto a Worker. They are not owned by the Worker.
- Placement considers: capability match, resource availability, artifact locality, cost.
- Lost Worker → RUNTIME_LOST event → Reconciler restarts on another Worker.

**v0.1 Status:** Deferred. Single local worker, hardcoded. `PlacementEngine` interface exists but always returns "local."

---

### Layer 11: Security / Permission / Sandbox

**Question:** Who allowed this agent to do that?

**Responsibility:** Authenticate execution principals, enforce capability-based permissions, isolate agent processes, inject secrets safely.

**Four-Layer Model:**

```text
Identity → Permission → Sandbox → Audit
```

**Key Interfaces:**

```go
type PolicyEngine interface {
    Allow(ctx context.Context, req PermissionRequest) error
    Grant(ctx context.Context, grant PermissionGrant) error
}

type Sandbox interface {
    Setup(ctx context.Context, spec SandboxSpec) (*SandboxContext, error)
    Teardown(ctx context.Context, ctx2 SandboxContext) error
}
```

**Key Types:**

```go
type ExecutionPrincipal struct {
    Actor       Principal       // who triggered it (user:alice)
    Agent       Principal       // what is acting (agent:coder)
    SessionID   string
    WorkflowID  string
    TaskID      string
    Intent      string          // purpose of this execution
}

type PermissionRequest struct {
    Principal ExecutionPrincipal
    Action    string            // "read", "write", "execute", "connect"
    Resource  string            // "file://pkg/auth/token.go", "api://github.com"
    Context   PermissionContext
}
```

**Secret Injection (Critical Design):**

```text
1. Secret stored in arlod secret store (never in code/prompt)
2. Node declares secret dependency in YAML
3. RuntimeManager mounts secret to /run/arlo/secrets/<NAME> in sandbox
4. Environment variable ARLO_SECRETS_DIR points to mount path
5. Prompt says "token at $ARLO_SECRETS_DIR/GITHUB_TOKEN" — value NEVER in LLM context
6. After session: sandbox teardown → secrets deleted
7. Secret Lease: temporary credential with auto-expiry
```

**Key Constraints:**
- Permission is evaluated per ExecutionPrincipal, not per Agent. Same agent, different task = different permissions.
- Secrets never appear in LLM context. Period.
- Audit records three phases: Intent (what agent wanted) → Decision (policy result) → Execution (what happened).
- Default v0.1 policy: deny `.env`, `*.pem`, `.git/config`. Allow everything else.

**v0.1 Implementation:** LocalPolicy only. Deny list for sensitive files. No Sandbox (local filesystem as-is). No Identity provider (default local user).

---

### Layer 12: Plugin / Extension System

**Question:** How does the platform grow?

**Responsibility:** Define extension contracts, manage plugin lifecycle, enforce isolation by entry kind.

**8 Extension Points:**

| Extension Point | Interface | v0.1 Status |
|----------------|-----------|-------------|
| Runtime Adapter | `RuntimeAdapter` | Builtin (Claude only) |
| Workspace Provider | `WorkspaceProvider` | Builtin (tmux only) |
| Skill Provider | `SkillRegistry` | Builtin |
| Context Strategy | `ContextBuilder` | Builtin (basic) |
| Memory Backend | `PatternLibrary` | Deferred |
| Policy Provider | `PolicyEngine` | Builtin (local policy) |
| Tool Provider | `ToolGateway` | Deferred |
| Event Hook | `EventHook` | Deferred |

**4 Entry Kinds (Isolation Levels):**

| Kind | Isolation | Use Case | v0.1 |
|------|-----------|----------|------|
| `builtin` | In-process, trusted | Core adapters (claude, tmux) | ✓ |
| `grpc` | Network boundary | Third-party services | ✗ |
| `process` | Process boundary | MCP servers, JSON-RPC tools | ✗ |
| `wasm` | Full sandbox | Untrusted community plugins | ✗ |

**Key Constraints:**
- Plugins are Principals. They request capabilities through the PolicyEngine.
- Plugin state is reconciled like everything else. Plugin Controller reads desired state, ensures actual state matches.
- Go's `plugin.Open()` (.so files) is explicitly NOT supported — version binding and unload issues make it unsuitable.
- v0.1: all extensions are builtin. Interfaces exist, loading mechanism is trivial `init()` registration.

---

## v0.1 MVP Scope

### The Question v0.1 Answers

> Can arlod reliably manage a multi-node Claude Code workflow in tmux, with all state persisted in SQLite?

### What's In

| Component | Implementation |
|-----------|---------------|
| Event Store | SQLite, single `events` table, append-only |
| State Store | In-memory projection, rebuilt from events on startup |
| gRPC API | Unix socket, 4 RPCs: CreateTask, GetTask, SubscribeEvents, AttachPTY |
| Workflow Engine | Static DAG, YAML parser, Evaluate → Decisions |
| Reconciler | Simple tick loop (5s) + event-triggered wakeup |
| RuntimeManager | ClaudeCodeAdapter only (`claude --headless` via `creack/pty`) |
| WorkspaceManager | TmuxProvider only (session + windows) |
| ContextBuilder | Upstream artifacts only, no compression |
| ArtifactStore | Local filesystem |
| Skill | Embedded in workflow YAML |
| Security | LocalPolicy: deny `.env`, `*.pem`, `.git/config` |
| CLI | `arlo run`, `arlo status`, `arlo attach`, `arlo artifacts` |

### What's Out (Deferred to v1+)

| Component | Reason |
|-----------|--------|
| Multi-worker / PlacementEngine | No remote execution yet |
| Docker/K8s workspace providers | Tmux is sufficient for single-machine |
| Pattern Library / Memory system | Need execution data first |
| Skill Evolution / Replay Engine | Need stable interfaces first |
| Dynamic DAG mutation | Static DAG + Transitions covers 90% |
| Plugin marketplace / wasm / grpc loading | Need real extension pain points first |
| Full Identity / RBAC / Vault | Single-user local tool |
| Codex / Gemini adapters | Start with one, add more when the interface is proven |
| Bubble Tea TUI | CLI commands first, TUI when the control plane is stable |

### End-to-End User Story

```bash
$ arlo run bugfix.yaml

# 1. CLI reads YAML, sends CreateTask gRPC to arlod
# 2. arlod compiles workflow: [analyze → implement → review]
# 3. arlod creates tmux session "arlo-bugfix" (detached)
# 4. arlod creates 3 windows: "analyze", "implement", "review"
# 5. Reconciler starts "analyze" (no dependencies)
#    → spawns claude in analyze window
# 6. analyze completes → artifact rca.md saved
# 7. Reconciler starts "implement"
#    → ContextBuilder gives implement: rca.md + source files
# 8. implement completes → diff saved
# 9. Reconciler starts "review"
#    → ContextBuilder gives review: diff + tests
# 10. review returns APPROVED → workflow complete
# 11. All events in SQLite, all artifacts on disk

$ arlo status
  TASK              STATUS
  fix-oauth-bug     completed
  Nodes: analyze ✓ | implement ✓ | review ✓

$ arlo attach review     # open tmux to see review agent output
$ arlo artifacts task-123  # list all produced artifacts
```

---

## v0.1 Implementation Path

7 steps, ordered by dependency. Each step produces a testable artifact.

### Step 1: Event Store + SQLite

```
Files:  internal/store/eventstore.go
        internal/store/sqlite.go

Produces: EventStore interface, SQLite implementation
          Append / Read / Subscribe
          events table + migration

Tests:  append, read, subscribe, concurrent writes

Dependencies: none — this is the first brick
```

### Step 2: Domain Model + Events

```
Files:  internal/domain/events.go
        internal/domain/task.go
        internal/domain/workflow.go
        internal/domain/node.go

Produces: Event types, EventType enum, Event schema (oneof payload)
          Task, Workflow, Node, Edge structs

Tests:  event marshaling/unmarshaling, event type validation

Dependencies: Step 1
```

### Step 3: State Store + Projections

```
Files:  internal/state/projection.go
        internal/state/workflow_projection.go

Produces: StateStore interface, WorkflowProjection
          RebuildAll(): replay events → current state

Tests:  replay known events → correct projection state

Dependencies: Steps 1, 2
```

### Step 4: Workflow Engine + YAML Parser

```
Files:  internal/workflow/engine.go
        internal/workflow/parser.go
        internal/workflow/evaluator.go

Produces: WorkflowEngine (Compile, Validate, Instantiate, Evaluate)
          YAML → ExecutableGraph
          Evaluate → []Decision

Tests:  parse YAML → valid graph, evaluate state → correct decisions
        cycles rejected, missing deps rejected

Dependencies: Steps 2, 3
```

### Step 5: Reconciler

```
Files:  internal/reconciler/reconciler.go

Produces: Reconciler (tick loop + event trigger)
          Reconcile(workflowID): read state → evaluate → execute decisions
          Idempotent: reconciling twice produces no duplicate actions

Tests:  given state X, produces decisions Y
        START_NODE only when node is READY
        duplicate reconcile is a no-op

Dependencies: Steps 3, 4
```

### Step 6: Runtime + Workspace (Claude + tmux)

```
Files:  internal/runtime/manager.go
        internal/runtime/adapter.go
        internal/runtime/claude_adapter.go
        internal/workspace/manager.go
        internal/workspace/provider.go
        internal/workspace/tmux_provider.go

Produces: RuntimeManager + ClaudeCodeAdapter
          WorkspaceManager + TmuxProvider
          Real process spawning, tmux session/window creation

Tests:  manual (real Claude + tmux required)
        unit tests for adapter logic (mock process)

Dependencies: Steps 2, 5
```

### Step 7: gRPC API + CLI

```
Files:  api/proto/arlo/v1/service.proto
        internal/service/arlo_service.go
        cmd/arlod/main.go
        cmd/arlo/main.go

Produces: gRPC service handlers, Unix socket server
          arlo CLI: run, status, attach, artifacts
          Complete wire-up of all 6 prior steps

Tests:  integration: full user story end-to-end
        events in SQLite, artifacts on disk, tmux windows created

Dependencies: Steps 1-6
```

### Verification at Each Step

| Step | Verifies |
|------|----------|
| 1 | `go test ./internal/store/...` — append, read, subscribe all pass |
| 2 | `go test ./internal/domain/...` — events marshal/unmarshal correctly |
| 3 | `go test ./internal/state/...` — replay events → correct projection |
| 4 | `go test ./internal/workflow/...` — parse YAML, validate DAG, evaluate decisions |
| 5 | `go test ./internal/reconciler/...` — state → decisions, idempotency |
| 6 | Manual: `arlo run bugfix.yaml` spawns real Claude in real tmux |
| 7 | End-to-end: full user story, events in SQLite, artifacts on disk |

---

## Non-Goals

These are explicitly ruled out — even in the North Star architecture:

1. **Arlo is not an LLM gateway.** It does not proxy or route LLM API calls. Runtime Adapters manage CLI processes, not API keys.
2. **Arlo is not a code editor.** It manages agent workspaces; it doesn't provide editing UI.
3. **Arlo is not a CI/CD system.** It schedules agent workflows, not build pipelines (though they may look similar).
4. **Arlo is not a model registry or prompt store.** Skills live in the skill registry, but this is not an ML model management system.
5. **Arlo is not a chat interface.** Users interact via CLI/TUI commands, not conversational threads with agents.
6. **Arlo is not multi-tenant in v0.1.** Single user, single machine. Multi-tenancy is a v2 concern.

---

## Design Decisions Log

| Decision | Rationale | Date |
|----------|-----------|------|
| Event Sourcing over CRUD | Audit trail, replay, crash recovery are essential for agent orchestration | 2026-08-03 |
| Reconciler over Event Handler | Events can be lost/duplicated. State comparison is more robust. | 2026-08-03 |
| gRPC over Unix Socket (v0.1) | Zero network overhead, process isolation, swap to TCP for remote later | 2026-08-03 |
| SQLite over Postgres (v0.1) | Zero deployment, WAL mode, single-file backup, sufficient for single-machine | 2026-08-03 |
| Tmux over custom PTY multiplexer | Tmux is battle-tested, has detach/attach, window management, and users already know it | 2026-08-03 |
| ExecutionSlot over Window | Provider-agnostic naming. A slot is a pane in tmux, a container exec in Docker, a pod in K8s. | 2026-08-03 |
| Skill as platform asset, not embedded in workflow | Skills are reusable across workflows, versioned independently. Workflows reference them. | 2026-08-03 |
| Secrets via filesystem, never in prompt | LLM context is not a secure channel. Filesystem mount with TTL is the correct pattern. | 2026-08-03 |
| No Go plugin (.so) support | Version binding, unload issues, and crash propagation make Go plugins unsuitable for production. gRPC/process/wasm are better isolation boundaries. | 2026-08-03 |
| Static DAG + Transitions over dynamic mutation | Dynamic mutation (adding/removing nodes at runtime) complicates replay, state migration, and UI. Transitions cover iterative loops without mutation. | 2026-08-03 |
| Claude-only v0.1 | Prove the adapter interface with one real implementation before adding more. | 2026-08-03 |
| Interfaces defined, implementations trivial in v0.1 | The architecture is correct. Premature implementation of all layers would delay validation of the core loop. | 2026-08-03 |
