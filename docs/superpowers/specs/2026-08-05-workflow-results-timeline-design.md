# Workflow Final Results → Timeline

**Date:** 2026-08-05  
**Status:** Approved design  
**Scope:** Declare final workflow result artifacts in graph YAML; surface resolved paths on `TASK_COMPLETED` in the TUI Timeline.

## Problem

Workflows like `architecture-review` produce a final deliverable (e.g. `architecture-review.md`). Node-level `ARTIFACT_CREATED` events already exist, but when a run finishes the Timeline only shows `workflow completed` — the operator cannot see the final result path without opening the Inspector.

## Goals

- Graph authors explicitly declare which node/artifact pairs are the workflow’s final results.
- On workflow completion, Timeline shows the resolved absolute path(s).
- Event log remains the source of truth (clients do not re-infer from graph + artifacts).

## Non-Goals

- New event types (e.g. `WORKFLOW_RESULT`).
- Changing Inspector Artifacts UX.
- Blocking completion if a declared result file is missing on disk.
- Requiring every graph to declare `results` (field is optional).

## Design

### 1. YAML schema

Optional top-level `results` on the graph:

```yaml
name: architecture-review
version: 1
# ...
nodes:
  - id: review
    skill: architecture-review
    # ...

results:
  - node: review
    artifact: architecture-review.md

policy:
  max_concurrent_nodes: 1
```

Each entry binds a **node id** + **artifact filename** (the name declared in the skill’s `output` list).

### 2. Domain / compile model

```go
type WorkflowResult struct {
    NodeID   string // graph node id
    Artifact string // filename, e.g. architecture-review.md
}

// ExecutableGraph gains:
Results []WorkflowResult
```

YAML compile (`internal/workflow`):

- Parse `results` into `ExecutableGraph.Results`.
- Validate: each `node` exists in the graph; `artifact` is non-empty.
- Optional soft check: if the skill resolves, warn/error when `artifact` is not in that skill’s `output` list. Prefer hard error at Validate time when skill registry is available; otherwise skip.

Missing `results` → empty slice → current behavior unchanged.

### 3. Event payload

Extend `TaskCompleted` (no new event type):

```go
type WorkflowResultRef struct {
    NodeID   string `json:"node_id"`
    Artifact string `json:"artifact"`
    Path     string `json:"path"` // absolute path at completion time
}

type TaskCompleted struct {
    TaskID  string              `json:"task_id"`
    Results []WorkflowResultRef `json:"results,omitempty"`
}
```

### 4. Reconciler

In `executeCompleteWorkflow`:

1. Load `graphRegistry[workflowID].Results`.
2. For each entry, set `Path = filepath.Join(workDir(), artifact)` — same base as `emitArtifacts`.
3. Emit `TASK_COMPLETED` with populated `Results`.
4. Do **not** fail completion if the file is absent; still emit the expected path.

Workflow projection may ignore `Results` (status machine does not need them).

### 5. TUI Timeline

- Extend `TaskCompletedItem` with results (`Artifact`, `Path`).
- `EventToItem` parses `results` from `TASK_COMPLETED` payload.
- `Render()`:
  - No results: `workflow completed` (unchanged).
  - One result: `workflow completed → <path>`.
  - Multiple: `workflow completed → <path1>; <path2>; ...`
- Compact-mode allowlist already includes `TaskCompletedItem`; no filter changes.

### 6. Example graph update

Update `graphs/architecture-review.yaml` with:

```yaml
results:
  - node: review
    artifact: architecture-review.md
```

`graphs/bugfix.yaml` may remain without `results` for this change.

## Data flow

```
graph YAML results
  → Compile → ExecutableGraph.Results
  → executeCompleteWorkflow
  → TASK_COMPLETED{results:[{node,artifact,path}]}
  → EventToItem → TaskCompletedItem
  → Timeline: "workflow completed → /abs/path/..."
```

## Testing

| Area | Cases |
|------|--------|
| Compile / Validate | Valid results; unknown node; empty artifact |
| Reconciler | Completion payload includes resolved paths from graph results |
| TUI | `EventToItem` + `Render` for zero / one / many results |

## Out of scope follow-ups

- Soft/hard skill-output cross-check if not done in v1 of this work.
- Declaring results on `bugfix` and other sample graphs.
- Click-to-open path from Timeline (Inspector already opens artifacts).
