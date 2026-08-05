# Workflow Final Results → Timeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let graph YAML declare final `results` (node + artifact); on workflow completion, emit resolved absolute paths on `TASK_COMPLETED` and show them in the TUI Timeline.

**Architecture:** Optional `results` on the graph compile into `ExecutableGraph.Results`. Reconciler fills `TaskCompleted.Results` with absolute paths via existing `workDir()`. TUI maps that payload onto `TaskCompletedItem` and renders `workflow completed → <path>`.

**Tech Stack:** Go, existing `internal/domain`, `internal/workflow`, `internal/reconciler`, `internal/tui`; `go test -race`.

**Spec:** `docs/superpowers/specs/2026-08-05-workflow-results-timeline-design.md`

## Global Constraints

- No new event types — extend `TASK_COMPLETED` / `TaskCompleted` only.
- Missing result files on disk must not block workflow completion.
- Graphs without `results` keep current Timeline text: `workflow completed`.
- v1 Validate: node exists + artifact non-empty only (no skill `output` cross-check).
- Path resolution: `filepath.Join(workDir(), artifact)` — same base as `emitArtifacts`.
- Do not commit unless the user asks (plan steps still show suggested commit messages).

## File map

| File | Responsibility |
|------|----------------|
| `internal/domain/workflow.go` | `WorkflowResult` type; `ExecutableGraph.Results` |
| `internal/domain/events.go` | `WorkflowResultRef`; extend `TaskCompleted` |
| `internal/workflow/engine.go` | YAML `results` parse + Validate |
| `internal/workflow/engine_test.go` | Compile/Validate tests |
| `internal/reconciler/reconciler.go` | Populate results in `executeCompleteWorkflow` |
| `internal/reconciler/reconciler_test.go` | Completion payload test |
| `internal/tui/timeline_items.go` | `TaskCompletedItem` + `EventToItem` + `Render` |
| `internal/tui/tui_test.go` | Timeline render / parse tests |
| `graphs/architecture-review.yaml` | Example `results` declaration |

---

### Task 1: Domain types + YAML compile/validate

**Files:**
- Modify: `internal/domain/workflow.go`
- Modify: `internal/domain/events.go`
- Modify: `internal/workflow/engine.go`
- Test: `internal/workflow/engine_test.go`

**Interfaces:**
- Produces:
  - `domain.WorkflowResult{NodeID, Artifact string}`
  - `ExecutableGraph.Results []WorkflowResult`
  - `domain.WorkflowResultRef{NodeID, Artifact, Path string}`
  - `domain.TaskCompleted{TaskID string; Results []WorkflowResultRef}`
  - YAML key `results: [{node, artifact}]` → compile → `graph.Results`
  - `Validate` errors on unknown node or empty artifact

- [ ] **Step 1: Write failing tests for compile + validate**

Add to `internal/workflow/engine_test.go`:

```go
func TestCompileResults(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()
	src := `
name: with-results
nodes:
  - id: review
    skill: architecture-review
results:
  - node: review
    artifact: architecture-review.md
`
	graph, err := eng.Compile(ctx, []byte(src))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(graph.Results) != 1 {
		t.Fatalf("Results len = %d, want 1", len(graph.Results))
	}
	if graph.Results[0].NodeID != "review" {
		t.Errorf("NodeID = %q, want review", graph.Results[0].NodeID)
	}
	if graph.Results[0].Artifact != "architecture-review.md" {
		t.Errorf("Artifact = %q, want architecture-review.md", graph.Results[0].Artifact)
	}
	if err := eng.Validate(ctx, graph); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateResultsUnknownNode(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()
	graph := &domain.ExecutableGraph{
		Name: "bad",
		Nodes: []domain.ExecutableNode{
			{ID: "review", SkillRef: domain.SkillRef{Name: "architecture-review"}},
		},
		Results: []domain.WorkflowResult{
			{NodeID: "missing", Artifact: "out.md"},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil || !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("Validate error = %v, want unknown node", err)
	}
}

func TestValidateResultsEmptyArtifact(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()
	graph := &domain.ExecutableGraph{
		Name: "bad",
		Nodes: []domain.ExecutableNode{
			{ID: "review", SkillRef: domain.SkillRef{Name: "architecture-review"}},
		},
		Results: []domain.WorkflowResult{
			{NodeID: "review", Artifact: ""},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil || !strings.Contains(err.Error(), "artifact") {
		t.Fatalf("Validate error = %v, want artifact", err)
	}
}
```

Ensure `strings` is imported in the test file (already used elsewhere in that package).

- [ ] **Step 2: Run tests — expect FAIL**

```bash
go test -race -run 'TestCompileResults|TestValidateResults' ./internal/workflow/
```

Expected: FAIL (no `Results` field / compile ignores `results`).

- [ ] **Step 3: Add domain types**

In `internal/domain/workflow.go`, add before or near `ExecutableGraph`:

```go
// WorkflowResult declares a final deliverable produced by a node.
type WorkflowResult struct {
	NodeID   string `json:"node_id"`
	Artifact string `json:"artifact"`
}
```

Add field on `ExecutableGraph`:

```go
Results []WorkflowResult `json:"results,omitempty"`
```

In `internal/domain/events.go`, replace `TaskCompleted` and add:

```go
// WorkflowResultRef is a resolved final result attached to TASK_COMPLETED.
type WorkflowResultRef struct {
	NodeID   string `json:"node_id"`
	Artifact string `json:"artifact"`
	Path     string `json:"path"`
}

// TaskCompleted is the payload for TASK_COMPLETED events.
type TaskCompleted struct {
	TaskID  string              `json:"task_id"`
	Results []WorkflowResultRef `json:"results,omitempty"`
}
```

- [ ] **Step 4: Parse + validate in workflow engine**

In `internal/workflow/engine.go`, extend YAML defs:

```go
type workflowDef struct {
	Name    string      `yaml:"name"`
	Version int         `yaml:"version"`
	Nodes   []nodeDef   `yaml:"nodes"`
	Results []resultDef `yaml:"results"`
	Policy  policyDef   `yaml:"policy"`
}

type resultDef struct {
	Node     string `yaml:"node"`
	Artifact string `yaml:"artifact"`
}
```

In `Compile`, after building `nodes`, before constructing `graph`:

```go
results := make([]domain.WorkflowResult, 0, len(def.Results))
for _, rd := range def.Results {
	results = append(results, domain.WorkflowResult{
		NodeID:   rd.Node,
		Artifact: rd.Artifact,
	})
}
```

Set `Results: results` on the `ExecutableGraph` literal.

In `Validate`, after dependency checks (before cycle check):

```go
for i, r := range graph.Results {
	if r.NodeID == "" {
		return fmt.Errorf("results[%d]: node is required", i)
	}
	if !ids[r.NodeID] {
		return fmt.Errorf("results[%d]: unknown node %s", i, r.NodeID)
	}
	if r.Artifact == "" {
		return fmt.Errorf("results[%d]: artifact is required", i)
	}
}
```

- [ ] **Step 5: Run tests — expect PASS**

```bash
go test -race -run 'TestCompileResults|TestValidateResults|TestCompileValid|TestCompileMinimal' ./internal/workflow/
```

Expected: PASS

- [ ] **Step 6: Commit (if user asked)**

```bash
git add internal/domain/workflow.go internal/domain/events.go internal/workflow/engine.go internal/workflow/engine_test.go
git commit -m "feat: parse and validate workflow results in graph YAML"
```

---

### Task 2: Reconciler populates results on TASK_COMPLETED

**Files:**
- Modify: `internal/reconciler/reconciler.go` (`executeCompleteWorkflow`)
- Test: `internal/reconciler/reconciler_test.go`

**Interfaces:**
- Consumes: `graphRegistry[workflowID].Results`, `workDir()`
- Produces: `TaskCompleted.Results` with `Path = filepath.Join(workDir(), artifact)`

- [ ] **Step 1: Write failing test**

Add a minimal graph YAML constant (or inline) with `results`, seed workflow, complete the single node, reconcile until `TASK_COMPLETED`, assert payload.

```go
const resultsYAML = `
name: results-demo
version: 1
nodes:
  - id: review
    skill: architecture-review
    runtime:
      provider: claude-code
    retry:
      max_retries: 1
results:
  - node: review
    artifact: architecture-review.md
policy:
  max_concurrent_nodes: 1
`

func TestCompleteWorkflowEmitsResultPaths(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)
	_, wfID := seedWorkflow(t, es, ss, r, eng, resultsYAML, "t-results")

	// Start then complete the only node.
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("reconcile start: %v", err)
	}
	ss.Rebuild(ctx)

	ns, err := ss.GetNodeState(ctx, "review")
	if err != nil {
		t.Fatalf("GetNodeState: %v", err)
	}
	appendEvent(t, es, "node-review", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "review", SessionID: ns.SessionID,
	})
	ss.Rebuild(ctx)

	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("reconcile complete: %v", err)
	}
	ss.Rebuild(ctx)

	events, err := es.Read(ctx, "workflow-"+wfID, 0)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	var found bool
	for _, ev := range events {
		if ev.Type != store.EventTaskCompleted {
			continue
		}
		found = true
		var payload domain.TaskCompleted
		if err := json.Unmarshal(ev.Payload, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(payload.Results) != 1 {
			t.Fatalf("Results len = %d, want 1", len(payload.Results))
		}
		got := payload.Results[0]
		if got.NodeID != "review" || got.Artifact != "architecture-review.md" {
			t.Errorf("result = %+v", got)
		}
		wantPath := filepath.Join(workDir(), "architecture-review.md")
		if got.Path != wantPath {
			t.Errorf("Path = %q, want %q", got.Path, wantPath)
		}
	}
	if !found {
		t.Fatal("TASK_COMPLETED not found")
	}
}
```

Add imports as needed: `encoding/json`, `path/filepath`. If `es.Read` signature differs, match the existing read helper pattern already used in reconciler/service tests.

- [ ] **Step 2: Run test — expect FAIL**

```bash
go test -race -run TestCompleteWorkflowEmitsResultPaths ./internal/reconciler/
```

Expected: FAIL (`Results` empty / missing).

- [ ] **Step 3: Implement resolve in executeCompleteWorkflow**

Replace the payload construction in `executeCompleteWorkflow` (`internal/reconciler/reconciler.go`):

```go
completed := domain.TaskCompleted{TaskID: workflowID}
if graph := r.graphRegistry[workflowID]; graph != nil {
	wd := workDir()
	for _, res := range graph.Results {
		completed.Results = append(completed.Results, domain.WorkflowResultRef{
			NodeID:   res.NodeID,
			Artifact: res.Artifact,
			Path:     filepath.Join(wd, res.Artifact),
		})
	}
}
payload, _ := json.Marshal(completed)
```

`filepath` is already imported in this file (used by `emitArtifacts`). Do not `Stat` the file — always emit the expected path.

- [ ] **Step 4: Run test — expect PASS**

```bash
go test -race -run TestCompleteWorkflowEmitsResultPaths ./internal/reconciler/
```

Expected: PASS

Also run a broader package smoke:

```bash
go test -race ./internal/reconciler/ ./internal/domain/
```

Expected: PASS

- [ ] **Step 5: Commit (if user asked)**

```bash
git add internal/reconciler/reconciler.go internal/reconciler/reconciler_test.go
git commit -m "feat: attach resolved result paths to TASK_COMPLETED"
```

---

### Task 3: TUI Timeline renders result paths

**Files:**
- Modify: `internal/tui/timeline_items.go`
- Test: `internal/tui/tui_test.go`

**Interfaces:**
- Consumes: `TASK_COMPLETED` JSON `results` array
- Produces: `TaskCompletedItem.Results`; `Render()` strings per spec

- [ ] **Step 1: Write failing tests**

Add to `internal/tui/tui_test.go`:

```go
func TestTaskCompletedItemRenderWithResults(t *testing.T) {
	item := TaskCompletedItem{
		Results: []TaskCompletedResult{
			{Artifact: "architecture-review.md", Path: "/tmp/architecture-review.md"},
		},
	}
	got := item.Render()
	want := "workflow completed → /tmp/architecture-review.md"
	if got != want {
		t.Errorf("Render() = %q, want %q", got, want)
	}
}

func TestTaskCompletedItemRenderMultipleResults(t *testing.T) {
	item := TaskCompletedItem{
		Results: []TaskCompletedResult{
			{Path: "/a.md"},
			{Path: "/b.md"},
		},
	}
	got := item.Render()
	want := "workflow completed → /a.md; /b.md"
	if got != want {
		t.Errorf("Render() = %q, want %q", got, want)
	}
}

func TestEventToItemTaskCompletedResults(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{
		"task_id": "wf1",
		"results": []map[string]string{
			{
				"node_id":  "review",
				"artifact": "architecture-review.md",
				"path":     "/repo/architecture-review.md",
			},
		},
	})
	ev := &arlov1.Event{
		Type:      "TASK_COMPLETED",
		Timestamp: time.Now().Format(time.RFC3339),
		Payload:   payload,
	}
	item := EventToItem(ev)
	tc, ok := item.(TaskCompletedItem)
	if !ok {
		t.Fatalf("got %T, want TaskCompletedItem", item)
	}
	if len(tc.Results) != 1 || tc.Results[0].Path != "/repo/architecture-review.md" {
		t.Fatalf("Results = %+v", tc.Results)
	}
	if tc.Render() != "workflow completed → /repo/architecture-review.md" {
		t.Errorf("Render = %q", tc.Render())
	}
}
```

Keep existing empty `TaskCompletedItem{}` level test — `Render()` must still return `workflow completed`.

- [ ] **Step 2: Run tests — expect FAIL**

```bash
go test -race -run 'TestTaskCompletedItemRender|TestEventToItemTaskCompletedResults' ./internal/tui/
```

Expected: FAIL (types/methods missing).

- [ ] **Step 3: Implement Timeline item**

In `internal/tui/timeline_items.go`, replace `TaskCompletedItem` section:

```go
type TaskCompletedResult struct {
	Artifact string
	Path     string
}

type TaskCompletedItem struct {
	Timestamp time.Time
	Results   []TaskCompletedResult
}

func (i TaskCompletedItem) Time() time.Time { return i.Timestamp }
func (i TaskCompletedItem) Level() Level    { return INFO }
func (i TaskCompletedItem) Render() string {
	if len(i.Results) == 0 {
		return "workflow completed"
	}
	paths := make([]string, 0, len(i.Results))
	for _, r := range i.Results {
		if r.Path != "" {
			paths = append(paths, r.Path)
		}
	}
	if len(paths) == 0 {
		return "workflow completed"
	}
	return "workflow completed → " + strings.Join(paths, "; ")
}
```

Add `"strings"` to imports.

In `EventToItem` for `TASK_COMPLETED`:

```go
case "TASK_COMPLETED":
	return TaskCompletedItem{Timestamp: t, Results: extractTaskCompletedResults(event)}
```

Add extractor:

```go
func extractTaskCompletedResults(event *arlov1.Event) []TaskCompletedResult {
	var payload struct {
		Results []struct {
			Artifact string `json:"artifact"`
			Path     string `json:"path"`
		} `json:"results"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return nil
	}
	out := make([]TaskCompletedResult, 0, len(payload.Results))
	for _, r := range payload.Results {
		out = append(out, TaskCompletedResult{Artifact: r.Artifact, Path: r.Path})
	}
	return out
}
```

- [ ] **Step 4: Run tests — expect PASS**

```bash
go test -race -run 'TestTaskCompletedItemRender|TestEventToItemTaskCompletedResults|TestTimelineItemLevels' ./internal/tui/
```

Expected: PASS

Full package:

```bash
go test -race ./internal/tui/
```

Expected: PASS

- [ ] **Step 5: Commit (if user asked)**

```bash
git add internal/tui/timeline_items.go internal/tui/tui_test.go
git commit -m "feat(tui): show workflow result paths on TASK_COMPLETED timeline"
```

---

### Task 4: Update architecture-review sample graph

**Files:**
- Modify: `graphs/architecture-review.yaml`

**Interfaces:**
- Consumes: Task 1 YAML schema
- Produces: example graph with declared final result

- [ ] **Step 1: Add results block**

Insert before `policy:` in `graphs/architecture-review.yaml`:

```yaml
results:
  - node: review
    artifact: architecture-review.md
```

Full tail of file should look like:

```yaml
  - id: review
    skill: architecture-review
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    depends_on:
      - identify
    gate: human_approval
    retry:
      max_retries: 1

results:
  - node: review
    artifact: architecture-review.md

policy:
  max_concurrent_nodes: 1
```

- [ ] **Step 2: Smoke-compile the sample**

```bash
go test -race ./internal/workflow/ ./internal/reconciler/ ./internal/tui/ ./internal/domain/
```

Expected: PASS

- [ ] **Step 3: Commit (if user asked)**

```bash
git add graphs/architecture-review.yaml
git commit -m "chore: declare architecture-review final result artifact"
```

---

## Spec coverage checklist

| Spec requirement | Task |
|------------------|------|
| YAML `results: [{node, artifact}]` | 1, 4 |
| `ExecutableGraph.Results` / `WorkflowResult` | 1 |
| Validate unknown node + empty artifact | 1 |
| `TaskCompleted.Results` / `WorkflowResultRef` | 1, 2 |
| Path via `workDir()` + artifact name | 2 |
| Missing file does not fail completion | 2 (no Stat) |
| Timeline render one / many / none | 3 |
| Update `architecture-review.yaml` | 4 |
| No new event type | all |
| Skill output cross-check | out of scope |

## Self-review notes

- Types named consistently: graph `WorkflowResult`, event `WorkflowResultRef`, TUI `TaskCompletedResult`.
- Reconciler test may need adjustment if `es.Read` API or `seedWorkflow` signature differs — match existing helpers in `reconciler_test.go` before inventing new ones.
- `arlo_service` cancel path misuses `TaskCompleted` for cancel; leave untouched (unrelated).
