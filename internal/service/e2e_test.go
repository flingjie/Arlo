//go:build integration
// +build integration

package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/reconciler"
	"github.com/lingjiefan/arlo/internal/runtime"
	"github.com/lingjiefan/arlo/internal/skill"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"github.com/lingjiefan/arlo/internal/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const bugfixYAML = `
name: bugfix-e2e
version: 1
description: "E2E test workflow"

nodes:
  - id: hello
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-haiku-4-5
    retry:
      max_retries: 0

policy:
  max_concurrent_nodes: 1
`

// dial connects to the running arlod instance.
func dial(t *testing.T) (arlov1.ArloServiceClient, *grpc.ClientConn) {
	t.Helper()

	socket := os.Getenv("ARLO_SOCKET")
	if socket == "" {
		home, _ := os.UserHomeDir()
		socket = home + "/.arlo/arlo.sock"
	}

	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial arlod at %s: %v", socket, err)
	}
	return arlov1.NewArloServiceClient(conn), conn
}

// TestE2EBugfixSimple runs a one-node workflow end-to-end via gRPC.
// Requires: arlod running, claude CLI in PATH.
func TestE2EBugfixSimple(t *testing.T) {
	client, conn := dial(t)
	defer conn.Close()
	ctx := context.Background()

	// 1. Create task.
	resp, err := client.CreateTask(ctx, &arlov1.CreateTaskRequest{
		Title:          "e2e-test",
		WorkflowSource: bugfixYAML,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	t.Logf("Task created: %s / %s", resp.TaskId, resp.WorkflowId)
	wfID := resp.WorkflowId

	// 2. Subscribe to events (replay from position 0).
	stream, err := client.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   wfID,
		FromPosition: 0,
	})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}

	// 3. Collect events with timeout.
	var events []*arlov1.Event
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			evt, err := stream.Recv()
			if err != nil {
				t.Logf("stream ended: %v", err)
				return
			}
			events = append(events, evt)
			// Stop when workflow is done or node fails.
			if evt.Type == "TASK_COMPLETED" || evt.Type == "TASK_FAILED" {
				return
			}
		}
	}()

	select {
	case <-done:
		// finished naturally
	case <-time.After(5 * time.Minute):
		t.Fatal("timeout waiting for workflow completion")
	}

	// 4. Verify event sequence.
	t.Logf("Received %d events:", len(events))
	eventTypes := make([]string, 0, len(events))
	seen := map[string]bool{}
	for _, e := range events {
		if seen[e.EventId] {
			continue
		}
		seen[e.EventId] = true
		eventTypes = append(eventTypes, e.Type)
		t.Logf("  %s: %s (stream=%s)", e.Type, summary(e), e.StreamId)
	}

	// Expected: TASK_CREATED → WORKFLOW_CREATED → NODE_CREATED → NODE_STARTED → METRICS_SNAPSHOT → NODE_COMPLETED → TASK_COMPLETED
	assertContains(t, eventTypes, "TASK_CREATED")
	assertContains(t, eventTypes, "WORKFLOW_CREATED")
	assertContains(t, eventTypes, "NODE_CREATED")
	assertContains(t, eventTypes, "NODE_STARTED")
	assertContains(t, eventTypes, "TASK_COMPLETED")

	// No duplicates.
	assertNoDuplicates(t, events)

	// 5. Verify snapshot.
	snap, err := client.GetWorkflowSnapshot(ctx, &arlov1.GetWorkflowSnapshotRequest{WorkflowId: wfID})
	if err != nil {
		t.Fatalf("GetWorkflowSnapshot: %v", err)
	}
	if snap.Status != "COMPLETED" && snap.Status != "ACTIVE" {
		t.Errorf("unexpected status: %s", snap.Status)
	}
	t.Logf("Snapshot: status=%s nodes=%d", snap.Status, len(snap.Nodes))
}

// TestE2EBugfixWithGate tests the human_approval gate flow.
func TestE2EBugfixWithGate(t *testing.T) {
	const gateYAML = `
name: gate-test
version: 1
nodes:
  - id: step
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-haiku-4-5
    gate: human_approval
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 1
`

	client, conn := dial(t)
	defer conn.Close()
	ctx := context.Background()

	resp, err := client.CreateTask(ctx, &arlov1.CreateTaskRequest{
		Title:          "gate-test",
		WorkflowSource: gateYAML,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	wfID := resp.WorkflowId
	t.Logf("Gate test: %s", wfID)

	// Subscribe to events.
	stream, err := client.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   wfID,
		FromPosition: 0,
	})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}

	// Collect events until node is WAITING.
	var events []*arlov1.Event
	nodeWaiting := make(chan struct{})
	go func() {
		defer close(nodeWaiting)
		for {
			evt, err := stream.Recv()
			if err != nil {
				t.Logf("stream ended: %v", err)
				return
			}
			events = append(events, evt)
			if evt.Type == "NODE_WAITING" {
				return
			}
			if evt.Type == "NODE_COMPLETED" || evt.Type == "TASK_COMPLETED" {
				return
			}
		}
	}()

	select {
	case <-nodeWaiting:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for NODE_WAITING")
	}

	// Verify node is WAITING, not RUNNING.
	assertContains(t, eventTypes(events), "NODE_WAITING")
	assertNotContains(t, eventTypes(events), "NODE_STARTED") // gated nodes go straight to WAITING
	t.Log("Node is WAITING — approving...")

	// Approve the gate.
	cmdResp, err := client.ExecuteCommand(ctx, &arlov1.CommandRequest{
		Command: "approve",
		Target:  "step",
	})
	if err != nil {
		t.Fatalf("ExecuteCommand: %v", err)
	}
	if !cmdResp.Success {
		t.Fatalf("approve failed: %s", cmdResp.Message)
	}
	t.Logf("Approved: %s", cmdResp.Message)

	// Resume event stream from where we left off to avoid duplicates.
	lastPosition := events[len(events)-1].Position
	stream2, err := client.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   wfID,
		FromPosition: lastPosition + 1,
	})
	if err != nil {
		t.Fatalf("re-SubscribeEvents: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			evt, err := stream2.Recv()
			if err != nil {
				return
			}
			events = append(events, evt)
			if evt.Type == "TASK_COMPLETED" || evt.Type == "TASK_FAILED" {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		t.Fatal("timeout waiting for completion after approve")
	}

	types := eventTypes(events)
	assertContains(t, types, "HUMAN_INPUT_RECEIVED")
	assertContains(t, types, "NODE_STARTED") // only after approval
	assertContains(t, types, "TASK_COMPLETED")
	assertNoDuplicates(t, events)
	t.Logf("Gate test passed with %d events", len(events))
}

// ── helpers ──────────────────────────────────────

func eventTypes(events []*arlov1.Event) []string {
	seen := map[string]bool{}
	var types []string
	for _, e := range events {
		if seen[e.EventId] {
			continue
		}
		seen[e.EventId] = true
		types = append(types, e.Type)
	}
	return types
}

func summary(e *arlov1.Event) string {
	var s struct {
		NodeID string `json:"node_id"`
		Reason string `json:"reason"`
		Title  string `json:"title"`
	}
	json.Unmarshal(e.Payload, &s)
	switch {
	case s.NodeID != "":
		return s.NodeID
	case s.Reason != "":
		return s.Reason
	case s.Title != "":
		return s.Title
	default:
		return "-"
	}
}

func assertContains(t *testing.T, slice []string, want string) {
	t.Helper()
	for _, s := range slice {
		if s == want {
			return
		}
	}
	t.Errorf("expected event type %q not found in %v", want, slice)
}

func assertNotContains(t *testing.T, slice []string, want string) {
	t.Helper()
	for _, s := range slice {
		if s == want {
			t.Errorf("unexpected event type %q found in %v", want, slice)
		}
	}
}

func assertNoDuplicates(t *testing.T, events []*arlov1.Event) {
	t.Helper()
	seen := map[string]int{}
	for _, e := range events {
		seen[e.EventId]++
		if seen[e.EventId] > 1 {
			t.Errorf("duplicate event: %s (type=%s)", e.EventId, e.Type)
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════
// In-process E2E Pipeline Tests
// ════════════════════════════════════════════════════════════════════════════
//
// These tests wire up the full Arlo stack in-process:
//   SQLite EventStore → InMemoryStateStore → WorkflowEngine → Reconciler → RuntimeManager
//
// No external arlod process or gRPC connection required. Every component is real
// except the RuntimeAdapter, which is a mock that tracks instances and allows
// manual state transitions via MarkExited.

var pipeSeq atomic.Int64

func nextPipeID() string {
	return fmt.Sprintf("pipe-%d", pipeSeq.Add(1))
}

// workDir returns the current directory, or a fallback for tests.
func workDir() string {
	if d, err := os.Getwd(); err == nil {
		return d
	}
	return "/tmp"
}

func mkSessID(nodeID string, attempt int) string {
	return fmt.Sprintf("sess-%s-%d", nodeID, attempt)
}

func mkInstID(nodeID string, attempt int) string {
	return fmt.Sprintf("rt-%s-%d", nodeID, attempt)
}

// ── Mock Runtime Adapter ─────────────────────────────────────────────────

// testAdapter implements runtime.Adapter and starts instances successfully.
// It also exposes a channel to signal when an instance is started.
type testAdapter struct {
	started chan string // sends instanceID when Start is called
}

func newTestAdapter() *testAdapter {
	return &testAdapter{started: make(chan string, 8)}
}

func (a *testAdapter) Prepare(ctx context.Context, inst domain.RuntimeInstance) error { return nil }
func (a *testAdapter) Start(ctx context.Context, inst domain.RuntimeInstance) error {
	select {
	case a.started <- inst.ID:
	default:
	}
	return nil
}
func (a *testAdapter) Stop(ctx context.Context, id string) error                     { return nil }
func (a *testAdapter) Destroy(ctx context.Context, id string) error                  { return nil }
func (a *testAdapter) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	return nil
}
func (a *testAdapter) Snapshot(ctx context.Context, id string) (domain.RuntimeSnapshot, error) {
	return domain.RuntimeSnapshot{State: domain.RuntimeStateRunning}, nil
}
func (a *testAdapter) Status(ctx context.Context, id string) (domain.RuntimeStatus, error) {
	return domain.RuntimeStatus{ID: id, State: domain.RuntimeStateRunning}, nil
}

// ── Test Stack Helpers ───────────────────────────────────────────────────

// pipeAppend appends an event to the store.
func pipeAppend(t *testing.T, es *store.SQLiteStore, streamID string, eventType store.EventType, payload interface{}) {
	t.Helper()
	data, _ := json.Marshal(payload)
	_, err := es.Append(context.Background(), streamID, []store.Event{{
		ID:      nextPipeID(),
		Type:    eventType,
		Payload: data,
	}})
	if err != nil {
		t.Fatalf("Append event %s to %s: %v", eventType, streamID, err)
	}
}

// newPipeReconciler creates a reconciler backed by a real SQLite store and
// in-memory state store. The runtime manager is nil (callers can set it later).
func newPipeReconciler(t *testing.T) (*reconciler.Reconciler, *store.SQLiteStore, *state.InMemoryStateStore, *workflow.Engine) {
	t.Helper()

	es, err := store.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	ss := state.NewInMemoryStateStore(es)
	eng := workflow.NewEngine()
	r := reconciler.New(ss, es, eng, nil, nil, nil).WithTickInterval(100 * time.Millisecond)

	return r, es, ss, eng
}

// newPipeReconcilerWithRuntime is like newPipeReconciler but also registers a
// test adapter and sets up a real runtime.Manager.
func newPipeReconcilerWithRuntime(t *testing.T) (*reconciler.Reconciler, *store.SQLiteStore, *state.InMemoryStateStore, *workflow.Engine, *runtime.Manager, *testAdapter) {
	t.Helper()

	es, err := store.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	ss := state.NewInMemoryStateStore(es)
	eng := workflow.NewEngine()
	adapter := newTestAdapter()
	mgr := runtime.NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, adapter)
	r := reconciler.New(ss, es, eng, mgr, nil, nil).WithTickInterval(100 * time.Millisecond)

	return r, es, ss, eng, mgr, adapter
}

// newPipeReconcilerWithSkills creates a reconciler with a skill registry
// containing the provided skills. Test uses this when artifact or checkpoint
// enrichment is needed.
func newPipeReconcilerWithSkills(t *testing.T, skills ...*domain.Skill) (*reconciler.Reconciler, *store.SQLiteStore, *state.InMemoryStateStore, *workflow.Engine, *runtime.Manager, *skill.Registry) {
	t.Helper()

	es, err := store.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	ss := state.NewInMemoryStateStore(es)
	eng := workflow.NewEngine()
	adapter := newTestAdapter()
	mgr := runtime.NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, adapter)
	reg := skill.NewRegistry()
	for _, s := range skills {
		reg.Register(s)
	}
	r := reconciler.New(ss, es, eng, mgr, nil, reg).WithTickInterval(100 * time.Millisecond)

	return r, es, ss, eng, mgr, reg
}

// seedPipeline compiles a YAML workflow, appends its initial events, submits
// the graph to the reconciler, and rebuilds projections.
func seedPipeline(t *testing.T, es *store.SQLiteStore, ss *state.InMemoryStateStore, r *reconciler.Reconciler, eng *workflow.Engine, yamlSource string, taskID string) (*domain.ExecutableGraph, string) {
	t.Helper()
	ctx := context.Background()

	graph, err := eng.Compile(ctx, []byte(yamlSource))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	wfID := "wf-" + taskID

	pipeAppend(t, es, "workflow-"+wfID, store.EventTaskCreated, domain.TaskCreated{
		TaskID: taskID, Title: "e2e test", CreatedBy: "tester", WorkflowID: wfID,
	})
	pipeAppend(t, es, "workflow-"+wfID, store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: wfID, TaskID: taskID, GraphName: graph.Name, Version: graph.Version,
	})

	for _, n := range graph.Nodes {
		pipeAppend(t, es, "node-"+n.ID, store.EventNodeCreated, domain.NodeCreated{
			NodeID:     n.ID,
			WorkflowID: wfID,
			SkillName:  n.SkillRef.Name,
			Runtime:    string(n.Runtime.Provider),
			DependsOn:  n.DependsOn,
			Gate:       string(n.Gate),
		})
	}

	r.Submit(wfID, graph)

	if err := ss.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	return graph, wfID
}

// collectStreamEventTypes reads all events from a stream and returns their type names.
func collectStreamEventTypes(t *testing.T, es *store.SQLiteStore, streamID string) []string {
	t.Helper()
	events, err := es.Read(context.Background(), streamID, 0)
	if err != nil {
		t.Fatalf("Read %s: %v", streamID, err)
	}
	types := make([]string, len(events))
	for i, e := range events {
		types[i] = string(e.Type)
	}
	return types
}

// assertStreamHas asserts that every wanted event type exists in the stream.
func assertStreamHas(t *testing.T, es *store.SQLiteStore, streamID string, wantTypes ...string) {
	t.Helper()
	events, err := es.Read(context.Background(), streamID, 0)
	if err != nil {
		t.Fatalf("Read %s: %v", streamID, err)
	}
	found := make(map[string]bool)
	for _, e := range events {
		found[string(e.Type)] = true
	}
	for _, wt := range wantTypes {
		if !found[wt] {
			allTypes := make([]string, 0, len(events))
			for _, e := range events {
				allTypes = append(allTypes, string(e.Type))
			}
			t.Errorf("stream %s: expected event type %q not found. Got: %v", streamID, wt, allTypes)
		}
	}
}

// assertStreamNotHas asserts that none of the given event types exist in the stream.
func assertStreamNotHas(t *testing.T, es *store.SQLiteStore, streamID string, forbidTypes ...string) {
	t.Helper()
	events, err := es.Read(context.Background(), streamID, 0)
	if err != nil {
		t.Fatalf("Read %s: %v", streamID, err)
	}
	for _, e := range events {
		for _, ft := range forbidTypes {
			if string(e.Type) == ft {
				t.Errorf("stream %s: unexpected event type %q found", streamID, ft)
			}
		}
	}
}

// findEventByType finds the first event of the given type in a stream.
func findEventByType(t *testing.T, es *store.SQLiteStore, streamID, eventType string) *store.Event {
	t.Helper()
	events, err := es.Read(context.Background(), streamID, 0)
	if err != nil {
		t.Fatalf("Read %s: %v", streamID, err)
	}
	for i := range events {
		if string(events[i].Type) == eventType {
			return &events[i]
		}
	}
	return nil
}

// ── Common YAML Workflow Definitions ─────────────────────────────────────

const twoNodeYAML = `
name: two-node-pipe
version: 1
nodes:
  - id: analyze
    skill: root-cause
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 0
  - id: implement
    skill: fix
    runtime:
      provider: claude-code
      model: test
    depends_on:
      - analyze
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 1
`

const retryFlowYAML = `
name: retry-flow
version: 1
nodes:
  - id: step1
    skill: fragile
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 2
policy:
  max_concurrent_nodes: 1
`

const gateYAML = `
name: gate-flow
version: 1
nodes:
  - id: step1
    skill: root-cause
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 0
  - id: step2
    skill: review
    runtime:
      provider: claude-code
      model: test
    depends_on:
      - step1
    gate: human_approval
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 1
`

const dagMultiDepYAML = `
name: dag-multi-dep
version: 1
nodes:
  - id: a
    skill: analyze
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 0
  - id: b
    skill: lint
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 0
  - id: c
    skill: merge
    runtime:
      provider: claude-code
      model: test
    depends_on:
      - a
      - b
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 2
`

const concurrentYAML = `
name: concurrent-flow
version: 1
nodes:
  - id: a
    skill: task-a
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 0
  - id: b
    skill: task-b
    runtime:
      provider: claude-code
      model: test
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 2
`

// ════════════════════════════════════════════════════════════════════════════
// Test 1: FullPipelineWithCheckpoint
// ════════════════════════════════════════════════════════════════════════════

// TestE2E_FullPipelineWithCheckpoint verifies a complete 2-node linear
// pipeline with runtime events, checkpointing, and artifact creation.
func TestE2E_FullPipelineWithCheckpoint(t *testing.T) {
	ctx := context.Background()

	// Register the skills that the nodes reference so that emitArtifacts
	// and createCheckpoint can resolve them.
	r, es, ss, eng, mgr, _ := newPipeReconcilerWithSkills(t,
		&domain.Skill{
			Name:    "root-cause",
			Version: "1.0",
			Prompt:  "find the root cause of the bug",
			Output:  []string{"rca.md"},
		},
		&domain.Skill{
			Name:    "fix",
			Version: "1.0",
			Prompt:  "implement the fix",
			Output:  []string{"fix.patch"},
		},
	)

	// Create temp files that emitArtifacts can stat.
	rcaFile := filepath.Join(workDir(), "rca.md")
	fixFile := filepath.Join(workDir(), "fix.patch")
	os.WriteFile(rcaFile, []byte("# root cause"), 0644)
	os.WriteFile(fixFile, []byte("--- fix"), 0644)
	t.Cleanup(func() { os.Remove(rcaFile); os.Remove(fixFile) })

	_, wfID := seedPipeline(t, es, ss, r, eng, twoNodeYAML, "t-checkpoint")

	// ── Phase 1: Verify initial events ─────────────────────
	initialTypes := collectStreamEventTypes(t, es, "workflow-"+wfID)
	assertContainsAny(t, initialTypes, "TASK_CREATED")
	assertContainsAny(t, initialTypes, "WORKFLOW_CREATED")

	// ── Phase 2: Start analyze node (reconciler auto-launches runtime) ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 2 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	// Report a tool call event to exercise the runtime event path.
	mgr.ReportEvent(mkInstID("analyze", 1), runtime.RuntimeEvent{
		Type:      runtime.RuntimeEventToolCall,
		Action:    "reading BASH",
		ToolName:  "Bash",
		Timestamp: time.Now(),
	})

	// Verify analyze is RUNNING (reconciler auto-started the instance).
	ns, _ := ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("analyze status = %s, want RUNNING", ns.Status)
	}

	// ── Phase 3: Complete analyze → verify NODE_COMPLETED, CHECKPOINT_CREATED, METRICS_SNAPSHOT, ARTIFACT_CREATED ──
	mgr.MarkExited(mkInstID("analyze", 1), 0, domain.RuntimeMetrics{
		TokensIn: 500, TokensOut: 200, ToolCalls: 3, DurationMs: 15000,
	})

	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 3 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	// Verify events on analyze stream.
	assertStreamHas(t, es, "node-analyze", "NODE_STARTED", "NODE_COMPLETED", "METRICS_SNAPSHOT", "CHECKPOINT_CREATED")

	// Verify ARTIFACT_CREATED was emitted for rca.md.
	artEvt := findEventByType(t, es, "node-analyze", "ARTIFACT_CREATED")
	if artEvt == nil {
		t.Error("expected ARTIFACT_CREATED in node-analyze stream after completion")
	} else {
		var art domain.ArtifactCreated
		json.Unmarshal(artEvt.Payload, &art)
		if art.Name != "rca.md" {
			t.Errorf("artifact name = %q, want rca.md", art.Name)
		}
	}

	// Verify CHECKPOINT_CREATED payload.
	cpEvt := findEventByType(t, es, "node-analyze", "CHECKPOINT_CREATED")
	if cpEvt == nil {
		t.Fatal("expected CHECKPOINT_CREATED")
	}
	var cp domain.CheckpointCreated
	if err := json.Unmarshal(cpEvt.Payload, &cp); err != nil {
		t.Fatalf("unmarshal CheckpointCreated: %v", err)
	}
	if cp.NodeID != "analyze" {
		t.Errorf("checkpoint NodeID = %s, want analyze", cp.NodeID)
	}
	if cp.GitCommit == "" {
		t.Log("GitCommit is empty (may not be in git repo)")
	}

	// Verify METRICS_SNAPSHOT payload.
	metricsEvt := findEventByType(t, es, "node-analyze", "METRICS_SNAPSHOT")
	if metricsEvt == nil {
		t.Fatal("expected METRICS_SNAPSHOT")
	}
	var ms domain.MetricsSnapshot
	if err := json.Unmarshal(metricsEvt.Payload, &ms); err != nil {
		t.Fatalf("unmarshal MetricsSnapshot: %v", err)
	}
	if ms.TokensIn != 500 {
		t.Errorf("TokensIn = %d, want 500", ms.TokensIn)
	}

	// ── Phase 4: Reconcile again → implement should start (dep satisfied, reconciler auto-starts) ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 4 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	// Emit a RUNTIME_ACTION event indirectly by having the reconciler observe
	// a runtime event reported through the manager.
	ch := mgr.Observe(mkInstID("implement", 1))
	defer mgr.StopObserving(mkInstID("implement", 1), ch)

	mgr.ReportEvent(mkInstID("implement", 1), runtime.RuntimeEvent{
		Type:      runtime.RuntimeEventToolCall,
		Action:    "running Edit on auth.go",
		ToolName:  "Edit",
		Timestamp: time.Now(),
	})

	// Verify the observe channel received the event.
	select {
	case rcv := <-ch:
		if rcv.ToolName != "Edit" {
			t.Errorf("observed tool = %q, want Edit", rcv.ToolName)
		}
	case <-time.After(time.Second):
		t.Error("timeout waiting for runtime event on observe channel")
	}

	// Mark implement as exited successfully.
	mgr.MarkExited(mkInstID("implement", 1), 0, domain.RuntimeMetrics{
		TokensIn: 300, TokensOut: 150, ToolCalls: 2, DurationMs: 8000,
	})

	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 5 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "implement")
	if ns.Status != domain.NodeStatusCompleted {
		t.Errorf("implement status = %s, want COMPLETED", ns.Status)
	}
	assertStreamHas(t, es, "node-implement", "NODE_COMPLETED", "CHECKPOINT_CREATED", "ARTIFACT_CREATED")

	// ── Phase 6: Final reconcile → workflow completes ───────
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 6 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusCompleted {
		t.Fatalf("workflow status = %s, want COMPLETED", wf.Status)
	}
	assertStreamHas(t, es, "workflow-"+wfID, "TASK_COMPLETED")

	// Verify node checkpoint data in state.
	ns, _ = ss.GetNodeState(ctx, "analyze")
	if ns.Checkpoint == nil {
		t.Error("analyze checkpoint is nil in state projection")
	} else if len(ns.Checkpoint.Artifacts) > 0 {
		t.Logf("analyze checkpoint artifacts: %v", ns.Checkpoint.Artifacts)
	}

	t.Logf("FullPipelineWithCheckpoint passed: workflow=%s completed", wfID)
}

// ════════════════════════════════════════════════════════════════════════════
// Test 2: RetryFlow
// ════════════════════════════════════════════════════════════════════════════

// TestE2E_RetryFlow verifies retry behavior: a node with max_retries=2 fails
// twice (retryable), then fails a third time (non-retryable), leading to
// workflow failure.
func TestE2E_RetryFlow(t *testing.T) {
	ctx := context.Background()

	r, es, ss, eng, mgr, _ := newPipeReconcilerWithRuntime(t)

	_, wfID := seedPipeline(t, es, ss, r, eng, retryFlowYAML, "t-retry")

	// ── Attempt 1: Start (reconciler auto-launches runtime) and fail (retryable) ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Attempt 1 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("step1 status = %s, want RUNNING", ns.Status)
	}

	// Fail — exit code 1, retryCount=0, maxRetries=2 → retryable.
	mgr.MarkExited(mkInstID("step1", 1), 1, domain.RuntimeMetrics{})
	// Single reconcile: reap detects exit → NODE_FAILED(retryable) → node READY → Evaluate → RETRY_NODE → auto-launch rt-step1-2.
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Reconcile after fail 1: %v", err)
	}
	ss.Rebuild(ctx)

	// After one reconcile tick: fail was reaped AND retry was auto-started.
	// The node should be RUNNING again (with retryCount=1 from the failed attempt).
	ns, _ = ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("after fail 1 + retry: step1 status = %s, want RUNNING", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Fatalf("after fail 1: retryCount = %d, want 1", ns.RetryCount)
	}
	if ns.SessionID == "" {
		t.Fatal("session ID should be set")
	}

	// Verify NODE_FAILED with Retryable=true was emitted.
	failEvt := findEventByType(t, es, "node-step1", "NODE_FAILED")
	if failEvt == nil {
		t.Fatal("expected at least 1 NODE_FAILED event")
	}
	var nf domain.NodeFailed
	if err := json.Unmarshal(failEvt.Payload, &nf); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !nf.Retryable {
		t.Error("first NODE_FAILED should be retryable (retryCount=0 < maxRetries=2)")
	}

	// ── Attempt 2: Fail again, auto-retry ──
	// The reconciler already launched rt-step1-2 in the previous reconcile.
	mgr.MarkExited(mkInstID("step1", 2), 1, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Reconcile after fail 2: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("after fail 2 + retry: step1 status = %s, want RUNNING", ns.Status)
	}
	if ns.RetryCount != 2 {
		t.Fatalf("after fail 2: retryCount = %d, want 2", ns.RetryCount)
	}

	// ── Attempt 3: Fail again, retries exhausted (non-retryable) ──
	mgr.MarkExited(mkInstID("step1", 3), 1, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Reconcile after fail 3: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusFailed {
		t.Fatalf("after fail 3: step1 status = %s, want FAILED", ns.Status)
	}
	if ns.RetryCount != 3 {
		t.Fatalf("after fail 3: retryCount = %d, want 3", ns.RetryCount)
	}

	// Verify second NODE_FAILED is non-retryable.
	allEvents, _ := es.Read(ctx, "node-step1", 0)
	var lastNF *store.Event
	for i := len(allEvents) - 1; i >= 0; i-- {
		if string(allEvents[i].Type) == "NODE_FAILED" {
			lastNF = &allEvents[i]
			break
		}
	}
	if lastNF == nil {
		t.Fatal("no NODE_FAILED event found")
	}
	var lastNFPayload domain.NodeFailed
	json.Unmarshal(lastNF.Payload, &lastNFPayload)
	if lastNFPayload.Retryable {
		t.Error("final NODE_FAILED should be non-retryable (retries exhausted)")
	}

	// ── Final reconcile → FAIL_WORKFLOW ──────────────────────
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Final reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusFailed {
		t.Fatalf("workflow status = %s, want FAILED", wf.Status)
	}
	assertStreamHas(t, es, "workflow-"+wfID, "TASK_FAILED")

	t.Logf("RetryFlow passed: workflow=%s failed after 3 attempts", wfID)
}

// ════════════════════════════════════════════════════════════════════════════
// Test 3: HumanApprovalGate
// ════════════════════════════════════════════════════════════════════════════

// TestE2E_HumanApprovalGate verifies that when a node has gate: human_approval,
// the reconciler pauses it (WAITING state) rather than starting it. The
// workflow stays ACTIVE and does NOT complete until the gate is approved.
func TestE2E_HumanApprovalGate(t *testing.T) {
	ctx := context.Background()

	r, es, ss, eng, mgr, _ := newPipeReconcilerWithRuntime(t)

	_, wfID := seedPipeline(t, es, ss, r, eng, gateYAML, "t-gate")

	// ── Phase 1: Start step1 (no gate, no deps — reconciler auto-launches) ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 1 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("step1 status = %s, want RUNNING", ns.Status)
	}

	// ── Phase 2: Complete step1 ─────────────────────────────────
	mgr.MarkExited(mkInstID("step1", 1), 0, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 2 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusCompleted {
		t.Fatalf("step1 status = %s, want COMPLETED", ns.Status)
	}

	// ── Phase 3: Reconcile → step2 enters WAITING (gate), not RUNNING ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 3 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "step2")
	if ns.Status != domain.NodeStatusWaiting {
		t.Fatalf("step2 status = %s, want WAITING (human_approval gate)", ns.Status)
	}

	// Verify NODE_WAITING event is emitted, NODE_STARTED is NOT.
	assertStreamHas(t, es, "node-step2", "NODE_WAITING")
	assertStreamNotHas(t, es, "node-step2", "NODE_STARTED")

	// ── Phase 4: Verify workflow is still ACTIVE (not COMPLETED) ──
	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusActive {
		t.Fatalf("workflow status = %s, want ACTIVE (awaiting gate approval)", wf.Status)
	}

	// Verify no TASK_COMPLETED or TASK_FAILED.
	assertStreamNotHas(t, es, "workflow-"+wfID, "TASK_COMPLETED", "TASK_FAILED")

	t.Logf("HumanApprovalGate passed: step2 is WAITING, workflow is ACTIVE awaiting approval")
}

// ════════════════════════════════════════════════════════════════════════════
// Test 4: DAGWithMultipleDeps
// ════════════════════════════════════════════════════════════════════════════

// TestE2E_DAGWithMultipleDeps verifies DAG dependency resolution: node C
// depends on both A and B, and only starts when BOTH are completed.
func TestE2E_DAGWithMultipleDeps(t *testing.T) {
	ctx := context.Background()

	r, es, ss, eng, mgr, _ := newPipeReconcilerWithRuntime(t)

	_, wfID := seedPipeline(t, es, ss, r, eng, dagMultiDepYAML, "t-dag")

	// ── Phase 1: Start both A and B (no deps, max_concurrent=2 — reconciler auto-launches) ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 1 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	nsA, _ := ss.GetNodeState(ctx, "a")
	nsB, _ := ss.GetNodeState(ctx, "b")
	if nsA.Status != domain.NodeStatusRunning {
		t.Fatalf("node a status = %s, want RUNNING", nsA.Status)
	}
	if nsB.Status != domain.NodeStatusRunning {
		t.Fatalf("node b status = %s, want RUNNING", nsB.Status)
	}

	// ── Phase 2: Complete A — C should NOT start (B still running) ──
	mgr.MarkExited(mkInstID("a", 1), 0, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 2 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	nsC, _ := ss.GetNodeState(ctx, "c")
	if nsC.Status != domain.NodeStatusPending {
		t.Errorf("node c status = %s, want PENDING (B not yet completed)", nsC.Status)
	}

	// ── Phase 3: Complete B — C SHOULD start now (reconciler auto-launches) ──
	mgr.MarkExited(mkInstID("b", 1), 0, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 3 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	nsC, _ = ss.GetNodeState(ctx, "c")
	if nsC.Status != domain.NodeStatusRunning {
		t.Fatalf("node c status = %s, want RUNNING (both deps satisfied)", nsC.Status)
	}

	// ── Phase 4: Complete C → workflow completes ──
	mgr.MarkExited(mkInstID("c", 1), 0, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 4 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	nsC, _ = ss.GetNodeState(ctx, "c")
	if nsC.Status != domain.NodeStatusCompleted {
		t.Fatalf("node c status = %s, want COMPLETED", nsC.Status)
	}

	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Final reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusCompleted {
		t.Fatalf("workflow status = %s, want COMPLETED", wf.Status)
	}

	t.Logf("DAGWithMultipleDeps passed: A and B completed before C, workflow done")
}

// ════════════════════════════════════════════════════════════════════════════
// Test 5: ConcurrentNodes
// ════════════════════════════════════════════════════════════════════════════

// TestE2E_ConcurrentNodes verifies that when nodes have no dependencies,
// both transition to RUNNING concurrently (max_concurrent_nodes: 2).
func TestE2E_ConcurrentNodes(t *testing.T) {
	ctx := context.Background()

	r, es, ss, eng, mgr, adapter := newPipeReconcilerWithRuntime(t)

	_, wfID := seedPipeline(t, es, ss, r, eng, concurrentYAML, "t-conc")

	// ── Phase 1: Reconcile → both A and B should start (reconciler auto-launches) ──
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 1 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	nsA, _ := ss.GetNodeState(ctx, "a")
	nsB, _ := ss.GetNodeState(ctx, "b")

	if nsA.Status != domain.NodeStatusRunning {
		t.Fatalf("node a status = %s, want RUNNING", nsA.Status)
	}
	if nsB.Status != domain.NodeStatusRunning {
		t.Fatalf("node b status = %s, want RUNNING", nsB.Status)
	}

	// Verify both instances were started via the adapter's started channel.
	started := make(map[string]bool)
drainLoop:
	for {
		select {
		case id := <-adapter.started:
			started[id] = true
		case <-time.After(100 * time.Millisecond):
			break drainLoop
		}
	}

	// Both rt-a-1 and rt-b-1 should have been started by the reconciler.
	if !started[mkInstID("a", 1)] {
		t.Error("runtime instance rt-a-1 was not started by the reconciler")
	}
	if !started[mkInstID("b", 1)] {
		t.Error("runtime instance rt-b-1 was not started by the reconciler")
	}

	// ── Phase 2: Complete A → workflow should NOT complete (B still running) ──
	mgr.MarkExited(mkInstID("a", 1), 0, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 2 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusActive {
		t.Fatalf("workflow status = %s, want ACTIVE (B still running)", wf.Status)
	}

	// ── Phase 3: Complete B → workflow completes ──
	mgr.MarkExited(mkInstID("b", 1), 0, domain.RuntimeMetrics{})
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Phase 3 reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	// Final reconcile to complete the workflow.
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Final reconcile: %v", err)
	}
	ss.Rebuild(ctx)

	wf, _ = ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusCompleted {
		t.Fatalf("workflow status = %s, want COMPLETED", wf.Status)
	}
	assertStreamHas(t, es, "workflow-"+wfID, "TASK_COMPLETED")

	t.Logf("ConcurrentNodes passed: both A and B ran concurrently, workflow completed")
}

// assertContainsAny checks a string slice for a value (mirrors assertContains
// but exists here to avoid name collision with the gRPC helper above).
func assertContainsAny(t *testing.T, slice []string, want string) {
	t.Helper()
	for _, s := range slice {
		if s == want {
			return
		}
	}
	t.Errorf("expected %q in %v", want, slice)
}
