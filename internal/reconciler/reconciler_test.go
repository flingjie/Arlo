package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"github.com/lingjiefan/arlo/internal/workflow"
)

var eventSeq atomic.Int64

func nextEventID() string {
	return fmt.Sprintf("evt-%d", eventSeq.Add(1))
}

// newTestReconciler sets up the full stack for testing.
func newTestReconciler(t *testing.T) (*Reconciler, *store.SQLiteStore, *state.InMemoryStateStore, *workflow.Engine) {
	t.Helper()

	es, err := store.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	ss := state.NewInMemoryStateStore(es)
	eng := workflow.NewEngine()
	r := New(ss, es, eng, nil, nil).WithTickInterval(100 * time.Millisecond)

	return r, es, ss, eng
}

// seedWorkflow creates a workflow in the event store and registers it with the reconciler.
func seedWorkflow(t *testing.T, es *store.SQLiteStore, ss *state.InMemoryStateStore, r *Reconciler, eng *workflow.Engine, yamlSource string, taskID string) (*domain.ExecutableGraph, string) {
	t.Helper()
	ctx := context.Background()

	graph, err := eng.Compile(ctx, []byte(yamlSource))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	wfID := "wf-" + taskID

	// Seed initial events.
	appendEvent(t, es, "workflow-"+wfID, store.EventTaskCreated, domain.TaskCreated{
		TaskID: taskID, Title: "test task", CreatedBy: "tester", WorkflowID: wfID,
	})
	appendEvent(t, es, "workflow-"+wfID, store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: wfID, TaskID: taskID, GraphName: graph.Name, Version: graph.Version,
	})

	// Create node events for each node.
	for _, n := range graph.Nodes {
		appendEvent(t, es, "node-"+n.ID, store.EventNodeCreated, domain.NodeCreated{
			NodeID: n.ID, WorkflowID: wfID, SkillName: n.SkillRef.Name, Runtime: n.Runtime.Provider,
		})
	}

	// Register the graph with the reconciler.
	r.Submit(wfID, graph)

	// Build projections.
	if err := ss.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	return graph, wfID
}

// helper to append an event.
func appendEvent(t *testing.T, es *store.SQLiteStore, streamID string, eventType store.EventType, payload interface{}) {
	t.Helper()
	data, _ := json.Marshal(payload)
	_, err := es.Append(context.Background(), streamID, []store.Event{{
		ID:      nextEventID(),
		Type:    eventType,
		Payload: data,
	}})
	if err != nil {
		t.Fatalf("Append event %s to %s: %v", eventType, streamID, err)
	}
}

// TestReconcileStartNode verifies the reconciler starts a pending node.
func TestReconcileStartNode(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Before reconcile: analyze should be PENDING.
	nsBefore, _ := ss.GetNodeState(ctx, "analyze")
	if nsBefore.Status != domain.NodeStatusPending {
		t.Fatalf("expected PENDING before reconcile, got %s", nsBefore.Status)
	}

	// Reconcile: analyze has no deps → should get START_NODE.
	if err := r.Reconcile(ctx, wfID); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Rebuild projections to see the new events.
	ss.Rebuild(ctx)

	// After reconcile: analyze should be RUNNING.
	nsAfter, _ := ss.GetNodeState(ctx, "analyze")
	if nsAfter.Status != domain.NodeStatusRunning {
		t.Errorf("expected RUNNING after reconcile, got %s", nsAfter.Status)
	}
	if nsAfter.SessionID == "" {
		t.Error("session ID should be set")
	}
}

// TestReconcileIdempotent verifies reconciling twice does not double-start a node.
func TestReconcileIdempotent(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// First reconcile: starts analyze.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns1, _ := ss.GetNodeState(ctx, "analyze")
	session1 := ns1.SessionID

	// Second reconcile: should be a no-op (analyze already RUNNING).
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns2, _ := ss.GetNodeState(ctx, "analyze")
	if ns2.SessionID != session1 {
		t.Errorf("session changed from %s to %s — reconcile was not idempotent", session1, ns2.SessionID)
	}
	if ns2.Status != domain.NodeStatusRunning {
		t.Errorf("status = %s, expected RUNNING", ns2.Status)
	}
}

// TestReconcileChain verifies a full workflow chain: analyze → implement → review.
func TestReconcileChain(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Step 1: Start analyze (no deps).
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("step 1: analyze status = %s, want RUNNING", ns.Status)
	}

	// Step 2: Complete analyze.
	appendEvent(t, es, "node-analyze", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "analyze", SessionID: ns.SessionID, Output: map[string]string{"rca.md": "art-1"},
	})
	ss.Rebuild(ctx)

	// Step 3: Reconcile → implement should now start.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "implement")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("step 3: implement status = %s, want RUNNING", ns.Status)
	}

	// Step 4: Complete implement.
	appendEvent(t, es, "node-implement", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "implement", SessionID: ns.SessionID, Output: map[string]string{"diff.patch": "art-2"},
	})
	ss.Rebuild(ctx)

	// Step 5: Reconcile → review should now start.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "review")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("step 5: review status = %s, want RUNNING", ns.Status)
	}

	// Step 6: Complete review.
	appendEvent(t, es, "node-review", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "review", SessionID: ns.SessionID, Output: map[string]string{"review.md": "art-3"},
	})
	ss.Rebuild(ctx)

	// Step 7: Reconcile → all done → COMPLETE_WORKFLOW.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusCompleted {
		t.Errorf("step 7: workflow status = %s, want COMPLETED", wf.Status)
	}
}

// TestReconcileFailWorkflow verifies a non-retryable failure marks the workflow failed.
func TestReconcileFailWorkflow(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Directly append a non-retryable NODE_FAILED.
	appendEvent(t, es, "node-analyze", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "analyze", SessionID: "sess-fail",
	})
	ss.Rebuild(ctx)

	// Manually set the node to FAILED (non-retryable) via event.
	appendEvent(t, es, "node-analyze", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "analyze", SessionID: "sess-fail", Reason: "fatal error", Retryable: false,
	})
	ss.Rebuild(ctx)

	// Reconcile → should detect permanent failure.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusFailed {
		t.Errorf("workflow status = %s, want FAILED", wf.Status)
	}
}

// TestReconcileNoDecisions verifies that reconciling a terminal workflow is a no-op.
func TestReconcileNoDecisions(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Manually complete everything.
	for _, nodeID := range []string{"analyze", "implement", "review"} {
		appendEvent(t, es, "node-"+nodeID, store.EventNodeCompleted, domain.NodeCompleted{
			NodeID: nodeID, SessionID: "sess-"+nodeID,
		})
	}
	ss.Rebuild(ctx)

	// First reconcile: completes the workflow.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusCompleted {
		t.Fatalf("expected COMPLETED, got %s", wf.Status)
	}

	// Second reconcile: should be a no-op.
	err := r.Reconcile(ctx, wfID)
	if err != nil {
		t.Errorf("reconcile after completion should not error: %v", err)
	}
}

// TestReconcileMultipleWorkflows verifies the reconciler handles multiple active workflows.
func TestReconcileMultipleWorkflows(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	// Use unique YAMLs with unique node IDs per workflow.
	yaml1 := `
name: wf1
version: 1
nodes:
  - id: analyze-1
    skill: root-cause
    runtime:
      provider: claude-code
    retry:
      max_retries: 1
      backoff: 10s
  - id: implement-1
    skill: implement-fix
    runtime:
      provider: claude-code
    depends_on:
      - analyze-1
    retry:
      max_retries: 1
`
	yaml2 := `
name: wf2
version: 1
nodes:
  - id: analyze-2
    skill: root-cause
    runtime:
      provider: claude-code
    retry:
      max_retries: 1
      backoff: 10s
  - id: implement-2
    skill: implement-fix
    runtime:
      provider: claude-code
    depends_on:
      - analyze-2
    retry:
      max_retries: 1
`

	_, wf1 := seedWorkflow(t, es, ss, r, eng, yaml1, "t1")
	_, wf2 := seedWorkflow(t, es, ss, r, eng, yaml2, "t2")

	// Reconcile both.
	r.Reconcile(ctx, wf1)
	r.Reconcile(ctx, wf2)
	ss.Rebuild(ctx)

	// Both should have their first node running.
	for _, wfID := range []string{wf1, wf2} {
		wf, _ := ss.GetWorkflow(ctx, wfID)
		if len(wf.Nodes) != 2 {
			t.Errorf("%s: expected 2 nodes, got %d", wfID, len(wf.Nodes))
		}

		// Find the analyze node (unique per workflow).
		for _, ns := range wf.Nodes {
			if ns.Status != domain.NodeStatusRunning && ns.Status != domain.NodeStatusPending {
				t.Errorf("%s: %s has unexpected status %s", wfID, ns.NodeID, ns.Status)
			}
		}
	}
}

// TestReconcileRetryableFailure verifies retryable failure → READY → restarted.
func TestReconcileRetryableFailure(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Start analyze.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Fail it retryably.
	ns, _ := ss.GetNodeState(ctx, "analyze")
	appendEvent(t, es, "node-analyze", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "analyze", SessionID: ns.SessionID, Reason: "OOM", Retryable: true,
	})
	ss.Rebuild(ctx)

	// After rebuild: analyze should be READY (projection sets it).
	ns, _ = ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusReady {
		t.Fatalf("after retryable failure: status = %s, want READY", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Fatalf("retry count = %d, want 1", ns.RetryCount)
	}

	// Reconcile should re-start it (new session).
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusRunning {
		t.Errorf("after re-start: status = %s, want RUNNING", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Errorf("retry count should still be 1 after restart, got %d", ns.RetryCount)
	}
}

// TestReconcilerStartStop verifies the Start/Stop lifecycle.
func TestReconcilerStartStop(t *testing.T) {
	_, es, ss, eng := newTestReconciler(t)
	r := New(ss, es, eng, nil, nil).WithTickInterval(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())

	// Start the reconciler in the background.
	done := make(chan error, 1)
	go func() {
		done <- r.Start(ctx)
	}()

	// Let it tick a few times.
	time.Sleep(200 * time.Millisecond)

	// Stop.
	cancel()

	// Wait for graceful shutdown.
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reconciler did not stop within timeout")
	}
}

// TestReconcileUnknownWorkflow verifies error handling for unknown workflow IDs.
func TestReconcileUnknownWorkflow(t *testing.T) {
	ctx := context.Background()
	r, _, _, _ := newTestReconciler(t)

	err := r.Reconcile(ctx, "nonexistent-workflow")
	if err == nil {
		t.Fatal("expected error for unknown workflow")
	}
}

// TestReconcileWithoutGraph verifies error when graph is not registered.
func TestReconcileWithoutGraph(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	// Seed a workflow but DON'T register the graph.
	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))
	wfID := "wf-nograph"

	appendEvent(t, es, "workflow-"+wfID, store.EventTaskCreated, domain.TaskCreated{
		TaskID: "t1", Title: "test", CreatedBy: "tester", WorkflowID: wfID,
	})
	appendEvent(t, es, "workflow-"+wfID, store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: wfID, TaskID: "t1", GraphName: graph.Name, Version: 1,
	})
	for _, n := range graph.Nodes {
		appendEvent(t, es, "node-"+n.ID, store.EventNodeCreated, domain.NodeCreated{
			NodeID: n.ID, WorkflowID: wfID, SkillName: n.SkillRef.Name, Runtime: n.Runtime.Provider,
		})
	}
	ss.Rebuild(ctx)

	// Should fail because graph is not registered.
	err := r.Reconcile(ctx, wfID)
	if err == nil {
		t.Fatal("expected error for missing graph")
	}
}

// ── Test YAML ────────────────────────────────────

const bugfixYAML = `
name: bugfix
version: 1

nodes:
  - id: analyze
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    retry:
      max_retries: 1
      backoff: 10s

  - id: implement
    skill: implement-fix
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    depends_on:
      - analyze
    retry:
      max_retries: 2
      backoff: 30s

  - id: review
    skill: code-review
    runtime:
      provider: claude-code
      model: claude-haiku-3.5
    depends_on:
      - implement
    gate: human_approval
    retry:
      max_retries: 1

policy:
  max_concurrent_nodes: 1
`
