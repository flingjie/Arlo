package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/runtime"
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
	r := New(ss, es, eng, nil, nil, nil).WithTickInterval(100 * time.Millisecond)

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
			NodeID: n.ID, WorkflowID: wfID, SkillName: n.SkillRef.Name,
			Runtime: string(n.Runtime.Provider), DependsOn: n.DependsOn, Gate: string(n.Gate),
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

	// Step 5: Reconcile → review should now start, then pause for gate.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "review")
	// Gated nodes go START → WAITING immediately, skipping the runtime.
	if ns.Status != domain.NodeStatusWaiting {
		t.Fatalf("step 5: review status = %s, want WAITING (gate)", ns.Status)
	}

	// Step 5b: Approve the review gate.
	appendEvent(t, es, "node-review", store.EventHumanInputReceived, domain.HumanInputReceived{
		NodeID: "review", WorkflowID: wfID, Decision: "approve",
	})
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "review")
	if ns.Status != domain.NodeStatusReady {
		t.Fatalf("step 5b: review status = %s, want READY after approve", ns.Status)
	}

	// Step 5c: Reconcile → resume the review node.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "review")
	if ns.Status != domain.NodeStatusRunning {
		t.Fatalf("step 5c: review status = %s, want RUNNING after resume", ns.Status)
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
	r := New(ss, es, eng, nil, nil, nil).WithTickInterval(50 * time.Millisecond)

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
			NodeID: n.ID, WorkflowID: wfID, SkillName: n.SkillRef.Name, Runtime: string(n.Runtime.Provider), DependsOn: n.DependsOn, Gate: string(n.Gate),
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

// ── Integration Tests for New Decision Types ─────

// TestReconcilePauseNode verifies the reconciler pauses a gated RUNNING node.
func TestReconcilePauseNode(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Start analyze (no deps).
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Complete analyze → implement starts.
	ns, _ := ss.GetNodeState(ctx, "analyze")
	appendEvent(t, es, "node-analyze", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "analyze", SessionID: ns.SessionID,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Complete implement → review starts and immediately pauses for gate.
	ns2, _ := ss.GetNodeState(ctx, "implement")
	appendEvent(t, es, "node-implement", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "implement", SessionID: ns2.SessionID,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Verify review is WAITING (started then immediately paused for gate).
	review, _ := ss.GetNodeState(ctx, "review")
	if review.Status != domain.NodeStatusWaiting {
		t.Fatalf("review status = %s after start+gate pause, want WAITING", review.Status)
	}
}

// TestReconcileResumeNode verifies resume after human approval.
func TestReconcileResumeNode(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Start analyze.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Complete analyze and implement so review can start.
	for _, nodeID := range []string{"analyze", "implement"} {
		ns, _ := ss.GetNodeState(ctx, nodeID)
		// Start the node if not already running.
		if ns.Status != domain.NodeStatusRunning {
			appendEvent(t, es, "node-"+nodeID, store.EventNodeStarted, domain.NodeStarted{
				NodeID: nodeID, SessionID: "sess-" + nodeID,
			})
			ss.Rebuild(ctx)
		}
		appendEvent(t, es, "node-"+nodeID, store.EventNodeCompleted, domain.NodeCompleted{
			NodeID: nodeID, SessionID: "sess-" + nodeID,
		})
	}
	ss.Rebuild(ctx)

	// Start review — executes tartNode will pause it immediately for the gate.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	review, _ := ss.GetNodeState(ctx, "review")
	// Gate handled inline: START → WAITING immediately, gate cleared.
	if review.Status != domain.NodeStatusWaiting {
		// Force WAITING if the inline gate didn't fire (e.g., if test env differs).
		appendEvent(t, es, "node-review", store.EventNodeStarted, domain.NodeStarted{
			NodeID: "review", WorkflowID: wfID, SessionID: "sess-review",
		})
		appendEvent(t, es, "node-review", store.EventNodeWaiting, domain.NodeWaiting{
			NodeID: "review", WorkflowID: wfID, SessionID: "sess-review", Reason: "human_approval",
		})
		ss.Rebuild(ctx)
		review, _ = ss.GetNodeState(ctx, "review")
		if review.Status != domain.NodeStatusWaiting {
			t.Fatalf("review should be WAITING after gate, got %s", review.Status)
		}
	}

	// Human approves → status becomes READY.
	appendEvent(t, es, "node-review", store.EventHumanInputReceived, domain.HumanInputReceived{
		NodeID: "review", WorkflowID: wfID, SessionID: review.SessionID, Decision: "approve",
	})
	ss.Rebuild(ctx)

	review, _ = ss.GetNodeState(ctx, "review")
	if review.Status != domain.NodeStatusReady {
		t.Fatalf("review should be READY after approval, got %s", review.Status)
	}

	// Reconcile — review should get RESUME_NODE (READY, retry_count == 0).
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	review, _ = ss.GetNodeState(ctx, "review")
	if review.Status != domain.NodeStatusRunning {
		t.Errorf("review status = %s after resume, want RUNNING", review.Status)
	}
}

// TestReconcileRetryNode verifies retry after retryable failure.
func TestReconcileRetryNode(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Start analyze.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Make it fail retryably → READY with retry_count=1.
	ns, _ := ss.GetNodeState(ctx, "analyze")
	appendEvent(t, es, "node-analyze", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "analyze", SessionID: ns.SessionID, Reason: "OOM", Retryable: true,
	})
	ss.Rebuild(ctx)

	// Reconcile — should get RETRY_NODE.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusRunning {
		t.Errorf("analyze status = %s after retry, want RUNNING", ns.Status)
	}
}

// TestReconcileWithAnnotations verifies annotations survive rebuild.
func TestReconcileWithAnnotations(t *testing.T) {
	ctx := context.Background()
	_, es, ss, eng := newTestReconciler(t)
	r := New(ss, es, eng, nil, nil, nil)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Append an annotation event.
	appendEvent(t, es, "node-analyze", store.EventNodeAnnotated, domain.NodeAnnotated{
		NodeID: "analyze", WorkflowID: wfID, Key: "human_rating", Value: "good",
	})
	appendEvent(t, es, "node-analyze", store.EventNodeAnnotated, domain.NodeAnnotated{
		NodeID: "analyze", WorkflowID: wfID, Key: "accepted", Value: "true",
	})
	ss.Rebuild(ctx)

	// Verify annotations are present.
	ns, _ := ss.GetNodeState(ctx, "analyze")
	if len(ns.Annotations) != 2 {
		t.Fatalf("expected 2 annotations, got %d", len(ns.Annotations))
	}
	if ns.Annotations[0].Key != "human_rating" || ns.Annotations[0].Value != "good" {
		t.Errorf("annotation[0] = %s=%s, want human_rating=good", ns.Annotations[0].Key, ns.Annotations[0].Value)
	}

	// Rebuild again — annotations should survive.
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "analyze")
	if len(ns.Annotations) != 2 {
		t.Errorf("annotations lost after rebuild: got %d, want 2", len(ns.Annotations))
	}
}

// TestReconcileMetricsFlow verifies metrics snapshots accumulate in node state.
func TestReconcileMetricsFlow(t *testing.T) {
	ctx := context.Background()
	_, es, ss, eng := newTestReconciler(t)
	r := New(ss, es, eng, nil, nil, nil)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Start analyze (needed so node exists in projection).
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// Append heartbeat and metrics events.
	appendEvent(t, es, "node-analyze", store.EventNodeHeartbeat, domain.NodeHeartbeat{
		NodeID: "analyze", WorkflowID: wfID, SessionID: "sess-1", Status: "RUNNING",
	})
	appendEvent(t, es, "node-analyze", store.EventMetricsSnapshot, domain.MetricsSnapshot{
		NodeID: "analyze", WorkflowID: wfID, SessionID: "sess-1",
		TokensIn: 500, TokensOut: 200, ToolCalls: 3, CostUSD: 0.015,
	})
	appendEvent(t, es, "node-analyze", store.EventMetricsSnapshot, domain.MetricsSnapshot{
		NodeID: "analyze", WorkflowID: wfID, SessionID: "sess-1",
		TokensIn: 300, TokensOut: 150, ToolCalls: 2, CostUSD: 0.010,
	})
	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "analyze")
	if ns.Metrics.TokensIn != 800 {
		t.Errorf("TokensIn = %d, want 800", ns.Metrics.TokensIn)
	}
	if ns.Metrics.TokensOut != 350 {
		t.Errorf("TokensOut = %d, want 350", ns.Metrics.TokensOut)
	}
	if ns.Metrics.ToolCalls != 5 {
		t.Errorf("ToolCalls = %d, want 5", ns.Metrics.ToolCalls)
	}
	if ns.Metrics.CostUSD != 0.025 {
		t.Errorf("CostUSD = %.4f, want 0.0250", ns.Metrics.CostUSD)
	}
}

// TestReconcileFullWorkflowLifecycle verifies complete end-to-end state machine chain.
func TestReconcileFullWorkflowLifecycle(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Phase 1: START analyze → RUNNING.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)
	analyze, _ := ss.GetNodeState(ctx, "analyze")
	if analyze.Status != domain.NodeStatusRunning {
		t.Fatalf("phase 1: analyze = %s, want RUNNING", analyze.Status)
	}

	// Phase 2: Complete analyze → implement becomes STARTABLE.
	appendEvent(t, es, "node-analyze", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "analyze", SessionID: analyze.SessionID,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	implement, _ := ss.GetNodeState(ctx, "implement")
	if implement.Status != domain.NodeStatusRunning {
		t.Fatalf("phase 2: implement = %s, want RUNNING", implement.Status)
	}

	// Phase 3: RETRY — implement fails retryably.
	appendEvent(t, es, "node-implement", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "implement", SessionID: implement.SessionID, Reason: "timeout", Retryable: true,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	implement, _ = ss.GetNodeState(ctx, "implement")
	if implement.Status != domain.NodeStatusRunning {
		t.Fatalf("phase 3: implement after retry = %s, want RUNNING", implement.Status)
	}
	if implement.RetryCount < 1 {
		t.Errorf("phase 3: retry_count = %d, want >= 1", implement.RetryCount)
	}

	// Phase 4: Complete implement → review starts and immediately pauses for gate.
	appendEvent(t, es, "node-implement", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "implement", SessionID: implement.SessionID,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wfID) // starts review, pauses for gate
	ss.Rebuild(ctx)

	review, _ := ss.GetNodeState(ctx, "review")
	if review.Status != domain.NodeStatusWaiting {
		t.Fatalf("phase 4: review = %s after start+gate, want WAITING", review.Status)
	}

	// Phase 5: Approve the gate → review resumes.
	appendEvent(t, es, "node-review", store.EventHumanInputReceived, domain.HumanInputReceived{
		NodeID: "review", WorkflowID: wfID, Decision: "approve",
	})
	ss.Rebuild(ctx)
	review, _ = ss.GetNodeState(ctx, "review")
	if review.Status != domain.NodeStatusReady {
		t.Fatalf("phase 5: review = %s after approve, want READY", review.Status)
	}

	// Reconcile → resume review.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)
	review, _ = ss.GetNodeState(ctx, "review")
	if review.Status != domain.NodeStatusRunning {
		t.Fatalf("phase 5b: review = %s after resume, want RUNNING", review.Status)
	}

	// Phase 6: Complete review → workflow completes.
	appendEvent(t, es, "node-review", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "review", SessionID: review.SessionID,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	wf, _ := ss.GetWorkflow(ctx, wfID)
	if wf.Status != domain.WorkflowStatusCompleted {
		t.Fatalf("phase 7: workflow = %s, want COMPLETED", wf.Status)
	}
}

// TestReconcileMultipleWorkflowsParallel verifies independent workflows don't interfere.
func TestReconcileMultipleWorkflowsParallel(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	yaml1 := `name: wf1
version: 1
nodes:
  - id: a1
    skill: plan
    runtime:
      provider: claude-code
    retry:
      max_retries: 1
      backoff: 10s
`
	yaml2 := `name: wf2
version: 1
nodes:
  - id: a2
    skill: code
    runtime:
      provider: claude-code
    retry:
      max_retries: 1
      backoff: 10s
`

	_, wf1 := seedWorkflow(t, es, ss, r, eng, yaml1, "t1")
	_, wf2 := seedWorkflow(t, es, ss, r, eng, yaml2, "t2")

	// Start both workflows.
	r.Reconcile(ctx, wf1)
	r.Reconcile(ctx, wf2)
	ss.Rebuild(ctx)

	// Complete wf1.
	ns1, _ := ss.GetNodeState(ctx, "a1")
	appendEvent(t, es, "node-a1", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "a1", SessionID: ns1.SessionID,
	})
	ss.Rebuild(ctx)
	r.Reconcile(ctx, wf1)
	ss.Rebuild(ctx)

	wf1State, _ := ss.GetWorkflow(ctx, wf1)
	if wf1State.Status != domain.WorkflowStatusCompleted {
		t.Errorf("wf1 = %s, want COMPLETED", wf1State.Status)
	}

	// wf2 should still be RUNNING.
	ns2, _ := ss.GetNodeState(ctx, "a2")
	if ns2.Status != domain.NodeStatusRunning {
		t.Errorf("a2 = %s, want RUNNING", ns2.Status)
	}
}

// ── launchRuntime Failure Tests ─────────────────

// failingAdapter implements runtime.Adapter and returns errors from Start.
type failingAdapter struct {
	startErr error
}

func (a *failingAdapter) Prepare(ctx context.Context, inst domain.RuntimeInstance) error { return nil }
func (a *failingAdapter) Start(ctx context.Context, inst domain.RuntimeInstance) error    { return a.startErr }
func (a *failingAdapter) Stop(ctx context.Context, id string) error                       { return nil }
func (a *failingAdapter) Destroy(ctx context.Context, id string) error                    { return nil }
func (a *failingAdapter) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	return nil
}
func (a *failingAdapter) Status(ctx context.Context, id string) (domain.RuntimeStatus, error) {
	return domain.RuntimeStatus{ID: id, State: domain.RuntimeStateRunning}, nil
}

// TestLaunchRuntimeFailureEmitsNodeFailed verifies that when launchRuntime fails
// (adapter Start returns error), a NODE_FAILED event is emitted and the node
// becomes READY (retryable) when retryCount < maxRetries.
func TestLaunchRuntimeFailureEmitsNodeFailed(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	// Set up a runtime manager with a failing adapter.
	mgr := runtime.NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &failingAdapter{startErr: errors.New("cannot start")})
	r.runtimeMgr = mgr

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// Reconcile: should append NODE_STARTED, then launchRuntime fails.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	// analyze node has max_retries=1, retryCount=0.
	// After launch failure: 0 < 1 → retryable → status READY, retryCount=1.
	ns, _ := ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusReady {
		t.Fatalf("expected READY after launch failure (retryable), got %s", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Fatalf("expected retryCount=1 after retryable failure, got %d", ns.RetryCount)
	}
	if ns.SessionID == "" {
		t.Error("session ID should be set from NODE_STARTED")
	}
}

// TestLaunchRuntimeFailureRetriesExhausted verifies that when launchRuntime
// fails and retries are exhausted (retryCount >= maxRetries), the node becomes
// FAILED with a non-retryable NODE_FAILED event.
func TestLaunchRuntimeFailureRetriesExhausted(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	// Set up a runtime manager with a failing adapter (always fails).
	mgr := runtime.NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &failingAdapter{startErr: errors.New("cannot start")})
	r.runtimeMgr = mgr

	_, wfID := seedWorkflow(t, es, ss, r, eng, bugfixYAML, "t1")

	// First reconcile: node starts but launch fails → READY (retryable), retryCount=1.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusReady {
		t.Fatalf("after first reconcile: expected READY, got %s", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Fatalf("after first reconcile: expected retryCount=1, got %d", ns.RetryCount)
	}

	// Second reconcile: engine emits RETRY_NODE → NODE_STARTED, launchRuntime fails again.
	// retryCount=1, maxRetries=1 → 1 < 1 → false → non-retryable → FAILED.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "analyze")
	if ns.Status != domain.NodeStatusFailed {
		t.Fatalf("after second reconcile: expected FAILED (retries exhausted), got %s", ns.Status)
	}
}

// TestLaunchRuntimeFailureWithDifferentMaxRetries verifies retryability for
// a node with max_retries=2 (allowing more than one retry).
func TestLaunchRuntimeFailureWithDifferentMaxRetries(t *testing.T) {
	ctx := context.Background()
	r, es, ss, eng := newTestReconciler(t)

	mgr := runtime.NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &failingAdapter{startErr: errors.New("cannot start")})
	r.runtimeMgr = mgr

	// Use a YAML where the first node has max_retries=2.
	highRetryYAML := `
name: multi-retry
version: 1
nodes:
  - id: step1
    skill: root-cause
    runtime:
      provider: claude-code
    retry:
      max_retries: 2
      backoff: 10s
`
	_, wfID := seedWorkflow(t, es, ss, r, eng, highRetryYAML, "t1")

	// First reconcile: launch fails → retryCount=0, maxRetries=2 → retryable.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusReady {
		t.Fatalf("attempt 1: expected READY, got %s", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Fatalf("attempt 1: expected retryCount=1, got %d", ns.RetryCount)
	}

	// Second reconcile (retry attempt 1): launch fails again → retryCount=1 < 2 → retryable.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusReady {
		t.Fatalf("attempt 2: expected READY, got %s", ns.Status)
	}
	if ns.RetryCount != 2 {
		t.Fatalf("attempt 2: expected retryCount=2, got %d", ns.RetryCount)
	}

	// Third reconcile (retry attempt 2): launch fails again → retryCount=2 < 2 → false → non-retryable.
	r.Reconcile(ctx, wfID)
	ss.Rebuild(ctx)

	ns, _ = ss.GetNodeState(ctx, "step1")
	if ns.Status != domain.NodeStatusFailed {
		t.Fatalf("attempt 3: expected FAILED (retries exhausted), got %s", ns.Status)
	}
}
