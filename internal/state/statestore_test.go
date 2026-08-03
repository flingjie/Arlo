package state

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/store"
)

// newTestStateStore creates a StateStore backed by an in-memory SQLite EventStore.
func newTestStateStore(t *testing.T) (*InMemoryStateStore, *store.SQLiteStore) {
	t.Helper()

	es, err := store.NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { es.Close() })

	ss := NewInMemoryStateStore(es)
	return ss, es
}

// helper: append a single event to the store.
func appendEvent(t *testing.T, es *store.SQLiteStore, streamID string, e store.Event) {
	t.Helper()
	_, err := es.Append(context.Background(), streamID, []store.Event{e})
	if err != nil {
		t.Fatalf("Append event %s: %v", e.ID, err)
	}
}

// helper: create an event with a typed payload.
func makeEvent(id string, eventType store.EventType, payload interface{}) store.Event {
	data, _ := json.Marshal(payload)
	return store.Event{
		ID:      id,
		Type:    eventType,
		Payload: json.RawMessage(data),
	}
}

// TestRebuildEmpty verifies Rebuild works when the event store is empty.
func TestRebuildEmpty(t *testing.T) {
	ss, _ := newTestStateStore(t)
	ctx := context.Background()

	if err := ss.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// After rebuild, there should be no active workflows.
	active, err := ss.ListActiveWorkflows(ctx)
	if err != nil {
		t.Fatalf("ListActiveWorkflows: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active workflows, got %d", len(active))
	}
}

// TestRebuildFromEvents verifies that replaying known events produces correct state.
func TestRebuildFromEvents(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	// Write a minimal workflow lifecycle.
	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventTaskCreated, domain.TaskCreated{
		TaskID: "task-1", Title: "fix bug", CreatedBy: "alice", WorkflowID: "w1",
	}))
	appendEvent(t, es, "workflow-w1", makeEvent("evt-2", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "task-1", GraphName: "bugfix", Version: 1,
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-3", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "analyze", WorkflowID: "w1", SkillName: "root-cause", Runtime: "claude-code",
	}))
	appendEvent(t, es, "node-n2", makeEvent("evt-4", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "implement", WorkflowID: "w1", SkillName: "implement-fix", Runtime: "claude-code",
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-5", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "analyze", SessionID: "sess-1",
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-6", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "analyze", SessionID: "sess-1", Output: map[string]string{"rca.md": "art-1"},
	}))

	// Rebuild projections.
	if err := ss.Rebuild(ctx); err != nil {
		t.Fatalf("Rebuild: %v", err)
	}

	// Verify workflow state.
	wf, err := ss.GetWorkflow(ctx, "w1")
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if wf.Status != domain.WorkflowStatusActive {
		t.Errorf("workflow status = %s, want ACTIVE", wf.Status)
	}
	if len(wf.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(wf.Nodes))
	}

	// analyze node should be completed with output.
	analyze, ok := wf.Nodes["analyze"]
	if !ok {
		t.Fatal("analyze node not found")
	}
	if analyze.Status != domain.NodeStatusCompleted {
		t.Errorf("analyze status = %s, want COMPLETED", analyze.Status)
	}
	if analyze.SessionID != "sess-1" {
		t.Errorf("analyze session = %s, want sess-1", analyze.SessionID)
	}
	if analyze.Output["rca.md"] != "art-1" {
		t.Errorf("analyze output[rca.md] = %s, want art-1", analyze.Output["rca.md"])
	}

	// implement node should still be pending (no START event yet).
	implement, ok := wf.Nodes["implement"]
	if !ok {
		t.Fatal("implement node not found")
	}
	if implement.Status != domain.NodeStatusPending {
		t.Errorf("implement status = %s, want PENDING", implement.Status)
	}
}

// TestNodeLifecycle verifies the full node state machine through events.
func TestNodeLifecycle(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "test", Version: 1,
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-2", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "coder", WorkflowID: "w1", SkillName: "code", Runtime: "claude-code",
	}))

	ss.Rebuild(ctx)

	// After NodeCreated, status should be PENDING.
	ns, _ := ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusPending {
		t.Errorf("after NodeCreated: status = %s, want PENDING", ns.Status)
	}

	// After NodeStarted, status should be RUNNING.
	appendEvent(t, es, "node-n1", makeEvent("evt-3", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "coder", SessionID: "sess-1",
	}))
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusRunning {
		t.Errorf("after NodeStarted: status = %s, want RUNNING", ns.Status)
	}

	// After NodeWaiting, status should be WAITING.
	appendEvent(t, es, "node-n1", makeEvent("evt-4", store.EventNodeWaiting, domain.NodeWaiting{
		NodeID: "coder", SessionID: "sess-1", Reason: "ambiguous approach",
	}))
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusWaiting {
		t.Errorf("after NodeWaiting: status = %s, want WAITING", ns.Status)
	}

	// After restart, status should be RUNNING.
	appendEvent(t, es, "node-n1", makeEvent("evt-5", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "coder", SessionID: "sess-2",
	}))
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusRunning {
		t.Errorf("after re-NodeStarted: status = %s, want RUNNING", ns.Status)
	}

	// After NodeCompleted, status should be COMPLETED.
	appendEvent(t, es, "node-n1", makeEvent("evt-6", store.EventNodeCompleted, domain.NodeCompleted{
		NodeID: "coder", SessionID: "sess-2", Output: map[string]string{"diff.patch": "art-2"},
	}))
	ss.Rebuild(ctx)
	ns, _ = ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusCompleted {
		t.Errorf("after NodeCompleted: status = %s, want COMPLETED", ns.Status)
	}
}

// TestRetryableFailure verifies a retryable NODE_FAILED resets the node to READY.
func TestRetryableFailure(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "test", Version: 1,
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-2", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "coder", WorkflowID: "w1", SkillName: "code", Runtime: "claude-code",
	}))
	// Start the node.
	appendEvent(t, es, "node-n1", makeEvent("evt-3", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "coder", SessionID: "sess-1",
	}))
	// Fail retryable.
	appendEvent(t, es, "node-n1", makeEvent("evt-4", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "coder", SessionID: "sess-1", Reason: "OOM", Retryable: true,
	}))

	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusReady {
		t.Errorf("after retryable failure: status = %s, want READY", ns.Status)
	}
	if ns.RetryCount != 1 {
		t.Errorf("retry count = %d, want 1", ns.RetryCount)
	}
}

// TestNonRetryableFailure verifies a non-retryable NODE_FAILED leaves status as FAILED.
func TestNonRetryableFailure(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "test", Version: 1,
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-2", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "coder", WorkflowID: "w1", SkillName: "code", Runtime: "claude-code",
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-3", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "coder", SessionID: "sess-1",
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-4", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "coder", SessionID: "sess-1", Reason: "permission denied", Retryable: false,
	}))

	ss.Rebuild(ctx)

	ns, _ := ss.GetNodeState(ctx, "coder")
	if ns.Status != domain.NodeStatusFailed {
		t.Errorf("after non-retryable failure: status = %s, want FAILED", ns.Status)
	}
}

// TestGetReadyNodes verifies filtering nodes by READY status.
func TestGetReadyNodes(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "test", Version: 1,
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-2", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "planner", WorkflowID: "w1", SkillName: "plan", Runtime: "claude-code",
	}))
	appendEvent(t, es, "node-n2", makeEvent("evt-3", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "coder", WorkflowID: "w1", SkillName: "code", Runtime: "claude-code",
	}))

	ss.Rebuild(ctx)

	// Both nodes should be PENDING, not READY (READY is set by the workflow engine when deps are met).
	ready, err := ss.GetReadyNodes(ctx, "w1")
	if err != nil {
		t.Fatalf("GetReadyNodes: %v", err)
	}
	if len(ready) != 0 {
		t.Errorf("expected 0 READY nodes (all are PENDING), got %d", len(ready))
	}

	// Now make one node READY by simulating a retryable failure (which sets status to READY).
	appendEvent(t, es, "node-n1", makeEvent("evt-4", store.EventNodeStarted, domain.NodeStarted{
		NodeID: "planner", SessionID: "sess-1",
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-5", store.EventNodeFailed, domain.NodeFailed{
		NodeID: "planner", SessionID: "sess-1", Reason: "timeout", Retryable: true,
	}))
	ss.Rebuild(ctx)

	ready, err = ss.GetReadyNodes(ctx, "w1")
	if err != nil {
		t.Fatalf("GetReadyNodes after retry: %v", err)
	}
	if len(ready) != 1 {
		t.Fatalf("expected 1 READY node, got %d", len(ready))
	}
	if ready[0].NodeID != "planner" {
		t.Errorf("expected planner to be READY, got %s", ready[0].NodeID)
	}
}

// TestListActiveWorkflows filters out completed/cancelled workflows.
func TestListActiveWorkflows(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	// Create two workflows.
	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "active", Version: 1,
	}))
	appendEvent(t, es, "workflow-w2", makeEvent("evt-2", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w2", TaskID: "t2", GraphName: "done", Version: 1,
	}))

	ss.Rebuild(ctx)

	active, _ := ss.ListActiveWorkflows(ctx)
	if len(active) != 2 {
		t.Fatalf("expected 2 active workflows, got %d", len(active))
	}

	// Manually mark w2 as completed (in real system, this would happen via events).
	ss.mu.Lock()
	ss.workflows["w2"].Status = domain.WorkflowStatusCompleted
	ss.mu.Unlock()

	active, _ = ss.ListActiveWorkflows(ctx)
	if len(active) != 1 {
		t.Errorf("expected 1 active workflow after manual completion, got %d", len(active))
	}
	if active[0].ID != "w1" {
		t.Errorf("expected w1 to be the only active, got %s", active[0].ID)
	}
}

// TestGetWorkflowCopy verifies GetWorkflow returns a copy, not a reference.
func TestGetWorkflowCopy(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "test", Version: 1,
	}))

	ss.Rebuild(ctx)

	wf1, _ := ss.GetWorkflow(ctx, "w1")
	wf2, _ := ss.GetWorkflow(ctx, "w1")

	// Mutate wf1 — should not affect wf2 or the internal state.
	wf1.Nodes["new-node"] = domain.NodeState{NodeID: "new-node"}

	if _, ok := wf2.Nodes["new-node"]; ok {
		t.Error("mutating returned copy should not affect another copy")
	}

	wf3, _ := ss.GetWorkflow(ctx, "w1")
	if _, ok := wf3.Nodes["new-node"]; ok {
		t.Error("mutating returned copy should not affect internal state")
	}
}

// TestNodeIndex verifies the reverse lookup from nodeID to workflowID.
func TestNodeIndex(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventWorkflowCreated, domain.WorkflowCreated{
		WorkflowID: "w1", TaskID: "t1", GraphName: "test", Version: 1,
	}))
	appendEvent(t, es, "node-n1", makeEvent("evt-2", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "coder", WorkflowID: "w1", SkillName: "code", Runtime: "claude-code",
	}))

	ss.Rebuild(ctx)

	// GetNodeState should find the node via the index.
	ns, err := ss.GetNodeState(ctx, "coder")
	if err != nil {
		t.Fatalf("GetNodeState: %v", err)
	}
	if ns.NodeID != "coder" {
		t.Errorf("node ID = %s, want coder", ns.NodeID)
	}

	// Non-existent node should return NotFoundError.
	_, err = ss.GetNodeState(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent node")
	}
	nf, ok := err.(*NotFoundError)
	if !ok || nf.Entity != "node" {
		t.Errorf("expected NotFoundError for node, got %T: %v", err, err)
	}
}

// TestRebuildIsolatedNodes verifies nodes can exist without a prior WORKFLOW_CREATED event.
// The TaskCreated event should have already created the workflow in the projection.
func TestRebuildIsolatedNodes(t *testing.T) {
	ss, es := newTestStateStore(t)
	ctx := context.Background()

	// Create workflow via TaskCreated (not WorkflowCreated).
	appendEvent(t, es, "workflow-w1", makeEvent("evt-1", store.EventTaskCreated, domain.TaskCreated{
		TaskID: "t1", Title: "test", CreatedBy: "alice", WorkflowID: "w1",
	}))

	// Create nodes directly.
	appendEvent(t, es, "node-n1", makeEvent("evt-2", store.EventNodeCreated, domain.NodeCreated{
		NodeID: "planner", WorkflowID: "w1", SkillName: "plan", Runtime: "claude-code",
	}))

	ss.Rebuild(ctx)

	wf, err := ss.GetWorkflow(ctx, "w1")
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if len(wf.Nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(wf.Nodes))
	}
}

// TestNotFoundError verifies the NotFoundError type.
func TestNotFoundError(t *testing.T) {
	err := &NotFoundError{Entity: "workflow", ID: "w1"}
	if err.Error() != "workflow not found: w1" {
		t.Errorf("error message = %s", err.Error())
	}

	err2 := &NotFoundError{ID: "n1"} // no entity set
	if err2.Error() != "entity not found: n1" {
		t.Errorf("error message = %s", err2.Error())
	}
}

// BenchmarkRebuild measures full replay performance.
func BenchmarkRebuild(b *testing.B) {
	es, esErr := store.NewSQLiteStore(":memory:")
	if esErr != nil {
		b.Fatalf("NewSQLiteStore: %v", esErr)
	}
	defer es.Close()

	ctx := context.Background()

	// Pre-populate 500 events across 5 workflows.
	for w := 0; w < 5; w++ {
		wfID := fmt.Sprintf("wf-%d", w)
		es.Append(ctx, "workflow-"+wfID, []store.Event{
			makeEvent(fmt.Sprintf("evt-wf-%d", w), store.EventWorkflowCreated, domain.WorkflowCreated{
				WorkflowID: wfID, TaskID: fmt.Sprintf("t-%d", w), GraphName: "bench", Version: 1,
			}),
		})
		for n := 0; n < 20; n++ {
			nodeID := fmt.Sprintf("node-%d-%d", w, n)
			es.Append(ctx, "node-"+nodeID, []store.Event{
				makeEvent(fmt.Sprintf("evt-nc-%d-%d", w, n), store.EventNodeCreated, domain.NodeCreated{
					NodeID: nodeID, WorkflowID: wfID, SkillName: "code", Runtime: "claude-code",
				}),
				makeEvent(fmt.Sprintf("evt-ns-%d-%d", w, n), store.EventNodeStarted, domain.NodeStarted{
					NodeID: nodeID, SessionID: fmt.Sprintf("sess-%d-%d", w, n),
				}),
				makeEvent(fmt.Sprintf("evt-nx-%d-%d", w, n), store.EventNodeCompleted, domain.NodeCompleted{
					NodeID: nodeID, SessionID: fmt.Sprintf("sess-%d-%d", w, n),
				}),
			})
		}
	}

	ss := NewInMemoryStateStore(es)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ss.Rebuild(ctx)
	}
}
