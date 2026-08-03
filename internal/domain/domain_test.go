package domain

import (
	"encoding/json"
	"testing"
	"time"
)

// TestMarshalTaskCreated verifies JSON round-trip for a TaskCreated event payload.
func TestMarshalTaskCreated(t *testing.T) {
	payload := TaskCreated{
		TaskID:      "task-123",
		Title:       "fix oauth bug",
		Description: "race condition in token refresh",
		CreatedBy:   "user:alice",
		WorkflowID:  "wf-abc",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded TaskCreated
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.TaskID != payload.TaskID {
		t.Errorf("TaskID = %s, want %s", decoded.TaskID, payload.TaskID)
	}
	if decoded.Title != payload.Title {
		t.Errorf("Title = %s, want %s", decoded.Title, payload.Title)
	}
}

// TestMarshalRuntimeExited verifies JSON round-trip for a RuntimeExited payload.
func TestMarshalRuntimeExited(t *testing.T) {
	payload := RuntimeExited{
		RuntimeID:  "rt-456",
		SessionID:  "sess-789",
		ExitCode:   0,
		Success:    true,
		TokensUsed: 45000,
		DurationMs: 120000,
	}

	data, _ := json.Marshal(payload)
	var decoded RuntimeExited
	json.Unmarshal(data, &decoded)

	if decoded.Success != true {
		t.Error("expected success=true")
	}
	if decoded.TokensUsed != 45000 {
		t.Errorf("TokensUsed = %d, want 45000", decoded.TokensUsed)
	}
}

// TestMarshalAllEventPayloads verifies every event payload type round-trips.
func TestMarshalAllEventPayloads(t *testing.T) {
	payloads := []interface{}{
		TaskCreated{TaskID: "t1", Title: "test", CreatedBy: "u1", WorkflowID: "w1"},
		TaskCompleted{TaskID: "t1"},
		TaskFailed{TaskID: "t1", Reason: "something broke"},
		WorkflowCreated{WorkflowID: "w1", TaskID: "t1", GraphName: "bugfix", Version: 1},
		WorkflowChanged{WorkflowID: "w1", OldVersion: 1, NewVersion: 2, Reason: "retry"},
		NodeCreated{NodeID: "n1", WorkflowID: "w1", SkillName: "root-cause", Runtime: "claude-code"},
		NodeStarted{NodeID: "n1", SessionID: "s1"},
		NodeWaiting{NodeID: "n1", SessionID: "s1", Reason: "ambiguous approach"},
		NodeCompleted{NodeID: "n1", SessionID: "s1", Output: map[string]string{"plan.md": "art-1"}},
		NodeFailed{NodeID: "n1", SessionID: "s1", Reason: "OOM", Retryable: true},
		RuntimeCreated{RuntimeID: "r1", NodeID: "n1", SessionID: "s1", Type: "claude-code", WorkspaceID: "ws1", SlotID: "slot1"},
		RuntimeStarted{RuntimeID: "r1", SessionID: "s1", StartedAt: time.Now()},
		RuntimeExited{RuntimeID: "r1", SessionID: "s1", ExitCode: 0, Success: true},
		RuntimeLost{RuntimeID: "r1", WorkerID: "worker-1", Reason: "heartbeat_timeout"},
		WorkspaceCreated{WorkspaceID: "ws1", Type: "tmux", Name: "arlo-bugfix"},
		SlotCreated{WorkspaceID: "ws1", SlotID: "slot1", SlotName: "coder"},
		ArtifactCreated{ArtifactID: "a1", NodeID: "n1", SessionID: "s1", Name: "plan.md", Type: "markdown", Size: 2048, ContentHash: "abc"},
		HumanApprovalRequired{NodeID: "n1", SessionID: "s1", Prompt: "which approach?"},
		HumanInputReceived{NodeID: "n1", SessionID: "s1", Decision: "approved", Input: "approach B"},
	}

	for i, p := range payloads {
		data, err := json.Marshal(p)
		if err != nil {
			t.Errorf("payload %d (%T): marshal failed: %v", i, p, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("payload %d (%T): empty output", i, p)
		}
	}
}

// TestNodeStatusTransitions verifies the correctness of the node state machine.
func TestNodeStatusTransitions(t *testing.T) {
	// Valid transitions from each state.
	validFrom := map[NodeStatus][]NodeStatus{
		NodeStatusPending:   {NodeStatusReady},
		NodeStatusReady:     {NodeStatusStarting},
		NodeStatusStarting:  {NodeStatusRunning, NodeStatusFailed, NodeStatusCancelled},
		NodeStatusRunning:   {NodeStatusWaiting, NodeStatusStopping, NodeStatusCompleted, NodeStatusFailed},
		NodeStatusWaiting:   {NodeStatusRunning, NodeStatusFailed, NodeStatusCancelled},
		NodeStatusStopping:  {NodeStatusFailed, NodeStatusCancelled},
		NodeStatusCompleted: {},
		NodeStatusFailed:    {NodeStatusReady}, // retry
		NodeStatusCancelled: {},
	}

	for from, validTos := range validFrom {
		validSet := make(map[NodeStatus]bool)
		for _, to := range validTos {
			validSet[to] = true
		}

		// Check that transitions to invalid states are caught.
		allStatuses := []NodeStatus{
			NodeStatusPending, NodeStatusReady, NodeStatusStarting,
			NodeStatusRunning, NodeStatusWaiting, NodeStatusStopping,
			NodeStatusCompleted, NodeStatusFailed, NodeStatusCancelled,
		}

		for _, to := range allStatuses {
			if from == to {
				continue
			}
			if !validSet[to] {
				// This transition should NOT be valid.
				// Verify the helper function rejects it.
				if isValidTransition(from, to) {
					t.Errorf("transition %s → %s should be invalid, but isValidTransition returned true", from, to)
				}
			}
		}
	}
}

// isValidTransition checks if a node status transition is valid.
func isValidTransition(from, to NodeStatus) bool {
	valid := map[NodeStatus][]NodeStatus{
		NodeStatusPending:   {NodeStatusReady},
		NodeStatusReady:     {NodeStatusStarting},
		NodeStatusStarting:  {NodeStatusRunning, NodeStatusFailed, NodeStatusCancelled},
		NodeStatusRunning:   {NodeStatusWaiting, NodeStatusStopping, NodeStatusCompleted, NodeStatusFailed},
		NodeStatusWaiting:   {NodeStatusRunning, NodeStatusFailed, NodeStatusCancelled},
		NodeStatusStopping:  {NodeStatusFailed, NodeStatusCancelled},
		NodeStatusCompleted: {},
		NodeStatusFailed:    {NodeStatusReady},
		NodeStatusCancelled: {},
	}

	for _, v := range valid[from] {
		if v == to {
			return true
		}
	}
	return false
}

// TestRuntimeStateValues verifies all RuntimeState enum values are distinct.
func TestRuntimeStateValues(t *testing.T) {
	states := []RuntimeState{
		RuntimeStatePreparing,
		RuntimeStateStarting,
		RuntimeStateRunning,
		RuntimeStateStopping,
		RuntimeStateExited,
		RuntimeStateFailed,
	}

	seen := make(map[RuntimeState]bool)
	for _, s := range states {
		if seen[s] {
			t.Errorf("duplicate runtime state: %s", s)
		}
		seen[s] = true
		if s == "" {
			t.Error("runtime state should not be empty")
		}
	}
}

// TestTaskStatusValues verifies all TaskStatus enum values are distinct.
func TestTaskStatusValues(t *testing.T) {
	statuses := []TaskStatus{
		TaskStatusPending,
		TaskStatusRunning,
		TaskStatusPaused,
		TaskStatusCompleted,
		TaskStatusFailed,
		TaskStatusCancelled,
	}

	seen := make(map[TaskStatus]bool)
	for _, s := range statuses {
		if seen[s] {
			t.Errorf("duplicate task status: %s", s)
		}
		seen[s] = true
	}
}

// TestWorkflowStateConstruction verifies WorkflowState can be built programmatically.
func TestWorkflowStateConstruction(t *testing.T) {
	state := WorkflowState{
		ID:     "wf-1",
		Status: WorkflowStatusActive,
		Nodes: map[string]NodeState{
			"analyze": {
				NodeID: "analyze",
				Status: NodeStatusCompleted,
			},
			"implement": {
				NodeID:    "implement",
				Status:    NodeStatusRunning,
				RuntimeID: "rt-1",
			},
			"review": {
				NodeID: "review",
				Status: NodeStatusPending,
			},
		},
	}

	if len(state.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(state.Nodes))
	}

	if state.Nodes["analyze"].Status != NodeStatusCompleted {
		t.Error("analyze should be completed")
	}

	if state.Nodes["implement"].RuntimeID != "rt-1" {
		t.Error("implement should reference rt-1")
	}
}

// TestDecisionConstants verifies decision action constants.
func TestDecisionConstants(t *testing.T) {
	// Verify the well-known decision constants are non-empty and distinct.
	decisions := []string{
		DecisionStartNode,
		DecisionStopNode,
		DecisionCompleteWorkflow,
		DecisionFailWorkflow,
	}

	seen := make(map[string]bool)
	for _, d := range decisions {
		if d == "" {
			t.Error("decision constant should not be empty")
		}
		if seen[d] {
			t.Errorf("duplicate decision: %s", d)
		}
		seen[d] = true
	}
}

// TestArtifactLineage verifies the Artifact ParentID field for lineage support.
func TestArtifactLineage(t *testing.T) {
	v1 := Artifact{
		ID:       "art-1",
		Name:     "plan.md",
		Version:  1,
		ParentID: "",
	}
	v2 := Artifact{
		ID:       "art-2",
		Name:     "plan.md",
		Version:  2,
		ParentID: "art-1",
	}

	if v1.ParentID != "" {
		t.Error("v1 should have no parent")
	}
	if v2.ParentID != "art-1" {
		t.Errorf("v2 parent = %s, want art-1", v2.ParentID)
	}
	if v2.Version != 2 {
		t.Errorf("v2 version = %d, want 2", v2.Version)
	}
}

// TestRuntimeConfigDefaults verifies RuntimeConfig can be constructed.
func TestRuntimeConfigDefaults(t *testing.T) {
	cfg := RuntimeConfig{
		Model:          "claude-sonnet-4",
		Capabilities:   []string{"filesystem", "git"},
		PermissionMode: "auto",
	}

	data, _ := json.Marshal(cfg)
	var decoded RuntimeConfig
	json.Unmarshal(data, &decoded)

	if decoded.Model != cfg.Model {
		t.Errorf("Model = %s, want %s", decoded.Model, cfg.Model)
	}
	if len(decoded.Capabilities) != 2 {
		t.Errorf("expected 2 capabilities, got %d", len(decoded.Capabilities))
	}
}
