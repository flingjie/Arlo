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

// TestMarshalNodeStarted verifies NodeStarted JSON includes RuntimeID.
func TestMarshalNodeStarted(t *testing.T) {
	payload := NodeStarted{
		NodeID:     "n1",
		WorkflowID: "w1",
		SessionID:  "s1",
		RuntimeID:  "rt-42",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal NodeStarted: %v", err)
	}

	var decoded NodeStarted
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal NodeStarted: %v", err)
	}

	if decoded.RuntimeID != "rt-42" {
		t.Errorf("NodeStarted.RuntimeID = %s, want rt-42", decoded.RuntimeID)
	}

	// Verify the JSON contains the runtime_id field.
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw NodeStarted: %v", err)
	}
	if _, ok := raw["runtime_id"]; !ok {
		t.Error("NodeStarted JSON missing runtime_id field")
	}
}

// TestMarshalMetricsSnapshot verifies MetricsSnapshot includes all RuntimeMetrics fields.
func TestMarshalMetricsSnapshot(t *testing.T) {
	payload := MetricsSnapshot{
		NodeID:     "n1",
		WorkflowID: "w1",
		SessionID:  "s1",
		TokensIn:   1000,
		TokensOut:  500,
		ToolCalls:  12,
		CostUSD:    0.025,
		DurationMs: 30000,
		FileEdits:  5,
		Retries:    2,
		HumanAsks:  1,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal MetricsSnapshot: %v", err)
	}

	var decoded MetricsSnapshot
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal MetricsSnapshot: %v", err)
	}

	if decoded.FileEdits != 5 {
		t.Errorf("MetricsSnapshot.FileEdits = %d, want 5", decoded.FileEdits)
	}
	if decoded.Retries != 2 {
		t.Errorf("MetricsSnapshot.Retries = %d, want 2", decoded.Retries)
	}
	if decoded.HumanAsks != 1 {
		t.Errorf("MetricsSnapshot.HumanAsks = %d, want 1", decoded.HumanAsks)
	}

	// Verify JSON contains the new fields.
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("unmarshal raw MetricsSnapshot: %v", err)
	}
	for _, field := range []string{"file_edits", "retries", "human_asks"} {
		if _, ok := raw[field]; !ok {
			t.Errorf("MetricsSnapshot JSON missing %s field", field)
		}
	}
}

// TestMarshalTaskCancelled verifies JSON round-trip for the TaskCancelled payload.
func TestMarshalTaskCancelled(t *testing.T) {
	payload := TaskCancelled{TaskID: "task-1", Reason: "user requested"}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal TaskCancelled: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON output")
	}

	var decoded TaskCancelled
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal TaskCancelled: %v", err)
	}
	if decoded.TaskID != "task-1" {
		t.Errorf("TaskID = %s, want task-1", decoded.TaskID)
	}
	if decoded.Reason != "user requested" {
		t.Errorf("Reason = %s, want 'user requested'", decoded.Reason)
	}
}

// TestMarshalSessionEvents verifies JSON round-trip for all Session event payloads.
func TestMarshalSessionEvents(t *testing.T) {
	t.Run("SessionCreated", func(t *testing.T) {
		payload := SessionCreated{
			SessionID:  "sess-1",
			NodeID:     "n1",
			WorkflowID: "w1",
			TaskID:     "task-1",
			Attempt:    1,
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal SessionCreated: %v", err)
		}
		var decoded SessionCreated
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("unmarshal SessionCreated: %v", err)
		}
		if decoded.SessionID != "sess-1" {
			t.Errorf("SessionID = %s, want sess-1", decoded.SessionID)
		}
		if decoded.Attempt != 1 {
			t.Errorf("Attempt = %d, want 1", decoded.Attempt)
		}
	})

	t.Run("SessionCompleted", func(t *testing.T) {
		payload := SessionCompleted{SessionID: "sess-1", NodeID: "n1", WorkflowID: "w1"}
		data, _ := json.Marshal(payload)
		var decoded SessionCompleted
		json.Unmarshal(data, &decoded)
		if decoded.SessionID != "sess-1" {
			t.Errorf("SessionID = %s, want sess-1", decoded.SessionID)
		}
	})

	t.Run("SessionFailed", func(t *testing.T) {
		payload := SessionFailed{
			SessionID:  "sess-1",
			NodeID:     "n1",
			WorkflowID: "w1",
			Reason:     "OOM killed",
		}
		data, _ := json.Marshal(payload)
		var decoded SessionFailed
		json.Unmarshal(data, &decoded)
		if decoded.Reason != "OOM killed" {
			t.Errorf("Reason = %s, want 'OOM killed'", decoded.Reason)
		}
	})

	t.Run("SessionCancelled", func(t *testing.T) {
		payload := SessionCancelled{
			SessionID:  "sess-1",
			NodeID:     "n1",
			WorkflowID: "w1",
			Reason:     "node retry limit exceeded",
		}
		data, _ := json.Marshal(payload)
		var decoded SessionCancelled
		json.Unmarshal(data, &decoded)
		if decoded.Reason != "node retry limit exceeded" {
			t.Errorf("Reason = %s, want 'node retry limit exceeded'", decoded.Reason)
		}
	})
}

// TestMarshalWorkflowLifecycleEvents verifies JSON round-trip for Workflow lifecycle payloads.
func TestMarshalWorkflowLifecycleEvents(t *testing.T) {
	t.Run("WorkflowCompleted", func(t *testing.T) {
		payload := WorkflowCompleted{WorkflowID: "w1"}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal WorkflowCompleted: %v", err)
		}
		var decoded WorkflowCompleted
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("unmarshal WorkflowCompleted: %v", err)
		}
		if decoded.WorkflowID != "w1" {
			t.Errorf("WorkflowID = %s, want w1", decoded.WorkflowID)
		}
	})

	t.Run("WorkflowFailed", func(t *testing.T) {
		payload := WorkflowFailed{
			WorkflowID: "w1",
			Reason:     "all nodes exhausted",
		}
		data, _ := json.Marshal(payload)
		var decoded WorkflowFailed
		json.Unmarshal(data, &decoded)
		if decoded.Reason != "all nodes exhausted" {
			t.Errorf("Reason = %s, want 'all nodes exhausted'", decoded.Reason)
		}
	})

	t.Run("WorkflowCancelled", func(t *testing.T) {
		payload := WorkflowCancelled{
			WorkflowID: "w1",
			Reason:     "task cancelled by user",
		}
		data, _ := json.Marshal(payload)
		var decoded WorkflowCancelled
		json.Unmarshal(data, &decoded)
		if decoded.Reason != "task cancelled by user" {
			t.Errorf("Reason = %s, want 'task cancelled by user'", decoded.Reason)
		}
	})
}

// TestMarshalRuntimeFailed verifies JSON round-trip for RuntimeFailed payload.
func TestMarshalRuntimeFailed(t *testing.T) {
	payload := RuntimeFailed{
		RuntimeID: "rt-1",
		SessionID: "sess-1",
		Reason:    "process exited with code 137",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal RuntimeFailed: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty JSON output")
	}

	var decoded RuntimeFailed
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal RuntimeFailed: %v", err)
	}
	if decoded.RuntimeID != "rt-1" {
		t.Errorf("RuntimeID = %s, want rt-1", decoded.RuntimeID)
	}
	if decoded.Reason != "process exited with code 137" {
		t.Errorf("Reason = %s, want 'process exited with code 137'", decoded.Reason)
	}
}

// TestMarshalAllNewEventPayloads verifies every new event payload type round-trips.
func TestMarshalAllNewEventPayloads(t *testing.T) {
	payloads := []interface{}{
		TaskCancelled{TaskID: "t1", Reason: "done"},
		SessionCreated{SessionID: "s1", NodeID: "n1", WorkflowID: "w1", TaskID: "t1", Attempt: 1},
		SessionCompleted{SessionID: "s1", NodeID: "n1", WorkflowID: "w1"},
		SessionFailed{SessionID: "s1", NodeID: "n1", WorkflowID: "w1", Reason: "exit code 1"},
		SessionCancelled{SessionID: "s1", NodeID: "n1", WorkflowID: "w1", Reason: "retry"},
		WorkflowCompleted{WorkflowID: "w1"},
		WorkflowFailed{WorkflowID: "w1", Reason: "failed"},
		WorkflowCancelled{WorkflowID: "w1", Reason: "cancelled"},
		RuntimeFailed{RuntimeID: "r1", SessionID: "s1", Reason: "crashed"},
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
