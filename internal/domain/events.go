package domain

import "time"

// ── Task Payloads ─────────────────────────────────

// TaskCreated is the payload for TASK_CREATED events.
type TaskCreated struct {
	TaskID      string `json:"task_id"`
	Title       string `json:"title"`
	Description string `json:"description,omitempty"`
	CreatedBy   string `json:"created_by"`
	WorkflowID  string `json:"workflow_id"`
}

// TaskCompleted is the payload for TASK_COMPLETED events.
type TaskCompleted struct {
	TaskID string `json:"task_id"`
}

// TaskFailed is the payload for TASK_FAILED events.
type TaskFailed struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

// ── Workflow Payloads ─────────────────────────────

// WorkflowCreated is the payload for WORKFLOW_CREATED events.
type WorkflowCreated struct {
	WorkflowID string `json:"workflow_id"`
	TaskID     string `json:"task_id"`
	GraphName  string `json:"graph_name"`
	Version    int    `json:"version"`
}

// WorkflowChanged is the payload for WORKFLOW_CHANGED events.
// Emitted when the DAG is mutated at runtime (e.g., adding nodes for retry loops).
type WorkflowChanged struct {
	WorkflowID string `json:"workflow_id"`
	OldVersion int    `json:"old_version"`
	NewVersion int    `json:"new_version"`
	Reason     string `json:"reason"`
}

// ── Node Payloads ─────────────────────────────────

// NodeCreated is the payload for NODE_CREATED events.
type NodeCreated struct {
	NodeID     string   `json:"node_id"`
	WorkflowID string   `json:"workflow_id"`
	SkillName  string   `json:"skill_name"`
	Runtime    string   `json:"runtime"`
	DependsOn  []string `json:"depends_on,omitempty"`
	Gate       string   `json:"gate,omitempty"`
}

// NodeStarted is the payload for NODE_STARTED events.
type NodeStarted struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	SessionID  string `json:"session_id"`
}

// NodeWaiting is the payload for NODE_WAITING events.
// Emitted when a node is blocked on human approval.
type NodeWaiting struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	SessionID  string `json:"session_id"`
	Reason     string `json:"reason"`
	Prompt     string `json:"prompt"` // what the agent is asking
}

// NodeCompleted is the payload for NODE_COMPLETED events.
type NodeCompleted struct {
	NodeID     string            `json:"node_id"`
	WorkflowID string            `json:"workflow_id"`
	SessionID  string            `json:"session_id"`
	Output     map[string]string `json:"output"` // artifact name → artifact ID
}

// NodeFailed is the payload for NODE_FAILED events.
type NodeFailed struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	SessionID  string `json:"session_id"`
	Reason     string `json:"reason"`
	Retryable  bool   `json:"retryable"`
}

// ── Runtime Payloads ──────────────────────────────

// RuntimeCreated is the payload for RUNTIME_CREATED events.
type RuntimeCreated struct {
	RuntimeID   string `json:"runtime_id"`
	NodeID      string `json:"node_id"`
	SessionID   string `json:"session_id"`
	Type        string `json:"type"`
	WorkspaceID string `json:"workspace_id"`
	SlotID      string `json:"slot_id"`
}

// RuntimeStarted is the payload for RUNTIME_STARTED events.
type RuntimeStarted struct {
	RuntimeID string    `json:"runtime_id"`
	SessionID string    `json:"session_id"`
	StartedAt time.Time `json:"started_at"`
}

// RuntimeExited is the payload for RUNTIME_EXITED events.
type RuntimeExited struct {
	RuntimeID  string `json:"runtime_id"`
	SessionID  string `json:"session_id"`
	ExitCode   int    `json:"exit_code"`
	Success    bool   `json:"success"`
	TokensUsed int64  `json:"tokens_used"`
	DurationMs int64  `json:"duration_ms"`
}

// RuntimeLost is the payload for RUNTIME_LOST events.
// Emitted when a worker heartbeat times out.
type RuntimeLost struct {
	RuntimeID string `json:"runtime_id"`
	WorkerID  string `json:"worker_id"`
	Reason    string `json:"reason"`
}

// ── Workspace Payloads ────────────────────────────

// WorkspaceCreated is the payload for WORKSPACE_CREATED events.
type WorkspaceCreated struct {
	WorkspaceID string `json:"workspace_id"`
	Type        string `json:"type"`
	Name        string `json:"name"`
}

// SlotCreated is the payload for SLOT_CREATED events.
type SlotCreated struct {
	WorkspaceID string `json:"workspace_id"`
	SlotID      string `json:"slot_id"`
	SlotName    string `json:"slot_name"`
}

// ── Artifact Payloads ─────────────────────────────

// ArtifactCreated is the payload for ARTIFACT_CREATED events.
type ArtifactCreated struct {
	ArtifactID  string `json:"artifact_id"`
	NodeID      string `json:"node_id"`
	SessionID   string `json:"session_id"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Size        int64  `json:"size"`
	ContentHash string `json:"content_hash"`
}

// ── Human Interaction Payloads ────────────────────

// HumanApprovalRequired is the payload for HUMAN_APPROVAL_REQUIRED events.
type HumanApprovalRequired struct {
	NodeID    string `json:"node_id"`
	SessionID string `json:"session_id"`
	Prompt    string `json:"prompt"`
	Options   []string `json:"options,omitempty"`
	Timeout   time.Duration `json:"timeout,omitempty"`
}

// HumanInputReceived is the payload for HUMAN_INPUT_RECEIVED events.
type HumanInputReceived struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	SessionID  string `json:"session_id"`
	Decision   string `json:"decision"` // "approved", "rejected", "custom"
	Input      string `json:"input,omitempty"`
}
