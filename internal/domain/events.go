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

// TaskFailed is the payload for TASK_FAILED events.
type TaskFailed struct {
	TaskID string `json:"task_id"`
	Reason string `json:"reason"`
}

// TaskCancelled is the payload for TASK_CANCELLED events.
type TaskCancelled struct {
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

// WorkflowCompleted is the payload for WORKFLOW_COMPLETED events.
type WorkflowCompleted struct {
	WorkflowID string `json:"workflow_id"`
}

// WorkflowFailed is the payload for WORKFLOW_FAILED events.
type WorkflowFailed struct {
	WorkflowID string `json:"workflow_id"`
	Reason     string `json:"reason"`
}

// WorkflowCancelled is the payload for WORKFLOW_CANCELLED events.
type WorkflowCancelled struct {
	WorkflowID string `json:"workflow_id"`
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
	RuntimeID  string `json:"runtime_id,omitempty"`
}

// NodeWaiting is the payload for NODE_WAITING events.
// Emitted when a node is blocked on human approval.
//
// TODO(v0.2): Rename to NODE_ENTERED_WAITING (past-tense convention).
//   Event type constant should become EventNodeEnteredWaiting = "NODE_ENTERED_WAITING".
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

// RuntimeFailed is the payload for RUNTIME_FAILED events.
// Emitted when a runtime exits with an error or is terminated abnormally.
type RuntimeFailed struct {
	RuntimeID string `json:"runtime_id"`
	SessionID string `json:"session_id"`
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
	Path        string `json:"path"`
}

// ── Observability Payloads ───────────────────────

// NodeHeartbeat is the payload for NODE_HEARTBEAT events.
// Emitted periodically by the runtime to signal liveness.
type NodeHeartbeat struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	SessionID  string `json:"session_id"`
	Status     string `json:"status"` // snapshot of current node status
}

// MetricsSnapshot is the payload for METRICS_SNAPSHOT events.
// Emitted to capture incremental resource usage of a running agent.
type MetricsSnapshot struct {
	NodeID     string  `json:"node_id"`
	WorkflowID string  `json:"workflow_id"`
	SessionID  string  `json:"session_id"`
	TokensIn   int64   `json:"tokens_in"`
	TokensOut  int64   `json:"tokens_out"`
	ToolCalls  int     `json:"tool_calls"`
	CostUSD    float64 `json:"cost_usd"`
	DurationMs int64   `json:"duration_ms"`
	FileEdits  int     `json:"file_edits,omitempty"`
	Retries    int     `json:"retries,omitempty"`
	HumanAsks  int     `json:"human_asks,omitempty"`
}

// NodeAnnotated is the payload for NODE_ANNOTATED events.
// Emitted when a human or system annotates a node execution.
type NodeAnnotated struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	Key        string `json:"key"`
	Value      string `json:"value"`
}

// ── Human Interaction Payloads ────────────────────

// HumanApprovalRequired is the payload for HUMAN_APPROVAL_REQUIRED events.
//
// TODO(v0.2): Rename to HUMAN_APPROVAL_REQUESTED (past-tense convention).
//   Event type constant should become EventHumanApprovalRequested = "HUMAN_APPROVAL_REQUESTED".
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

// ── Session Payloads ───────────────────────────────

// SessionCreated is the payload for SESSION_CREATED events.
// Emitted when a new execution session (attempt) is created for a node.
type SessionCreated struct {
	SessionID  string `json:"session_id"`
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	TaskID     string `json:"task_id"`
	Attempt    int    `json:"attempt"`
}

// SessionCompleted is the payload for SESSION_COMPLETED events.
// Emitted when a session finishes successfully.
type SessionCompleted struct {
	SessionID  string `json:"session_id"`
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
}

// SessionFailed is the payload for SESSION_FAILED events.
// Emitted when a session terminates with an error.
type SessionFailed struct {
	SessionID  string `json:"session_id"`
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	Reason     string `json:"reason"`
}

// SessionCancelled is the payload for SESSION_CANCELLED events.
// Emitted when a session is cancelled before completion.
type SessionCancelled struct {
	SessionID  string `json:"session_id"`
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	Reason     string `json:"reason"`
}

// ── Checkpoint Payload ───────────────────────────────

// CheckpointCreated is the payload for CHECKPOINT_CREATED events.
// Emitted when a node completes successfully, capturing enough state
// to resume from this point if the workflow crashes mid-execution.
type CheckpointCreated struct {
	NodeID     string            `json:"node_id"`
	WorkflowID string            `json:"workflow_id"`
	SessionID  string            `json:"session_id"`
	Artifacts  []string          `json:"artifacts,omitempty"`  // artifact IDs produced
	GitCommit  string            `json:"git_commit,omitempty"` // HEAD commit at checkpoint
	Output     map[string]string `json:"output,omitempty"`     // node output summary
}

// ── Runtime Action Payload ────────────────────────────

// RuntimeAction is the payload for RUNTIME_ACTION events.
// Emitted by the reconciler when it observes real-time agent activity
// from a running runtime instance (THINKING, TOOL_CALL, etc.).
type RuntimeAction struct {
	NodeID     string `json:"node_id"`
	WorkflowID string `json:"workflow_id"`
	RuntimeID  string `json:"runtime_id"`
	Action     string `json:"action"`            // human-readable: "running pytest"
	ToolName   string `json:"tool_name,omitempty"`
	Detail     string `json:"detail,omitempty"`
}
