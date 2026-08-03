package domain

import "time"

// NodeStatus represents the lifecycle state of a workflow node.
type NodeStatus string

const (
	NodeStatusPending    NodeStatus = "PENDING"
	NodeStatusReady      NodeStatus = "READY"
	NodeStatusStarting   NodeStatus = "STARTING"
	NodeStatusRunning    NodeStatus = "RUNNING"
	NodeStatusWaiting    NodeStatus = "WAITING"    // blocked on human approval
	NodeStatusStopping   NodeStatus = "STOPPING"
	NodeStatusCompleted  NodeStatus = "COMPLETED"
	NodeStatusFailed     NodeStatus = "FAILED"
	NodeStatusCancelled  NodeStatus = "CANCELLED"
)

// NodeState is the runtime state of a single workflow node,
// built from event projections.
type NodeState struct {
	NodeID      string            `json:"node_id"`
	WorkflowID  string            `json:"workflow_id"`
	Status      NodeStatus        `json:"status"`
	SessionID   string            `json:"session_id,omitempty"`
	RuntimeID   string            `json:"runtime_id,omitempty"`
	StartedAt   *time.Time        `json:"started_at,omitempty"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	RetryCount  int               `json:"retry_count"`
	Output      map[string]string `json:"output,omitempty"` // artifact name → artifact ID
	DependsOn   []string          `json:"depends_on,omitempty"`
	Children    []string          `json:"children,omitempty"`
	Gate        string            `json:"gate,omitempty"`
	Annotations []Annotation      `json:"annotations,omitempty"`
	Metrics     NodeMetrics       `json:"metrics,omitempty"`
}

// Annotation is a human or system annotation attached to a node execution.
// Annotations enable evaluation, fine-tuning, and replay loops.
type Annotation struct {
	Key       string    `json:"key"`       // "human_rating", "bug_found", "accepted"
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

// NodeMetrics captures runtime performance data for a node execution.
type NodeMetrics struct {
	TokensIn   int64   `json:"tokens_in"`
	TokensOut  int64   `json:"tokens_out"`
	ToolCalls  int     `json:"tool_calls"`
	CostUSD    float64 `json:"cost_usd"`
	DurationMs int64   `json:"duration_ms"`
}

// Decision represents an action the Reconciler should take.
// Produced by WorkflowEngine.Evaluate().
type Decision struct {
	Action string `json:"action"` // "START_NODE", "PAUSE_NODE", "RESUME_NODE", "RETRY_NODE", ...
	NodeID string `json:"node_id,omitempty"`
	Reason string `json:"reason"`
}

// Well-known decision actions.
const (
	DecisionStartNode          = "START_NODE"
	DecisionStopNode           = "STOP_NODE"
	DecisionPauseNode          = "PAUSE_NODE"
	DecisionResumeNode         = "RESUME_NODE"
	DecisionRetryNode          = "RETRY_NODE"
	DecisionCompleteWorkflow   = "COMPLETE_WORKFLOW"
	DecisionFailWorkflow       = "FAIL_WORKFLOW"
)
