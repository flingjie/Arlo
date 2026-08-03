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
}

// Decision represents an action the Reconciler should take.
// Produced by WorkflowEngine.Evaluate().
type Decision struct {
	Action string `json:"action"` // "START_NODE", "STOP_NODE", "COMPLETE_WORKFLOW"
	NodeID string `json:"node_id,omitempty"`
	Reason string `json:"reason"`
}

// Well-known decision actions.
const (
	DecisionStartNode          = "START_NODE"
	DecisionStopNode           = "STOP_NODE"
	DecisionCompleteWorkflow   = "COMPLETE_WORKFLOW"
	DecisionFailWorkflow       = "FAIL_WORKFLOW"
)
