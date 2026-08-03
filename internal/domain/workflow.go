// Package domain defines the core types and event payloads for arlod.
// These types form the ubiquitous language of the AgentOS domain.
//
// No implementation logic lives here — only data structures and their
// validation rules. Business logic belongs in higher packages (workflow,
// reconciler, runtime).
package domain

import "time"

// ── Workflow ──────────────────────────────────────

// WorkflowStatus represents the lifecycle state of a workflow execution.
type WorkflowStatus string

const (
	WorkflowStatusActive    WorkflowStatus = "ACTIVE"
	WorkflowStatusPaused    WorkflowStatus = "PAUSED"
	WorkflowStatusCompleted WorkflowStatus = "COMPLETED"
	WorkflowStatusFailed    WorkflowStatus = "FAILED"
	WorkflowStatusCancelled WorkflowStatus = "CANCELLED"
)

// ExecutableGraph is a compiled workflow ready for execution.
type ExecutableGraph struct {
	ID      string           `json:"id"`
	Name    string           `json:"name"`
	Version int              `json:"version"`
	Nodes   []ExecutableNode `json:"nodes"`
	Edges   []Edge           `json:"edges,omitempty"`
	Policy  SchedulingPolicy `json:"policy,omitempty"`
}

// ExecutableNode is one step in the workflow DAG.
type ExecutableNode struct {
	ID         string       `json:"id"`
	SkillRef   SkillRef     `json:"skill_ref"`
	Runtime    RuntimeRef   `json:"runtime"`
	DependsOn  []string     `json:"depends_on,omitempty"`
	Gate       ApprovalGate `json:"gate,omitempty"`
	Retry      RetryPolicy  `json:"retry,omitempty"`
	Transitions []Transition `json:"transitions,omitempty"`
}

// SkillRef points to a skill in the skill registry.
type SkillRef struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

// RuntimeRef specifies which runtime adapter executes this node.
type RuntimeRef struct {
	Provider string `json:"provider"` // "claude-code", "codex"
	Model    string `json:"model,omitempty"`
}

// Edge is a dependency edge in the workflow DAG.
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// ApprovalGate controls whether a node requires human approval before/after execution.
type ApprovalGate string

const (
	GateNone           ApprovalGate = "none"
	GateHumanApproval  ApprovalGate = "human_approval"
)

// Transition defines a conditional edge for iterative loops.
// e.g., from review → iterate when verdict != "APPROVED"
type Transition struct {
	From string `json:"from"`
	To   string `json:"to"`
	When string `json:"when"` // expression evaluated against node output
}

// SchedulingPolicy controls concurrency and resource limits per workflow.
type SchedulingPolicy struct {
	MaxConcurrentNodes int           `json:"max_concurrent_nodes"`
	ResourceQuota      ResourceQuota `json:"resource_quota,omitempty"`
}

// ResourceQuota limits resource consumption per workflow.
type ResourceQuota struct {
	MaxTokens  int64   `json:"max_tokens,omitempty"`
	MaxCostUSD float64 `json:"max_cost_usd,omitempty"`
}

// RetryPolicy controls retry behavior for a node.
type RetryPolicy struct {
	MaxRetries    int           `json:"max_retries"`
	Backoff       time.Duration `json:"backoff"`
	MaxBackoff    time.Duration `json:"max_backoff,omitempty"`
	BackoffFactor float64       `json:"backoff_factor,omitempty"` // default 2.0
}

// WorkflowInstance is a running instance of an ExecutableGraph.
type WorkflowInstance struct {
	ID        string          `json:"id"`
	TaskID    string          `json:"task_id"`
	Graph     *ExecutableGraph `json:"graph"`
	Status    WorkflowStatus  `json:"status"`
	Version   int             `json:"version"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// WorkflowState is the runtime state of a workflow, built from event projections.
type WorkflowState struct {
	ID        string              `json:"id"`
	Status    WorkflowStatus      `json:"status"`
	Nodes     map[string]NodeState `json:"nodes"`
	CreatedAt time.Time           `json:"created_at"`
	UpdatedAt time.Time           `json:"updated_at"`
}
