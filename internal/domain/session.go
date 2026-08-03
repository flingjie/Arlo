package domain

import "time"

// Session represents a node's execution — one attempt.
// A node can have multiple sessions (retries). Each session produces one Trace.
type Session struct {
	ID         string    `json:"id"`
	NodeID     string    `json:"node_id"`
	WorkflowID string    `json:"workflow_id"`
	TaskID     string    `json:"task_id"`
	RuntimeID  string    `json:"runtime_id"`
	Attempt    int       `json:"attempt"` // 1-based retry attempt
	Status     SessionStatus `json:"status"`
	StartedAt  time.Time `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
}

// SessionStatus represents the lifecycle state of a session.
type SessionStatus string

const (
	SessionStatusRunning   SessionStatus = "RUNNING"
	SessionStatusCompleted SessionStatus = "COMPLETED"
	SessionStatusFailed    SessionStatus = "FAILED"
	SessionStatusCancelled SessionStatus = "CANCELLED"
)

// ── Artifact ──────────────────────────────────────

// Artifact is a versioned output produced by a node session.
type Artifact struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`       // "plan.md", "diff.patch"
	Type        string            `json:"type"`       // "markdown", "diff", "log", "code"
	Path        string            `json:"path"`       // storage path
	NodeID      string            `json:"node_id"`
	SessionID   string            `json:"session_id"`
	WorkflowID  string            `json:"workflow_id"`
	ParentID    string            `json:"parent_id,omitempty"` // lineage
	Version     int               `json:"version"`
	Size        int64             `json:"size"`
	ContentHash string            `json:"content_hash"` // sha256
	CreatedAt   time.Time         `json:"created_at"`
	Tags        []string          `json:"tags,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"` // semantic metadata
}

// ── Skill ─────────────────────────────────────────

// Skill defines an agent capability — what it does and what it needs.
type Skill struct {
	Name          string        `json:"name"`
	Version       string        `json:"version"`
	Description   string        `json:"description,omitempty"`
	Prompt        string        `json:"prompt"`
	Output        []string      `json:"output,omitempty"`     // expected output patterns
	ContextPolicy ContextPolicy `json:"context_policy,omitempty"`
}

// ContextPolicy defines what context a skill needs to execute.
type ContextPolicy struct {
	Strategy      string   `json:"strategy,omitempty"`     // "coding", "review", "planning"
	IncludeArtifacts []string `json:"include_artifacts,omitempty"` // artifact tags to include
	IncludeFiles  []string `json:"include_files,omitempty"`  // file globs
	MaxTokens     int      `json:"max_tokens,omitempty"`
}

// ContextSpec defines what a node should see when it starts.
type ContextSpec struct {
	NodeID     string        `json:"node_id"`
	WorkflowID string        `json:"workflow_id"`
	DependsOn  []string      `json:"depends_on"`
	Policy     ContextPolicy `json:"policy"`
	MaxTokens  int           `json:"max_tokens"`
}

// Context is the assembled context for a node.
type Context struct {
	SystemPrompt string             `json:"system_prompt"`
	Artifacts    []ContextArtifact  `json:"artifacts"`
	Files        []ContextFile      `json:"files,omitempty"`
	TokenCount   int                `json:"token_count"`
	BudgetLeft   int                `json:"budget_left"`
}

// ContextArtifact is an artifact included (or omitted) in the context.
type ContextArtifact struct {
	Artifact
	Included      bool   `json:"included"`
	OmittedReason string `json:"omitted_reason,omitempty"`
	Priority      int    `json:"priority"`
}

// ContextFile is a file included in the context.
type ContextFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
	Tokens  int    `json:"tokens"`
}

// AssembledPrompt is the final prompt sent to the runtime.
type AssembledPrompt struct {
	System  string   `json:"system"`
	Context string   `json:"context"`
	Tokens  int      `json:"tokens"`
	Omitted []string `json:"omitted,omitempty"` // what got dropped due to budget
}
