package domain

// RuntimeState represents the lifecycle state of a RuntimeInstance.
type RuntimeState string

const (
	RuntimeStatePreparing RuntimeState = "PREPARING"
	RuntimeStateStarting  RuntimeState = "STARTING"
	RuntimeStateRunning   RuntimeState = "RUNNING"
	RuntimeStateStopping  RuntimeState = "STOPPING"
	RuntimeStateExited    RuntimeState = "EXITED"
	RuntimeStateFailed    RuntimeState = "FAILED"
)

// RuntimeInstance represents a running agent process.
type RuntimeInstance struct {
	ID          string        `json:"id"`
	Type        string        `json:"type"` // "claude-code", "codex"
	Config      RuntimeConfig `json:"config"`
	SessionID   string        `json:"session_id"`
	WorkspaceID string        `json:"workspace_id"`
	SlotID      string        `json:"slot_id"`
	WorkDir     string        `json:"work_dir"`
	Prompt      string        `json:"prompt"`
	State       RuntimeState  `json:"state"`
}

// RuntimeConfig configures how an agent runtime behaves.
type RuntimeConfig struct {
	Model          string   `json:"model,omitempty"`
	Capabilities   []string `json:"capabilities,omitempty"`   // "filesystem", "git", "browser"
	PermissionMode string   `json:"permission_mode,omitempty"` // "auto", "manual"
}

// RuntimeStatus is the observable status of a RuntimeInstance.
// PID is intentionally excluded — it's an implementation detail.
type RuntimeStatus struct {
	ID      string        `json:"id"`
	State   RuntimeState  `json:"state"`
	Metrics RuntimeMetrics `json:"metrics"`
}

// RuntimeMetrics captures resource usage of an agent execution.
type RuntimeMetrics struct {
	TokensIn   int64   `json:"tokens_in"`
	TokensOut  int64   `json:"tokens_out"`
	CostUSD    float64 `json:"cost_usd"`
	ToolCalls  int     `json:"tool_calls"`
	FileEdits  int     `json:"file_edits"`
	Retries    int     `json:"retries"`
	HumanAsks  int     `json:"human_asks"`
	DurationMs int64   `json:"duration_ms"`
}

// Instruction is a control message sent to a running agent.
type Instruction struct {
	Type    string `json:"type"`    // "approve", "reject", "hint", "context"
	Content string `json:"content"`
}

// PTYFrame carries raw terminal output from an agent session.
type PTYFrame struct {
	SessionID string `json:"session_id"`
	Data      []byte `json:"data"`
	Seq       int64  `json:"seq"`
}
