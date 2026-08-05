// Package runtime defines the RuntimeAdapter interface and manages agent process lifecycles.
//
// A RuntimeAdapter translates between arlod's execution model and a specific
// agent CLI (Claude Code, Codex, etc.). It manages Prepare → Start → Stop → Destroy.
//
// The Control Plane and Human Plane are strictly separated:
//   - Events() channel: structured control events (RUNTIME_STARTED, RUNTIME_EXITED)
//   - Attach() PTY stream: raw terminal output for human consumption
package runtime

import (
	"context"
	"io"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
)

// Adapter manages the lifecycle of an agent process.
type Adapter interface {
	// Prepare sets up prerequisites (env vars, credentials, workspace) before starting.
	Prepare(ctx context.Context, inst domain.RuntimeInstance) error

	// Start launches the agent process.
	Start(ctx context.Context, inst domain.RuntimeInstance) error

	// Stop gracefully terminates the agent process.
	Stop(ctx context.Context, id string) error

	// Destroy cleans up all resources associated with the instance.
	Destroy(ctx context.Context, id string) error

	// SendInstruction routes a control message to the agent.
	SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error

	// Status returns the current observable status of the runtime instance.
	Status(ctx context.Context, id string) (domain.RuntimeStatus, error)

	// Snapshot returns the current observable state of the runtime process.
	// Used by the TUI inspector and GetRuntimeSnapshot RPC.
	Snapshot(ctx context.Context, id string) (domain.RuntimeSnapshot, error)
}

// InteractiveRuntime is an optional interface for runtimes that expose a PTY stream.
// Not all runtimes have terminal interfaces (e.g., API-based agents).
type InteractiveRuntime interface {
	// Attach returns a reader for the PTY output stream and a writer for stdin input.
	Attach(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error)
}

// RuntimeEventType categorizes real-time events emitted by a running agent.
type RuntimeEventType string

const (
	RuntimeEventThinking    RuntimeEventType = "THINKING"
	RuntimeEventToolCall    RuntimeEventType = "TOOL_CALL"
	RuntimeEventToolResult  RuntimeEventType = "TOOL_RESULT"
	RuntimeEventFileChanged RuntimeEventType = "FILE_CHANGED"
	RuntimeEventCommandExec RuntimeEventType = "COMMAND_EXEC"
	RuntimeEventTokenUsage  RuntimeEventType = "TOKEN_USAGE"
	RuntimeEventError       RuntimeEventType = "ERROR"
	RuntimeEventCompleted   RuntimeEventType = "COMPLETED"
)

// RuntimeEvent is a structured observation emitted by a runtime adapter.
type RuntimeEvent struct {
	Type      RuntimeEventType `json:"type"`
	RuntimeID string           `json:"runtime_id"`
	Action    string           `json:"action"`              // human-readable: "running pytest"
	ToolName  string           `json:"tool_name,omitempty"`
	FilePath  string           `json:"file_path,omitempty"`
	Detail    string           `json:"detail,omitempty"`    // e.g., command output preview
	Timestamp time.Time        `json:"timestamp"`
}

// OutputProvider is an optional interface for adapters that capture process stdout.
// The reconciler uses it to save skill output files after node completion.
type OutputProvider interface {
	Output(ctx context.Context, id string) ([]byte, error)
}
