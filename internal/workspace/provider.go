// Package workspace defines the WorkspaceProvider interface and manages
// execution environments. A Workspace is a logical resource that houses
// RuntimeInstances in ExecutionSlots.
//
// Provider mapping (v0.1 → v1):
//
//	TmuxProvider:    Workspace = tmux session,    Slot = tmux window
//	DockerProvider:  Workspace = container,       Slot = exec session
//	K8sProvider:     Workspace = namespace,        Slot = pod
package workspace

import (
	"context"
	"io"

	"github.com/lingjiefan/arlo/internal/domain"
)

// Provider creates and manages execution environments.
type Provider interface {
	// Create creates a new workspace.
	Create(ctx context.Context, spec domain.WorkspaceSpec) (*domain.Workspace, error)

	// Destroy tears down a workspace and all its slots.
	Destroy(ctx context.Context, wsID string) error

	// CreateSlot creates a new execution slot within a workspace.
	CreateSlot(ctx context.Context, wsID string, spec domain.SlotSpec) (*domain.ExecutionSlot, error)

	// DeleteSlot removes an execution slot.
	DeleteSlot(ctx context.Context, slotID string) error

	// BindRuntime records that a runtime is assigned to a slot.
	// Does NOT start the process — that's the RuntimeAdapter's job.
	BindRuntime(ctx context.Context, slotID string, runtimeID string) error

	// Attach connects to a slot's terminal stream.
	Attach(ctx context.Context, slotID string) (<-chan domain.PTYFrame, io.Writer, error)

	// Status returns the current status of a workspace.
	Status(ctx context.Context, wsID string) (domain.WorkspaceStatus, error)
}
