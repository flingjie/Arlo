// Package service implements the gRPC service handlers for arlod.
package service

import (
	"context"
	"io"

	"github.com/lingjiefan/arlo/internal/domain"
)

// WorkflowEngine is the consumer-side interface that service needs from the
// workflow engine: just Compile and Validate.
type WorkflowEngine interface {
	Compile(ctx context.Context, source []byte) (*domain.ExecutableGraph, error)
	Validate(ctx context.Context, graph *domain.ExecutableGraph) error
}

// Reconciler is the consumer-side interface that service needs from the
// reconciler: Submit (register a graph) and Reconcile (trigger one reconciliation).
type Reconciler interface {
	Submit(workflowID string, graph *domain.ExecutableGraph)
	Reconcile(ctx context.Context, workflowID string) error
}

// RuntimeManager is the consumer-side interface that service needs from the
// runtime manager: AttachInstance for PTY streaming and input.
type RuntimeManager interface {
	AttachInstance(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error)
}
