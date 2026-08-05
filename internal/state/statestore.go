// Package state provides query-optimized views (projections) built from the event log.
//
// Every view is a projection: replay events → current state.
// Projections are disposable — they can be fully rebuilt from the Event Store at any time.
//
// The StateStore is the read side of the CQRS pattern:
//
//	Write: Command → EventStore.Append()
//	Read:  StateStore.GetWorkflow() (reads from in-memory projections)
package state

import (
	"context"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/store"
)

// Projection is a materialized view that subscribes to events and updates
// its internal state. Used by StateStore internally.
type Projection interface {
	// Apply updates the projection with a new event.
	// Returns an error if the event cannot be applied (e.g., unknown event type).
	Apply(event store.Event) error

	// Reset clears the projection for a full rebuild.
	Reset()
}

// StateStore provides query-optimized views of the system state.
// All state is derived from events — StateStore is read-only from the
// application's perspective. Only projections write to it.
//
// In v0.1, state is kept entirely in memory as Go maps.
// Future: snapshot + incremental rebuild for faster startup.
type StateStore interface {
	// ── Workflow views ──────────────────────────

	// GetWorkflow returns the current state of a workflow, including all node states.
	GetWorkflow(ctx context.Context, workflowID string) (*domain.WorkflowState, error)

	// ListActiveWorkflows returns all workflows that are not in a terminal state.
	ListActiveWorkflows(ctx context.Context) ([]domain.WorkflowState, error)

	// ── Node views ──────────────────────────────

	// GetNodeState returns the current state of a single node.
	GetNodeState(ctx context.Context, nodeID string) (*domain.NodeState, error)

	// GetNodeStateBySession returns the node state for a given session ID.
	// Uses a session index for O(1) lookup instead of scanning all workflows.
	GetNodeStateBySession(ctx context.Context, sessionID string) (*domain.NodeState, error)

	// GetReadyNodes returns all nodes in READY state for a given workflow.
	GetReadyNodes(ctx context.Context, workflowID string) ([]domain.NodeState, error)

	// ── Rebuild ─────────────────────────────────

	// Rebuild replays all events from the Event Store to reconstruct projections.
	// Called on startup. Blocks until all events are processed.
	Rebuild(ctx context.Context) error

	// Apply updates projections with a single newly-appended event.
	// This is the incremental path — after an event is appended to the Event
	// Store, call Apply to keep projections in sync without a full Rebuild.
	Apply(event store.Event) error
}

// ErrNotFound is returned when a workflow or node is not found.
var ErrNotFound = &NotFoundError{}

// NotFoundError is returned when a requested entity doesn't exist in the state store.
type NotFoundError struct {
	Entity string
	ID     string
}

func (e *NotFoundError) Error() string {
	if e.Entity == "" {
		e.Entity = "entity"
	}
	return e.Entity + " not found: " + e.ID
}
