// Package store defines the EventStore — the append-only, immutable source of truth
// for all state changes in arlod. Every domain event flows through here.
//
// In v0.1, the implementation is SQLite with WAL mode.
// The interface is designed to support future backends (Postgres, NATS, etc.).
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Sentinel errors for store operations.
var (
	// ErrDuplicateEventID is returned when an event with the same ID already exists.
	ErrDuplicateEventID = errors.New("duplicate event ID")
)

// Event is the universal envelope for all domain events in arlod.
// It is immutable once written — never update or delete an event.
type Event struct {
	// ID is a unique identifier for this event (UUID).
	ID string `json:"id"`

	// StreamID identifies the aggregate this event belongs to.
	// Streams partition the event log by domain entity:
	//   workflow-{id}  — workflow-level events
	//   node-{id}      — node-level events
	//   runtime-{id}   — runtime instance events
	//   workspace-{id} — workspace events
	StreamID string `json:"stream_id"`

	// Version is the monotonically increasing sequence number within this stream.
	// The first event in a stream has version 1.
	Version int `json:"version"`

	// Position is the global position of this event across all streams.
	// Used for subscriptions ("give me everything after position 42").
	Position int64 `json:"position"`

	// Type identifies what happened (TASK_CREATED, NODE_STARTED, etc.).
	Type EventType `json:"type"`

	// Payload contains the type-specific event data as JSON.
	Payload json.RawMessage `json:"payload"`

	// Timestamp records when this event was appended.
	Timestamp time.Time `json:"timestamp"`
}

// EventType is a string enum identifying the kind of event.
// Using string for extensibility — new event types don't require code changes.
type EventType string

// Well-known event types. This list will grow as the domain model expands.
const (
	// Task lifecycle
	EventTaskCreated   EventType = "TASK_CREATED"
	EventTaskCompleted EventType = "TASK_COMPLETED"
	EventTaskFailed    EventType = "TASK_FAILED"
	EventTaskCancelled EventType = "TASK_CANCELLED"

	// Workflow lifecycle
	EventWorkflowCreated   EventType = "WORKFLOW_CREATED"
	EventWorkflowChanged   EventType = "WORKFLOW_CHANGED"
	EventWorkflowCompleted EventType = "WORKFLOW_COMPLETED"
	EventWorkflowFailed    EventType = "WORKFLOW_FAILED"
	EventWorkflowCancelled EventType = "WORKFLOW_CANCELLED"

	// Node lifecycle
	EventNodeCreated   EventType = "NODE_CREATED"
	EventNodeStarted   EventType = "NODE_STARTED"
	EventNodeCompleted EventType = "NODE_COMPLETED"
	EventNodeFailed    EventType = "NODE_FAILED"
	// TODO(v0.2): Rename to EventNodeEnteredWaiting = "NODE_ENTERED_WAITING"
	EventNodeWaiting EventType = "NODE_WAITING"

	// Runtime lifecycle
	EventRuntimeCreated EventType = "RUNTIME_CREATED"
	EventRuntimeStarted EventType = "RUNTIME_STARTED"
	EventRuntimeExited  EventType = "RUNTIME_EXITED"
	EventRuntimeLost    EventType = "RUNTIME_LOST"
	EventRuntimeFailed  EventType = "RUNTIME_FAILED"

	// Session lifecycle
	EventSessionCreated   EventType = "SESSION_CREATED"
	EventSessionCompleted EventType = "SESSION_COMPLETED"
	EventSessionFailed    EventType = "SESSION_FAILED"
	EventSessionCancelled EventType = "SESSION_CANCELLED"

	// Workspace
	EventWorkspaceCreated EventType = "WORKSPACE_CREATED"
	EventSlotCreated      EventType = "SLOT_CREATED"

	// Artifacts
	EventArtifactCreated EventType = "ARTIFACT_CREATED"

	// Human-in-loop
	// TODO(v0.2): Rename to EventHumanApprovalRequested = "HUMAN_APPROVAL_REQUESTED"
	EventHumanApprovalRequired EventType = "HUMAN_APPROVAL_REQUIRED"
	EventHumanInputReceived    EventType = "HUMAN_INPUT_RECEIVED"

	// Observability
	EventNodeHeartbeat   EventType = "NODE_HEARTBEAT"
	EventMetricsSnapshot EventType = "METRICS_SNAPSHOT"

	// Annotations
	EventNodeAnnotated EventType = "NODE_ANNOTATED"
)

// EventStore is the append-only, immutable event log.
// It is the single source of truth for all state in arlod.
//
// Key properties:
//   - Events are never modified or deleted once written.
//   - Events within a stream have monotonically increasing versions.
//   - The global position ordering reflects append order across all streams.
//   - Subscriptions deliver events in position order.
type EventStore interface {
	// Append writes events atomically to a stream.
	// All events in the batch share the same stream and are assigned
	// consecutive version numbers starting from the stream's current max version + 1.
	//
	// Returns the assigned global positions, which can be used for subscriptions.
	Append(ctx context.Context, streamID string, events []Event) ([]int64, error)

	// Read reads events from a stream starting at fromVersion (inclusive).
	// Returns an empty slice if the stream doesn't exist or fromVersion is beyond the last event.
	Read(ctx context.Context, streamID string, fromVersion int) ([]Event, error)

	// ReadAll reads events across all streams starting at fromPosition (inclusive),
	// ordered by global position. Returns up to limit events and the next position
	// to read from. Used for rebuilding projections on startup.
	ReadAll(ctx context.Context, fromPosition int64, limit int) ([]Event, int64, error)

	// Subscribe returns a channel that receives all new events as they are appended.
	// Events are delivered in position order starting from fromPosition (exclusive).
	// The channel is closed when ctx is cancelled.
	//
	// Only one subscription is expected in v0.1. Multi-subscriber fan-out
	// (Event Bus pattern) will be added when needed.
	Subscribe(ctx context.Context, fromPosition int64) (<-chan Event, error)

	// Close gracefully shuts down the event store, flushing any pending writes.
	Close() error

	// LastPosition returns the current max global position.
	// Used by StateStore to know where to resume incremental rebuilds.
	LastPosition() int64
}
