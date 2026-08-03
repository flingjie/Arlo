package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/store"
)

// InMemoryStateStore implements StateStore with in-memory projections.
// It subscribes to the Event Store and maintains materialized views
// of workflow state. On startup, Rebuild() replays all events.
//
// Thread-safe: all methods are guarded by a read-write mutex.
type InMemoryStateStore struct {
	mu           sync.RWMutex
	eventStore   store.EventStore
	projections  map[string]Projection
	workflows    map[string]*domain.WorkflowState // workflowID → state
	nodeIndex    map[string]string                // nodeID → workflowID (reverse lookup)
}

// NewInMemoryStateStore creates a new state store backed by the given Event Store.
func NewInMemoryStateStore(eventStore store.EventStore) *InMemoryStateStore {
	ss := &InMemoryStateStore{
		eventStore:  eventStore,
		projections: make(map[string]Projection),
		workflows:   make(map[string]*domain.WorkflowState),
		nodeIndex:   make(map[string]string),
	}

	// Register built-in projections.
	ss.projections["workflow"] = &workflowProjection{store: ss}

	return ss
}

// GetWorkflow returns the current state of a workflow.
func (ss *InMemoryStateStore) GetWorkflow(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	wf, ok := ss.workflows[workflowID]
	if !ok {
		return nil, &NotFoundError{Entity: "workflow", ID: workflowID}
	}
	// Return a copy to prevent mutation.
	cp := *wf
	cp.Nodes = make(map[string]domain.NodeState, len(wf.Nodes))
	for k, v := range wf.Nodes {
		cp.Nodes[k] = v
	}
	return &cp, nil
}

// ListActiveWorkflows returns all workflows with non-terminal status.
func (ss *InMemoryStateStore) ListActiveWorkflows(ctx context.Context) ([]domain.WorkflowState, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var active []domain.WorkflowState
	for _, wf := range ss.workflows {
		if !isTerminal(wf.Status) {
			cp := *wf
			cp.Nodes = make(map[string]domain.NodeState, len(wf.Nodes))
			for k, v := range wf.Nodes {
				cp.Nodes[k] = v
			}
			active = append(active, cp)
		}
	}
	return active, nil
}

// GetNodeState returns the current state of a single node.
func (ss *InMemoryStateStore) GetNodeState(ctx context.Context, nodeID string) (*domain.NodeState, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	wfID, ok := ss.nodeIndex[nodeID]
	if !ok {
		return nil, &NotFoundError{Entity: "node", ID: nodeID}
	}
	wf, ok := ss.workflows[wfID]
	if !ok {
		return nil, &NotFoundError{Entity: "node", ID: nodeID}
	}
	ns, ok := wf.Nodes[nodeID]
	if !ok {
		return nil, &NotFoundError{Entity: "node", ID: nodeID}
	}
	return &ns, nil
}

// GetReadyNodes returns all READY nodes for a workflow.
func (ss *InMemoryStateStore) GetReadyNodes(ctx context.Context, workflowID string) ([]domain.NodeState, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	wf, ok := ss.workflows[workflowID]
	if !ok {
		return nil, &NotFoundError{Entity: "workflow", ID: workflowID}
	}

	var ready []domain.NodeState
	for _, ns := range wf.Nodes {
		if ns.Status == domain.NodeStatusReady {
			ready = append(ready, ns)
		}
	}
	return ready, nil
}

// Rebuild replays all events from the Event Store to reconstruct projections.
// It resets all projections and replays every event in position order.
// The lock is not held during the full replay — individual Apply calls
// acquire the lock themselves. This is safe because Rebuild is called at
// startup before any concurrent readers exist.
func (ss *InMemoryStateStore) Rebuild(ctx context.Context) error {
	// Reset all projections (requires lock for mutation).
	ss.mu.Lock()
	for _, proj := range ss.projections {
		proj.Reset()
	}
	ss.mu.Unlock()

	// Replay all events from position 1.
	pos := int64(1)
	for {
		events, nextPos, err := ss.eventStore.ReadAll(ctx, pos, 1000)
		if err != nil {
			return fmt.Errorf("rebuild: read at position %d: %w", pos, err)
		}
		if len(events) == 0 {
			break
		}
		for _, event := range events {
			for _, proj := range ss.projections {
				if err := proj.Apply(event); err != nil {
					return fmt.Errorf("rebuild: apply event %s (pos %d): %w", event.ID, event.Position, err)
				}
			}
		}
		pos = nextPos
	}

	return nil
}

// ── Internal: Projection helpers ───────────────────

// upsertWorkflow ensures a workflow exists, creating it if needed.
// Must be called with the write lock held.
func (ss *InMemoryStateStore) upsertWorkflow(id string) *domain.WorkflowState {
	wf, ok := ss.workflows[id]
	if !ok {
		wf = &domain.WorkflowState{
			ID:     id,
			Status: domain.WorkflowStatusActive,
			Nodes:  make(map[string]domain.NodeState),
		}
		ss.workflows[id] = wf
	}
	return wf
}

// upsertNode ensures a node state exists within a workflow.
// Must be called with the write lock held.
func (ss *InMemoryStateStore) upsertNode(workflowID, nodeID string) *domain.NodeState {
	wf := ss.upsertWorkflow(workflowID)
	ns, ok := wf.Nodes[nodeID]
	if !ok {
		ns = domain.NodeState{
			NodeID: nodeID,
			Status: domain.NodeStatusPending,
		}
		wf.Nodes[nodeID] = ns
	}
	ss.nodeIndex[nodeID] = workflowID
	return &ns
}

// isTerminal returns true if the workflow status is final.
func isTerminal(s domain.WorkflowStatus) bool {
	return s == domain.WorkflowStatusCompleted ||
		s == domain.WorkflowStatusFailed ||
		s == domain.WorkflowStatusCancelled
}

// ── workflowProjection ────────────────────────────

// workflowProjection applies events to the in-memory state store.
type workflowProjection struct {
	store *InMemoryStateStore
}

func (p *workflowProjection) Apply(event store.Event) error {
	p.store.mu.Lock()
	defer p.store.mu.Unlock()

	switch event.Type {
	// ── Task events ───────────────────────────────
	case store.EventTaskCreated:
		return p.applyTaskCreated(event)

	// ── Workflow events ───────────────────────────
	case store.EventWorkflowCreated:
		return p.applyWorkflowCreated(event)

	// ── Node events ───────────────────────────────
	case store.EventNodeCreated:
		return p.applyNodeCreated(event)
	case store.EventNodeStarted:
		return p.applyNodeStarted(event)
	case store.EventNodeCompleted:
		return p.applyNodeCompleted(event)
	case store.EventNodeFailed:
		return p.applyNodeFailed(event)
	case store.EventNodeWaiting:
		return p.applyNodeWaiting(event)

	default:
		// Unknown event types are silently ignored — projections are
		// additive and don't need to know every event type.
		return nil
	}
}

func (p *workflowProjection) Reset() {
	// Reset is called while the store's write lock is held in Rebuild.
	p.store.workflows = make(map[string]*domain.WorkflowState)
	p.store.nodeIndex = make(map[string]string)
}

// ── Event applicators ─────────────────────────────

func (p *workflowProjection) applyTaskCreated(event store.Event) error {
	var payload domain.TaskCreated
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal TaskCreated: %w", err)
	}
	p.store.upsertWorkflow(payload.WorkflowID)
	return nil
}

func (p *workflowProjection) applyWorkflowCreated(event store.Event) error {
	var payload domain.WorkflowCreated
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal WorkflowCreated: %w", err)
	}
	p.store.upsertWorkflow(payload.WorkflowID)
	return nil
}

func (p *workflowProjection) applyNodeCreated(event store.Event) error {
	var payload domain.NodeCreated
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeCreated: %w", err)
	}
	p.store.upsertNode(payload.WorkflowID, payload.NodeID)
	return nil
}

func (p *workflowProjection) applyNodeStarted(event store.Event) error {
	var payload domain.NodeStarted
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeStarted: %w", err)
	}

	// Find the node by searching all workflows.
	for _, wf := range p.store.workflows {
		if ns, ok := wf.Nodes[payload.NodeID]; ok {
			ns.Status = domain.NodeStatusRunning
			ns.SessionID = payload.SessionID
			now := time.Now()
			ns.StartedAt = &now
			wf.Nodes[payload.NodeID] = ns
			return nil
		}
	}
	return fmt.Errorf("node %s not found for NodeStarted event", payload.NodeID)
}

func (p *workflowProjection) applyNodeCompleted(event store.Event) error {
	var payload domain.NodeCompleted
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeCompleted: %w", err)
	}

	for _, wf := range p.store.workflows {
		if ns, ok := wf.Nodes[payload.NodeID]; ok {
			ns.Status = domain.NodeStatusCompleted
			ns.Output = payload.Output
			now := time.Now()
			ns.CompletedAt = &now
			wf.Nodes[payload.NodeID] = ns
			return nil
		}
	}
	return fmt.Errorf("node %s not found for NodeCompleted event", payload.NodeID)
}

func (p *workflowProjection) applyNodeFailed(event store.Event) error {
	var payload domain.NodeFailed
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeFailed: %w", err)
	}

	for _, wf := range p.store.workflows {
		if ns, ok := wf.Nodes[payload.NodeID]; ok {
			if payload.Retryable {
				ns.Status = domain.NodeStatusReady
			} else {
				ns.Status = domain.NodeStatusFailed
			}
			ns.RetryCount++
			wf.Nodes[payload.NodeID] = ns
			return nil
		}
	}
	return fmt.Errorf("node %s not found for NodeFailed event", payload.NodeID)
}

func (p *workflowProjection) applyNodeWaiting(event store.Event) error {
	var payload domain.NodeWaiting
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeWaiting: %w", err)
	}

	for _, wf := range p.store.workflows {
		if ns, ok := wf.Nodes[payload.NodeID]; ok {
			ns.Status = domain.NodeStatusWaiting
			wf.Nodes[payload.NodeID] = ns
			return nil
		}
	}
	return fmt.Errorf("node %s not found for NodeWaiting event", payload.NodeID)
}
