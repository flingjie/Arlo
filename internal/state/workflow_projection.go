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

	// lastRebuiltPos tracks the last global position that was rebuilt.
	// Rebuild() uses this to only replay new events, avoiding O(n²) behavior.
	lastRebuiltPos int64
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

// Rebuild replays events from the Event Store to bring projections up to date.
// On first call, it replays all events. On subsequent calls, it only replays
// events since the last known position (incremental rebuild), avoiding O(n²).
//
// The lock is not held during the replay — individual Apply calls
// acquire it themselves.
func (ss *InMemoryStateStore) Rebuild(ctx context.Context) error {
	// Determine where to start replaying.
	startPos := ss.lastRebuiltPos + 1

	// Only reset on a full rebuild (first call).
	if ss.lastRebuiltPos == 0 {
		ss.mu.Lock()
		for _, proj := range ss.projections {
			proj.Reset()
		}
		ss.mu.Unlock()
		startPos = 1
	}

	// Replay events.
	pos := startPos
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

	// Track the last position we've rebuilt to.
	// nextPos is one past the last event; the last built position is the max
	// event position we processed in the last batch.
	currentPos := ss.eventStore.LastPosition()
	if currentPos > ss.lastRebuiltPos {
		ss.lastRebuiltPos = currentPos
	}


		// Compute workflow trees (Children from DependsOn).
		ss.mu.Lock()
		ss.rebuildTree()
		ss.mu.Unlock()

	return nil
}

// Apply updates projections with a single newly-appended event.
// This is the incremental path — use it after appending an event to the Event
// Store to keep projections in sync without a full Rebuild.
func (ss *InMemoryStateStore) Apply(event store.Event) error {
	// Apply to all registered projections.
	for _, proj := range ss.projections {
		if err := proj.Apply(event); err != nil {
			return fmt.Errorf("apply event %s (pos %d): %w", event.ID, event.Position, err)
		}
	}

	// Recompute Children trees after every event — this is cheap for
	// the expected volume of events in a single reconciliation tick.
	ss.mu.Lock()
	ss.rebuildTree()
	ss.lastRebuiltPos = event.Position
	ss.mu.Unlock()

	return nil
}

// ── Internal: Projection helpers ───────────────────

// rebuildTree computes Children from DependsOn for all nodes in all workflows.
// Must be called with the write lock held.
func (ss *InMemoryStateStore) rebuildTree() {
	for _, wf := range ss.workflows {
		for nodeID, ns := range wf.Nodes {
			for _, dep := range ns.DependsOn {
				if depNS, ok := wf.Nodes[dep]; ok {
					found := false
					for _, child := range depNS.Children {
						if child == nodeID {
							found = true
							break
						}
					}
					if !found {
						depNS.Children = append(depNS.Children, nodeID)
						wf.Nodes[dep] = depNS
					}
				}
			}
		}
	}
}

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
	case store.EventTaskCompleted:
		return p.applyTaskCompleted(event)
	case store.EventTaskFailed:
		return p.applyTaskFailed(event)
	case store.EventTaskCancelled:
		return p.applyTaskCancelled(event)

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

	// ── Human interaction ─────────────────────────
	case store.EventHumanInputReceived:
		return p.applyHumanInputReceived(event)

	// ── Observability ────────────────────────────
	case store.EventNodeHeartbeat:
		return p.applyNodeHeartbeat(event)
	case store.EventMetricsSnapshot:
		return p.applyMetricsSnapshot(event)

	// ── Annotations ───────────────────────────────
	case store.EventNodeAnnotated:
		return p.applyNodeAnnotated(event)

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

	dependsOn := payload.DependsOn
	if dependsOn == nil {
		dependsOn = []string{}
	}

	ns := domain.NodeState{
		NodeID:     payload.NodeID,
		WorkflowID: payload.WorkflowID,
		Status:     domain.NodeStatusPending,
		DependsOn:  dependsOn,
		Gate:       payload.Gate,
	}

	wf := p.store.upsertWorkflow(payload.WorkflowID)
	wf.Nodes[payload.NodeID] = ns
	p.store.nodeIndex[payload.NodeID] = payload.WorkflowID
	return nil
}

// lookupNode finds the correct workflow and node for a node event.
// Uses WorkflowID from the payload when available (exact match); falls back
// to nodeIndex for backward compatibility with older events.
func (p *workflowProjection) lookupNode(nodeID, workflowID string) (*domain.WorkflowState, *domain.NodeState, error) {
	if workflowID == "" {
		var ok bool
		workflowID, ok = p.store.nodeIndex[nodeID]
		if !ok {
			return nil, nil, fmt.Errorf("node %s not found in index", nodeID)
		}
	}
	wf, ok := p.store.workflows[workflowID]
	if !ok {
		return nil, nil, fmt.Errorf("workflow %s not found", workflowID)
	}
	ns, ok := wf.Nodes[nodeID]
	if !ok {
		return nil, nil, fmt.Errorf("node %s not found in workflow %s", nodeID, workflowID)
	}
	return wf, &ns, nil
}

func (p *workflowProjection) applyNodeStarted(event store.Event) error {
	var payload domain.NodeStarted
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeStarted: %w", err)
	}

	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("NodeStarted: %w", err)
	}
	ns.Status = domain.NodeStatusRunning
	ns.SessionID = payload.SessionID
	now := time.Now()
	ns.StartedAt = &now
	wf.Nodes[payload.NodeID] = *ns
	return nil
}

func (p *workflowProjection) applyNodeCompleted(event store.Event) error {
	var payload domain.NodeCompleted
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeCompleted: %w", err)
	}

	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("NodeCompleted: %w", err)
	}
	ns.Status = domain.NodeStatusCompleted
	ns.Output = payload.Output
	now := time.Now()
	ns.CompletedAt = &now
	wf.Nodes[payload.NodeID] = *ns
	return nil
}

func (p *workflowProjection) applyNodeFailed(event store.Event) error {
	var payload domain.NodeFailed
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeFailed: %w", err)
	}

	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("NodeFailed: %w", err)
	}
	if payload.Retryable {
		ns.Status = domain.NodeStatusReady
	} else {
		ns.Status = domain.NodeStatusFailed
	}
	ns.RetryCount++
	wf.Nodes[payload.NodeID] = *ns
	return nil
}

func (p *workflowProjection) applyNodeWaiting(event store.Event) error {
	var payload domain.NodeWaiting
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeWaiting: %w", err)
	}

	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("NodeWaiting: %w", err)
	}
	ns.Status = domain.NodeStatusWaiting
	ns.Gate = "" // gate satisfied — clear to prevent re-pausing after resume
	wf.Nodes[payload.NodeID] = *ns
	return nil
}

func (p *workflowProjection) applyTaskCompleted(event store.Event) error {
	var payload domain.TaskCompleted
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal TaskCompleted: %w", err)
	}

	// TaskCompleted payload has TaskID, but we need the workflowID.
	// In v0.1, the event streamID is "workflow-<id>", so we extract the workflow ID
	// from the stream, or match by task ID by searching workflows.
	// Simplification: mark all active workflows with matching task as completed.
	// The event.StreamID is "workflow-<workflowID>".
	wfID := event.StreamID
	if len(wfID) > 9 && wfID[:9] == "workflow-" {
		wfID = wfID[9:]
	}
	if wf, ok := p.store.workflows[wfID]; ok {
		wf.Status = domain.WorkflowStatusCompleted
	}
	return nil
}

func (p *workflowProjection) applyTaskFailed(event store.Event) error {
	var payload domain.TaskFailed
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal TaskFailed: %w", err)
	}
	wfID := event.StreamID
	if len(wfID) > 9 && wfID[:9] == "workflow-" {
		wfID = wfID[9:]
	}
	if wf, ok := p.store.workflows[wfID]; ok {
		wf.Status = domain.WorkflowStatusFailed
	}
	return nil
}

func (p *workflowProjection) applyTaskCancelled(event store.Event) error {
	wfID := event.StreamID
	if len(wfID) > 9 && wfID[:9] == "workflow-" {
		wfID = wfID[9:]
	}
	if wf, ok := p.store.workflows[wfID]; ok {
		wf.Status = domain.WorkflowStatusCancelled
	}
	return nil
}

func (p *workflowProjection) applyHumanInputReceived(event store.Event) error {
	var payload domain.HumanInputReceived
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal HumanInputReceived: %w", err)
	}
	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("HumanInputReceived: %w", err)
	}
	if payload.Decision == "approve" {
		ns.Status = domain.NodeStatusReady
	} else {
		ns.Status = domain.NodeStatusFailed
	}
	wf.Nodes[payload.NodeID] = *ns
	return nil
}

// ── Observability applicators ─────────────────────

func (p *workflowProjection) applyNodeHeartbeat(event store.Event) error {
	var payload domain.NodeHeartbeat
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeHeartbeat: %w", err)
	}
	// Heartbeat is a lightweight liveness check — just verify the node exists.
	// The node status in the event payload is informational.
	_, _, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("NodeHeartbeat: %w", err)
	}
	return nil
}

func (p *workflowProjection) applyMetricsSnapshot(event store.Event) error {
	var payload domain.MetricsSnapshot
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal MetricsSnapshot: %w", err)
	}
	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("MetricsSnapshot: %w", err)
	}
	ns.Metrics = domain.NodeMetrics{
		TokensIn:   ns.Metrics.TokensIn + payload.TokensIn,
		TokensOut:  ns.Metrics.TokensOut + payload.TokensOut,
		ToolCalls:  ns.Metrics.ToolCalls + payload.ToolCalls,
		CostUSD:    ns.Metrics.CostUSD + payload.CostUSD,
		DurationMs: ns.Metrics.DurationMs + payload.DurationMs,
	}
	wf.Nodes[payload.NodeID] = *ns
	return nil
}

// ── Annotation applicators ────────────────────────

func (p *workflowProjection) applyNodeAnnotated(event store.Event) error {
	var payload domain.NodeAnnotated
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal NodeAnnotated: %w", err)
	}
	wf, ns, err := p.lookupNode(payload.NodeID, payload.WorkflowID)
	if err != nil {
		return fmt.Errorf("NodeAnnotated: %w", err)
	}
	ns.Annotations = append(ns.Annotations, domain.Annotation{
		Key:       payload.Key,
		Value:     payload.Value,
		Timestamp: event.Timestamp,
	})
	wf.Nodes[payload.NodeID] = *ns
	return nil
}
