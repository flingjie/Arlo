// Package reconciler implements arlod's control loop — the heart of the system.
//
// The Reconciler reads current state from the StateStore, calls the WorkflowEngine
// to evaluate what should happen next, executes those decisions by appending events
// to the EventStore, and the cycle repeats.
//
// Key properties:
//   - Idempotent: reconciling the same workflow 100 times produces no duplicate actions.
//   - Self-healing: after a crash, Rebuild projections + Reconcile all active workflows
//     brings the system back to the correct state.
//   - Two triggers: periodic tick (safety net) + optional event subscription (low latency).
//
// In v0.1, decisions only transition node states via events — actual agent launching
// happens in Step 6 (Runtime + Workspace).
package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/runtime"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"github.com/lingjiefan/arlo/internal/workflow"
	"github.com/lingjiefan/arlo/internal/workspace"
)

// Reconciler is the control loop that drives the system toward desired state.
type Reconciler struct {
	stateStore  state.StateStore
	eventStore  store.EventStore
	engine      *workflow.Engine
		runtimeMgr   *runtime.Manager
		workspaceMgr *workspace.Manager


	// graphRegistry maps workflowID → compiled graph.
	// The Reconciler needs both state and graph to evaluate.
	graphRegistry map[string]*domain.ExecutableGraph

	tickInterval time.Duration
}

// New creates a new Reconciler.
func New(
	stateStore state.StateStore,
	eventStore store.EventStore,
	engine *workflow.Engine,
	runtimeMgr *runtime.Manager,
	workspaceMgr *workspace.Manager,
) *Reconciler {
	return &Reconciler{
		stateStore:    stateStore,
		eventStore:    eventStore,
		engine:        engine,
		runtimeMgr:    runtimeMgr,
		workspaceMgr:  workspaceMgr,
		graphRegistry: make(map[string]*domain.ExecutableGraph),
		tickInterval:  5 * time.Second,
	}
}

// WithTickInterval sets the reconciliation tick interval.
func (r *Reconciler) WithTickInterval(d time.Duration) *Reconciler {
	r.tickInterval = d
	return r
}

// Submit registers a workflow for reconciliation.
// The graph is stored so Evaluate() has access to dependency information.
func (r *Reconciler) Submit(workflowID string, graph *domain.ExecutableGraph) {
	r.graphRegistry[workflowID] = graph
}

// Start begins the reconciliation loop. It periodically reconciles all
// active workflows. Returns when ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) error {
	slog.Info("reconciler started", "interval", r.tickInterval)

	ticker := time.NewTicker(r.tickInterval)
	defer ticker.Stop()

	// Reconcile immediately on startup.
	r.reconcileAll(ctx)

	for {
		select {
		case <-ctx.Done():
			slog.Info("reconciler stopped")
			return ctx.Err()
		case <-ticker.C:
			r.reconcileAll(ctx)
		}
	}
}

// Reconcile reconciles a single workflow. It is idempotent — calling it
// multiple times with the same state produces no duplicate side effects.
func (r *Reconciler) Reconcile(ctx context.Context, workflowID string) error {
	// 1. READ current state.
	currentState, err := r.stateStore.GetWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("reconcile: get workflow %s: %w", workflowID, err)
	}

	graph, ok := r.graphRegistry[workflowID]
	if !ok {
		return fmt.Errorf("reconcile: graph not found for workflow %s", workflowID)
	}

	// 2. COMPUTE desired actions.
	decisions, err := r.engine.Evaluate(ctx, graph, *currentState)
	if err != nil {
		return fmt.Errorf("reconcile: evaluate %s: %w", workflowID, err)
	}

	if len(decisions) == 0 {
		return nil
	}

	// 3. ACT — execute each decision.
	for _, d := range decisions {
		if err := r.executeDecision(ctx, workflowID, d); err != nil {
			slog.Error("reconcile: execute decision failed",
				"workflow", workflowID,
				"action", d.Action,
				"node", d.NodeID,
				"error", err,
			)
			// Don't return — continue with other decisions.
			// The next reconciliation tick will retry.
		}
	}

	return nil
}

// ── Decision Execution ───────────────────────────

func (r *Reconciler) executeDecision(ctx context.Context, workflowID string, d domain.Decision) error {
	switch d.Action {
	case domain.DecisionStartNode:
		return r.executeStartNode(ctx, workflowID, d)
	case domain.DecisionCompleteWorkflow:
		return r.executeCompleteWorkflow(ctx, workflowID, d)
	case domain.DecisionFailWorkflow:
		return r.executeFailWorkflow(ctx, workflowID, d)
	default:
		return fmt.Errorf("unknown decision action: %s", d.Action)
	}
}

// executeStartNode transitions a node from PENDING/READY to RUNNING.
// In v0.1, this only appends NODE_STARTED. The actual agent launch (Step 6)
// will be added later.
func (r *Reconciler) executeStartNode(ctx context.Context, workflowID string, d domain.Decision) error {
	// Verify the node is actually in a startable state.
	ns, err := r.stateStore.GetNodeState(ctx, d.NodeID)
	if err != nil {
		return fmt.Errorf("start node: get state for %s: %w", d.NodeID, err)
	}

	if ns.Status != domain.NodeStatusPending && ns.Status != domain.NodeStatusReady {
		// Already started or completed — idempotent no-op.
		slog.Debug("start node: node already in non-startable state, skipping",
			"node", d.NodeID,
			"status", ns.Status,
		)
		return nil
	}

	sessionID := fmt.Sprintf("sess-%s-%d", d.NodeID, ns.RetryCount+1)

	// Append NODE_STARTED event.
	payload, _ := json.Marshal(domain.NodeStarted{
		NodeID:    d.NodeID,
		SessionID: sessionID,
	})

	_, err = r.eventStore.Append(ctx, "node-"+d.NodeID, []store.Event{{
		ID:      fmt.Sprintf("evt-ns-%s-%d", d.NodeID, time.Now().UnixNano()),
		Type:    store.EventNodeStarted,
		Payload: payload,
	}})
	if err != nil {
		return fmt.Errorf("start node: append NODE_STARTED for %s: %w", d.NodeID, err)
	}

	slog.Info("node started",
		"workflow", workflowID,
		"node", d.NodeID,
		"session", sessionID,
		"reason", d.Reason,
	)

	// Actually launch the agent runtime (v0.2).
	if r.runtimeMgr != nil {
		graph := r.graphRegistry[workflowID]
		if graph != nil {
			// Find the node in the graph to get runtime config.
			for _, n := range graph.Nodes {
				if n.ID == d.NodeID {
					_, err := r.runtimeMgr.StartInstance(ctx, runtime.RuntimeSpec{
						InstanceID:  fmt.Sprintf("rt-%s-%d", d.NodeID, ns.RetryCount+1),
						Type:        n.Runtime.Provider,
						Config: domain.RuntimeConfig{
							Model:          n.Runtime.Model,
							PermissionMode: "auto",
						},
						SessionID: sessionID,
						WorkDir:    "/tmp",
						Prompt:     "Run skill: " + n.SkillRef.Name,
					})
					if err != nil {
						slog.Warn("failed to launch runtime", "node", d.NodeID, "error", err)
					}
					break
				}
			}
		}
	}

	return nil
}

// executeCompleteWorkflow marks a workflow as completed.
func (r *Reconciler) executeCompleteWorkflow(ctx context.Context, workflowID string, d domain.Decision) error {
	// Check if already completed (idempotent).
	current, _ := r.stateStore.GetWorkflow(ctx, workflowID)
	if current != nil && current.Status == domain.WorkflowStatusCompleted {
		slog.Debug("complete workflow: already completed, skipping", "workflow", workflowID)
		return nil
	}

	payload, _ := json.Marshal(domain.TaskCompleted{TaskID: workflowID})

	_, err := r.eventStore.Append(ctx, "workflow-"+workflowID, []store.Event{{
		ID:      fmt.Sprintf("evt-wc-%s-%d", workflowID, time.Now().UnixNano()),
		Type:    store.EventTaskCompleted,
		Payload: payload,
	}})
	if err != nil {
		return fmt.Errorf("complete workflow: append for %s: %w", workflowID, err)
	}

	slog.Info("workflow completed", "workflow", workflowID, "reason", d.Reason)
	return nil
}

// executeFailWorkflow marks a workflow as failed.
func (r *Reconciler) executeFailWorkflow(ctx context.Context, workflowID string, d domain.Decision) error {
	// Check if already failed (idempotent).
	current, _ := r.stateStore.GetWorkflow(ctx, workflowID)
	if current != nil && current.Status == domain.WorkflowStatusFailed {
		slog.Debug("fail workflow: already failed, skipping", "workflow", workflowID)
		return nil
	}

	payload, _ := json.Marshal(domain.TaskFailed{
		TaskID: workflowID,
		Reason: d.Reason,
	})

	_, err := r.eventStore.Append(ctx, "workflow-"+workflowID, []store.Event{{
		ID:      fmt.Sprintf("evt-wf-%s-%d", workflowID, time.Now().UnixNano()),
		Type:    store.EventTaskFailed,
		Payload: payload,
	}})
	if err != nil {
		return fmt.Errorf("fail workflow: append for %s: %w", workflowID, err)
	}

	slog.Info("workflow failed", "workflow", workflowID, "reason", d.Reason)
	return nil
}

// ── Internal: reconcile all ─────────────────────

func (r *Reconciler) reconcileAll(ctx context.Context) {
	workflows, err := r.stateStore.ListActiveWorkflows(ctx)
	if err != nil {
		slog.Error("reconcileAll: list workflows", "error", err)
		return
	}

	for _, wf := range workflows {
		if err := r.Reconcile(ctx, wf.ID); err != nil {
			slog.Error("reconcileAll: reconcile workflow",
				"workflow", wf.ID,
				"error", err,
			)
		}
	}

	if len(workflows) > 0 {
		slog.Debug("reconcileAll: tick complete", "workflows", len(workflows))
	}
}
