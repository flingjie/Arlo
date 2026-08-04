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
	"os"
	"strings"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/runtime"
	"github.com/lingjiefan/arlo/internal/skill"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"github.com/lingjiefan/arlo/internal/workflow"
	"github.com/lingjiefan/arlo/internal/workspace"
)

// Reconciler is the control loop that drives the system toward desired state.
type Reconciler struct {
	stateStore    state.StateStore
	eventStore    store.EventStore
	engine        *workflow.Engine
	runtimeMgr    *runtime.Manager
	workspaceMgr  *workspace.Manager
	skillRegistry *skill.Registry

	// graphRegistry maps workflowID → compiled graph.
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
	skillRegistry *skill.Registry,
) *Reconciler {
	return &Reconciler{
		stateStore:    stateStore,
		eventStore:    eventStore,
		engine:        engine,
		runtimeMgr:    runtimeMgr,
		workspaceMgr:  workspaceMgr,
		skillRegistry: skillRegistry,
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

	// First reconcile is triggered by CreateTask, not here — avoids a
	// race where both see PENDING and produce duplicate NODE_STARTED.

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

	// 2. REAP — detect exited runtimes and mark nodes as COMPLETED/FAILED.
	//    Must happen BEFORE Evaluate so newly-completed nodes unblock dependents
	//    within the same tick.
	r.reapRuntimes(ctx, workflowID, currentState)

	// Re-read state after reaping — reaped nodes may have changed status.
	currentState, err = r.stateStore.GetWorkflow(ctx, workflowID)
	if err != nil {
		return fmt.Errorf("reconcile: re-read workflow %s after reap: %w", workflowID, err)
	}

	// 3. COMPUTE desired actions.
	decisions, err := r.engine.Evaluate(ctx, graph, *currentState)
	if err != nil {
		return fmt.Errorf("reconcile: evaluate %s: %w", workflowID, err)
	}

	if len(decisions) == 0 {
		return nil
	}

	// 4. ACT — execute each decision.
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

// reapRuntimes checks RUNNING nodes for exited runtime instances and emits
// NODE_COMPLETED or NODE_FAILED events.
func (r *Reconciler) reapRuntimes(ctx context.Context, workflowID string, state *domain.WorkflowState) {
	if r.runtimeMgr == nil {
		return
	}

	graph := r.graphRegistry[workflowID]
	if graph == nil {
		return
	}
	// Build node lookup for retry policy.
	nodeMap := make(map[string]domain.ExecutableNode, len(graph.Nodes))
	for _, n := range graph.Nodes {
		nodeMap[n.ID] = n
	}

	for _, ns := range state.Nodes {
		if ns.Status != domain.NodeStatusRunning {
			continue
		}
		instanceID := mkInstanceID(ns.NodeID, ns.RetryCount+1)
		inst, err := r.runtimeMgr.GetInstance(ctx, instanceID)
		if err != nil {
			// Runtime instance not found — the process may have died without
			// notifying MarkExited (e.g., launch failure that didn't register).
			// Emit NODE_FAILED so the node doesn't stay RUNNING forever.
			maxRetries := 0
			if gn, ok := nodeMap[ns.NodeID]; ok {
				maxRetries = gn.Retry.MaxRetries
			}
			retryable := ns.RetryCount < maxRetries
			reason := fmt.Sprintf("runtime instance not found: %v", err)
			if !retryable {
				reason = fmt.Sprintf("runtime instance not found (retries exhausted): %v", err)
			}
			if emitErr := r.emitNodeEvent(ctx, workflowID, ns, store.EventNodeFailed,
				domain.NodeFailed{
					NodeID:     ns.NodeID,
					WorkflowID: workflowID,
					SessionID:  ns.SessionID,
					Reason:     reason,
					Retryable:  retryable,
				}); emitErr != nil {
				slog.Error("reap: emit NODE_FAILED for missing instance failed",
					"node", ns.NodeID, "error", emitErr)
			}
			continue
		}
		if inst.State != domain.RuntimeStateExited {
			continue
		}

		// Emit metrics snapshot.
		if err := r.emitNodeEvent(ctx, workflowID, ns, store.EventMetricsSnapshot,
			domain.MetricsSnapshot{
				NodeID:     ns.NodeID,
				WorkflowID: workflowID,
				SessionID:  ns.SessionID,
				TokensIn:   inst.Metrics.TokensIn,
				TokensOut:  inst.Metrics.TokensOut,
				ToolCalls:  inst.Metrics.ToolCalls,
				DurationMs: inst.Metrics.DurationMs,
			}); err != nil {
				slog.Warn("reap: emit metrics snapshot failed", "node", ns.NodeID, "error", err)
			}

		if inst.ExitCode == 0 {
			slog.Info("runtime exited",
				"workflow", workflowID,
				"node", ns.NodeID,
				"exit_code", inst.ExitCode,
				"tokens_in", inst.Metrics.TokensIn,
				"tokens_out", inst.Metrics.TokensOut,
				"tool_calls", inst.Metrics.ToolCalls,
				"duration_ms", inst.Metrics.DurationMs,
			)
			if err := r.emitNodeEvent(ctx, workflowID, ns, store.EventNodeCompleted,
				domain.NodeCompleted{
					NodeID:     ns.NodeID,
					WorkflowID: workflowID,
					SessionID:  ns.SessionID,
				}); err != nil {
				slog.Error("reap: emit NODE_COMPLETED failed", "node", ns.NodeID, "error", err)
			}
			continue
		}

		// Use the node's max_retries from the graph, not a hardcoded value.
		maxRetries := 0
		if gn, ok := nodeMap[ns.NodeID]; ok {
			maxRetries = gn.Retry.MaxRetries
		}
		if ns.RetryCount < maxRetries {
			if err := r.emitNodeEvent(ctx, workflowID, ns, store.EventNodeFailed,
				domain.NodeFailed{
					NodeID:     ns.NodeID,
					WorkflowID: workflowID,
					SessionID:  ns.SessionID,
					Reason:     fmt.Sprintf("exit code %d", inst.ExitCode),
					Retryable:  true,
				}); err != nil {
				slog.Error("reap: emit NODE_FAILED failed", "node", ns.NodeID, "error", err)
			}
		} else {
			if err := r.emitNodeEvent(ctx, workflowID, ns, store.EventNodeFailed,
				domain.NodeFailed{
					NodeID:     ns.NodeID,
					WorkflowID: workflowID,
					SessionID:  ns.SessionID,
					Reason:     fmt.Sprintf("exit code %d (retries exhausted)", inst.ExitCode),
					Retryable:  false,
				}); err != nil {
				slog.Error("reap: emit NODE_FAILED failed", "node", ns.NodeID, "error", err)
			}
		}
	}
}

// mkSessionID builds a session identifier for a node attempt.
func mkSessionID(nodeID string, attempt int) string {
	return fmt.Sprintf("sess-%s-%d", nodeID, attempt)
}

// mkInstanceID builds a runtime instance identifier for a node attempt.
func mkInstanceID(nodeID string, attempt int) string {
	return fmt.Sprintf("rt-%s-%d", nodeID, attempt)
}

// emitNodeEvent appends a node event and applies it to projections.
func (r *Reconciler) emitNodeEvent(ctx context.Context, workflowID string, ns domain.NodeState, eventType store.EventType, payload interface{}) error {
	data, _ := json.Marshal(payload)

	event := store.Event{
		ID:       fmt.Sprintf("evt-%s-%s-%d", strings.ToLower(string(eventType)), ns.NodeID, time.Now().UnixNano()),
		Type:     eventType,
		StreamID: "node-" + ns.NodeID,
		Payload:  data,
	}
	positions, err := r.eventStore.Append(ctx, "node-"+ns.NodeID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("emit event: append %s for %s: %w", eventType, ns.NodeID, err)
	}
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("emit event: apply %s for %s: %w", eventType, ns.NodeID, err)
	}
	return nil
}

// launchRuntime resolves the skill prompt and starts the runtime process for a node.
func (r *Reconciler) launchRuntime(ctx context.Context, workflowID, nodeID string, retryCount int) {
	if r.runtimeMgr == nil {
		return
	}
	graph := r.graphRegistry[workflowID]
	if graph == nil {
		return
	}
	for _, n := range graph.Nodes {
		if n.ID != nodeID {
			continue
		}

		prompt := "Run skill: " + n.SkillRef.Name
		if r.skillRegistry != nil {
			if sk, err := r.skillRegistry.Resolve(n.SkillRef); err == nil && sk.Prompt != "" {
				prompt = sk.Prompt
			}
		}

		sessionID := mkSessionID(nodeID, retryCount+1)
		_, err := r.runtimeMgr.StartInstance(ctx, runtime.RuntimeSpec{
			InstanceID: mkInstanceID(nodeID, retryCount+1),
			Type:       n.Runtime.Provider,
			Config: domain.RuntimeConfig{
				Model:          n.Runtime.Model,
				PermissionMode: "auto",
			},
			SessionID: sessionID,
			WorkDir:    workDir(),
			Prompt:     prompt,
		})
		if err != nil {
			// Fetch current node state to get the SessionID set by NODE_STARTED.
			ns, nsErr := r.stateStore.GetNodeState(ctx, nodeID)
			if nsErr != nil {
				slog.Warn("failed to launch runtime and failed to get node state",
					"node", nodeID, "error", err, "stateErr", nsErr)
				return
			}

			// Determine retryability based on the graph's max_retries.
			// retryCount is the number of previous failures (before this attempt).
			retryable := retryCount < n.Retry.MaxRetries
			reason := fmt.Sprintf("runtime launch failed: %v", err)
			if !retryable {
				reason = fmt.Sprintf("runtime launch failed (retries exhausted): %v", err)
			}

			if emitErr := r.emitNodeEvent(ctx, workflowID, *ns, store.EventNodeFailed,
				domain.NodeFailed{
					NodeID:     ns.NodeID,
					WorkflowID: workflowID,
					SessionID:  ns.SessionID,
					Reason:     reason,
					Retryable:  retryable,
				}); emitErr != nil {
				slog.Error("launchRuntime: emit NODE_FAILED failed", "node", nodeID, "error", emitErr)
			}
			slog.Warn("failed to launch runtime",
				"node", nodeID, "error", err, "retryable", retryable)
		}
		return
	}
}

// ── Decision Execution ───────────────────────────

func (r *Reconciler) executeDecision(ctx context.Context, workflowID string, d domain.Decision) error {
	switch d.Action {
	case domain.DecisionStartNode:
		return r.executeStartNode(ctx, workflowID, d)
	case domain.DecisionRetryNode:
		return r.executeRetryNode(ctx, workflowID, d)
	case domain.DecisionResumeNode:
		return r.executeResumeNode(ctx, workflowID, d)
	case domain.DecisionPauseNode:
		return r.executePauseNode(ctx, workflowID, d)
	case domain.DecisionCompleteWorkflow:
		return r.executeCompleteWorkflow(ctx, workflowID, d)
	case domain.DecisionFailWorkflow:
		return r.executeFailWorkflow(ctx, workflowID, d)
	default:
		return fmt.Errorf("unknown decision action: %s", d.Action)
	}
}

// executeRetryNode transitions a READY node (after retryable failure) back to RUNNING.
// It uses a new session to distinguish retry attempts.
func (r *Reconciler) executeRetryNode(ctx context.Context, workflowID string, d domain.Decision) error {
	ns, err := r.stateStore.GetNodeState(ctx, d.NodeID)
	if err != nil {
		return fmt.Errorf("retry node: get state for %s: %w", d.NodeID, err)
	}

	if ns.Status != domain.NodeStatusReady || ns.RetryCount == 0 {
		slog.Debug("retry node: node not in retryable state, skipping",
			"node", d.NodeID,
			"status", ns.Status,
			"retry_count", ns.RetryCount,
		)
		return nil
	}

	sessionID := mkSessionID(d.NodeID, ns.RetryCount+1)

	payload, _ := json.Marshal(domain.NodeStarted{
		NodeID:     d.NodeID,
		WorkflowID: workflowID,
		SessionID:  sessionID,
	})

	event := store.Event{
		ID:       fmt.Sprintf("evt-ns-%s-%d", d.NodeID, time.Now().UnixNano()),
		Type:     store.EventNodeStarted,
		StreamID: "node-" + d.NodeID,
		Payload:  payload,
	}
	positions, err := r.eventStore.Append(ctx, "node-"+d.NodeID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("retry node: append NODE_STARTED for %s: %w", d.NodeID, err)
	}
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("retry node: apply NODE_STARTED for %s: %w", d.NodeID, err)
	}

	slog.Info("node retried",
		"workflow", workflowID,
		"node", d.NodeID,
		"session", sessionID,
		"attempt", ns.RetryCount+1,
		"reason", d.Reason,
	)

	r.launchRuntime(ctx, workflowID, d.NodeID, ns.RetryCount)
	return nil
}

// executeResumeNode transitions a READY node (after human approval) back to RUNNING.
func (r *Reconciler) executeResumeNode(ctx context.Context, workflowID string, d domain.Decision) error {
	ns, err := r.stateStore.GetNodeState(ctx, d.NodeID)
	if err != nil {
		return fmt.Errorf("resume node: get state for %s: %w", d.NodeID, err)
	}

	if ns.Status != domain.NodeStatusReady {
		slog.Debug("resume node: node not ready, skipping",
			"node", d.NodeID,
			"status", ns.Status,
		)
		return nil
	}

	// Reuse the existing session ID if available, otherwise create new.
	sessionID := ns.SessionID
	if sessionID == "" {
		sessionID = mkSessionID(d.NodeID, ns.RetryCount+1)
	}

	payload, _ := json.Marshal(domain.NodeStarted{
		NodeID:     d.NodeID,
		WorkflowID: workflowID,
		SessionID:  sessionID,
	})

	event := store.Event{
		ID:       fmt.Sprintf("evt-ns-%s-%d", d.NodeID, time.Now().UnixNano()),
		Type:     store.EventNodeStarted,
		StreamID: "node-" + d.NodeID,
		Payload:  payload,
	}
	positions, err := r.eventStore.Append(ctx, "node-"+d.NodeID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("resume node: append NODE_STARTED for %s: %w", d.NodeID, err)
	}
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("resume node: apply NODE_STARTED for %s: %w", d.NodeID, err)
	}

	slog.Info("node resumed",
		"workflow", workflowID,
		"node", d.NodeID,
		"session", sessionID,
		"reason", d.Reason,
	)

	// Launch the runtime after resume (e.g., after human approval).
	r.launchRuntime(ctx, workflowID, d.NodeID, ns.RetryCount)
	return nil
}

// executePauseNode transitions a RUNNING node to WAITING for human input.
func (r *Reconciler) executePauseNode(ctx context.Context, workflowID string, d domain.Decision) error {
	ns, err := r.stateStore.GetNodeState(ctx, d.NodeID)
	if err != nil {
		return fmt.Errorf("pause node: get state for %s: %w", d.NodeID, err)
	}

	if ns.Status != domain.NodeStatusRunning {
		slog.Debug("pause node: node not running, skipping",
			"node", d.NodeID,
			"status", ns.Status,
		)
		return nil
	}

	payload, _ := json.Marshal(domain.NodeWaiting{
		NodeID:     d.NodeID,
		WorkflowID: workflowID,
		SessionID:  ns.SessionID,
		Reason:     d.Reason,
		Prompt:     "Human approval required for node " + d.NodeID,
	})

	event := store.Event{
		ID:       fmt.Sprintf("evt-nw-%s-%d", d.NodeID, time.Now().UnixNano()),
		Type:     store.EventNodeWaiting,
		StreamID: "node-" + d.NodeID,
		Payload:  payload,
	}
	positions, err := r.eventStore.Append(ctx, "node-"+d.NodeID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("pause node: append NODE_WAITING for %s: %w", d.NodeID, err)
	}
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("pause node: apply NODE_WAITING for %s: %w", d.NodeID, err)
	}

	slog.Info("node paused for human approval",
		"workflow", workflowID,
		"node", d.NodeID,
		"gate", ns.Gate,
		"reason", d.Reason,
	)
	return nil
}

// executeStartNode transitions a node from PENDING/READY to RUNNING,
// handles human_approval gate pausing, and launches the agent runtime.
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

	sessionID := mkSessionID(d.NodeID, ns.RetryCount+1)

	// If the node has a human_approval gate, go straight to WAITING —
	// no NODE_STARTED, no runtime. On resume, NODE_STARTED fires once.
	if ns.Gate == string(domain.GateHumanApproval) {
		slog.Info("node waiting for human approval",
			"workflow", workflowID,
			"node", d.NodeID,
			"gate", ns.Gate,
		)

		waitPayload, _ := json.Marshal(domain.NodeWaiting{
			NodeID:     d.NodeID,
			WorkflowID: workflowID,
			SessionID:  sessionID,
			Reason:     "human_approval gate",
			Prompt:     "Approve or reject this node",
		})
		waitEvent := store.Event{
			ID:       fmt.Sprintf("evt-nw-%s-%d", d.NodeID, time.Now().UnixNano()),
			Type:     store.EventNodeWaiting,
			StreamID: "node-" + d.NodeID,
			Payload:  waitPayload,
		}
		waitPositions, waitErr := r.eventStore.Append(ctx, "node-"+d.NodeID, []store.Event{waitEvent})
		if waitErr != nil {
			return fmt.Errorf("start node: append NODE_WAITING for %s: %w", d.NodeID, waitErr)
		}
		waitEvent.Position = waitPositions[0]
		if err := r.stateStore.Apply(waitEvent); err != nil {
			return fmt.Errorf("start node: apply NODE_WAITING for %s: %w", d.NodeID, err)
		}
		return nil
	}

	// Append NODE_STARTED event.
	payload, _ := json.Marshal(domain.NodeStarted{
		NodeID:     d.NodeID,
		WorkflowID: workflowID,
		SessionID:  sessionID,
	})

	event := store.Event{
		ID:       fmt.Sprintf("evt-ns-%s-%d", d.NodeID, time.Now().UnixNano()),
		Type:     store.EventNodeStarted,
		StreamID: "node-" + d.NodeID,
		Payload:  payload,
	}
	positions, err := r.eventStore.Append(ctx, "node-"+d.NodeID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("start node: append NODE_STARTED for %s: %w", d.NodeID, err)
	}
	// Keep projections in sync incrementally — the projection needs
	// the event's Type and Payload. Position is set from Append's return.
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("start node: apply NODE_STARTED for %s: %w", d.NodeID, err)
	}

	slog.Info("node started",
		"workflow", workflowID,
		"node", d.NodeID,
		"session", sessionID,
		"reason", d.Reason,
	)

	// Actually launch the agent runtime.
	r.launchRuntime(ctx, workflowID, d.NodeID, ns.RetryCount)
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

	event := store.Event{
		ID:       fmt.Sprintf("evt-wc-%s-%d", workflowID, time.Now().UnixNano()),
		Type:     store.EventTaskCompleted,
		StreamID: "workflow-" + workflowID,
		Payload:  payload,
	}
	positions, err := r.eventStore.Append(ctx, "workflow-"+workflowID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("complete workflow: append for %s: %w", workflowID, err)
	}
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("complete workflow: apply for %s: %w", workflowID, err)
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

	event := store.Event{
		ID:       fmt.Sprintf("evt-wf-%s-%d", workflowID, time.Now().UnixNano()),
		Type:     store.EventTaskFailed,
		StreamID: "workflow-" + workflowID,
		Payload:  payload,
	}
	positions, err := r.eventStore.Append(ctx, "workflow-"+workflowID, []store.Event{event})
	if err != nil {
		return fmt.Errorf("fail workflow: append for %s: %w", workflowID, err)
	}
	event.Position = positions[0]
	if err := r.stateStore.Apply(event); err != nil {
		return fmt.Errorf("fail workflow: apply for %s: %w", workflowID, err)
	}

	slog.Info("workflow failed", "workflow", workflowID, "reason", d.Reason)
	return nil
}

// workDir returns the current working directory, or /tmp as fallback.
func workDir() string {
	if d, err := os.Getwd(); err == nil {
		return d
	}
	return "/tmp"
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
