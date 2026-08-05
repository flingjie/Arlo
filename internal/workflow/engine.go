// Package workflow compiles, validates, and evaluates workflow DAGs.
//
// The WorkflowEngine is the "brain" of the Reconciler — it answers
// "what should happen next?" given the current workflow state.
//
// Flow: YAML → Compile → ExecutableGraph → Instantiate → WorkflowInstance
//                                               Evaluate(state) → Decisions
package workflow

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
	"gopkg.in/yaml.v3"
)

// Engine compiles and evaluates workflow DAGs.
type Engine struct{}

// NewEngine creates a new workflow engine.
func NewEngine() *Engine {
	return &Engine{}
}

// Compile parses YAML source into an ExecutableGraph.
func (e *Engine) Compile(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
	var def workflowDef
	if err := yaml.Unmarshal(source, &def); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}

	if def.Name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}

	nodes := make([]domain.ExecutableNode, 0, len(def.Nodes))
	for i, nd := range def.Nodes {
		if nd.ID == "" {
			return nil, fmt.Errorf("node %d: id is required", i)
		}
		if nd.Skill == "" {
			return nil, fmt.Errorf("node %s: skill is required", nd.ID)
		}

		dependsOn := nd.DependsOn
		if dependsOn == nil {
			dependsOn = []string{}
		}

		retry := domain.RetryPolicy{
			MaxRetries:    nd.Retry.MaxRetries,
			BackoffFactor: 2.0,
		}
		if retry.MaxRetries == 0 {
			retry.MaxRetries = 1 // default: one retry
		}
		if nd.Retry.Backoff != "" {
			d, err := time.ParseDuration(nd.Retry.Backoff)
			if err != nil {
				return nil, fmt.Errorf("node %s: invalid backoff %q: %w", nd.ID, nd.Retry.Backoff, err)
			}
			retry.Backoff = d
		} else {
			retry.Backoff = 10 * time.Second
		}
		if nd.Retry.MaxBackoff != "" {
			d, err := time.ParseDuration(nd.Retry.MaxBackoff)
			if err != nil {
				return nil, fmt.Errorf("node %s: invalid max_backoff %q: %w", nd.ID, nd.Retry.MaxBackoff, err)
			}
			retry.MaxBackoff = d
		}

		gate := domain.GateNone
		switch nd.Gate {
		case "":
			// no gate — default GateNone
		case "human_approval":
			gate = domain.GateHumanApproval
		default:
			return nil, fmt.Errorf("node %s: unknown gate %q", nd.ID, nd.Gate)
		}

		runtime := domain.RuntimeRef{
			Provider: domain.RuntimeProvider(nd.Runtime.Provider),
			Model:    nd.Runtime.Model,
		}
		if runtime.Provider == "" {
			runtime.Provider = domain.RuntimeProviderClaudeCode
		}

		nodes = append(nodes, domain.ExecutableNode{
			ID:        nd.ID,
			SkillRef:  domain.SkillRef{Name: nd.Skill},
			Runtime:   runtime,
			DependsOn: dependsOn,
			Gate:      gate,
			Retry:       retry,
			Transitions: mapTransitions(nd.ID, nd.Transitions),
		})
	}

	results := make([]domain.WorkflowResult, 0, len(def.Results))
	for _, rd := range def.Results {
		results = append(results, domain.WorkflowResult{
			NodeID:   rd.Node,
			Artifact: rd.Artifact,
		})
	}

	graph := &domain.ExecutableGraph{
		Name:    def.Name,
		Version: def.Version,
		Nodes:   nodes,
		Edges:   buildEdges(nodes),
		Results: results,
		Policy: domain.SchedulingPolicy{
			MaxConcurrentNodes: def.Policy.MaxConcurrentNodes,
		},
	}
	if graph.Policy.MaxConcurrentNodes == 0 {
		graph.Policy.MaxConcurrentNodes = 1
	}
	if graph.Version == 0 {
		graph.Version = 1
	}

	// Validate non-negative values.
	for _, n := range graph.Nodes {
		if n.Retry.MaxRetries < 0 {
			return nil, fmt.Errorf("node %s: max_retries must not be negative, got %d", n.ID, n.Retry.MaxRetries)
		}
	}
	if graph.Policy.MaxConcurrentNodes < 0 {
		return nil, fmt.Errorf("max_concurrent_nodes must not be negative, got %d", graph.Policy.MaxConcurrentNodes)
	}

	return graph, nil
}

// Validate checks the graph for structural correctness.
func (e *Engine) Validate(ctx context.Context, graph *domain.ExecutableGraph) error {
	if graph.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if len(graph.Nodes) == 0 {
		return fmt.Errorf("workflow must have at least one node")
	}

	ids := make(map[string]bool)
	for _, n := range graph.Nodes {
		if ids[n.ID] {
			return fmt.Errorf("duplicate node id: %s", n.ID)
		}
		ids[n.ID] = true
	}

	// Validate dependencies reference existing nodes.
	for _, n := range graph.Nodes {
		for _, dep := range n.DependsOn {
			if dep == n.ID {
				return fmt.Errorf("node %s: cannot depend on itself", n.ID)
			}
			if !ids[dep] {
				return fmt.Errorf("node %s: depends on unknown node %s", n.ID, dep)
			}
		}
	}

	for i, r := range graph.Results {
		if r.NodeID == "" {
			return fmt.Errorf("results[%d]: node is required", i)
		}
		if !ids[r.NodeID] {
			return fmt.Errorf("results[%d]: unknown node %s", i, r.NodeID)
		}
		if r.Artifact == "" {
			return fmt.Errorf("results[%d]: artifact is required", i)
		}
	}

	// Check for cycles using topological sort.
	if _, err := topologicalSort(graph.Nodes); err != nil {
		return fmt.Errorf("cycle detected: %w", err)
	}

	return nil
}

// Instantiate creates a WorkflowInstance from a compiled graph.
func (e *Engine) Instantiate(ctx context.Context, taskID string, graph *domain.ExecutableGraph) (*domain.WorkflowInstance, error) {
	// Copy the graph to avoid mutating the caller's reference.
	g := *graph
	g.ID = taskID + "-" + g.Name

	return &domain.WorkflowInstance{
		ID:        g.ID,
		TaskID:    taskID,
		Graph:     &g,
		Status:    domain.WorkflowStatusActive,
		Version:   g.Version,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

// Evaluate produces decisions based on the current workflow state and graph.
// It answers: "given where we are, what should the Reconciler do?"
func (e *Engine) Evaluate(ctx context.Context, graph *domain.ExecutableGraph, state domain.WorkflowState) ([]domain.Decision, error) {
	var decisions []domain.Decision

	// Check if workflow has reached a terminal state.
	if state.Status == domain.WorkflowStatusCompleted ||
		state.Status == domain.WorkflowStatusFailed ||
		state.Status == domain.WorkflowStatusCancelled ||
		state.Status == domain.WorkflowStatusPaused {
		return nil, nil
	}

	allDone := true
	anyFailed := false

	for _, ns := range state.Nodes {
		switch ns.Status {
		case domain.NodeStatusFailed:
			anyFailed = true
			allDone = false // a failed node means the workflow is not successfully done
		case domain.NodeStatusCompleted:
			// done
		default:
			allDone = false
		}
	}

	// If all nodes completed, the workflow is done.
	if allDone && len(state.Nodes) > 0 {
		decisions = append(decisions, domain.Decision{
			Action: domain.DecisionCompleteWorkflow,
			Reason: "all nodes completed",
		})
		return decisions, nil
	}

	// If any node permanently failed, fail the workflow.
	if anyFailed {
		decisions = append(decisions, domain.Decision{
			Action: domain.DecisionFailWorkflow,
			Reason: "a node permanently failed",
		})
		return decisions, nil
	}

	// Find nodes ready to start: PENDING or READY status with all deps satisfied.
	// The distinction between PENDING and READY determines the decision action:
	//   PENDING → START_NODE  (cold start)
	//   READY, retry_count > 0 → RETRY_NODE  (retry after retryable failure)
	//   READY, retry_count == 0 → RESUME_NODE (resume after human approval)
	startedNodes := make(map[string]bool)
	for _, ns := range state.Nodes {
		if ns.Status != domain.NodeStatusPending && ns.Status != domain.NodeStatusReady {
			continue
		}
		if !depsSatisfied(ns.NodeID, graph, state) {
			continue
		}

		var action, reason string
		switch {
		case ns.Status == domain.NodeStatusPending:
			action = domain.DecisionStartNode
			reason = "dependencies satisfied"
		case ns.RetryCount > 0:
			action = domain.DecisionRetryNode
			reason = fmt.Sprintf("retry %d after retryable failure", ns.RetryCount)
		default:
			action = domain.DecisionResumeNode
			reason = "resume after approval or wait"
		}

		decisions = append(decisions, domain.Decision{
			Action: action,
			NodeID: ns.NodeID,
			Reason: reason,
		})
		startedNodes[ns.NodeID] = true
	}

	// Safety net: pause any RUNNING nodes that have a gate.
	// Normally executeStartNode pauses gated nodes immediately, but this
	// catches nodes that reach RUNNING via event replay or manual injection.
	for _, ns := range state.Nodes {
		if ns.Status != domain.NodeStatusRunning {
			continue
		}
		if ns.Gate == "" || ns.Gate == string(domain.GateNone) {
			continue
		}
		if startedNodes[ns.NodeID] {
			continue // already getting START_NODE — don't conflict
		}
		decisions = append(decisions, domain.Decision{
			Action: domain.DecisionPauseNode,
			NodeID: ns.NodeID,
			Reason: fmt.Sprintf("gate %s requires human approval", ns.Gate),
		})
	}

	// Check for conditional transitions (v0.2).
	// Build node map once outside the loop (O(M) not O(N*M)).
	nodeMap := buildNodeMap(graph)
	for _, ns := range state.Nodes {
		if ns.Status != domain.NodeStatusCompleted {
			continue
		}
		node, ok := nodeMap[ns.NodeID]
		if !ok {
			continue
		}
		for _, tr := range node.Transitions {
			if tr.When != "" && !evaluateCondition(tr.When, ns) {
				continue
			}
			if startedNodes[tr.To] {
				continue // prevent duplicate with dep-satisfied start
			}
			targetState, exists := state.Nodes[tr.To]
			if !exists {
				continue
			}
			if targetState.Status != domain.NodeStatusPending && targetState.Status != domain.NodeStatusReady {
				continue
			}
			decisions = append(decisions, domain.Decision{
				Action: domain.DecisionStartNode,
				NodeID: tr.To,
				Reason: "conditional transition from " + ns.NodeID,
			})
			startedNodes[tr.To] = true
		}
	}

	return decisions, nil
}


// buildNodeMap creates an ID→Node lookup from the graph.
func buildNodeMap(graph *domain.ExecutableGraph) map[string]domain.ExecutableNode {
	m := make(map[string]domain.ExecutableNode, len(graph.Nodes))
	for _, n := range graph.Nodes {
		m[n.ID] = n
	}
	return m
}


// evaluateCondition evaluates a simple expression against a node's output.
// In v0.2, supports: "verdict == APPROVED", "verdict != APPROVED", and truthy checks.
// '==' is checked before '!=' to prevent the '!=' substring from matching '==' expressions.
func evaluateCondition(expr string, ns domain.NodeState) bool {
	if strings.Contains(expr, "==") {
		parts := strings.SplitN(expr, "==", 2)
		key := strings.TrimSpace(parts[0])
		want := strings.TrimSpace(parts[1])
		val, ok := ns.Output[key]
		return ok && val == want
	}
	if strings.Contains(expr, "!=") {
		parts := strings.SplitN(expr, "!=", 2)
		key := strings.TrimSpace(parts[0])
		want := strings.TrimSpace(parts[1])
		val, ok := ns.Output[key]
		return !ok || val != want
	}
	_, ok := ns.Output[strings.TrimSpace(expr)]
	return ok
}


// depsSatisfied checks whether all dependencies of a node are completed.
// depsSatisfied checks whether all dependencies of a node are completed.
func depsSatisfied(nodeID string, graph *domain.ExecutableGraph, state domain.WorkflowState) bool {
	nodeMap := buildNodeMap(graph)
	node, ok := nodeMap[nodeID]
	if !ok {
		return false
	}
	for _, dep := range node.DependsOn {
		depState, ok := state.Nodes[dep]
		if !ok {
			return false
		}
		if depState.Status != domain.NodeStatusCompleted {
			return false
		}
	}
	return true
}

// ── YAML Schema ──────────────────────────────────

type workflowDef struct {
	Name    string      `yaml:"name"`
	Version int         `yaml:"version"`
	Nodes   []nodeDef   `yaml:"nodes"`
	Results []resultDef `yaml:"results"`
	Policy  policyDef   `yaml:"policy"`
}

type resultDef struct {
	Node     string `yaml:"node"`
	Artifact string `yaml:"artifact"`
}

type nodeDef struct {
	ID          string          `yaml:"id"`
	Skill       string          `yaml:"skill"`
	DependsOn   []string        `yaml:"depends_on"`
	Gate        string          `yaml:"gate"`
	Retry       retryDef        `yaml:"retry"`
	Runtime     runtimeDef      `yaml:"runtime"`
	Transitions []transitionDef `yaml:"transitions"`
}

type runtimeDef struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
}

type transitionDef struct {
	To   string `yaml:"to"`
	When string `yaml:"when"`
}

type retryDef struct {
	MaxRetries int    `yaml:"max_retries"`
	Backoff    string `yaml:"backoff"`
	MaxBackoff string `yaml:"max_backoff"`
}

type policyDef struct {
	MaxConcurrentNodes int `yaml:"max_concurrent_nodes"`
}

// ── Graph Utilities ──────────────────────────────

// mapTransitions converts YAML transition defs to domain Transitions,
// setting From to the parent node's ID.
func mapTransitions(nodeID string, defs []transitionDef) []domain.Transition {
	if len(defs) == 0 {
		return nil
	}
	transitions := make([]domain.Transition, 0, len(defs))
	for _, td := range defs {
		transitions = append(transitions, domain.Transition{
			From: nodeID,
			To:   td.To,
			When: td.When,
		})
	}
	return transitions
}

// buildEdges creates explicit Edge structs from node DependsOn declarations.
func buildEdges(nodes []domain.ExecutableNode) []domain.Edge {
	var edges []domain.Edge
	for _, n := range nodes {
		for _, dep := range n.DependsOn {
			edges = append(edges, domain.Edge{From: dep, To: n.ID})
		}
	}
	return edges
}

// topologicalSort returns nodes in dependency order, or an error if a cycle exists.
func topologicalSort(nodes []domain.ExecutableNode) ([]domain.ExecutableNode, error) {
	inDegree := make(map[string]int)
	adj := make(map[string][]string)

	for _, n := range nodes {
		inDegree[n.ID] = 0
	}
	for _, n := range nodes {
		for _, dep := range n.DependsOn {
			adj[dep] = append(adj[dep], n.ID)
			inDegree[n.ID]++
		}
	}

	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	var sorted []domain.ExecutableNode
	nodeMap := make(map[string]domain.ExecutableNode)
	for _, n := range nodes {
		nodeMap[n.ID] = n
	}

	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		sorted = append(sorted, nodeMap[id])

		for _, neighbor := range adj[id] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	if len(sorted) != len(nodes) {
		return nil, fmt.Errorf("cycle detected: %d nodes sorted out of %d", len(sorted), len(nodes))
	}

	return sorted, nil
}
