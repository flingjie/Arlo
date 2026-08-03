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
		if nd.Gate == "human_approval" {
			gate = domain.GateHumanApproval
		}

		runtime := domain.RuntimeRef{
			Provider: nd.Runtime.Provider,
			Model:    nd.Runtime.Model,
		}
		if runtime.Provider == "" {
			runtime.Provider = "claude-code"
		}

		nodes = append(nodes, domain.ExecutableNode{
			ID:        nd.ID,
			SkillRef:  domain.SkillRef{Name: nd.Skill},
			Runtime:   runtime,
			DependsOn: dependsOn,
			Gate:      gate,
			Retry:     retry,
		})
	}

	graph := &domain.ExecutableGraph{
		Name:    def.Name,
		Version: def.Version,
		Nodes:   nodes,
		Edges:   buildEdges(nodes),
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
			if !ids[dep] {
				return fmt.Errorf("node %s: depends on unknown node %s", n.ID, dep)
			}
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
	graph.ID = taskID + "-" + graph.Name

	return &domain.WorkflowInstance{
		ID:        graph.ID,
		TaskID:    taskID,
		Graph:     graph,
		Status:    domain.WorkflowStatusActive,
		Version:   graph.Version,
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
		state.Status == domain.WorkflowStatusCancelled {
		return nil, nil
	}

	allDone := true
	anyFailed := false

	for _, ns := range state.Nodes {
		switch ns.Status {
		case domain.NodeStatusFailed:
			anyFailed = true
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
	for _, ns := range state.Nodes {
		if ns.Status != domain.NodeStatusPending && ns.Status != domain.NodeStatusReady {
			continue
		}

		// Check dependencies using the original graph.
		if !depsSatisfied(ns.NodeID, graph, state) {
			continue
		}

		decisions = append(decisions, domain.Decision{
			Action: domain.DecisionStartNode,
			NodeID: ns.NodeID,
			Reason: "dependencies satisfied",
		})
	}

	return decisions, nil
}

// depsSatisfied checks whether all dependencies of a node are completed.
func depsSatisfied(nodeID string, graph *domain.ExecutableGraph, state domain.WorkflowState) bool {
	// Find the node in the graph to read its DependsOn.
	nodeMap := make(map[string]domain.ExecutableNode)
	for _, n := range graph.Nodes {
		nodeMap[n.ID] = n
	}

	node, ok := nodeMap[nodeID]
	if !ok {
		return false
	}

	for _, dep := range node.DependsOn {
		depState, ok := state.Nodes[dep]
		if !ok {
			// Dependency doesn't exist in state — not satisfied.
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
	Name    string     `yaml:"name"`
	Version int        `yaml:"version"`
	Nodes   []nodeDef  `yaml:"nodes"`
	Policy  policyDef  `yaml:"policy"`
}

type nodeDef struct {
	ID        string      `yaml:"id"`
	Skill     string      `yaml:"skill"`
	DependsOn []string    `yaml:"depends_on"`
	Gate      string      `yaml:"gate"`
	Retry     retryDef    `yaml:"retry"`
	Runtime   runtimeDef  `yaml:"runtime"`
}

type runtimeDef struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
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
