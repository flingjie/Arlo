package workflow

import (
	"context"
	"strings"
	"testing"

	"github.com/lingjiefan/arlo/internal/domain"
)

const bugfixYAML = `
name: bugfix
version: 1

nodes:
  - id: analyze
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    retry:
      max_retries: 1
      backoff: 10s

  - id: implement
    skill: implement-fix
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    depends_on:
      - analyze
    retry:
      max_retries: 2
      backoff: 30s

  - id: review
    skill: code-review
    runtime:
      provider: claude-code
      model: claude-haiku-3.5
    depends_on:
      - implement
    gate: human_approval
    retry:
      max_retries: 1

policy:
  max_concurrent_nodes: 1
`

// TestCompileValid verifies a complete YAML workflow compiles correctly.
func TestCompileValid(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, err := eng.Compile(ctx, []byte(bugfixYAML))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	if graph.Name != "bugfix" {
		t.Errorf("name = %s, want bugfix", graph.Name)
	}
	if graph.Version != 1 {
		t.Errorf("version = %d, want 1", graph.Version)
	}
	if len(graph.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(graph.Nodes))
	}

	// Check first node.
	analyze := graph.Nodes[0]
	if analyze.ID != "analyze" {
		t.Errorf("node[0].id = %s, want analyze", analyze.ID)
	}
	if analyze.SkillRef.Name != "root-cause" {
		t.Errorf("node[0].skill = %s, want root-cause", analyze.SkillRef.Name)
	}
	if analyze.Runtime.Provider != domain.RuntimeProviderClaudeCode {
		t.Errorf("node[0].runtime.provider = %s, want claude-code", analyze.Runtime.Provider)
	}
	if len(analyze.DependsOn) != 0 {
		t.Errorf("analyze should have no deps, got %v", analyze.DependsOn)
	}
	if analyze.Retry.MaxRetries != 1 {
		t.Errorf("analyze retry max = %d, want 1", analyze.Retry.MaxRetries)
	}

	// Check second node.
	implement := graph.Nodes[1]
	if implement.ID != "implement" {
		t.Errorf("node[1].id = %s, want implement", implement.ID)
	}
	if len(implement.DependsOn) != 1 || implement.DependsOn[0] != "analyze" {
		t.Errorf("implement deps = %v, want [analyze]", implement.DependsOn)
	}

	// Check third node.
	review := graph.Nodes[2]
	if review.ID != "review" {
		t.Errorf("node[2].id = %s, want review", review.ID)
	}
	if review.Gate != domain.GateHumanApproval {
		t.Errorf("review gate = %s, want human_approval", review.Gate)
	}
	if review.Runtime.Model != "claude-haiku-3.5" {
		t.Errorf("review model = %s, want claude-haiku-3.5", review.Runtime.Model)
	}

	// Check edges.
	if len(graph.Edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(graph.Edges))
	}

	// Check policy defaults.
	if graph.Policy.MaxConcurrentNodes != 1 {
		t.Errorf("max_concurrent = %d, want 1", graph.Policy.MaxConcurrentNodes)
	}
}

// TestCompileMinimal verifies a minimal YAML with defaults compiles.
func TestCompileMinimal(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	minimal := `
name: minimal
nodes:
  - id: step1
    skill: do-something
`
	graph, err := eng.Compile(ctx, []byte(minimal))
	if err != nil {
		t.Fatalf("Compile minimal: %v", err)
	}
	if len(graph.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(graph.Nodes))
	}
	// Defaults should be applied.
	if graph.Version != 1 {
		t.Errorf("default version = %d, want 1", graph.Version)
	}
	if graph.Policy.MaxConcurrentNodes != 1 {
		t.Errorf("default max_concurrent = %d, want 1", graph.Policy.MaxConcurrentNodes)
	}
	if graph.Nodes[0].Runtime.Provider != domain.RuntimeProviderClaudeCode {
		t.Errorf("default runtime provider = %s, want claude-code", graph.Nodes[0].Runtime.Provider)
	}
	if graph.Nodes[0].Retry.Backoff != 10*1e9 { // 10s in nanoseconds
		t.Errorf("default backoff = %v, want 10s", graph.Nodes[0].Retry.Backoff)
	}
}

func TestCompileResults(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()
	src := `
name: with-results
nodes:
  - id: review
    skill: architecture-review
results:
  - node: review
    artifact: architecture-review.md
`
	graph, err := eng.Compile(ctx, []byte(src))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if len(graph.Results) != 1 {
		t.Fatalf("Results len = %d, want 1", len(graph.Results))
	}
	if graph.Results[0].NodeID != "review" {
		t.Errorf("NodeID = %q, want review", graph.Results[0].NodeID)
	}
	if graph.Results[0].Artifact != "architecture-review.md" {
		t.Errorf("Artifact = %q, want architecture-review.md", graph.Results[0].Artifact)
	}
	if err := eng.Validate(ctx, graph); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateResultsUnknownNode(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()
	graph := &domain.ExecutableGraph{
		Name: "bad",
		Nodes: []domain.ExecutableNode{
			{ID: "review", SkillRef: domain.SkillRef{Name: "architecture-review"}},
		},
		Results: []domain.WorkflowResult{
			{NodeID: "missing", Artifact: "out.md"},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil || !strings.Contains(err.Error(), "unknown node") {
		t.Fatalf("Validate error = %v, want unknown node", err)
	}
}

func TestValidateResultsEmptyArtifact(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()
	graph := &domain.ExecutableGraph{
		Name: "bad",
		Nodes: []domain.ExecutableNode{
			{ID: "review", SkillRef: domain.SkillRef{Name: "architecture-review"}},
		},
		Results: []domain.WorkflowResult{
			{NodeID: "review", Artifact: ""},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil || !strings.Contains(err.Error(), "artifact") {
		t.Fatalf("Validate error = %v, want artifact", err)
	}
}

// TestCompileErrors verifies various parse errors.
func TestCompileErrors(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	tests := []struct {
		name     string
		yaml     string
		wantErr  string
	}{
		{
			name:    "missing name",
			yaml:    `nodes: [{id: n1, skill: s1}]`,
			wantErr: "workflow name is required",
		},
		{
			name: "missing node id",
			yaml: `
name: test
nodes:
  - skill: something
`,
			wantErr: "id is required",
		},
		{
			name: "missing skill",
			yaml: `
name: test
nodes:
  - id: n1
`,
			wantErr: "skill is required",
		},
		{
			name: "invalid yaml",
			yaml: `{{{`,
			wantErr: "parse yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := eng.Compile(ctx, []byte(tt.yaml))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// TestValidateValid verifies a valid graph passes validation.
func TestValidateValid(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))
	if err := eng.Validate(ctx, graph); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

// TestValidateNoNodes verifies empty graph is rejected.
func TestValidateNoNodes(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph := &domain.ExecutableGraph{Name: "empty"}
	err := eng.Validate(ctx, graph)
	if err == nil {
		t.Fatal("expected error for empty graph")
	}
}

// TestValidateDuplicateIDs verifies duplicate node IDs are rejected.
func TestValidateDuplicateIDs(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph := &domain.ExecutableGraph{
		Name: "dupes",
		Nodes: []domain.ExecutableNode{
			{ID: "n1", SkillRef: domain.SkillRef{Name: "s1"}},
			{ID: "n1", SkillRef: domain.SkillRef{Name: "s2"}},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil {
		t.Fatal("expected error for duplicate IDs")
	}
}

// TestValidateUnknownDep verifies unknown dependency is rejected.
func TestValidateUnknownDep(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph := &domain.ExecutableGraph{
		Name: "unknown-dep",
		Nodes: []domain.ExecutableNode{
			{ID: "n1", SkillRef: domain.SkillRef{Name: "s1"}, DependsOn: []string{"nonexistent"}},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil {
		t.Fatal("expected error for unknown dependency")
	}
}

// TestValidateCycle verifies cyclic dependencies are rejected.
func TestValidateCycle(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph := &domain.ExecutableGraph{
		Name: "cycle",
		Nodes: []domain.ExecutableNode{
			{ID: "A", SkillRef: domain.SkillRef{Name: "s1"}, DependsOn: []string{"B"}},
			{ID: "B", SkillRef: domain.SkillRef{Name: "s2"}, DependsOn: []string{"A"}},
		},
	}
	err := eng.Validate(ctx, graph)
	if err == nil {
		t.Fatal("expected error for cycle")
	}
	if !contains(err.Error(), "cycle") {
		t.Errorf("error should mention cycle: %v", err)
	}
}

// TestInstantiate verifies a graph can be instantiated into a WorkflowInstance.
func TestInstantiate(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))
	inst, err := eng.Instantiate(ctx, "task-123", graph)
	if err != nil {
		t.Fatalf("Instantiate: %v", err)
	}
	if inst.TaskID != "task-123" {
		t.Errorf("TaskID = %s, want task-123", inst.TaskID)
	}
	if inst.Status != domain.WorkflowStatusActive {
		t.Errorf("status = %s, want ACTIVE", inst.Status)
	}
	if inst.Graph == nil {
		t.Fatal("instance should carry the graph")
	}
}

// TestEvaluateAllDepsSatisfied verifies nodes with satisfied deps get START_NODE.
func TestEvaluateAllDepsSatisfied(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// State: analyze completed, implement pending.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusCompleted},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Only implement should be started (review still has implement as dep).
	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].Action != domain.DecisionStartNode {
		t.Errorf("action = %s, want START_NODE", decisions[0].Action)
	}
	if decisions[0].NodeID != "implement" {
		t.Errorf("node = %s, want implement", decisions[0].NodeID)
	}
}

// TestEvaluateNothingReady verifies no decisions when deps aren't satisfied.
func TestEvaluateNothingReady(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// State: nothing done yet, analyze is pending.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusPending},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// analyze has no deps → should be started.
	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision (analyze), got %d", len(decisions))
	}
	if decisions[0].NodeID != "analyze" {
		t.Errorf("node = %s, want analyze", decisions[0].NodeID)
	}
}

// TestEvaluateAllComplete verifies COMPLETE_WORKFLOW when all nodes are done.
func TestEvaluateAllComplete(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusCompleted},
			"implement": {NodeID: "implement", Status: domain.NodeStatusCompleted},
			"review":    {NodeID: "review", Status: domain.NodeStatusCompleted},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].Action != domain.DecisionCompleteWorkflow {
		t.Errorf("action = %s, want COMPLETE_WORKFLOW", decisions[0].Action)
	}
}

// TestEvaluateNodeFailed verifies FAIL_WORKFLOW when a node permanently failed.
func TestEvaluateNodeFailed(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusFailed},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].Action != domain.DecisionFailWorkflow {
		t.Errorf("action = %s, want FAIL_WORKFLOW", decisions[0].Action)
	}
}

// TestEvaluateTerminalState verifies no decisions for already-completed workflows.
func TestEvaluateTerminalState(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	terminals := []domain.WorkflowStatus{
		domain.WorkflowStatusCompleted,
		domain.WorkflowStatusFailed,
		domain.WorkflowStatusCancelled,
	}

	for _, status := range terminals {
		state := domain.WorkflowState{
			ID:     "wf-1",
			Status: status,
			Nodes: map[string]domain.NodeState{
				"analyze": {NodeID: "analyze", Status: domain.NodeStatusPending},
			},
		}

		decisions, err := eng.Evaluate(ctx, graph, state)
		if err != nil {
			t.Fatalf("Evaluate with status %s: %v", status, err)
		}
		if len(decisions) != 0 {
			t.Errorf("status %s: expected 0 decisions, got %d", status, len(decisions))
		}
	}
}

// TestEvaluateReadyNode verifies READY nodes with satisfied deps get START_NODE.
func TestEvaluateReadyNode(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// analyze is READY (after retryable failure), no deps → should start.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusReady},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if len(decisions) != 1 || decisions[0].NodeID != "analyze" {
		t.Errorf("expected START_NODE analyze, got %v", decisions)
	}
}

// TestTopologicalSort verifies nodes are returned in dependency order.
func TestTopologicalSort(t *testing.T) {
	nodes := []domain.ExecutableNode{
		{ID: "C", SkillRef: domain.SkillRef{Name: "s"}, DependsOn: []string{"A", "B"}},
		{ID: "B", SkillRef: domain.SkillRef{Name: "s"}, DependsOn: []string{"A"}},
		{ID: "A", SkillRef: domain.SkillRef{Name: "s"}},
	}

	sorted, err := topologicalSort(nodes)
	if err != nil {
		t.Fatalf("topologicalSort: %v", err)
	}

	// A must come before B, B must come before C.
	idx := make(map[string]int)
	for i, n := range sorted {
		idx[n.ID] = i
	}
	if idx["A"] > idx["B"] {
		t.Error("A should be before B")
	}
	if idx["B"] > idx["C"] {
		t.Error("B should be before C")
	}
}

// TestDepsSatisfied verifies dependency satisfaction logic.
func TestDepsSatisfied(t *testing.T) {
	graph := &domain.ExecutableGraph{
		Name: "test",
		Nodes: []domain.ExecutableNode{
			{ID: "A", SkillRef: domain.SkillRef{Name: "s"}},
			{ID: "B", SkillRef: domain.SkillRef{Name: "s"}, DependsOn: []string{"A"}},
			{ID: "C", SkillRef: domain.SkillRef{Name: "s"}, DependsOn: []string{"B"}},
		},
	}

	tests := []struct {
		name      string
		nodeID    string
		nodeStates map[string]domain.NodeState
		want      bool
	}{
		{
			name:   "no deps",
			nodeID: "A",
			nodeStates: map[string]domain.NodeState{
				"A": {NodeID: "A", Status: domain.NodeStatusPending},
			},
			want: true,
		},
		{
			name:   "dep satisfied",
			nodeID: "B",
			nodeStates: map[string]domain.NodeState{
				"A": {NodeID: "A", Status: domain.NodeStatusCompleted},
				"B": {NodeID: "B", Status: domain.NodeStatusPending},
			},
			want: true,
		},
		{
			name:   "dep not satisfied",
			nodeID: "B",
			nodeStates: map[string]domain.NodeState{
				"A": {NodeID: "A", Status: domain.NodeStatusRunning},
				"B": {NodeID: "B", Status: domain.NodeStatusPending},
			},
			want: false,
		},
		{
			name:   "dep missing from state",
			nodeID: "B",
			nodeStates: map[string]domain.NodeState{
				"B": {NodeID: "B", Status: domain.NodeStatusPending},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := domain.WorkflowState{
				ID:    "wf-1",
				Nodes: tt.nodeStates,
			}
			got := depsSatisfied(tt.nodeID, graph, state)
			if got != tt.want {
				t.Errorf("depsSatisfied(%s) = %v, want %v", tt.nodeID, got, tt.want)
			}
		})
	}
}

// TestEvaluateMultiDep verifies multi-level dependency chains.
func TestEvaluateMultiDep(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph := &domain.ExecutableGraph{
		Name: "multi",
		Nodes: []domain.ExecutableNode{
			{ID: "A", SkillRef: domain.SkillRef{Name: "s"}},
			{ID: "B", SkillRef: domain.SkillRef{Name: "s"}, DependsOn: []string{"A"}},
			{ID: "C", SkillRef: domain.SkillRef{Name: "s"}, DependsOn: []string{"A", "B"}},
		},
	}

	// A completed, B running → C should NOT start (B not done).
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"A": {NodeID: "A", Status: domain.NodeStatusCompleted},
			"B": {NodeID: "B", Status: domain.NodeStatusRunning},
			"C": {NodeID: "C", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if len(decisions) != 0 {
		t.Errorf("expected 0 decisions (C blocked on B), got %d", len(decisions))
	}

	// Now B also completed → C should start.
	state.Nodes["B"] = domain.NodeState{NodeID: "B", Status: domain.NodeStatusCompleted}

	decisions, err = eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate after B done: %v", err)
	}
	if len(decisions) != 1 || decisions[0].NodeID != "C" {
		t.Errorf("expected START_NODE C, got %v", decisions)
	}
}

// contains checks if s contains substr.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ── New Decision Tests ────────────────────────────

// TestEvaluateRetryNode verifies READY nodes with retry_count > 0 get RETRY_NODE.
func TestEvaluateRetryNode(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// analyze is READY with retry_count > 0 → should get RETRY_NODE.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusReady, RetryCount: 1},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].Action != domain.DecisionRetryNode {
		t.Errorf("action = %s, want RETRY_NODE", decisions[0].Action)
	}
	if decisions[0].NodeID != "analyze" {
		t.Errorf("node = %s, want analyze", decisions[0].NodeID)
	}
}

// TestEvaluateResumeNode verifies READY nodes with retry_count == 0 get RESUME_NODE.
func TestEvaluateResumeNode(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// analyze is READY with retry_count == 0 (after human approval) → RESUME_NODE.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusReady, RetryCount: 0},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].Action != domain.DecisionResumeNode {
		t.Errorf("action = %s, want RESUME_NODE", decisions[0].Action)
	}
}

// TestEvaluatePauseNode verifies RUNNING nodes with a human_approval gate get PAUSE_NODE.
func TestEvaluatePauseNode(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// review is RUNNING with gate=human_approval → should get PAUSE_NODE.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":    {NodeID: "analyze", Status: domain.NodeStatusCompleted},
			"implement":  {NodeID: "implement", Status: domain.NodeStatusCompleted},
			"review":     {NodeID: "review", Status: domain.NodeStatusRunning, Gate: "human_approval"},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Should have PAUSE_NODE for review.
	found := false
	for _, d := range decisions {
		if d.Action == domain.DecisionPauseNode && d.NodeID == "review" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected PAUSE_NODE for review, got %v", decisions)
	}
}

// TestEvaluateNoPauseForNonGated verifies RUNNING nodes without a gate don't get PAUSE_NODE.
func TestEvaluateNoPauseForNonGated(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	graph, _ := eng.Compile(ctx, []byte(bugfixYAML))

	// All nodes running without gates (or completed). No pause decisions.
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":   {NodeID: "analyze", Status: domain.NodeStatusRunning},
			"implement": {NodeID: "implement", Status: domain.NodeStatusPending},
			"review":    {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	for _, d := range decisions {
		if d.Action == domain.DecisionPauseNode {
			t.Errorf("unexpected PAUSE_NODE for %s (no gate set)", d.NodeID)
		}
	}
}

// TestEvaluateMixedDecisions verifies mixed decisions: START + PAUSE in one evaluation.
func TestEvaluateMixedDecisions(t *testing.T) {
	ctx := context.Background()
	eng := NewEngine()

	// Use a custom DAG: analyze → implement; analyze → review (both depend on analyze).
	// This allows review to start while implement is running.
	customYAML := `name: mixed
version: 1
nodes:
  - id: analyze
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    retry:
      max_retries: 1
      backoff: 10s
  - id: implement
    skill: code
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    depends_on:
      - analyze
    gate: human_approval
    retry:
      max_retries: 1
      backoff: 10s
  - id: review
    skill: review
    runtime:
      provider: claude-code
      model: claude-sonnet-4
    depends_on:
      - analyze
    retry:
      max_retries: 1
      backoff: 10s
`
	graph, err := eng.Compile(ctx, []byte(customYAML))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// analyze completed, implement RUNNING with gate, review PENDING (dep satisfied).
	state := domain.WorkflowState{
		ID:     "wf-1",
		Status: domain.WorkflowStatusActive,
		Nodes: map[string]domain.NodeState{
			"analyze":    {NodeID: "analyze", Status: domain.NodeStatusCompleted},
			"implement":  {NodeID: "implement", Status: domain.NodeStatusRunning, Gate: "human_approval"},
			"review":     {NodeID: "review", Status: domain.NodeStatusPending},
		},
	}

	decisions, err := eng.Evaluate(ctx, graph, state)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Should have START_NODE for review (dep satisfied) + PAUSE_NODE for implement (gated).
	var hasStart, hasPause bool
	for _, d := range decisions {
		if d.Action == domain.DecisionStartNode && d.NodeID == "review" {
			hasStart = true
		}
		if d.Action == domain.DecisionPauseNode && d.NodeID == "implement" {
			hasPause = true
		}
	}
	if !hasStart {
		t.Error("expected START_NODE for review")
	}
	if !hasPause {
		t.Error("expected PAUSE_NODE for implement")
	}
}
