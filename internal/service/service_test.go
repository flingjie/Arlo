package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ── Mock EventStore ──────────────────────────────────────

type mockEventStore struct {
	appendFn       func(ctx context.Context, streamID string, events []store.Event) ([]int64, error)
	readFn         func(ctx context.Context, streamID string, fromVersion int) ([]store.Event, error)
	readAllFn      func(ctx context.Context, fromPosition int64, limit int) ([]store.Event, int64, error)
	subscribeFn    func(ctx context.Context, fromPosition int64) (<-chan store.Event, error)
	closeFn        func() error
	lastPositionFn func() int64
}

func (m *mockEventStore) Append(ctx context.Context, streamID string, events []store.Event) ([]int64, error) {
	if m.appendFn != nil {
		return m.appendFn(ctx, streamID, events)
	}
	positions := make([]int64, len(events))
	for i := range events {
		positions[i] = int64(i + 1)
	}
	return positions, nil
}

func (m *mockEventStore) Read(ctx context.Context, streamID string, fromVersion int) ([]store.Event, error) {
	if m.readFn != nil {
		return m.readFn(ctx, streamID, fromVersion)
	}
	return nil, nil
}

func (m *mockEventStore) ReadAll(ctx context.Context, fromPosition int64, limit int) ([]store.Event, int64, error) {
	if m.readAllFn != nil {
		return m.readAllFn(ctx, fromPosition, limit)
	}
	return nil, 0, nil
}

func (m *mockEventStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan store.Event, error) {
	if m.subscribeFn != nil {
		return m.subscribeFn(ctx, fromPosition)
	}
	ch := make(chan store.Event)
	close(ch)
	return ch, nil
}

func (m *mockEventStore) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func (m *mockEventStore) LastPosition() int64 {
	if m.lastPositionFn != nil {
		return m.lastPositionFn()
	}
	return 0
}

// ── Mock StateStore ─────────────────────────────────────

type mockStateStore struct {
	getWorkflowFn        func(ctx context.Context, workflowID string) (*domain.WorkflowState, error)
	listActiveWorkflowsFn func(ctx context.Context) ([]domain.WorkflowState, error)
	getNodeStateFn       func(ctx context.Context, nodeID string) (*domain.NodeState, error)
	getReadyNodesFn      func(ctx context.Context, workflowID string) ([]domain.NodeState, error)
	rebuildFn            func(ctx context.Context) error
	applyFn              func(event store.Event) error
}

func (m *mockStateStore) GetWorkflow(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
	if m.getWorkflowFn != nil {
		return m.getWorkflowFn(ctx, workflowID)
	}
	return nil, &state.NotFoundError{Entity: "workflow", ID: workflowID}
}

func (m *mockStateStore) ListActiveWorkflows(ctx context.Context) ([]domain.WorkflowState, error) {
	if m.listActiveWorkflowsFn != nil {
		return m.listActiveWorkflowsFn(ctx)
	}
	return nil, nil
}

func (m *mockStateStore) GetNodeState(ctx context.Context, nodeID string) (*domain.NodeState, error) {
	if m.getNodeStateFn != nil {
		return m.getNodeStateFn(ctx, nodeID)
	}
	return nil, &state.NotFoundError{Entity: "node", ID: nodeID}
}

func (m *mockStateStore) GetReadyNodes(ctx context.Context, workflowID string) ([]domain.NodeState, error) {
	if m.getReadyNodesFn != nil {
		return m.getReadyNodesFn(ctx, workflowID)
	}
	return nil, nil
}

func (m *mockStateStore) Rebuild(ctx context.Context) error {
	if m.rebuildFn != nil {
		return m.rebuildFn(ctx)
	}
	return nil
}

func (m *mockStateStore) Apply(event store.Event) error {
	if m.applyFn != nil {
		return m.applyFn(event)
	}
	return nil
}

// ── Mock WorkflowEngine ─────────────────────────────────

type mockWorkflowEngine struct {
	compileFn  func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error)
	validateFn func(ctx context.Context, graph *domain.ExecutableGraph) error
}

func (m *mockWorkflowEngine) Compile(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
	if m.compileFn != nil {
		return m.compileFn(ctx, source)
	}
	return &domain.ExecutableGraph{Name: "test", Version: 1, Nodes: []domain.ExecutableNode{}}, nil
}

func (m *mockWorkflowEngine) Validate(ctx context.Context, graph *domain.ExecutableGraph) error {
	if m.validateFn != nil {
		return m.validateFn(ctx, graph)
	}
	return nil
}

// ── Mock Reconciler ─────────────────────────────────────

type mockReconciler struct {
	mu          sync.Mutex
	submitted   map[string]*domain.ExecutableGraph
	reconcileFn func(ctx context.Context, workflowID string) error
}

func newMockReconciler() *mockReconciler {
	return &mockReconciler{submitted: make(map[string]*domain.ExecutableGraph)}
}

func (m *mockReconciler) Submit(workflowID string, graph *domain.ExecutableGraph) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submitted[workflowID] = graph
}

func (m *mockReconciler) submittedGraph(workflowID string) *domain.ExecutableGraph {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.submitted[workflowID]
}

func (m *mockReconciler) Reconcile(ctx context.Context, workflowID string) error {
	if m.reconcileFn != nil {
		return m.reconcileFn(ctx, workflowID)
	}
	return nil
}

// ── Mock RuntimeManager ─────────────────────────────────

type mockRuntimeManager struct {
	attachInstanceFn func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error)
}

func (m *mockRuntimeManager) AttachInstance(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
	if m.attachInstanceFn != nil {
		return m.attachInstanceFn(ctx, id)
	}
	ch := make(chan domain.PTYFrame)
	close(ch)
	return ch, nil, nil
}

// ── Helpers ──────────────────────────────────────────────

func makeValidYAML() string {
	return `
name: test-wf
version: 1
nodes:
  - id: node1
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-haiku-4-5
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 1
`
}

func makeValidGraph() *domain.ExecutableGraph {
	return &domain.ExecutableGraph{
		Name:    "test-wf",
		Version: 1,
		Nodes: []domain.ExecutableNode{
			{
				ID:       "node1",
				SkillRef: domain.SkillRef{Name: "root-cause"},
				Runtime:  domain.RuntimeRef{Provider: domain.RuntimeProviderClaudeCode, Model: "claude-haiku-4-5"},
			},
		},
		Policy: domain.SchedulingPolicy{MaxConcurrentNodes: 1},
	}
}

func makeWorkflowState(id string, status domain.WorkflowStatus) *domain.WorkflowState {
	now := time.Now()
	return &domain.WorkflowState{
		ID:        id,
		Status:    status,
		CreatedAt: now,
		Nodes: map[string]domain.NodeState{
			"node1": {
				NodeID:     "node1",
				WorkflowID: id,
				Status:     domain.NodeStatusPending,
			},
		},
	}
}

// newTestService creates a service wired with mocks.
func newTestService(es *mockEventStore, ss *mockStateStore, eng *mockWorkflowEngine, rec *mockReconciler, rm *mockRuntimeManager) *ArloService {
	return &ArloService{
		eventStore: es,
		stateStore: ss,
		engine:     eng,
		reconciler: rec,
		runtimeMgr: rm,
	}
}

func gRPCStatus(err error) codes.Code {
	s, ok := status.FromError(err)
	if !ok {
		return codes.OK
	}
	return s.Code()
}

// ── Mock Stream Types ───────────────────────────────────

type eventCollector struct {
	grpc.ServerStream
	ctx    context.Context
	events []*arlov1.Event
}

func (e *eventCollector) Context() context.Context {
	if e.ctx != nil {
		return e.ctx
	}
	return context.Background()
}

func (e *eventCollector) Send(evt *arlov1.Event) error {
	e.events = append(e.events, evt)
	return nil
}

type ptyCollector struct {
	grpc.ServerStream
	ctx    context.Context
	frames []*arlov1.PTYFrame
}

func (p *ptyCollector) Context() context.Context {
	if p.ctx != nil {
		return p.ctx
	}
	return context.Background()
}

func (p *ptyCollector) Send(frame *arlov1.PTYFrame) error {
	p.frames = append(p.frames, frame)
	return nil
}

// ============================================================================
// CreateTask Tests
// ============================================================================

func TestCreateTask_Success(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			return makeValidGraph(), nil
		},
	}
	rec := newMockReconciler()
	svc := newTestService(&mockEventStore{}, &mockStateStore{}, eng, rec, nil)

	resp, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test task",
		WorkflowSource: makeValidYAML(),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.TaskId == "" {
		t.Error("expected non-empty TaskId")
	}
	if resp.WorkflowId == "" {
		t.Error("expected non-empty WorkflowId")
	}
	if !strings.HasPrefix(resp.TaskId, "task-") {
		t.Errorf("TaskId should start with 'task-', got %q", resp.TaskId)
	}
	if !strings.HasPrefix(resp.WorkflowId, "wf-task-") {
		t.Errorf("WorkflowId should start with 'wf-task-', got %q", resp.WorkflowId)
	}
	if g := rec.submittedGraph(resp.WorkflowId); g == nil {
		t.Error("expected reconciler.Submit to be called with the graph")
	}
}

func TestCreateTask_CompileError(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			return nil, errors.New("invalid yaml syntax")
		},
	}
	svc := newTestService(&mockEventStore{}, &mockStateStore{}, eng, newMockReconciler(), nil)

	_, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test",
		WorkflowSource: "bad: [",
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", gRPCStatus(err))
	}
}

func TestCreateTask_ValidateError(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			return makeValidGraph(), nil
		},
		validateFn: func(ctx context.Context, graph *domain.ExecutableGraph) error {
			return errors.New("duplicate node id")
		},
	}
	svc := newTestService(&mockEventStore{}, &mockStateStore{}, eng, newMockReconciler(), nil)

	_, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test",
		WorkflowSource: makeValidYAML(),
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %v", gRPCStatus(err))
	}
}

func TestCreateTask_EventStoreAppendWorkflowError(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			return makeValidGraph(), nil
		},
	}
	es := &mockEventStore{
		appendFn: func(ctx context.Context, streamID string, events []store.Event) ([]int64, error) {
			if strings.HasPrefix(streamID, "workflow-") {
				return nil, errors.New("disk full")
			}
			return []int64{1, 2}, nil
		},
	}
	svc := newTestService(es, &mockStateStore{}, eng, newMockReconciler(), nil)

	_, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test",
		WorkflowSource: makeValidYAML(),
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Internal {
		t.Errorf("expected Internal, got %v", gRPCStatus(err))
	}
}

func TestCreateTask_EventStoreAppendNodeError(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			g := makeValidGraph()
			g.Nodes = append(g.Nodes, domain.ExecutableNode{
				ID:       "node2",
				SkillRef: domain.SkillRef{Name: "implement"},
				Runtime:  domain.RuntimeRef{Provider: domain.RuntimeProviderClaudeCode},
			})
			return g, nil
		},
	}
	callCount := 0
	es := &mockEventStore{
		appendFn: func(ctx context.Context, streamID string, events []store.Event) ([]int64, error) {
			callCount++
			if callCount > 1 {
				return nil, errors.New("disk full")
			}
			return []int64{1, 2}, nil
		},
	}
	svc := newTestService(es, &mockStateStore{}, eng, newMockReconciler(), nil)

	_, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test",
		WorkflowSource: makeValidYAML(),
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Internal {
		t.Errorf("expected Internal, got %v", gRPCStatus(err))
	}
}

func TestCreateTask_RebuildError(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			return makeValidGraph(), nil
		},
	}
	ss := &mockStateStore{
		rebuildFn: func(ctx context.Context) error {
			return errors.New("rebuild failed")
		},
	}
	svc := newTestService(&mockEventStore{}, ss, eng, newMockReconciler(), nil)

	_, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test",
		WorkflowSource: makeValidYAML(),
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Internal {
		t.Errorf("expected Internal, got %v", gRPCStatus(err))
	}
}

func TestCreateTask_ReconcileErrorNonFatal(t *testing.T) {
	eng := &mockWorkflowEngine{
		compileFn: func(ctx context.Context, source []byte) (*domain.ExecutableGraph, error) {
			return makeValidGraph(), nil
		},
	}
	rec := newMockReconciler()
	rec.reconcileFn = func(ctx context.Context, workflowID string) error {
		return errors.New("reconcile boom")
	}
	svc := newTestService(&mockEventStore{}, &mockStateStore{}, eng, rec, nil)

	resp, err := svc.CreateTask(context.Background(), &arlov1.CreateTaskRequest{
		Title:          "test",
		WorkflowSource: makeValidYAML(),
	})

	// Reconcile failure should NOT fail the RPC — it's a warn log, not an error.
	if err != nil {
		t.Fatalf("unexpected error on reconcile failure: %v", err)
	}
	if resp.WorkflowId == "" {
		t.Error("expected WorkflowId even when reconcile fails")
	}
}

// ============================================================================
// GetTask Tests
// ============================================================================

func TestGetTask_Success(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return makeWorkflowState(workflowID, domain.WorkflowStatusActive), nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.GetTask(context.Background(), &arlov1.GetTaskRequest{TaskId: "task-123"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.TaskId != "task-123" {
		t.Errorf("expected TaskId='task-123', got %q", resp.TaskId)
	}
	if resp.Status != string(domain.WorkflowStatusActive) {
		t.Errorf("expected Status='ACTIVE', got %q", resp.Status)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return nil, &state.NotFoundError{Entity: "workflow", ID: workflowID}
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.GetTask(context.Background(), &arlov1.GetTaskRequest{TaskId: "nonexistent"})

	if err == nil {
		t.Fatal("expected NotFound error")
	}
	if gRPCStatus(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", gRPCStatus(err))
	}
}

// ============================================================================
// ListTasks Tests
// ============================================================================

func TestListTasks_Success(t *testing.T) {
	ss := &mockStateStore{
		listActiveWorkflowsFn: func(ctx context.Context) ([]domain.WorkflowState, error) {
			return []domain.WorkflowState{
				{ID: "wf-1", Status: domain.WorkflowStatusActive},
				{ID: "wf-2", Status: domain.WorkflowStatusActive},
			}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.ListTasks(context.Background(), &arlov1.ListTasksRequest{})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(resp.Tasks))
	}
}

func TestListTasks_Empty(t *testing.T) {
	ss := &mockStateStore{
		listActiveWorkflowsFn: func(ctx context.Context) ([]domain.WorkflowState, error) {
			return []domain.WorkflowState{}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.ListTasks(context.Background(), &arlov1.ListTasksRequest{})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Tasks) != 0 {
		t.Errorf("expected 0 tasks, got %d", len(resp.Tasks))
	}
}

func TestListTasks_Error(t *testing.T) {
	ss := &mockStateStore{
		listActiveWorkflowsFn: func(ctx context.Context) ([]domain.WorkflowState, error) {
			return nil, errors.New("database down")
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.ListTasks(context.Background(), &arlov1.ListTasksRequest{})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Internal {
		t.Errorf("expected Internal, got %v", gRPCStatus(err))
	}
}

// ============================================================================
// GetWorkflow Tests
// ============================================================================

func TestGetWorkflow_Success(t *testing.T) {
	now := time.Now()
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return &domain.WorkflowState{
				ID:     workflowID,
				Status: domain.WorkflowStatusActive,
				Nodes: map[string]domain.NodeState{
					"node1": {
						NodeID:     "node1",
						WorkflowID: workflowID,
						Status:     domain.NodeStatusRunning,
						SessionID:  "sess-1",
						StartedAt:  &now,
					},
				},
			}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.GetWorkflow(context.Background(), &arlov1.GetWorkflowRequest{WorkflowId: "wf-1"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.WorkflowId != "wf-1" {
		t.Errorf("expected WorkflowId='wf-1', got %q", resp.WorkflowId)
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(resp.Nodes))
	}
	if resp.Nodes[0].NodeId != "node1" {
		t.Errorf("expected NodeId='node1', got %q", resp.Nodes[0].NodeId)
	}
}

func TestGetWorkflow_NotFound(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return nil, &state.NotFoundError{Entity: "workflow", ID: workflowID}
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.GetWorkflow(context.Background(), &arlov1.GetWorkflowRequest{WorkflowId: "no-such"})

	if err == nil {
		t.Fatal("expected NotFound error")
	}
	if gRPCStatus(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", gRPCStatus(err))
	}
}

// ============================================================================
// GetWorkflowSnapshot Tests
// ============================================================================

func TestGetWorkflowSnapshot_Success(t *testing.T) {
	now := time.Now()
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return &domain.WorkflowState{
				ID:        workflowID,
				Status:    domain.WorkflowStatusCompleted,
				CreatedAt: now,
				Nodes: map[string]domain.NodeState{
					"node1": {
						NodeID:      "node1",
						WorkflowID:  workflowID,
						Status:      domain.NodeStatusCompleted,
						StartedAt:   &now,
						CompletedAt: &now,
					},
				},
			}, nil
		},
	}
	es := &mockEventStore{
		lastPositionFn: func() int64 { return 42 },
	}
	svc := newTestService(es, ss, nil, nil, nil)

	resp, err := svc.GetWorkflowSnapshot(context.Background(), &arlov1.GetWorkflowSnapshotRequest{WorkflowId: "wf-1"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != "COMPLETED" {
		t.Errorf("expected Status='COMPLETED', got %q", resp.Status)
	}
	if resp.Version != 42 {
		t.Errorf("expected Version=42, got %d", resp.Version)
	}
	if len(resp.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(resp.Nodes))
	}
}

func TestGetWorkflowSnapshot_NilTimeFields(t *testing.T) {
	// Node state with nil timestamps — should not crash.
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return &domain.WorkflowState{
				ID:     workflowID,
				Status: domain.WorkflowStatusActive,
				Nodes: map[string]domain.NodeState{
					"node1": {
						NodeID:     "node1",
						WorkflowID: workflowID,
						Status:     domain.NodeStatusPending,
						// StartedAt and CompletedAt are nil
					},
				},
			}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.GetWorkflowSnapshot(context.Background(), &arlov1.GetWorkflowSnapshotRequest{WorkflowId: "wf-1"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Nodes[0].StartedAt != "" {
		t.Errorf("expected empty StartedAt for nil time, got %q", resp.Nodes[0].StartedAt)
	}
	if resp.Nodes[0].CompletedAt != "" {
		t.Errorf("expected empty CompletedAt for nil time, got %q", resp.Nodes[0].CompletedAt)
	}
}

func TestGetWorkflowSnapshot_NotFound(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return nil, &state.NotFoundError{Entity: "workflow", ID: workflowID}
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.GetWorkflowSnapshot(context.Background(), &arlov1.GetWorkflowSnapshotRequest{WorkflowId: "missing"})

	if err == nil {
		t.Fatal("expected NotFound error")
	}
	if gRPCStatus(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", gRPCStatus(err))
	}
}

// ============================================================================
// GetSession Tests
// ============================================================================

func TestGetSession_Success(t *testing.T) {
	ss := &mockStateStore{
		listActiveWorkflowsFn: func(ctx context.Context) ([]domain.WorkflowState, error) {
			return []domain.WorkflowState{{
				ID:     "wf-1",
				Status: domain.WorkflowStatusActive,
				Nodes: map[string]domain.NodeState{
					"node1": {
						NodeID:     "node1",
						WorkflowID: "wf-1",
						SessionID:  "sess-abc",
						Status:     domain.NodeStatusRunning,
					},
				},
			}}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.GetSession(context.Background(), &arlov1.GetSessionRequest{SessionId: "sess-abc"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.SessionId != "sess-abc" {
		t.Errorf("expected SessionId='sess-abc', got %q", resp.SessionId)
	}
	if resp.NodeId != "node1" {
		t.Errorf("expected NodeId='node1', got %q", resp.NodeId)
	}
}

func TestGetSession_NotFound(t *testing.T) {
	ss := &mockStateStore{
		listActiveWorkflowsFn: func(ctx context.Context) ([]domain.WorkflowState, error) {
			return []domain.WorkflowState{}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.GetSession(context.Background(), &arlov1.GetSessionRequest{SessionId: "no-such"})

	if err == nil {
		t.Fatal("expected NotFound error")
	}
	if gRPCStatus(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", gRPCStatus(err))
	}
}

func TestGetSession_ListError(t *testing.T) {
	ss := &mockStateStore{
		listActiveWorkflowsFn: func(ctx context.Context) ([]domain.WorkflowState, error) {
			return nil, errors.New("state store error")
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.GetSession(context.Background(), &arlov1.GetSessionRequest{SessionId: "sess-1"})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Internal {
		t.Errorf("expected Internal, got %v", gRPCStatus(err))
	}
}

// ============================================================================
// ListSessions Tests
// ============================================================================

func TestListSessions_Success(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return &domain.WorkflowState{
				ID:     workflowID,
				Status: domain.WorkflowStatusActive,
				Nodes: map[string]domain.NodeState{
					"node1": {NodeID: "node1", SessionID: "sess-1", Status: domain.NodeStatusRunning},
					"node2": {NodeID: "node2", SessionID: "sess-2", Status: domain.NodeStatusCompleted},
					"node3": {NodeID: "node3", SessionID: "", Status: domain.NodeStatusPending},
				},
			}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.ListSessions(context.Background(), &arlov1.ListSessionsRequest{WorkflowId: "wf-1"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Sessions) != 2 {
		t.Errorf("expected 2 sessions (non-empty SessionIDs), got %d", len(resp.Sessions))
	}
}

func TestListSessions_NoSessions(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return &domain.WorkflowState{
				ID:    workflowID,
				Nodes: map[string]domain.NodeState{},
			}, nil
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.ListSessions(context.Background(), &arlov1.ListSessionsRequest{WorkflowId: "wf-1"})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Sessions) != 0 {
		t.Errorf("expected 0 sessions, got %d", len(resp.Sessions))
	}
}

func TestListSessions_NotFound(t *testing.T) {
	ss := &mockStateStore{
		getWorkflowFn: func(ctx context.Context, workflowID string) (*domain.WorkflowState, error) {
			return nil, &state.NotFoundError{Entity: "workflow", ID: workflowID}
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	_, err := svc.ListSessions(context.Background(), &arlov1.ListSessionsRequest{WorkflowId: "missing"})

	if err == nil {
		t.Fatal("expected NotFound error")
	}
	if gRPCStatus(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", gRPCStatus(err))
	}
}

// ============================================================================
// SubscribeEvents Tests
// ============================================================================

func TestSubscribeEvents_Success(t *testing.T) {
	ch := make(chan store.Event, 2)
	ch <- store.Event{
		ID:       "evt-1",
		StreamID: "workflow-wf-1",
		Type:     store.EventTaskCreated,
		Version:  1,
		Position: 1,
		Payload:  json.RawMessage(`{"workflow_id":"wf-1"}`),
	}
	ch <- store.Event{
		ID:       "evt-2",
		StreamID: "node-node1",
		Type:     store.EventNodeStarted,
		Version:  1,
		Position: 2,
		Payload:  json.RawMessage(`{"workflow_id":"wf-1","node_id":"node1"}`),
	}
	close(ch)

	es := &mockEventStore{
		subscribeFn: func(ctx context.Context, fromPosition int64) (<-chan store.Event, error) {
			return ch, nil
		},
	}
	svc := newTestService(es, nil, nil, nil, nil)
	collector := &eventCollector{}

	err := svc.SubscribeEvents(&arlov1.SubscribeEventsRequest{
		WorkflowId:   "wf-1",
		FromPosition: 0,
	}, collector)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Channel closed, so SubscribeEvents returns nil.
	if len(collector.events) != 2 {
		t.Errorf("expected 2 events, got %d", len(collector.events))
	}
}

func TestSubscribeEvents_FilterByStreamID(t *testing.T) {
	ch := make(chan store.Event, 3)
	ch <- store.Event{
		ID:       "evt-1",
		StreamID: "workflow-wf-1",
		Type:     store.EventTaskCreated,
		Version:  1,
		Position: 1,
		Payload:  json.RawMessage(`{"workflow_id":"wf-1"}`),
	}
	ch <- store.Event{
		ID:       "evt-2",
		StreamID: "workflow-wf-2", // different workflow
		Type:     store.EventTaskCreated,
		Version:  1,
		Position: 2,
		Payload:  json.RawMessage(`{"workflow_id":"wf-2"}`),
	}
	ch <- store.Event{
		ID:       "evt-3",
		StreamID: "node-node1",
		Type:     store.EventNodeStarted,
		Version:  1,
		Position: 3,
		Payload:  json.RawMessage(`{"workflow_id":"wf-1","node_id":"node1"}`),
	}
	close(ch)

	es := &mockEventStore{
		subscribeFn: func(ctx context.Context, fromPosition int64) (<-chan store.Event, error) {
			return ch, nil
		},
	}
	svc := newTestService(es, nil, nil, nil, nil)
	collector := &eventCollector{}

	svc.SubscribeEvents(&arlov1.SubscribeEventsRequest{
		WorkflowId:   "wf-1",
		FromPosition: 0,
	}, collector)

	// evt-2 (wf-2) should be filtered out.
	if len(collector.events) != 2 {
		t.Errorf("expected 2 events (wf-2 filtered out), got %d", len(collector.events))
	}
}

func TestSubscribeEvents_NodeEventWrongWorkflow(t *testing.T) {
	// Node events with non-matching workflow_id should be filtered.
	ch := make(chan store.Event, 2)
	ch <- store.Event{
		ID:       "evt-1",
		StreamID: "node-node1",
		Type:     store.EventNodeStarted,
		Version:  1,
		Position: 1,
		Payload:  json.RawMessage(`{"workflow_id":"wf-OTHER","node_id":"node1"}`),
	}
	ch <- store.Event{
		ID:       "evt-2",
		StreamID: "node-node1",
		Type:     store.EventNodeCompleted,
		Version:  2,
		Position: 2,
		Payload:  json.RawMessage(`{"workflow_id":"wf-1","node_id":"node1"}`),
	}
	close(ch)

	es := &mockEventStore{
		subscribeFn: func(ctx context.Context, fromPosition int64) (<-chan store.Event, error) {
			return ch, nil
		},
	}
	svc := newTestService(es, nil, nil, nil, nil)
	collector := &eventCollector{}

	svc.SubscribeEvents(&arlov1.SubscribeEventsRequest{
		WorkflowId:   "wf-1",
		FromPosition: 0,
	}, collector)

	if len(collector.events) != 1 {
		t.Errorf("expected 1 event (wf-OTHER node filtered), got %d", len(collector.events))
	}
	if collector.events[0].Type != "NODE_COMPLETED" {
		t.Errorf("expected NODE_COMPLETED, got %s", collector.events[0].Type)
	}
}

func TestSubscribeEvents_SubscribeError(t *testing.T) {
	es := &mockEventStore{
		subscribeFn: func(ctx context.Context, fromPosition int64) (<-chan store.Event, error) {
			return nil, errors.New("subscription failed")
		},
	}
	svc := newTestService(es, nil, nil, nil, nil)

	err := svc.SubscribeEvents(&arlov1.SubscribeEventsRequest{FromPosition: 0}, &eventCollector{})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Internal {
		t.Errorf("expected Internal, got %v", gRPCStatus(err))
	}
}

func TestSubscribeEvents_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	ch := make(chan store.Event) // never closed, but context is cancelled
	es := &mockEventStore{
		subscribeFn: func(ctx context.Context, fromPosition int64) (<-chan store.Event, error) {
			return ch, nil
		},
	}
	svc := newTestService(es, nil, nil, nil, nil)

	err := svc.SubscribeEvents(&arlov1.SubscribeEventsRequest{FromPosition: 0},
		&eventCollector{ctx: ctx})

	if err == nil {
		t.Fatal("expected context cancelled error")
	}
}

// ============================================================================
// ExecuteCommand Tests
// ============================================================================

func TestExecuteCommand_CancelTask_Success(t *testing.T) {
	es := &mockEventStore{}
	ss := &mockStateStore{}
	svc := newTestService(es, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "cancel_task",
		Target:  "wf-123",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true, got false: %s", resp.Message)
	}
}

func TestExecuteCommand_CancelTask_AppendError(t *testing.T) {
	es := &mockEventStore{
		appendFn: func(ctx context.Context, streamID string, events []store.Event) ([]int64, error) {
			return nil, errors.New("disk error")
		},
	}
	svc := newTestService(es, &mockStateStore{}, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "cancel_task",
		Target:  "wf-123",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false on append error")
	}
}

func TestExecuteCommand_Approve_Success(t *testing.T) {
	es := &mockEventStore{}
	ss := &mockStateStore{
		getNodeStateFn: func(ctx context.Context, nodeID string) (*domain.NodeState, error) {
			return &domain.NodeState{
				NodeID:     nodeID,
				WorkflowID: "wf-1",
				SessionID:  "sess-1",
				Status:     domain.NodeStatusWaiting,
			}, nil
		},
	}
	svc := newTestService(es, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "approve",
		Target:  "step",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true, got false: %s", resp.Message)
	}
}

func TestExecuteCommand_Approve_NodeNotFound(t *testing.T) {
	ss := &mockStateStore{
		getNodeStateFn: func(ctx context.Context, nodeID string) (*domain.NodeState, error) {
			return nil, &state.NotFoundError{Entity: "node", ID: nodeID}
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "approve",
		Target:  "no-such",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false for unknown node")
	}
}

func TestExecuteCommand_Reject_Success(t *testing.T) {
	es := &mockEventStore{}
	ss := &mockStateStore{
		getNodeStateFn: func(ctx context.Context, nodeID string) (*domain.NodeState, error) {
			return &domain.NodeState{
				NodeID:     nodeID,
				WorkflowID: "wf-1",
				SessionID:  "sess-1",
				Status:     domain.NodeStatusWaiting,
			}, nil
		},
	}
	svc := newTestService(es, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "reject",
		Target:  "step",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true, got false: %s", resp.Message)
	}
}

func TestExecuteCommand_Annotate_WithEquals(t *testing.T) {
	es := &mockEventStore{}
	ss := &mockStateStore{
		getNodeStateFn: func(ctx context.Context, nodeID string) (*domain.NodeState, error) {
			return &domain.NodeState{
				NodeID:     nodeID,
				WorkflowID: "wf-1",
			}, nil
		},
	}
	svc := newTestService(es, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "annotate",
		Target:  "node1",
		Input:   "priority=high",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true, got false: %s", resp.Message)
	}
	if !strings.Contains(resp.Message, "priority=high") {
		t.Errorf("expected annotation message to contain key=value, got %q", resp.Message)
	}
}

func TestExecuteCommand_Annotate_WithoutEquals(t *testing.T) {
	es := &mockEventStore{}
	ss := &mockStateStore{
		getNodeStateFn: func(ctx context.Context, nodeID string) (*domain.NodeState, error) {
			return &domain.NodeState{
				NodeID:     nodeID,
				WorkflowID: "wf-1",
			}, nil
		},
	}
	svc := newTestService(es, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "annotate",
		Target:  "node1",
		Input:   "just-a-tag",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Success {
		t.Errorf("expected Success=true, got false: %s", resp.Message)
	}
}

func TestExecuteCommand_Annotate_NodeNotFound(t *testing.T) {
	ss := &mockStateStore{
		getNodeStateFn: func(ctx context.Context, nodeID string) (*domain.NodeState, error) {
			return nil, &state.NotFoundError{Entity: "node", ID: nodeID}
		},
	}
	svc := newTestService(&mockEventStore{}, ss, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "annotate",
		Target:  "ghost",
		Input:   "x=y",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false for unknown node")
	}
}

func TestExecuteCommand_UnknownCommand(t *testing.T) {
	svc := newTestService(&mockEventStore{}, &mockStateStore{}, nil, nil, nil)

	resp, err := svc.ExecuteCommand(context.Background(), &arlov1.CommandRequest{
		Command: "make_coffee",
		Target:  "x",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Success {
		t.Error("expected Success=false for unknown command")
	}
	if !strings.Contains(resp.Message, "unknown command") {
		t.Errorf("expected message to mention 'unknown command', got %q", resp.Message)
	}
}

// ============================================================================
// AttachPTY Tests
// ============================================================================

func TestAttachPTY_Success(t *testing.T) {
	ch := make(chan domain.PTYFrame, 1)
	ch <- domain.PTYFrame{SessionID: "sess-1", Data: []byte("hello"), Seq: 1}
	close(ch)

	rm := &mockRuntimeManager{
		attachInstanceFn: func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
			return ch, nil, nil
		},
	}
	svc := newTestService(nil, nil, nil, nil, rm)
	collector := &ptyCollector{}

	err := svc.AttachPTY(&arlov1.AttachPTYRequest{SessionId: "sess-1"}, collector)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(collector.frames) != 1 {
		t.Errorf("expected 1 frame, got %d", len(collector.frames))
	}
	if string(collector.frames[0].Data) != "hello" {
		t.Errorf("expected data='hello', got %q", string(collector.frames[0].Data))
	}
}

func TestAttachPTY_AttachError(t *testing.T) {
	rm := &mockRuntimeManager{
		attachInstanceFn: func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
			return nil, nil, errors.New("session not found")
		},
	}
	svc := newTestService(nil, nil, nil, nil, rm)

	err := svc.AttachPTY(&arlov1.AttachPTYRequest{SessionId: "bogus"}, &ptyCollector{})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", gRPCStatus(err))
	}
}

func TestAttachPTY_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan domain.PTYFrame) // never closed
	rm := &mockRuntimeManager{
		attachInstanceFn: func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
			cancel() // cancel after attach
			return ch, nil, nil
		},
	}
	svc := newTestService(nil, nil, nil, nil, rm)

	err := svc.AttachPTY(&arlov1.AttachPTYRequest{SessionId: "sess-1"},
		&ptyCollector{ctx: ctx})

	if err == nil {
		t.Fatal("expected context cancelled error")
	}
}

// ============================================================================
// SendPTYInput Tests
// ============================================================================

func TestSendPTYInput_Success(t *testing.T) {
	var written []byte
	rm := &mockRuntimeManager{
		attachInstanceFn: func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
			return nil, &testWriter{fn: func(p []byte) (int, error) {
				written = append(written, p...)
				return len(p), nil
			}}, nil
		},
	}
	svc := newTestService(nil, nil, nil, nil, rm)

	_, err := svc.SendPTYInput(context.Background(), &arlov1.SendPTYInputRequest{
		SessionId: "sess-1",
		Data:      []byte("ls\n"),
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(written) != "ls\n" {
		t.Errorf("expected 'ls\\n', got %q", string(written))
	}
}

func TestSendPTYInput_AttachError(t *testing.T) {
	rm := &mockRuntimeManager{
		attachInstanceFn: func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
			return nil, nil, errors.New("not found")
		},
	}
	svc := newTestService(nil, nil, nil, nil, rm)

	_, err := svc.SendPTYInput(context.Background(), &arlov1.SendPTYInputRequest{
		SessionId: "bad",
		Data:      []byte("x"),
	})

	if err == nil {
		t.Fatal("expected error")
	}
	if gRPCStatus(err) != codes.Unavailable {
		t.Errorf("expected Unavailable, got %v", gRPCStatus(err))
	}
}

func TestSendPTYInput_NilWriter(t *testing.T) {
	// Writer is nil — should not crash.
	rm := &mockRuntimeManager{
		attachInstanceFn: func(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
			return make(chan domain.PTYFrame), nil, nil
		},
	}
	svc := newTestService(nil, nil, nil, nil, rm)

	_, err := svc.SendPTYInput(context.Background(), &arlov1.SendPTYInputRequest{
		SessionId: "sess-1",
		Data:      []byte("x"),
	})

	if err != nil {
		t.Fatalf("unexpected error with nil writer: %v", err)
	}
}

// ============================================================================
// buildNodeState Tests
// ============================================================================

func TestBuildNodeState_Full(t *testing.T) {
	now := time.Now()
	ns := &domain.NodeState{
		NodeID:      "node1",
		Status:      domain.NodeStatusCompleted,
		SessionID:   "sess-1",
		RuntimeID:   "rt-1",
		RetryCount:  2,
		DependsOn:   []string{"dep1"},
		Children:    []string{"child1"},
		Gate:        "human_approval",
		StartedAt:   &now,
		CompletedAt: &now,
	}
	result := buildNodeState(ns)

	if result.NodeId != "node1" {
		t.Errorf("NodeId: expected 'node1', got %q", result.NodeId)
	}
	if result.Status != "COMPLETED" {
		t.Errorf("Status: expected 'COMPLETED', got %q", result.Status)
	}
	if result.SessionId != "sess-1" {
		t.Errorf("SessionId: expected 'sess-1', got %q", result.SessionId)
	}
	if result.RuntimeId != "rt-1" {
		t.Errorf("RuntimeId: expected 'rt-1', got %q", result.RuntimeId)
	}
	if result.RetryCount != 2 {
		t.Errorf("RetryCount: expected 2, got %d", result.RetryCount)
	}
	if result.Gate != "human_approval" {
		t.Errorf("Gate: expected 'human_approval', got %q", result.Gate)
	}
	if result.DependsOn[0] != "dep1" {
		t.Errorf("DependsOn[0]: expected 'dep1', got %q", result.DependsOn[0])
	}
	if result.Children[0] != "child1" {
		t.Errorf("Children[0]: expected 'child1', got %q", result.Children[0])
	}
}

func TestBuildNodeState_NilTimes(t *testing.T) {
	ns := &domain.NodeState{
		NodeID: "node1",
		Status: domain.NodeStatusPending,
		// StartedAt, CompletedAt are nil
	}
	result := buildNodeState(ns)

	if result.StartedAt != "" {
		t.Errorf("expected empty StartedAt, got %q", result.StartedAt)
	}
	if result.CompletedAt != "" {
		t.Errorf("expected empty CompletedAt, got %q", result.CompletedAt)
	}
}

func TestBuildNodeState_EmptySlices(t *testing.T) {
	ns := &domain.NodeState{
		NodeID:    "node1",
		Status:    domain.NodeStatusPending,
		DependsOn: nil,
		Children:  nil,
	}
	result := buildNodeState(ns)

	// nil slices should stay nil (protobuf preserves the distinction).
	if result.DependsOn != nil {
		t.Errorf("expected nil DependsOn, got %v", result.DependsOn)
	}
	if result.Children != nil {
		t.Errorf("expected nil Children, got %v", result.Children)
	}
}

// ============================================================================
// nodeEventMatchesWorkflow Tests
// ============================================================================

func TestNodeEventMatchesWorkflow_Match(t *testing.T) {
	event := store.Event{
		Payload: json.RawMessage(`{"workflow_id":"wf-abc"}`),
	}
	if !nodeEventMatchesWorkflow(event, "wf-abc") {
		t.Error("expected match")
	}
}

func TestNodeEventMatchesWorkflow_NoMatch(t *testing.T) {
	event := store.Event{
		Payload: json.RawMessage(`{"workflow_id":"wf-xyz"}`),
	}
	if nodeEventMatchesWorkflow(event, "wf-abc") {
		t.Error("expected no match")
	}
}

func TestNodeEventMatchesWorkflow_InvalidJSON(t *testing.T) {
	event := store.Event{
		Payload: json.RawMessage(`not-json`),
	}
	if nodeEventMatchesWorkflow(event, "wf-abc") {
		t.Error("expected false for invalid JSON")
	}
}

func TestNodeEventMatchesWorkflow_EmptyPayload(t *testing.T) {
	event := store.Event{
		Payload: json.RawMessage(`{}`),
	}
	if nodeEventMatchesWorkflow(event, "wf-abc") {
		t.Error("expected false for empty payload (no workflow_id field)")
	}
}

// ============================================================================
// marshalJSON Tests
// ============================================================================

func TestMarshalJSON_ValidStruct(t *testing.T) {
	result := marshalJSON(domain.TaskCreated{TaskID: "t1", Title: "hello"})
	var decoded domain.TaskCreated
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if decoded.TaskID != "t1" {
		t.Errorf("expected TaskID='t1', got %q", decoded.TaskID)
	}
}

func TestMarshalJSON_Unmarshallable(t *testing.T) {
	// Sending a channel should cause json.Marshal to fail, returning "{}".
	ch := make(chan int)
	result := marshalJSON(ch)
	if string(result) != "{}" {
		t.Errorf("expected '{}' for unmarshallable type, got %s", string(result))
	}
}

// ── testWriter: io.Writer for test assertions ───────────

type testWriter struct {
	fn func([]byte) (int, error)
}

func (w *testWriter) Write(p []byte) (int, error) {
	return w.fn(p)
}
