// Package service implements the gRPC service handlers for arlod.
// This is a thin layer that delegates to the control plane components.
package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/reconciler"
	"github.com/lingjiefan/arlo/internal/runtime"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"github.com/lingjiefan/arlo/internal/workflow"
	"github.com/lingjiefan/arlo/internal/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ArloService implements the gRPC ArloServiceServer interface.
type ArloService struct {
	arlov1.UnimplementedArloServiceServer

	eventStore   store.EventStore
	stateStore   state.StateStore
	engine       *workflow.Engine
	reconciler   *reconciler.Reconciler
	runtimeMgr   *runtime.Manager
	workspaceMgr *workspace.Manager
}

// New creates a new ArloService.
func New(
	es store.EventStore,
	ss state.StateStore,
	eng *workflow.Engine,
	rec *reconciler.Reconciler,
	rm *runtime.Manager,
	wm *workspace.Manager,
) *ArloService {
	return &ArloService{
		eventStore:   es,
		stateStore:   ss,
		engine:       eng,
		reconciler:   rec,
		runtimeMgr:   rm,
		workspaceMgr: wm,
	}
}

// ── Task ─────────────────────────────────────────

// CreateTask compiles a workflow, seeds events, and triggers reconciliation.
func (s *ArloService) CreateTask(ctx context.Context, req *arlov1.CreateTaskRequest) (*arlov1.CreateTaskResponse, error) {
	taskID := fmt.Sprintf("task-%d", time.Now().UnixNano())
	wfID := "wf-" + taskID

	// Compile the workflow YAML.
	graph, err := s.engine.Compile(ctx, []byte(req.WorkflowSource))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "compile workflow: %v", err)
	}
	if err := s.engine.Validate(ctx, graph); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "validate workflow: %v", err)
	}
	graph.ID = wfID

	// Seed initial events.
	if _, err := s.eventStore.Append(ctx, "workflow-"+wfID, []store.Event{
		{
			ID:   fmt.Sprintf("evt-task-%s", taskID),
			Type: store.EventTaskCreated,
			Payload: marshalJSON(domain.TaskCreated{
				TaskID: taskID, Title: req.Title, Description: req.Description,
				CreatedBy: "cli", WorkflowID: wfID,
			}),
		},
		{
			ID:   fmt.Sprintf("evt-wf-%s", wfID),
			Type: store.EventWorkflowCreated,
			Payload: marshalJSON(domain.WorkflowCreated{
				WorkflowID: wfID, TaskID: taskID, GraphName: graph.Name, Version: graph.Version,
			}),
		},
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "seed events: %v", err)
	}

	// Create node events.
	for _, n := range graph.Nodes {
		if _, err := s.eventStore.Append(ctx, "node-"+n.ID, []store.Event{{
			ID:   fmt.Sprintf("evt-node-%s-%s", n.ID, wfID),
			Type: store.EventNodeCreated,
			Payload: marshalJSON(domain.NodeCreated{
				NodeID:     n.ID,
				WorkflowID: wfID,
				SkillName:  n.SkillRef.Name,
				Runtime:    n.Runtime.Provider,
				DependsOn:  n.DependsOn,
				Gate:       string(n.Gate),
			}),
		}}); err != nil {
			return nil, status.Errorf(codes.Internal, "seed node event: %v", err)
		}
	}

	// Rebuild projections to see the new events.
	if err := s.stateStore.Rebuild(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "rebuild projections: %v", err)
	}

	// Register graph with the Reconciler and reconcile immediately.
	s.reconciler.Submit(wfID, graph)
	if err := s.reconciler.Reconcile(ctx, wfID); err != nil {
		slog.Warn("initial reconcile failed", "workflow", wfID, "error", err)
	}

	slog.Info("task created", "task_id", taskID, "workflow", wfID, "title", req.Title)
	return &arlov1.CreateTaskResponse{
		TaskId:     taskID,
		WorkflowId: wfID,
	}, nil
}

// GetTask returns a task summary.
func (s *ArloService) GetTask(ctx context.Context, req *arlov1.GetTaskRequest) (*arlov1.GetTaskResponse, error) {
	wf, err := s.stateStore.GetWorkflow(ctx, "wf-"+req.TaskId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "task not found: %v", err)
	}
	return &arlov1.GetTaskResponse{
		TaskId:     req.TaskId,
		Status:     string(wf.Status),
		WorkflowId: wf.ID,
	}, nil
}

// ListTasks returns all tasks.
func (s *ArloService) ListTasks(ctx context.Context, req *arlov1.ListTasksRequest) (*arlov1.ListTasksResponse, error) {
	workflows, err := s.stateStore.ListActiveWorkflows(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflows: %v", err)
	}
	var tasks []*arlov1.TaskSummary
	for _, wf := range workflows {
		tasks = append(tasks, &arlov1.TaskSummary{
			WorkflowId: wf.ID,
			Status:     string(wf.Status),
		})
	}
	return &arlov1.ListTasksResponse{Tasks: tasks}, nil
}

// ── Workflow ─────────────────────────────────────

// GetWorkflow returns the current state of a workflow.
func (s *ArloService) GetWorkflow(ctx context.Context, req *arlov1.GetWorkflowRequest) (*arlov1.GetWorkflowResponse, error) {
	wf, err := s.stateStore.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}

	var nodes []*arlov1.NodeState
	for _, ns := range wf.Nodes {
		nodes = append(nodes, &arlov1.NodeState{
			NodeId:     ns.NodeID,
			Status:     string(ns.Status),
			SessionId:  ns.SessionID,
			RuntimeId:  ns.RuntimeID,
			RetryCount: int32(ns.RetryCount),
			DependsOn:  ns.DependsOn,
			Children:   ns.Children,
			Gate:       ns.Gate,
		})
	}

	return &arlov1.GetWorkflowResponse{
		WorkflowId: wf.ID,
		Status:     string(wf.Status),
		Nodes:      nodes,
	}, nil
}

// GetWorkflowSnapshot returns the current workflow state plus a monotonic version
// for gap detection after stream reconnect.
func (s *ArloService) GetWorkflowSnapshot(ctx context.Context, req *arlov1.GetWorkflowSnapshotRequest) (*arlov1.GetWorkflowSnapshotResponse, error) {
	wf, err := s.stateStore.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}

	var nodes []*arlov1.NodeState
	for _, ns := range wf.Nodes {
		nodes = append(nodes, &arlov1.NodeState{
			NodeId:     ns.NodeID,
			Status:     string(ns.Status),
			SessionId:  ns.SessionID,
			RuntimeId:  ns.RuntimeID,
			RetryCount: int32(ns.RetryCount),
			DependsOn:  ns.DependsOn,
			Children:   ns.Children,
			Gate:       ns.Gate,
		})
	}

	version := s.eventStore.LastPosition()

	return &arlov1.GetWorkflowSnapshotResponse{
		WorkflowId: wf.ID,
		Status:     string(wf.Status),
		Version:    uint64(version),
		Nodes:      nodes,
		StartedAt:  wf.CreatedAt.Format(time.RFC3339),
	}, nil
}

// ── Session ──────────────────────────────────────

// GetSession returns session details.
func (s *ArloService) GetSession(ctx context.Context, req *arlov1.GetSessionRequest) (*arlov1.GetSessionResponse, error) {
	// In v0.1, sessions are tracked via node state.
	// Walk all workflows to find the session.
	workflows, err := s.stateStore.ListActiveWorkflows(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list workflows: %v", err)
	}
	for _, wf := range workflows {
		for _, ns := range wf.Nodes {
			if ns.SessionID == req.SessionId {
				return &arlov1.GetSessionResponse{
					SessionId:  ns.SessionID,
					NodeId:     ns.NodeID,
					WorkflowId: wf.ID,
					Status:     string(ns.Status),
				}, nil
			}
		}
	}
	return nil, status.Errorf(codes.NotFound, "session not found: %s", req.SessionId)
}

// ListSessions lists sessions for a workflow.
func (s *ArloService) ListSessions(ctx context.Context, req *arlov1.ListSessionsRequest) (*arlov1.ListSessionsResponse, error) {
	wf, err := s.stateStore.GetWorkflow(ctx, req.WorkflowId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "workflow not found: %v", err)
	}
	var sessions []*arlov1.SessionSummary
	for _, ns := range wf.Nodes {
		if ns.SessionID != "" {
			sessions = append(sessions, &arlov1.SessionSummary{
				SessionId: ns.SessionID,
				NodeId:    ns.NodeID,
				Status:    string(ns.Status),
			})
		}
	}
	return &arlov1.ListSessionsResponse{Sessions: sessions}, nil
}

// ── Event Stream ─────────────────────────────────

// SubscribeEvents streams events to the client in real-time.
func (s *ArloService) SubscribeEvents(req *arlov1.SubscribeEventsRequest, stream grpc.ServerStreamingServer[arlov1.Event]) error {
	ctx := stream.Context()

	// Start subscription from the given position or from current end.
	ch, err := s.eventStore.Subscribe(ctx, req.FromPosition)
	if err != nil {
		return status.Errorf(codes.Internal, "subscribe: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-ch:
			if !ok {
				return nil
			}
			if req.WorkflowId != "" && event.StreamID != "workflow-"+req.WorkflowId && !strings.HasPrefix(event.StreamID, "node-") {
				continue // filter by workflow
			}
			if err := stream.Send(&arlov1.Event{
				EventId:   event.ID,
				StreamId:  event.StreamID,
				Version:   int32(event.Version),
				Position:  event.Position,
				Type:      string(event.Type),
				Payload:   event.Payload,
				Timestamp: event.Timestamp.Format(time.RFC3339),
			}); err != nil {
				return err
			}
		}
	}
}

// ── PTY ──────────────────────────────────────────

// AttachPTY streams PTY output to the client.
func (s *ArloService) AttachPTY(req *arlov1.AttachPTYRequest, stream grpc.ServerStreamingServer[arlov1.PTYFrame]) error {
	ctx := stream.Context()

	frames, _, err := s.runtimeMgr.AttachInstance(ctx, req.SessionId)
	if err != nil {
		return status.Errorf(codes.Unavailable, "attach PTY: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case frame, ok := <-frames:
			if !ok {
				return nil
			}
			if err := stream.Send(&arlov1.PTYFrame{
				SessionId: frame.SessionID,
				Data:      frame.Data,
				Seq:       frame.Seq,
			}); err != nil {
				return err
			}
		}
	}
}

// SendPTYInput sends input to a PTY session.
func (s *ArloService) SendPTYInput(ctx context.Context, req *arlov1.SendPTYInputRequest) (*arlov1.SendPTYInputResponse, error) {
	_, w, err := s.runtimeMgr.AttachInstance(ctx, req.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "attach PTY: %v", err)
	}
	if w != nil {
		w.Write(req.Data)
	}
	return &arlov1.SendPTYInputResponse{}, nil
}

// ── Command ──────────────────────────────────────

// ExecuteCommand handles human-in-loop commands.
func (s *ArloService) ExecuteCommand(ctx context.Context, req *arlov1.CommandRequest) (*arlov1.CommandResponse, error) {
	switch req.Command {
	case "cancel_task":
		// Append cancellation event and reconcile.
		wfID := req.Target
		_, err := s.eventStore.Append(ctx, "workflow-"+wfID, []store.Event{{
			ID:   fmt.Sprintf("evt-cancel-%d", time.Now().UnixNano()),
			Type: store.EventTaskCancelled,
			Payload: marshalJSON(domain.TaskCompleted{
				TaskID: wfID,
			}),
		}})
		if err != nil {
			return &arlov1.CommandResponse{Success: false, Message: err.Error()}, nil
		}
		s.stateStore.Rebuild(ctx)
		return &arlov1.CommandResponse{Success: true, Message: "task cancelled"}, nil

	case "approve", "reject":
		// Record human input for a waiting node.
		nodeID := req.Target

		// Look up the node to find its workflow and session.
		ns, nsErr := s.stateStore.GetNodeState(ctx, nodeID)
		if nsErr != nil {
			return &arlov1.CommandResponse{Success: false, Message: fmt.Sprintf("node %s not found: %v", nodeID, nsErr)}, nil
		}

		_, gerr := s.eventStore.Append(ctx, "node-"+nodeID, []store.Event{{
			ID:   fmt.Sprintf("evt-human-%d", time.Now().UnixNano()),
			Type: store.EventHumanInputReceived,
			Payload: marshalJSON(domain.HumanInputReceived{
				NodeID:     nodeID,
				WorkflowID: ns.WorkflowID,
				SessionID:  ns.SessionID,
				Decision:   req.Command,
				Input:      req.Input,
			}),
		}})
		if gerr != nil {
			return &arlov1.CommandResponse{Success: false, Message: gerr.Error()}, nil
		}
		s.stateStore.Rebuild(ctx)
		return &arlov1.CommandResponse{Success: true, Message: fmt.Sprintf("node %s %sd", nodeID, req.Command)}, nil

	default:
		return &arlov1.CommandResponse{Success: false, Message: "unknown command: " + req.Command}, nil
	}
}

// ── Helpers ──────────────────────────────────────

func marshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		slog.Error("marshalJSON: failed to marshal payload", "error", err, "type", fmt.Sprintf("%T", v))
		// Return an empty JSON object rather than nil to prevent event store corruption.
		return []byte("{}")
	}
	return data
}

// Ensure unused imports don't cause errors.
var _ = io.Discard
var _ = emptypb.Empty{}
