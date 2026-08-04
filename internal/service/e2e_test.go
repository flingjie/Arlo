//go:build integration
// +build integration

package service_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const bugfixYAML = `
name: bugfix-e2e
version: 1
description: "E2E test workflow"

nodes:
  - id: hello
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-haiku-4-5
    retry:
      max_retries: 0

policy:
  max_concurrent_nodes: 1
`

// dial connects to the running arlod instance.
func dial(t *testing.T) (arlov1.ArloServiceClient, *grpc.ClientConn) {
	t.Helper()

	socket := os.Getenv("ARLO_SOCKET")
	if socket == "" {
		home, _ := os.UserHomeDir()
		socket = home + "/.arlo/arlo.sock"
	}

	conn, err := grpc.NewClient("unix://"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial arlod at %s: %v", socket, err)
	}
	return arlov1.NewArloServiceClient(conn), conn
}

// TestE2EBugfixSimple runs a one-node workflow end-to-end via gRPC.
// Requires: arlod running, claude CLI in PATH.
func TestE2EBugfixSimple(t *testing.T) {
	client, conn := dial(t)
	defer conn.Close()
	ctx := context.Background()

	// 1. Create task.
	resp, err := client.CreateTask(ctx, &arlov1.CreateTaskRequest{
		Title:          "e2e-test",
		WorkflowSource: bugfixYAML,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	t.Logf("Task created: %s / %s", resp.TaskId, resp.WorkflowId)
	wfID := resp.WorkflowId

	// 2. Subscribe to events (replay from position 0).
	stream, err := client.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   wfID,
		FromPosition: 0,
	})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}

	// 3. Collect events with timeout.
	var events []*arlov1.Event
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			evt, err := stream.Recv()
			if err != nil {
				t.Logf("stream ended: %v", err)
				return
			}
			events = append(events, evt)
			// Stop when workflow is done or node fails.
			if evt.Type == "TASK_COMPLETED" || evt.Type == "TASK_FAILED" {
				return
			}
		}
	}()

	select {
	case <-done:
		// finished naturally
	case <-time.After(5 * time.Minute):
		t.Fatal("timeout waiting for workflow completion")
	}

	// 4. Verify event sequence.
	t.Logf("Received %d events:", len(events))
	eventTypes := make([]string, 0, len(events))
	seen := map[string]bool{}
	for _, e := range events {
		if seen[e.EventId] {
			continue
		}
		seen[e.EventId] = true
		eventTypes = append(eventTypes, e.Type)
		t.Logf("  %s: %s (stream=%s)", e.Type, summary(e), e.StreamId)
	}

	// Expected: TASK_CREATED → WORKFLOW_CREATED → NODE_CREATED → NODE_STARTED → METRICS_SNAPSHOT → NODE_COMPLETED → TASK_COMPLETED
	assertContains(t, eventTypes, "TASK_CREATED")
	assertContains(t, eventTypes, "WORKFLOW_CREATED")
	assertContains(t, eventTypes, "NODE_CREATED")
	assertContains(t, eventTypes, "NODE_STARTED")
	assertContains(t, eventTypes, "TASK_COMPLETED")

	// No duplicates.
	assertNoDuplicates(t, events)

	// 5. Verify snapshot.
	snap, err := client.GetWorkflowSnapshot(ctx, &arlov1.GetWorkflowSnapshotRequest{WorkflowId: wfID})
	if err != nil {
		t.Fatalf("GetWorkflowSnapshot: %v", err)
	}
	if snap.Status != "COMPLETED" && snap.Status != "ACTIVE" {
		t.Errorf("unexpected status: %s", snap.Status)
	}
	t.Logf("Snapshot: status=%s nodes=%d", snap.Status, len(snap.Nodes))
}

// TestE2EBugfixWithGate tests the human_approval gate flow.
func TestE2EBugfixWithGate(t *testing.T) {
	const gateYAML = `
name: gate-test
version: 1
nodes:
  - id: step
    skill: root-cause
    runtime:
      provider: claude-code
      model: claude-haiku-4-5
    gate: human_approval
    retry:
      max_retries: 0
policy:
  max_concurrent_nodes: 1
`

	client, conn := dial(t)
	defer conn.Close()
	ctx := context.Background()

	resp, err := client.CreateTask(ctx, &arlov1.CreateTaskRequest{
		Title:          "gate-test",
		WorkflowSource: gateYAML,
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	wfID := resp.WorkflowId
	t.Logf("Gate test: %s", wfID)

	// Subscribe to events.
	stream, err := client.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   wfID,
		FromPosition: 0,
	})
	if err != nil {
		t.Fatalf("SubscribeEvents: %v", err)
	}

	// Collect events until node is WAITING.
	var events []*arlov1.Event
	nodeWaiting := make(chan struct{})
	go func() {
		defer close(nodeWaiting)
		for {
			evt, err := stream.Recv()
			if err != nil {
				t.Logf("stream ended: %v", err)
				return
			}
			events = append(events, evt)
			if evt.Type == "NODE_WAITING" {
				return
			}
			if evt.Type == "NODE_COMPLETED" || evt.Type == "TASK_COMPLETED" {
				return
			}
		}
	}()

	select {
	case <-nodeWaiting:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for NODE_WAITING")
	}

	// Verify node is WAITING, not RUNNING.
	assertContains(t, eventTypes(events), "NODE_WAITING")
	assertNotContains(t, eventTypes(events), "NODE_STARTED") // gated nodes go straight to WAITING
	t.Log("Node is WAITING — approving...")

	// Approve the gate.
	cmdResp, err := client.ExecuteCommand(ctx, &arlov1.CommandRequest{
		Command: "approve",
		Target:  "step",
	})
	if err != nil {
		t.Fatalf("ExecuteCommand: %v", err)
	}
	if !cmdResp.Success {
		t.Fatalf("approve failed: %s", cmdResp.Message)
	}
	t.Logf("Approved: %s", cmdResp.Message)

	// Resume event stream to catch the rest.
	stream2, err := client.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   wfID,
		FromPosition: 0,
	})
	if err != nil {
		t.Fatalf("re-SubscribeEvents: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			evt, err := stream2.Recv()
			if err != nil {
				return
			}
			events = append(events, evt)
			if evt.Type == "TASK_COMPLETED" || evt.Type == "TASK_FAILED" {
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		t.Fatal("timeout waiting for completion after approve")
	}

	types := eventTypes(events)
	assertContains(t, types, "HUMAN_INPUT_RECEIVED")
	assertContains(t, types, "NODE_STARTED") // only after approval
	assertContains(t, types, "TASK_COMPLETED")
	assertNoDuplicates(t, events)
	t.Logf("Gate test passed with %d events", len(events))
}

// ── helpers ──────────────────────────────────────

func eventTypes(events []*arlov1.Event) []string {
	seen := map[string]bool{}
	var types []string
	for _, e := range events {
		if seen[e.EventId] {
			continue
		}
		seen[e.EventId] = true
		types = append(types, e.Type)
	}
	return types
}

func summary(e *arlov1.Event) string {
	var s struct {
		NodeID string `json:"node_id"`
		Reason string `json:"reason"`
		Title  string `json:"title"`
	}
	json.Unmarshal(e.Payload, &s)
	switch {
	case s.NodeID != "":
		return s.NodeID
	case s.Reason != "":
		return s.Reason
	case s.Title != "":
		return s.Title
	default:
		return "-"
	}
}

func assertContains(t *testing.T, slice []string, want string) {
	t.Helper()
	for _, s := range slice {
		if s == want {
			return
		}
	}
	t.Errorf("expected event type %q not found in %v", want, slice)
}

func assertNotContains(t *testing.T, slice []string, want string) {
	t.Helper()
	for _, s := range slice {
		if s == want {
			t.Errorf("unexpected event type %q found in %v", want, slice)
		}
	}
}

func assertNoDuplicates(t *testing.T, events []*arlov1.Event) {
	t.Helper()
	seen := map[string]int{}
	for _, e := range events {
		seen[e.EventId]++
		if seen[e.EventId] > 1 {
			t.Errorf("duplicate event: %s (type=%s)", e.EventId, e.Type)
		}
	}
}
