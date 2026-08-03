package tui

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	tea "github.com/charmbracelet/bubbletea"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client wraps the gRPC connection to arlod.
type Client struct {
	socket string
	conn   *grpc.ClientConn
	api    arlov1.ArloServiceClient

	// Event stream — one persistent gRPC stream per client lifetime.
	streamOnce sync.Once
	eventCh    chan tea.Msg
}

// NewClient creates a new gRPC client for the given Unix socket.
func NewClient(socket string) *Client {
	return &Client{socket: socket}
}

// Connect establishes the gRPC connection.
func (c *Client) Connect() error {
	conn, err := grpc.NewClient(
		"unix://"+c.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	c.conn = conn
	c.api = arlov1.NewArloServiceClient(conn)
	return nil
}

// Close shuts down the gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// GetSnapshot fetches the current workflow snapshot.
func (c *Client) GetSnapshot(workflowID string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := c.api.GetWorkflowSnapshot(ctx, &arlov1.GetWorkflowSnapshotRequest{
			WorkflowId: workflowID,
		})
		if err != nil {
			return snapshotMsg{err: err}
		}
		return snapshotMsg{
			workflowID: resp.WorkflowId,
			status:     resp.Status,
			version:    resp.Version,
			nodes:      resp.Nodes,
			startedAt:  resp.StartedAt,
		}
	}
}

type snapshotMsg struct {
	workflowID string
	status     string
	version    uint64
	nodes      []*arlov1.NodeState
	startedAt  string
	err        error
}

// StartEventStream opens a single persistent gRPC event stream and starts
// a background goroutine that feeds events into a channel. Event replay
// from position 0 is handled server-side, so the TUI sees all historical
// events followed by live ones. Call once; subsequent calls are no-ops.
func (c *Client) StartEventStream(workflowID string) tea.Cmd {
	return func() tea.Msg {
		c.streamOnce.Do(func() {
			c.eventCh = make(chan tea.Msg, 256)
			go c.runEventStream(workflowID)
		})
		return streamReadyMsg{}
	}
}

func (c *Client) runEventStream(workflowID string) {
	defer close(c.eventCh)

	ctx := context.Background()
	stream, err := c.api.SubscribeEvents(ctx, &arlov1.SubscribeEventsRequest{
		WorkflowId:   workflowID,
		FromPosition: 0,
	})
	if err != nil {
		c.eventCh <- streamErrMsg{err: fmt.Errorf("subscribe: %w", err)}
		return
	}

	for {
		event, err := stream.Recv()
		if err == io.EOF {
			c.eventCh <- streamEndMsg{}
			return
		}
		if err != nil {
			c.eventCh <- streamErrMsg{err: fmt.Errorf("stream recv: %w", err)}
			return
		}
		c.eventCh <- eventMsg{event: event}
	}
}

// RecvEvent returns a command that blocks until the next event arrives
// from the persistent event stream. This avoids opening a new gRPC stream
// per event.
func (c *Client) RecvEvent() tea.Cmd {
	return func() tea.Msg {
		msg, ok := <-c.eventCh
		if !ok {
			return streamEndMsg{}
		}
		return msg
	}
}

type streamReadyMsg struct{}

// ExecuteCommand sends a human-in-the-loop command (approve/reject/cancel_task).
func (c *Client) ExecuteCommand(command, target, input string) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := c.api.ExecuteCommand(ctx, &arlov1.CommandRequest{
			Command: command,
			Target:  target,
			Input:   input,
		})
		if err != nil {
			return commandResultMsg{err: err}
		}
		return commandResultMsg{
			success: resp.Success,
			message: resp.Message,
		}
	}
}

type commandResultMsg struct {
	success bool
	message string
	err     error
}

type eventMsg struct {
	event *arlov1.Event
}

type streamErrMsg struct {
	err error
}

type streamEndMsg struct{}
