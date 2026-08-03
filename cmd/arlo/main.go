// arlo is the CLI client for arlod — the AgentOS Control Plane.
//
// Usage:
//
//	arlo run <workflow.yaml>    Create and start a new task
//	arlo status                  List all active tasks
//	arlo attach <session_id>     Stream PTY output from a session
//	arlo artifacts <task_id>     List artifacts for a task
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/lingjiefan/arlo/internal/tui"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var socketPath string

func init() {
	home, _ := os.UserHomeDir()
	defaultSocket := filepath.Join(home, ".arlo", "arlo.sock")

	flag.StringVar(&socketPath, "socket", defaultSocket, "Unix socket path for arlod")
}

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "run":
		run(cmdArgs)
	case "status":
		status(cmdArgs)
	case "attach":
		attach(cmdArgs)
	case "artifacts":
		artifacts(cmdArgs)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`arlo — AgentOS CLI

Usage:
  arlo run <workflow.yaml>     Create and start a new task
  arlo status                   List all active tasks
  arlo attach <session_id>      Stream PTY output from a session
  arlo artifacts <task_id>      List artifacts for a task

Flags:
  -socket string   Unix socket path for arlod (default: ~/.arlo/arlo.sock)`)
}

func dial() (*grpc.ClientConn, arlov1.ArloServiceClient, context.Context) {
	ctx := context.Background()

	conn, err := grpc.NewClient(
		"unix://"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect to arlod: %v", err)
	}

	client := arlov1.NewArloServiceClient(conn)
	return conn, client, ctx
}

func mustCreateDir() {
	home, _ := os.UserHomeDir()
	os.MkdirAll(filepath.Join(home, ".arlo"), 0700)
}

// ── run ──────────────────────────────────────────

func run(args []string) {
	if len(args) < 1 {
		log.Fatal("usage: arlo run <workflow.yaml>")
	}

	mustCreateDir()

	yamlPath := args[0]
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		log.Fatalf("failed to read workflow file: %v", err)
	}

	conn, client, ctx := dial()
	defer conn.Close()

	resp, err := client.CreateTask(ctx, &arlov1.CreateTaskRequest{
		Title:          yamlPath,
		WorkflowSource: string(data),
	})
	if err != nil {
		log.Fatalf("create task: %v", err)
	}

	fmt.Printf("Task created: %s\n", resp.TaskId)
	fmt.Printf("Workflow:    %s\n", resp.WorkflowId)
	fmt.Println("Launching TUI...")

	// Close the CLI's gRPC connection — TUI creates its own.
	conn.Close()

	time.Sleep(500 * time.Millisecond)

	if err := tui.Run(socketPath, resp.WorkflowId); err != nil {
		log.Fatalf("tui: %v", err)
	}
}

// ── status ───────────────────────────────────────

func status(args []string) {
	conn, client, ctx := dial()
	defer conn.Close()

	resp, err := client.ListTasks(ctx, &arlov1.ListTasksRequest{})
	if err != nil {
		log.Fatalf("list tasks: %v", err)
	}

	if len(resp.Tasks) == 0 {
		fmt.Println("No active tasks.")
		return
	}

	fmt.Printf("%-36s  %-12s\n", "WORKFLOW", "STATUS")
	for _, t := range resp.Tasks {
		fmt.Printf("%-36s  %-12s\n", t.WorkflowId, t.Status)
	}
}

// ── attach ───────────────────────────────────────

func attach(args []string) {
	if len(args) < 1 {
		log.Fatal("usage: arlo attach <session_id>")
	}

	conn, client, ctx := dial()
	defer conn.Close()

	stream, err := client.AttachPTY(ctx, &arlov1.AttachPTYRequest{
		SessionId: args[0],
	})
	if err != nil {
		log.Fatalf("attach: %v", err)
	}

	fmt.Printf("Attached to session %s (Ctrl+C to detach)\n", args[0])
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("PTY stream: %v", err)
		}
		fmt.Print(string(frame.Data))
	}
}

// ── artifacts ────────────────────────────────────

func artifacts(args []string) {
	if len(args) < 1 {
		log.Fatal("usage: arlo artifacts <task_id>")
	}

	conn, client, ctx := dial()
	defer conn.Close()

	resp, err := client.GetWorkflow(ctx, &arlov1.GetWorkflowRequest{
		WorkflowId: "wf-" + args[0],
	})
	if err != nil {
		log.Fatalf("get workflow: %v", err)
	}

	fmt.Printf("Workflow: %s (%s)\n", resp.WorkflowId, resp.Status)
	fmt.Println()
	fmt.Printf("%-20s  %-12s  %s\n", "NODE", "STATUS", "SESSION")
	for _, n := range resp.Nodes {
		fmt.Printf("%-20s  %-12s  %s\n", n.NodeId, n.Status, n.SessionId)
	}
}
