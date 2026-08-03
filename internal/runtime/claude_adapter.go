package runtime

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"

	"github.com/lingjiefan/arlo/internal/domain"
)

// ClaudeAdapter manages a Claude Code CLI process.
// It spawns `claude` inside a PTY and monitors the process lifecycle.
type ClaudeAdapter struct {
	instances map[string]*claudeInstance
	mu        sync.RWMutex
}

// claudeInstance tracks a running Claude process.
type claudeInstance struct {
	inst domain.RuntimeInstance
	cmd  *exec.Cmd
	pty  *os.File // PTY master
}

// NewClaudeAdapter creates a new Claude Code adapter.
func NewClaudeAdapter() *ClaudeAdapter {
	return &ClaudeAdapter{
		instances: make(map[string]*claudeInstance),
	}
}

// Prepare validates that the Claude CLI is available.
func (a *ClaudeAdapter) Prepare(ctx context.Context, inst domain.RuntimeInstance) error {
	_, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("claude CLI not found in PATH: %w", err)
	}
	return nil
}

// Start launches a Claude Code process with the given prompt.
func (a *ClaudeAdapter) Start(ctx context.Context, inst domain.RuntimeInstance) error {
	_, err := exec.LookPath("claude")
	if err != nil {
		return fmt.Errorf("claude CLI not found: %w", err)
	}

	cmd := exec.CommandContext(ctx, "claude",
		"--print",                    // non-interactive output
		"--output-format", "stream-json",
	)

	// Set working directory.
	if inst.WorkDir != "" {
		cmd.Dir = inst.WorkDir
	}

	// Pass the prompt via stdin.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}

	// Capture stdout/stderr for event parsing.
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start claude: %w", err)
	}

	// Write the prompt to stdin and close it.
	go func() {
		defer stdin.Close()
		io.WriteString(stdin, inst.Prompt)
	}()

	ci := &claudeInstance{
		inst: inst,
		cmd:  cmd,
	}

	a.mu.Lock()
	a.instances[inst.ID] = ci
	a.mu.Unlock()

	// Monitor process exit in background.
	go func() {
		// Drain stdout/stderr.
		go bufio.NewReader(stdout).WriteTo(io.Discard)
		go bufio.NewReader(stderr).WriteTo(io.Discard)

		cmd.Wait()

		a.mu.Lock()
		delete(a.instances, inst.ID)
		a.mu.Unlock()
	}()

	return nil
}

// Stop gracefully terminates the Claude process.
func (a *ClaudeAdapter) Stop(ctx context.Context, id string) error {
	a.mu.RLock()
	ci, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return fmt.Errorf("instance not found: %s", id)
	}

	if ci.cmd.Process != nil {
		return ci.cmd.Process.Signal(os.Interrupt)
	}
	return nil
}

// Destroy forcefully kills and cleans up the Claude process.
func (a *ClaudeAdapter) Destroy(ctx context.Context, id string) error {
	a.mu.RLock()
	ci, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return nil // already cleaned up
	}

	if ci.cmd.Process != nil {
		ci.cmd.Process.Kill()
	}
	if ci.pty != nil {
		ci.pty.Close()
	}

	a.mu.Lock()
	delete(a.instances, id)
	a.mu.Unlock()

	return nil
}

// SendInstruction writes a control message to the Claude process stdin.
func (a *ClaudeAdapter) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	a.mu.RLock()
	_, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return fmt.Errorf("instance not found: %s", id)
	}

	// In v0.1, instructions are logged but not routed to the process.
	// Future: write to stdin or control socket.
	return nil
}

// Status returns the current status of the Claude process.
func (a *ClaudeAdapter) Status(ctx context.Context, id string) (domain.RuntimeStatus, error) {
	a.mu.RLock()
	ci, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return domain.RuntimeStatus{
			ID:    id,
			State: domain.RuntimeStateExited,
		}, nil
	}

	status := domain.RuntimeStatus{
		ID:    id,
		State: domain.RuntimeStateRunning,
	}

	if ci.cmd.ProcessState != nil && ci.cmd.ProcessState.Exited() {
		status.State = domain.RuntimeStateExited
	}

	return status, nil
}
