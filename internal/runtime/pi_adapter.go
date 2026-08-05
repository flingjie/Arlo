package runtime

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
)

// PiAdapter manages a Pi coding agent CLI process.
// It spawns `pi` with --mode json --print --no-session and monitors
// the process lifecycle.
//
// Key differences from ClaudeAdapter:
//   - Prompt is passed as a CLI argument (not via stdin)
//   - Output format flag is --mode json (not --output-format stream-json)
//   - Provider and model are set via --provider / --model flags
type PiAdapter struct {
	instances map[string]*piInstance
	mu        sync.RWMutex
	mgr       *Manager // set after construction for exit notification
}

// piInstance tracks a running Pi process.
type piInstance struct {
	inst   domain.RuntimeInstance
	cmd    *exec.Cmd
	stdout *bytes.Buffer
	stderr *bytes.Buffer
}

// NewPiAdapter creates a new Pi adapter.
func NewPiAdapter() *PiAdapter {
	return &PiAdapter{
		instances: make(map[string]*piInstance),
	}
}

// SetManager wires the adapter to the Manager for exit notification.
func (a *PiAdapter) SetManager(mgr *Manager) {
	a.mgr = mgr
}

// Prepare validates that the pi CLI is available.
func (a *PiAdapter) Prepare(ctx context.Context, inst domain.RuntimeInstance) error {
	_, err := exec.LookPath("pi")
	if err != nil {
		return fmt.Errorf("pi CLI not found in PATH: %w", err)
	}
	return nil
}

// Start launches a Pi process with the given prompt.
// The prompt is passed as a CLI argument (unlike Claude which uses stdin).
func (a *PiAdapter) Start(ctx context.Context, inst domain.RuntimeInstance) error {
	_, err := exec.LookPath("pi")
	if err != nil {
		return fmt.Errorf("pi CLI not found: %w", err)
	}

	// Build args: pi --print --mode json --no-session [--provider X] [--model Y] <prompt>
	args := []string{
		"--print",
		"--mode", "json",
		"--no-session",
	}

	provider := string(inst.Config.Model) // default via config
	if inst.Config.PermissionMode != "" {
		provider = inst.Config.PermissionMode
	}
	_ = provider // provider is resolved from config, defaulting to env

	// Use provider from runtime config if specified (via model field as convention).
	// The reconciler sets Config.Model to the node's runtime.model,
	// and the node's runtime.provider is already encoded in the RuntimeRef.
	// For Pi, we extract model from Config.
	model := inst.Config.Model
	if model == "" {
		model = "deepseek-v4-pro" // default model
	}
	args = append(args, "--model", model)

	// Append the prompt as the final positional argument.
	args = append(args, inst.Prompt)

	cmd := exec.Command("pi", args...)

	// Set working directory.
	if inst.WorkDir != "" {
		cmd.Dir = inst.WorkDir
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
		return fmt.Errorf("start pi: %w", err)
	}

	stdoutBuf := &bytes.Buffer{}
	stderrBuf := &bytes.Buffer{}

	pi := &piInstance{
		inst:   inst,
		cmd:    cmd,
		stdout: stdoutBuf,
		stderr: stderrBuf,
	}

	a.mu.Lock()
	a.instances[inst.ID] = pi
	a.mu.Unlock()

	// Monitor process exit in background.
	go func() {
		start := time.Now()
		var cum struct {
			tokensIn, tokensOut int64
			toolCalls           int
		}

		// Drain stderr concurrently to avoid pipe-buffer deadlock.
		stderrDone := make(chan struct{})
		go func() {
			io.Copy(stderrBuf, stderr)
			close(stderrDone)
		}()

		scanner := bufio.NewScanner(stdout)
		// Allow large JSON lines (default 64KB is too small).
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			stdoutBuf.Write(line)
			stdoutBuf.WriteByte('\n')

			if event, ok := ParseStreamJSON(line); ok {
				// result.modelUsage is the authoritative session total; replace
				// rather than sum so we don't double-count.
				if event.Type == "result" && (event.TokensIn > 0 || event.TokensOut > 0) {
					cum.tokensIn = event.TokensIn
					cum.tokensOut = event.TokensOut
				} else {
					cum.tokensIn += event.TokensIn
					cum.tokensOut += event.TokensOut
				}
				if event.ToolName != "" {
					cum.toolCalls++
				}
			}
		}
		<-stderrDone

		err := cmd.Wait()

		exitCode := 0
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				exitCode = ee.ExitCode()
			} else {
				exitCode = 1
			}
		}

		// Notify the Manager so the reconciler can detect the exit.
		if a.mgr != nil {
			a.mgr.MarkExited(inst.ID, exitCode, domain.RuntimeMetrics{
				TokensIn:   cum.tokensIn,
				TokensOut:  cum.tokensOut,
				ToolCalls:  cum.toolCalls,
				DurationMs: time.Since(start).Milliseconds(),
			})
		}
	}()

	return nil
}

// Stop gracefully terminates the Pi process.
func (a *PiAdapter) Stop(ctx context.Context, id string) error {
	a.mu.RLock()
	pi, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return fmt.Errorf("instance not found: %s", id)
	}

	if pi.cmd.Process != nil {
		return pi.cmd.Process.Signal(os.Interrupt)
	}
	return nil
}

// Destroy forcefully kills and cleans up the Pi process.
func (a *PiAdapter) Destroy(ctx context.Context, id string) error {
	a.mu.RLock()
	pi, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return nil // already cleaned up
	}

	if pi.cmd.Process != nil {
		pi.cmd.Process.Kill()
	}

	a.mu.Lock()
	delete(a.instances, id)
	a.mu.Unlock()

	return nil
}

// SendInstruction routes a control message to the Pi process.
// In v0.1, instructions are accepted but not routed to the process.
func (a *PiAdapter) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	a.mu.RLock()
	_, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return fmt.Errorf("instance not found: %s", id)
	}

	// In v0.1, instructions are accepted but not routed.
	// Future: write to stdin or control socket.
	return nil
}

// Status returns the current status of the Pi process.
func (a *PiAdapter) Status(ctx context.Context, id string) (domain.RuntimeStatus, error) {
	a.mu.RLock()
	pi, ok := a.instances[id]
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

	if pi.cmd.ProcessState != nil && pi.cmd.ProcessState.Exited() {
		status.State = domain.RuntimeStateExited
	}

	return status, nil
}
