package runtime

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
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
	exited bool // set under mu write lock after cmd.Wait()
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

	// Build args: pi --print --mode json --no-session --provider <provider> --model <model> <prompt>
	args := []string{
		"--print",
		"--mode", "json",
		"--no-session",
	}

	// Default to openai-codex provider (matches pi's defaultProvider setting).
	// Without --provider, pi cannot resolve API keys for model-only lookups.
	provider := "openai-codex"
	if inst.Config.Capabilities != nil {
		for _, cap := range inst.Config.Capabilities {
			if cap == "anthropic" || cap == "google" || cap == "deepseek" {
				provider = cap
				break
			}
		}
	}
	args = append(args, "--provider", provider)

	model := inst.Config.Model
	if model == "" {
		model = "deepseek-v4-pro" // default model
	}
	args = append(args, "--model", model)

	// Append the prompt as the final positional argument.
	args = append(args, inst.Prompt)

	cmd := exec.Command("pi", args...)

	// Propagate local proxy environment for API access.
	// When ANTHROPIC_BASE_URL points to a local proxy (e.g., http://127.0.0.1:15721),
	// set the OpenAI-compatible env vars so Pi's openai-codex provider can use it.
	env := os.Environ()
	if baseURL := os.Getenv("ANTHROPIC_BASE_URL"); baseURL != "" {
		env = append(env, "OPENAI_BASE_URL="+baseURL)
	}
	if authToken := os.Getenv("ANTHROPIC_AUTH_TOKEN"); authToken != "" {
		env = append(env, "OPENAI_API_KEY="+authToken)
	}
	cmd.Env = env

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

	// Open a log file so users can tail the agent's output in real-time.
	logDir := filepath.Join(os.Getenv("HOME"), ".arlo", "runtime")
	os.MkdirAll(logDir, 0755)
	logPath := filepath.Join(logDir, inst.ID+".log")
	logFile, logErr := os.Create(logPath)
	if logErr != nil {
		slog.Warn("failed to create runtime log", "path", logPath, "error", logErr)
	}

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

			// Also write to the runtime log file for real-time visibility.
			if logFile != nil {
				logFile.Write(line)
				logFile.Write([]byte{'\n'})
			}

			if event, ok := ParsePiJSON(line); ok {
				// Pi's message_end events carry cumulative usage (replace, not sum).
				// The last message_end for the assistant has the authoritative session total.
				if event.Type == "message_end" && (event.TokensIn > 0 || event.TokensOut > 0) {
					cum.tokensIn = event.TokensIn
					cum.tokensOut = event.TokensOut
				}
				if event.ToolName != "" {
					cum.toolCalls++

					// Emit real-time RuntimeEvent for tool calls.
					if a.mgr != nil {
						a.mgr.ReportEvent(inst.ID, RuntimeEvent{
							Type:      RuntimeEventToolCall,
							RuntimeID: inst.ID,
							Action:    event.ToolName,
							ToolName:  event.ToolName,
							Timestamp: time.Now(),
						})
					}
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

		// Mark the instance as exited under the lock.
		a.mu.Lock()
		if tracked, ok := a.instances[inst.ID]; ok {
			tracked.exited = true
		}
		a.mu.Unlock()

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

// Snapshot returns the current observable state of the runtime process.
func (a *PiAdapter) Snapshot(ctx context.Context, id string) (domain.RuntimeSnapshot, error) {
	a.mu.RLock()
	pi, ok := a.instances[id]
	if !ok {
		a.mu.RUnlock()
		return domain.RuntimeSnapshot{State: domain.RuntimeStateExited}, nil
	}

	state := domain.RuntimeStateRunning
	if pi.exited {
		state = domain.RuntimeStateExited
	}

	// Include stdout buffer summary.
	var lastMsg string
	if pi.stdout != nil {
		out := pi.stdout.String()
		if len(out) > 500 {
			out = out[len(out)-500:]
		}
		lastMsg = out
	}
	a.mu.RUnlock()

	return domain.RuntimeSnapshot{
		State:       state,
		LastMessage: lastMsg,
	}, nil
}

// Output returns the captured stdout of the pi process.
// Used by the reconciler to save skill output files after node completion.
func (a *PiAdapter) Output(ctx context.Context, id string) ([]byte, error) {
	a.mu.RLock()
	pi, ok := a.instances[id]
	a.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("instance not found: %s", id)
	}
	if pi.stdout == nil {
		return nil, nil
	}
	return pi.stdout.Bytes(), nil
}
