package workspace

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
)

// TmuxProvider creates workspaces as tmux sessions and slots as tmux windows.
type TmuxProvider struct {
	socketName   string                     // tmux socket name, e.g. "arlo"
	sessions     map[string]*tmuxSession    // workspaceID → session
	PollInterval time.Duration              // interval between pane content polls
	mu           sync.RWMutex
}

type tmuxSession struct {
	name    string
	windows map[string]*tmuxWindow // slotID → window
}

type tmuxWindow struct {
	name   string
	index  int
	slotID string
}

// NewTmuxProvider creates a new TmuxProvider.
func NewTmuxProvider(socketName string) *TmuxProvider {
	return &TmuxProvider{
		socketName:   socketName,
		sessions:     make(map[string]*tmuxSession),
		PollInterval: 100 * time.Millisecond,
	}
}

func (p *TmuxProvider) tmux(args ...string) *exec.Cmd {
	cmdArgs := []string{"-L", p.socketName}
	cmdArgs = append(cmdArgs, args...)
	return exec.Command("tmux", cmdArgs...)
}

func (p *TmuxProvider) tmuxOutput(args ...string) (string, error) {
	cmd := p.tmux(args...)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("tmux %v: %w (stderr: %s)", args, err, string(out))
	}
	return strings.TrimSpace(string(out)), nil
}

// Create creates a new detached tmux session.
func (p *TmuxProvider) Create(ctx context.Context, spec domain.WorkspaceSpec) (*domain.Workspace, error) {
	sessionName := spec.Name
	if _, err := p.tmuxOutput("new-session", "-d", "-s", sessionName); err != nil {
		return nil, fmt.Errorf("create tmux session %s: %w", sessionName, err)
	}

	p.mu.Lock()
	p.sessions[spec.Name] = &tmuxSession{
		name:    sessionName,
		windows: make(map[string]*tmuxWindow),
	}
	p.mu.Unlock()

	return &domain.Workspace{
		ID:     spec.Name,
		Name:   spec.Name,
		Type:   "tmux",
		Status: domain.WorkspaceStatusRunning,
	}, nil
}

// Destroy kills the tmux session and all its windows.
func (p *TmuxProvider) Destroy(ctx context.Context, wsID string) error {
	if _, err := p.tmuxOutput("kill-session", "-t", wsID); err != nil {
		// Ignore "session not found" — already destroyed.
		if !strings.Contains(err.Error(), "session not found") {
			return fmt.Errorf("destroy tmux session %s: %w", wsID, err)
		}
	}

	p.mu.Lock()
	delete(p.sessions, wsID)
	p.mu.Unlock()

	return nil
}

// CreateSlot creates a new window in a tmux session.
func (p *TmuxProvider) CreateSlot(ctx context.Context, wsID string, spec domain.SlotSpec) (*domain.ExecutionSlot, error) {
	p.mu.RLock()
	session, ok := p.sessions[wsID]
	p.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("tmux session not found: %s", wsID)
	}

	// Create new window.
	args := []string{"new-window", "-t", wsID, "-n", spec.Name}
	if spec.Dir != "" {
		args = append(args, "-c", spec.Dir)
	}
	if _, err := p.tmuxOutput(args...); err != nil {
		return nil, fmt.Errorf("create window %s in session %s: %w", spec.Name, wsID, err)
	}

	// Get the window index.
	windowList, err := p.tmuxOutput("list-windows", "-t", wsID, "-F", "#{window_index}:#{window_name}")
	if err != nil {
		return nil, fmt.Errorf("list windows: %w", err)
	}

	var windowIndex int
	for _, line := range strings.Split(windowList, "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[1] == spec.Name {
			fmt.Sscanf(parts[0], "%d", &windowIndex)
			break
		}
	}

	slotID := fmt.Sprintf("%s:%s", wsID, spec.Name)

	win := &tmuxWindow{
		name:   spec.Name,
		index:  windowIndex,
		slotID: slotID,
	}

	p.mu.Lock()
	session.windows[slotID] = win
	p.mu.Unlock()

	return &domain.ExecutionSlot{
		ID:   slotID,
		Name: spec.Name,
	}, nil
}

// DeleteSlot kills a window in a tmux session.
func (p *TmuxProvider) DeleteSlot(ctx context.Context, slotID string) error {
	// slotID format is "session:window_name"
	parts := strings.SplitN(slotID, ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid slot ID: %s", slotID)
	}
	sessionName := parts[0]

	p.mu.RLock()
	session, ok := p.sessions[sessionName]
	p.mu.RUnlock()

	if !ok {
		return fmt.Errorf("session not found: %s", sessionName)
	}

	if _, err := p.tmuxOutput("kill-window", "-t", slotID); err != nil {
		return fmt.Errorf("kill window %s: %w", slotID, err)
	}

	p.mu.Lock()
	delete(session.windows, slotID)
	p.mu.Unlock()

	return nil
}

// BindRuntime records that a runtime is assigned to a slot.
// For tmux, this is a no-op — the runtime process is launched by the RuntimeAdapter.
func (p *TmuxProvider) BindRuntime(ctx context.Context, slotID string, runtimeID string) error {
	// Tmux doesn't need explicit binding — the process is launched separately.
	// We just record the association.
	return nil
}

// Attach captures the pane content of a tmux window.
func (p *TmuxProvider) Attach(ctx context.Context, slotID string) (<-chan domain.PTYFrame, io.Writer, error) {
	ch := make(chan domain.PTYFrame, 64)

	// In v0.1, we capture pane content via polling.
	// Future: use tmux control mode for real-time streaming.
	go func() {
		defer close(ch)

		ticker := time.NewTicker(p.PollInterval)
		defer ticker.Stop()

		for {
			content, err := p.tmuxOutput("capture-pane", "-t", slotID, "-p", "-e")
			if err != nil {
				return
			}

			select {
			case ch <- domain.PTYFrame{
				SessionID: slotID,
				Data:      []byte(content),
			}:
			case <-ctx.Done():
				return
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// Writer sends keystrokes to the tmux pane.
	writer := &tmuxWriter{
		provider: p,
		slotID:   slotID,
	}

	return ch, writer, nil
}

// tmuxWriter sends input to a tmux pane.
type tmuxWriter struct {
	provider *TmuxProvider
	slotID   string
}

func (w *tmuxWriter) Write(p []byte) (int, error) {
	escaped := escapeTmuxKeys(string(p))
	cmd := w.provider.tmux("send-keys", "-t", w.slotID, escaped)
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("send-keys to %s: %w", w.slotID, err)
	}
	return len(p), nil
}

// escapeTmuxKeys escapes special characters that tmux would otherwise interpret
// in send-keys arguments: semicolons, backslashes, and dollar signs.
func escapeTmuxKeys(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `;`, `\;`)
	s = strings.ReplaceAll(s, `$`, `\$`)
	return s
}

// Status returns the current status of a tmux session.
func (p *TmuxProvider) Status(ctx context.Context, wsID string) (domain.WorkspaceStatus, error) {
	_, err := p.tmuxOutput("has-session", "-t", wsID)
	if err != nil {
		return domain.WorkspaceStatusDestroyed, nil
	}
	return domain.WorkspaceStatusRunning, nil
}
