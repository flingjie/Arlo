package runtime

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
)

// Manager orchestrates RuntimeAdapter instances across their lifecycle.
// It maintains a registry of adapters (one per runtime type) and tracks
// active instances.
type Manager struct {
	mu        sync.RWMutex
	adapters  map[domain.RuntimeProvider]Adapter // type → adapter
	instances map[string]*domain.RuntimeInstance  // instanceID → instance

	obsMu     sync.Mutex
	observers map[string][]chan RuntimeEvent // instanceID → observer channels
}

// NewManager creates a new RuntimeManager.
func NewManager() *Manager {
	return &Manager{
		adapters:  make(map[domain.RuntimeProvider]Adapter),
		instances: make(map[string]*domain.RuntimeInstance),
		observers: make(map[string][]chan RuntimeEvent),
	}
}

// RegisterAdapter adds a runtime adapter for a given type name.
func (m *Manager) RegisterAdapter(typeName domain.RuntimeProvider, adapter Adapter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.adapters[typeName] = adapter
}

// StartInstance creates and starts a RuntimeInstance using the correct adapter.
func (m *Manager) StartInstance(ctx context.Context, spec RuntimeSpec) (*domain.RuntimeInstance, error) {
	m.mu.Lock()
	adapter, ok := m.adapters[spec.Type]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("no adapter registered for runtime type: %s", spec.Type)
	}
	if _, exists := m.instances[spec.InstanceID]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("runtime instance already exists: %s", spec.InstanceID)
	}

	// Pre-insert a placeholder in PREPARING state so concurrent calls with the same
	// InstanceID are rejected atomically. The state is updated after adapter calls.
	inst := &domain.RuntimeInstance{
		ID:          spec.InstanceID,
		Type:        spec.Type,
		Config:      spec.Config,
		SessionID:   spec.SessionID,
		WorkspaceID: spec.WorkspaceID,
		SlotID:      spec.SlotID,
		WorkDir:     spec.WorkDir,
		Prompt:      spec.Prompt,
		State:       domain.RuntimeStatePreparing,
		StartedAt:   time.Now(),
	}
	m.instances[inst.ID] = inst
	m.mu.Unlock()

	// Prepare → Start.
	if err := adapter.Prepare(ctx, *inst); err != nil {
		inst.State = domain.RuntimeStateFailed
		return inst, fmt.Errorf("prepare: %w", err)
	}

	if err := adapter.Start(ctx, *inst); err != nil {
		inst.State = domain.RuntimeStateFailed
		return inst, fmt.Errorf("start: %w", err)
	}

	inst.State = domain.RuntimeStateRunning

	return inst, nil
}

// StopInstance gracefully stops a running instance.
func (m *Manager) StopInstance(ctx context.Context, id string) error {
	m.mu.RLock()
	inst, ok := m.instances[id]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("runtime instance not found: %s", id)
	}

	m.mu.RLock()
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !adapterOk {
		return fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	return adapter.Stop(ctx, id)
}

// DestroyInstance cleans up all resources for an instance.
func (m *Manager) DestroyInstance(ctx context.Context, id string) error {
	m.mu.RLock()
	inst, ok := m.instances[id]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("runtime instance not found: %s", id)
	}

	m.mu.RLock()
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !adapterOk {
		return fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	if err := adapter.Destroy(ctx, id); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.instances, id)
	m.mu.Unlock()

	return nil
}

// MarkExited is called by adapters when the underlying process exits.
// It updates the instance state so the reconciler can detect the exit via polling.
func (m *Manager) MarkExited(id string, exitCode int, metrics domain.RuntimeMetrics) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inst, ok := m.instances[id]; ok {
		inst.State = domain.RuntimeStateExited
		inst.ExitCode = exitCode
		inst.Metrics = metrics
	}
}

// ReportEvent is called by adapters to emit a real-time RuntimeEvent.
// It updates the instance's CurrentAction / LastEventAt and fans out the
// event to all observer channels registered for the instance.
func (m *Manager) ReportEvent(id string, event RuntimeEvent) {
	m.mu.Lock()
	if inst, ok := m.instances[id]; ok {
		inst.LastEventAt = event.Timestamp
		inst.CurrentAction = event.Action
	}
	m.mu.Unlock()

	m.obsMu.Lock()
	defer m.obsMu.Unlock()
	for _, ch := range m.observers[id] {
		select {
		case ch <- event:
		default:
			// Drop if channel buffer is full (non-blocking fan-out).
		}
	}
}

// Observe returns a channel that receives RuntimeEvents for the given
// instance ID. Call StopObserving when done to prevent goroutine leaks.
func (m *Manager) Observe(id string) <-chan RuntimeEvent {
	ch := make(chan RuntimeEvent, 64)

	m.obsMu.Lock()
	defer m.obsMu.Unlock()
	m.observers[id] = append(m.observers[id], ch)

	return ch
}

// StopObserving removes an observer channel for the given instance ID.
// After this call, no further events will be sent to the channel.
func (m *Manager) StopObserving(id string, ch <-chan RuntimeEvent) {
	m.obsMu.Lock()
	defer m.obsMu.Unlock()

	channels := m.observers[id]
	for i, oc := range channels {
		if (<-chan RuntimeEvent)(oc) == ch {
			// Remove by replacing with the last element and truncating.
			channels[i] = channels[len(channels)-1]
			m.observers[id] = channels[:len(channels)-1]
			close(oc)
			break
		}
	}
	if len(m.observers[id]) == 0 {
		delete(m.observers, id)
	}
}

// GetInstance returns a runtime instance by ID.
// It returns a copy to prevent data races with internal state mutations.
func (m *Manager) GetInstance(ctx context.Context, id string) (*domain.RuntimeInstance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, ok := m.instances[id]
	if !ok {
		return nil, fmt.Errorf("runtime instance not found: %s", id)
	}
	copy := *inst
	return &copy, nil
}

// SendInstruction routes a control message to a running instance.
func (m *Manager) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	m.mu.RLock()
	inst, ok := m.instances[id]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("runtime instance not found: %s", id)
	}

	m.mu.RLock()
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !adapterOk {
		return fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	return adapter.SendInstruction(ctx, id, instruction)
}

// AttachInstance connects to the PTY stream of an interactive runtime.
func (m *Manager) AttachInstance(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
	m.mu.RLock()
	inst, ok := m.instances[id]
	m.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("runtime instance not found: %s", id)
	}

	m.mu.RLock()
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !adapterOk {
		return nil, nil, fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	ir, ok := adapter.(InteractiveRuntime)
	if !ok {
		return nil, nil, fmt.Errorf("runtime type %s does not support PTY attach", inst.Type)
	}

	return ir.Attach(ctx, id)
}

// Snapshot returns the current observable state of the runtime process
// by delegating to the appropriate adapter.
func (m *Manager) Snapshot(ctx context.Context, id string) (domain.RuntimeSnapshot, error) {
	m.mu.RLock()
	inst, ok := m.instances[id]
	if !ok {
		m.mu.RUnlock()
		return domain.RuntimeSnapshot{}, fmt.Errorf("runtime instance not found: %s", id)
	}

	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !adapterOk {
		return domain.RuntimeSnapshot{}, fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	return adapter.Snapshot(ctx, id)
}

// RuntimeSpec contains all information needed to start a RuntimeInstance.
type RuntimeSpec struct {
	InstanceID  string
	Type        domain.RuntimeProvider
	Config      domain.RuntimeConfig
	SessionID   string
	WorkspaceID string
	SlotID      string
	WorkDir     string
	Prompt      string
}
