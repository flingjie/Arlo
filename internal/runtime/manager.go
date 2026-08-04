package runtime

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/lingjiefan/arlo/internal/domain"
)

// Manager orchestrates RuntimeAdapter instances across their lifecycle.
// It maintains a registry of adapters (one per runtime type) and tracks
// active instances.
type Manager struct {
	mu        sync.RWMutex
	adapters  map[domain.RuntimeProvider]Adapter       // type → adapter
	instances map[string]*domain.RuntimeInstance        // instanceID → instance
}

// NewManager creates a new RuntimeManager.
func NewManager() *Manager {
	return &Manager{
		adapters:  make(map[domain.RuntimeProvider]Adapter),
		instances: make(map[string]*domain.RuntimeInstance),
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
	m.mu.Unlock()

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
	}

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

	m.mu.Lock()
	m.instances[inst.ID] = inst
	m.mu.Unlock()

	return inst, nil
}

// StopInstance gracefully stops a running instance.
func (m *Manager) StopInstance(ctx context.Context, id string) error {
	m.mu.RLock()
	inst, ok := m.instances[id]
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("runtime instance not found: %s", id)
	}
	if !adapterOk {
		return fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	return adapter.Stop(ctx, id)
}

// DestroyInstance cleans up all resources for an instance.
func (m *Manager) DestroyInstance(ctx context.Context, id string) error {
	m.mu.RLock()
	inst, ok := m.instances[id]
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("runtime instance not found: %s", id)
	}
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
func (m *Manager) MarkExited(id string, exitCode int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inst, ok := m.instances[id]; ok {
		inst.State = domain.RuntimeStateExited
		inst.ExitCode = exitCode
	}
}

// GetInstance returns a runtime instance by ID.
func (m *Manager) GetInstance(ctx context.Context, id string) (*domain.RuntimeInstance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, ok := m.instances[id]
	if !ok {
		return nil, fmt.Errorf("runtime instance not found: %s", id)
	}
	return inst, nil
}

// SendInstruction routes a control message to a running instance.
func (m *Manager) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	m.mu.RLock()
	inst, ok := m.instances[id]
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("runtime instance not found: %s", id)
	}
	if !adapterOk {
		return fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	return adapter.SendInstruction(ctx, id, instruction)
}

// AttachInstance connects to the PTY stream of an interactive runtime.
func (m *Manager) AttachInstance(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
	m.mu.RLock()
	inst, ok := m.instances[id]
	adapter, adapterOk := m.adapters[inst.Type]
	m.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("runtime instance not found: %s", id)
	}
	if !adapterOk {
		return nil, nil, fmt.Errorf("adapter not found for type: %s", inst.Type)
	}

	ir, ok := adapter.(InteractiveRuntime)
	if !ok {
		return nil, nil, fmt.Errorf("runtime type %s does not support PTY attach", inst.Type)
	}

	return ir.Attach(ctx, id)
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
