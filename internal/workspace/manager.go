package workspace

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/lingjiefan/arlo/internal/domain"
)

// Manager orchestrates workspace providers and tracks active workspaces.
type Manager struct {
	mu         sync.RWMutex
	providers  map[string]Provider              // type → provider
	workspaces map[string]*domain.Workspace     // workspaceID → workspace
	slots      map[string]*domain.ExecutionSlot // slotID → slot
}

// NewManager creates a new WorkspaceManager.
func NewManager() *Manager {
	return &Manager{
		providers:  make(map[string]Provider),
		workspaces: make(map[string]*domain.Workspace),
		slots:      make(map[string]*domain.ExecutionSlot),
	}
}

// RegisterProvider adds a workspace provider for a given type name.
func (m *Manager) RegisterProvider(typeName string, provider Provider) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.providers[typeName] = provider
}

// Create creates a new workspace using the appropriate provider.
func (m *Manager) Create(ctx context.Context, spec domain.WorkspaceSpec) (*domain.Workspace, error) {
	m.mu.RLock()
	p, ok := m.providers[spec.Type]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no provider registered for workspace type: %s", spec.Type)
	}

	ws, err := p.Create(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("create workspace: %w", err)
	}

	m.mu.Lock()
	m.workspaces[ws.ID] = ws
	m.mu.Unlock()

	return ws, nil
}

// Destroy tears down a workspace and all its resources.
func (m *Manager) Destroy(ctx context.Context, wsID string) error {
	m.mu.RLock()
	ws, ok := m.workspaces[wsID]
	var p Provider
	if ok {
		p = m.providers[ws.Type]
	}
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("workspace not found: %s", wsID)
	}

	if err := p.Destroy(ctx, wsID); err != nil {
		return err
	}

	m.mu.Lock()
	delete(m.workspaces, wsID)
	// Remove only slots belonging to this workspace.
	// In v0.1 we identify by name prefix; v1 will add a workspaceID field to ExecutionSlot.
	// Slot IDs use the format "workspaceID:slotName".
	prefix := wsID + ":"
	for slotID := range m.slots {
		if strings.HasPrefix(slotID, prefix) {
			delete(m.slots, slotID)
		}
	}
	m.mu.Unlock()

	return nil
}

// CreateSlot creates a new execution slot in a workspace.
func (m *Manager) CreateSlot(ctx context.Context, wsID string, spec domain.SlotSpec) (*domain.ExecutionSlot, error) {
	m.mu.RLock()
	ws, ok := m.workspaces[wsID]
	p := m.providers[ws.Type]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("workspace not found: %s", wsID)
	}

	slot, err := p.CreateSlot(ctx, wsID, spec)
	if err != nil {
		return nil, fmt.Errorf("create slot: %w", err)
	}

	m.mu.Lock()
	m.slots[slot.ID] = slot
	m.mu.Unlock()

	return slot, nil
}

// BindRuntime records a runtime assignment to a slot.
func (m *Manager) BindRuntime(ctx context.Context, slotID string, runtimeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	slot, ok := m.slots[slotID]
	if !ok {
		return fmt.Errorf("slot not found: %s", slotID)
	}

	slot.RuntimeID = runtimeID
	return nil
}

// Attach connects to a slot's terminal.
func (m *Manager) Attach(ctx context.Context, slotID string) (<-chan domain.PTYFrame, io.Writer, error) {
	m.mu.RLock()
	slot, ok := m.slots[slotID]
	m.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("slot not found: %s", slotID)
	}

	// Find which workspace this slot belongs to.
	m.mu.RLock()
	var provider Provider
	for _, ws := range m.workspaces {
		for _, s := range ws.Slots {
			if s.ID == slotID {
				provider = m.providers[ws.Type]
				break
			}
		}
	}
	m.mu.RUnlock()

	_ = slot
	if provider == nil {
		return nil, nil, fmt.Errorf("provider not found for slot: %s", slotID)
	}

	return provider.Attach(ctx, slotID)
}

// GetSlot returns a slot by ID.
func (m *Manager) GetSlot(ctx context.Context, slotID string) (*domain.ExecutionSlot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	slot, ok := m.slots[slotID]
	if !ok {
		return nil, fmt.Errorf("slot not found: %s", slotID)
	}
	return slot, nil
}

// GetWorkspace returns a workspace by ID.
func (m *Manager) GetWorkspace(ctx context.Context, wsID string) (*domain.Workspace, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ws, ok := m.workspaces[wsID]
	if !ok {
		return nil, fmt.Errorf("workspace not found: %s", wsID)
	}
	return ws, nil
}
