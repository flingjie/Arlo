package workspace

import (
	"context"
	"io"
	"testing"

	"github.com/lingjiefan/arlo/internal/domain"
)

// mockProvider implements Provider for testing.
type mockProvider struct {
	createErr error
	slots     map[string]*domain.ExecutionSlot
	workspaces map[string]*domain.Workspace
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		slots:     make(map[string]*domain.ExecutionSlot),
		workspaces: make(map[string]*domain.Workspace),
	}
}

func (m *mockProvider) Create(ctx context.Context, spec domain.WorkspaceSpec) (*domain.Workspace, error) {
	if m.createErr != nil {
		return nil, m.createErr
	}
	ws := &domain.Workspace{
		ID:   spec.Name,
		Name: spec.Name,
		Type: spec.Type,
	}
	m.workspaces[ws.ID] = ws
	return ws, nil
}
func (m *mockProvider) Destroy(ctx context.Context, wsID string) error {
	delete(m.workspaces, wsID)
	return nil
}
func (m *mockProvider) CreateSlot(ctx context.Context, wsID string, spec domain.SlotSpec) (*domain.ExecutionSlot, error) {
	slot := &domain.ExecutionSlot{
		ID:   wsID + ":" + spec.Name,
		Name: spec.Name,
	}
	m.slots[slot.ID] = slot
	return slot, nil
}
func (m *mockProvider) DeleteSlot(ctx context.Context, slotID string) error {
	delete(m.slots, slotID)
	return nil
}
func (m *mockProvider) BindRuntime(ctx context.Context, slotID string, runtimeID string) error {
	slot, ok := m.slots[slotID]
	if ok {
		slot.RuntimeID = runtimeID
	}
	return nil
}
func (m *mockProvider) Attach(ctx context.Context, slotID string) (<-chan domain.PTYFrame, io.Writer, error) {
	return nil, nil, nil
}
func (m *mockProvider) Status(ctx context.Context, wsID string) (domain.WorkspaceStatus, error) {
	return domain.WorkspaceStatusRunning, nil
}

// TestCreateWorkspace verifies workspace creation through the manager.
func TestCreateWorkspace(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterProvider("mock", newMockProvider())

	ws, err := mgr.Create(ctx, domain.WorkspaceSpec{
		Name: "test-ws",
		Type: "mock",
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ws.Name != "test-ws" {
		t.Errorf("name = %s, want test-ws", ws.Name)
	}
}

// TestCreateWorkspaceUnknownType verifies error for unregistered provider.
func TestCreateWorkspaceUnknownType(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	_, err := mgr.Create(ctx, domain.WorkspaceSpec{
		Name: "test",
		Type: "unknown",
	})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

// TestCreateSlot verifies slot creation.
func TestCreateSlot(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterProvider("mock", newMockProvider())

	mgr.Create(ctx, domain.WorkspaceSpec{Name: "ws1", Type: "mock"})

	slot, err := mgr.CreateSlot(ctx, "ws1", domain.SlotSpec{Name: "coder"})
	if err != nil {
		t.Fatalf("CreateSlot: %v", err)
	}
	if slot.Name != "coder" {
		t.Errorf("slot name = %s, want coder", slot.Name)
	}
}

// TestBindRuntime verifies runtime binding to a slot.
func TestBindRuntime(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterProvider("mock", newMockProvider())

	mgr.Create(ctx, domain.WorkspaceSpec{Name: "ws1", Type: "mock"})
	mgr.CreateSlot(ctx, "ws1", domain.SlotSpec{Name: "coder"})

	err := mgr.BindRuntime(ctx, "ws1:coder", "rt-1")
	if err != nil {
		t.Fatalf("BindRuntime: %v", err)
	}

	slot, _ := mgr.GetSlot(ctx, "ws1:coder")
	if slot.RuntimeID != "rt-1" {
		t.Errorf("runtimeID = %s, want rt-1", slot.RuntimeID)
	}
}

// TestDestroyWorkspace verifies workspace cleanup.
func TestDestroyWorkspace(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	prov := newMockProvider()
	mgr.RegisterProvider("mock", prov)

	mgr.Create(ctx, domain.WorkspaceSpec{Name: "ws1", Type: "mock"})

	if len(prov.workspaces) != 1 {
		t.Fatal("workspace should be tracked by provider")
	}

	mgr.Destroy(ctx, "ws1")

	if len(prov.workspaces) != 0 {
		t.Error("provider should have cleaned up workspace")
	}
}
