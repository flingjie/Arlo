package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/lingjiefan/arlo/internal/domain"
)

// mockAdapter implements Adapter for testing.
type mockAdapter struct {
	prepareErr error
	startErr   error
	stopErr    error
	destroyErr error
	started    []string
	stopped    []string
}

func (m *mockAdapter) Prepare(ctx context.Context, inst domain.RuntimeInstance) error { return m.prepareErr }
func (m *mockAdapter) Start(ctx context.Context, inst domain.RuntimeInstance) error {
	m.started = append(m.started, inst.ID)
	return m.startErr
}
func (m *mockAdapter) Stop(ctx context.Context, id string) error {
	m.stopped = append(m.stopped, id)
	return m.stopErr
}
func (m *mockAdapter) Destroy(ctx context.Context, id string) error           { return m.destroyErr }
func (m *mockAdapter) SendInstruction(ctx context.Context, id string, instruction domain.Instruction) error {
	return nil
}
func (m *mockAdapter) Status(ctx context.Context, id string) (domain.RuntimeStatus, error) {
	return domain.RuntimeStatus{ID: id, State: domain.RuntimeStateRunning}, nil
}

// TestStartInstance verifies basic start through the manager.
func TestStartInstance(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	adapter := &mockAdapter{}
	mgr.RegisterAdapter("claude-code", adapter)

	inst, err := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID:  "inst-1",
		Type:        "claude-code",
		SessionID:   "sess-1",
		WorkspaceID: "ws-1",
		SlotID:      "slot-1",
		WorkDir:     "/tmp",
		Prompt:      "fix the bug",
	})
	if err != nil {
		t.Fatalf("StartInstance: %v", err)
	}
	if inst.State != domain.RuntimeStateRunning {
		t.Errorf("state = %s, want RUNNING", inst.State)
	}
	if len(adapter.started) != 1 || adapter.started[0] != "inst-1" {
		t.Errorf("adapter started = %v, want [inst-1]", adapter.started)
	}
}

// TestStartInstanceUnknownType verifies error for unregistered type.
func TestStartInstanceUnknownType(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	_, err := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID: "inst-1",
		Type:       "unknown-runtime",
	})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

// TestStartInstancePrepareError verifies error propagation from Prepare.
func TestStartInstancePrepareError(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterAdapter("claude-code", &mockAdapter{
		prepareErr: errors.New("claude not installed"),
	})

	t.Run("prepare fails", func(t *testing.T) {
		inst, err := mgr.StartInstance(ctx, RuntimeSpec{
			InstanceID: "inst-1",
			Type:       "claude-code",
		})
		if err == nil {
			t.Fatal("expected error from Prepare")
		}
		if inst.State != domain.RuntimeStateFailed {
			t.Errorf("state = %s, want FAILED", inst.State)
		}
	})
}


// TestStopInstance verifies graceful stop.
func TestStopInstance(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	adapter := &mockAdapter{}
	mgr.RegisterAdapter("claude-code", adapter)

	inst, _ := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID: "inst-1",
		Type:       "claude-code",
	})

	err := mgr.StopInstance(ctx, inst.ID)
	if err != nil {
		t.Fatalf("StopInstance: %v", err)
	}
	if len(adapter.stopped) != 1 || adapter.stopped[0] != "inst-1" {
		t.Errorf("adapter stopped = %v, want [inst-1]", adapter.stopped)
	}
}

// TestGetInstance verifies instance retrieval.
func TestGetInstance(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterAdapter("claude-code", &mockAdapter{})

	inst, _ := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID: "inst-1",
		Type:       "claude-code",
	})

	retrieved, err := mgr.GetInstance(ctx, inst.ID)
	if err != nil {
		t.Fatalf("GetInstance: %v", err)
	}
	if retrieved.ID != "inst-1" {
		t.Errorf("retrieved ID = %s, want inst-1", retrieved.ID)
	}
	if retrieved.State != domain.RuntimeStateRunning {
		t.Errorf("state = %s, want RUNNING", retrieved.State)
	}
}

// TestGetInstanceNotFound verifies error for unknown instance.
func TestGetInstanceNotFound(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	_, err := mgr.GetInstance(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}
