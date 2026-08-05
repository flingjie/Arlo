package runtime

import (
	"context"
	"errors"
	"io"
	"sync"
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

// interactiveMockAdapter extends mockAdapter with Attach support for testing.
type interactiveMockAdapter struct {
	mockAdapter
}

func (m *interactiveMockAdapter) Attach(ctx context.Context, id string) (<-chan domain.PTYFrame, io.Writer, error) {
	return nil, nil, nil
}

// TestStartInstance verifies basic start through the manager.
func TestStartInstance(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	adapter := &mockAdapter{}
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, adapter)

	inst, err := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID:  "inst-1",
		Type:        domain.RuntimeProviderClaudeCode,
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
		Type:       domain.RuntimeProvider("unknown-runtime"),
	})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

// TestStartInstancePrepareError verifies error propagation from Prepare.
func TestStartInstancePrepareError(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &mockAdapter{
		prepareErr: errors.New("claude not installed"),
	})

	t.Run("prepare fails", func(t *testing.T) {
		inst, err := mgr.StartInstance(ctx, RuntimeSpec{
			InstanceID: "inst-1",
			Type:       domain.RuntimeProviderClaudeCode,
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
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, adapter)

	inst, _ := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID: "inst-1",
		Type:       domain.RuntimeProviderClaudeCode,
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
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &mockAdapter{})

	inst, _ := mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID: "inst-1",
		Type:       domain.RuntimeProviderClaudeCode,
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

// TestStopInstanceNonexistent verifies StopInstance returns an error (not panic)
// for a nonexistent instance ID. This guards against nil-pointer dereference.
func TestStopInstanceNonexistent(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	err := mgr.StopInstance(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}

// TestDestroyInstanceNonexistent verifies DestroyInstance returns an error (not panic)
// for a nonexistent instance ID.
func TestDestroyInstanceNonexistent(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	err := mgr.DestroyInstance(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}

// TestSendInstructionNonexistent verifies SendInstruction returns an error (not panic)
// for a nonexistent instance ID.
func TestSendInstructionNonexistent(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	err := mgr.SendInstruction(ctx, "nonexistent", domain.Instruction{Type: "hint", Content: "try harder"})
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}

// TestAttachInstanceNonexistent verifies AttachInstance returns an error (not panic)
// for a nonexistent instance ID.
func TestAttachInstanceNonexistent(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()

	_, _, err := mgr.AttachInstance(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent instance")
	}
}

// TestGetInstanceReturnsCopy verifies that GetInstance returns a safe copy, not a pointer
// into the internal map. Mutating the returned struct must not affect internal state.
func TestGetInstanceReturnsCopy(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &mockAdapter{})

	_, _ = mgr.StartInstance(ctx, RuntimeSpec{
		InstanceID: "inst-1",
		Type:       domain.RuntimeProviderClaudeCode,
	})

	// Fetch the instance.
	inst, err := mgr.GetInstance(ctx, "inst-1")
	if err != nil {
		t.Fatalf("GetInstance: %v", err)
	}

	// Mutate the returned struct.
	inst.State = domain.RuntimeStateFailed
	inst.ExitCode = 42

	// Fetch again and verify internal state is unchanged.
	inst2, err := mgr.GetInstance(ctx, "inst-1")
	if err != nil {
		t.Fatalf("GetInstance (second): %v", err)
	}
	if inst2.State != domain.RuntimeStateRunning {
		t.Errorf("state = %s, want RUNNING (copy was not returned)", inst2.State)
	}
	if inst2.ExitCode != 0 {
		t.Errorf("exitCode = %d, want 0 (copy was not returned)", inst2.ExitCode)
	}
}

// TestConcurrentStartInstanceRejected verifies that starting an instance with the same
// ID twice returns an error on the second call, preventing leaks.
func TestConcurrentStartInstanceRejected(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &mockAdapter{})

	spec := RuntimeSpec{
		InstanceID: "inst-1",
		Type:       domain.RuntimeProviderClaudeCode,
		SessionID:  "sess-1",
		WorkDir:    "/tmp",
	}

	// First start succeeds.
	inst1, err := mgr.StartInstance(ctx, spec)
	if err != nil {
		t.Fatalf("first StartInstance: %v", err)
	}
	if inst1.State != domain.RuntimeStateRunning {
		t.Errorf("state = %s, want RUNNING", inst1.State)
	}

	// Second start with same InstanceID must return an error.
	_, err = mgr.StartInstance(ctx, spec)
	if err == nil {
		t.Fatal("expected error for duplicate instance ID")
	}
}

// TestConcurrentStartInstanceStress verifies that concurrent StartInstance calls
// with the same ID are serialized correctly and only one succeeds.
func TestConcurrentStartInstanceStress(t *testing.T) {
	ctx := context.Background()
	mgr := NewManager()
	mgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, &mockAdapter{})

	spec := RuntimeSpec{
		InstanceID: "inst-stress",
		Type:       domain.RuntimeProviderClaudeCode,
		SessionID:  "sess-stress",
		WorkDir:    "/tmp",
	}

	var wg sync.WaitGroup
	successes := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mgr.StartInstance(ctx, spec)
			mu.Lock()
			if err == nil {
				successes++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if successes != 1 {
		t.Errorf("expected exactly 1 successful start, got %d", successes)
	}
}
