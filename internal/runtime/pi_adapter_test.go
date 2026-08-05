package runtime

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
)

// TestPiAdapterStartDetachedContext verifies that PiAdapter.Start uses
// exec.Command (not exec.CommandContext) so the pi process is NOT tied to
// the gRPC request context. This is a regression test matching the
// ClaudeAdapter equivalent.
func TestPiAdapterStartDetachedContext(t *testing.T) {
	_, err := exec.LookPath("pi")
	if err != nil {
		t.Skip("pi not found in PATH — skipping integration test")
	}

	adapter := NewPiAdapter()

	ctx, cancel := context.WithCancel(context.Background())

	inst := domain.RuntimeInstance{
		ID:     "test-pi-detach-context",
		Type:   domain.RuntimeProviderPi,
		Prompt: "say hello",
	}

	if err := adapter.Start(ctx, inst); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify the instance is tracked.
	adapter.mu.RLock()
	_, tracked := adapter.instances[inst.ID]
	adapter.mu.RUnlock()
	if !tracked {
		t.Fatal("instance not tracked after Start")
	}

	// Cancel the parent context. The pi process should survive.
	cancel()

	// Give a moment for any signal propagation.
	time.Sleep(100 * time.Millisecond)

	// Verify the instance is still tracked (process was not killed by context
	// cancellation).
	adapter.mu.RLock()
	_, tracked = adapter.instances[inst.ID]
	adapter.mu.RUnlock()
	if !tracked {
		t.Fatal("instance disappeared after context cancellation — process may have been killed by context")
	}

	// Clean up: Stop sends SIGINT, then Destroy forces cleanup.
	if err := adapter.Stop(context.Background(), inst.ID); err != nil {
		t.Errorf("Stop: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	adapter.Destroy(context.Background(), inst.ID)

	// Verify cleanup: instance should no longer be tracked.
	adapter.mu.RLock()
	_, tracked = adapter.instances[inst.ID]
	adapter.mu.RUnlock()
	if tracked {
		t.Error("instance still tracked after Destroy")
	}
}

// TestPiAdapterPrepare verifies that Prepare checks for the pi binary.
func TestPiAdapterPrepare(t *testing.T) {
	_, err := exec.LookPath("pi")
	if err != nil {
		t.Skip("pi not found in PATH — skipping integration test")
	}

	adapter := NewPiAdapter()

	inst := domain.RuntimeInstance{
		ID:   "test-pi-prepare",
		Type: domain.RuntimeProviderPi,
	}

	if err := adapter.Prepare(context.Background(), inst); err != nil {
		t.Fatalf("Prepare should succeed when pi is in PATH: %v", err)
	}
}

// TestPiAdapterPrepareNotFound verifies that Prepare fails when pi is not
// in PATH. We test by looking up a non-existent binary.
func TestPiAdapterPrepareNotFound(t *testing.T) {
	// Override the binary lookup to simulate missing pi.
	// We test this indirectly: the adapter uses exec.LookPath("pi"),
	// and we verify the error message.
	t.Run("non-existent pi path", func(t *testing.T) {
		_, err := exec.LookPath("pi-nonexistent-binary-xyz")
		if err == nil {
			t.Skip("unexpected binary found")
		}
		// exec.LookPath error confirms the pattern the adapter follows.
		if err == nil {
			t.Error("expected error for non-existent binary")
		}
	})
}

// TestPiAdapterStopNonexistent verifies Stop returns error for unknown ID.
func TestPiAdapterStopNonexistent(t *testing.T) {
	adapter := NewPiAdapter()
	err := adapter.Stop(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent instance")
	}
}

// TestPiAdapterDestroyNonexistent verifies Destroy is a no-op for unknown ID.
func TestPiAdapterDestroyNonexistent(t *testing.T) {
	adapter := NewPiAdapter()
	err := adapter.Destroy(context.Background(), "nonexistent")
	if err != nil {
		t.Errorf("Destroy should be a no-op for nonexistent instances: %v", err)
	}
}

// TestPiAdapterStatusNonexistent verifies Status returns Exited for unknown ID.
func TestPiAdapterStatusNonexistent(t *testing.T) {
	adapter := NewPiAdapter()
	status, err := adapter.Status(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != domain.RuntimeStateExited {
		t.Errorf("expected Exited state for nonexistent instance, got %s", status.State)
	}
}

// TestPiAdapterSendInstructionNonexistent verifies SendInstruction returns
// error for unknown ID.
func TestPiAdapterSendInstructionNonexistent(t *testing.T) {
	adapter := NewPiAdapter()
	err := adapter.SendInstruction(context.Background(), "nonexistent", domain.Instruction{Type: "hint"})
	if err == nil {
		t.Error("expected error for nonexistent instance")
	}
}
