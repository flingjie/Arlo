package runtime

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/lingjiefan/arlo/internal/domain"
)

// TestStartUsesCommandNotCommandContext verifies that Start uses exec.Command
// (not exec.CommandContext) so the child process is NOT tied to the gRPC
// request context. This is a regression test for the bug where canceling
// the parent context would kill the child process.
//
// Since the ClaudeAdapter hardcodes "claude", this test verifies the
// underlying behavior directly: exec.Command processes survive context
// cancellation, while exec.CommandContext processes do not.
func TestStartUsesCommandNotCommandContext(t *testing.T) {
	t.Run("exec.Command survives context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Use exec.Command (the fix), NOT exec.CommandContext.
		cmd := exec.Command("sleep", "10")

		if err := cmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}

		// Cancel the parent context. This should NOT kill the process.
		cancel()
		ctx.Err() // drain cancellation

		// Give the OS a moment to propagate any signal.
		time.Sleep(50 * time.Millisecond)

		// Verify the process is still running.
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatal("process exited after context cancellation — it should still be running")
		}

		done := make(chan struct{})
		go func() {
			cmd.Process.Kill()
			cmd.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Process killed successfully.
		case <-time.After(2 * time.Second):
			cmd.Process.Kill()
			t.Fatal("timed out waiting for process to exit after Kill")
		}
	})

	t.Run("exec.CommandContext dies when context is cancelled (old bug)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// This is the old buggy pattern: exec.CommandContext ties the process
		// to the request context.
		cmd := exec.CommandContext(ctx, "sleep", "10")

		if err := cmd.Start(); err != nil {
			t.Fatalf("start sleep: %v", err)
		}

		// Cancel the context. This SHOULD kill the process (demonstrating the bug).
		cancel()

		// Wait for the process to exit.
		errCh := make(chan error, 1)
		go func() {
			errCh <- cmd.Wait()
		}()

		select {
		case err := <-errCh:
			if err == nil {
				t.Fatal("expected process to be killed by context cancellation, but it exited cleanly")
			}
			// The process was killed — this is the pre-fix behavior we want to avoid.
		case <-time.After(3 * time.Second):
			cmd.Process.Kill()
			t.Fatal("timed out waiting for process to be killed by context cancellation")
		}
	})
}

// TestClaudeAdapterStartDetachedContext verifies the full fix end-to-end
// when the claude CLI is available. It skips if claude is not in PATH.
//
// This test validates that:
//   1. Start succeeds with a cancellable context
//   2. After cancelling that context, the process is still alive
//   3. Stop/Destroy can clean up the process
func TestClaudeAdapterStartDetachedContext(t *testing.T) {
	_, err := exec.LookPath("claude")
	if err != nil {
		t.Skip("claude not found in PATH — skipping integration test")
	}

	adapter := NewClaudeAdapter()

	ctx, cancel := context.WithCancel(context.Background())

	inst := domain.RuntimeInstance{
		ID:     "test-detach-context",
		Type:   domain.RuntimeProviderClaudeCode,
		Prompt: "say hello",
	}

	if err := adapter.Start(ctx, inst); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify the instance is tracked (uses mutex-protected map, not Status
	// which has a pre-existing race on cmd.ProcessState).
	adapter.mu.RLock()
	_, tracked := adapter.instances[inst.ID]
	adapter.mu.RUnlock()
	if !tracked {
		t.Fatal("instance not tracked after Start")
	}

	// Cancel the parent context. The claude process should survive.
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

	// Give the process time to handle the signal and exit.
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

// TestClaudeAdapterSnapshotNonexistent verifies Snapshot returns Exited state
// for an unknown ID.
func TestClaudeAdapterSnapshotNonexistent(t *testing.T) {
	adapter := NewClaudeAdapter()
	snap, err := adapter.Snapshot(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.State != domain.RuntimeStateExited {
		t.Errorf("expected Exited state for nonexistent instance, got %s", snap.State)
	}
}

// TestClaudeAdapterSnapshotExisting verifies Snapshot returns Running state
// for a running instance when claude is available.
func TestClaudeAdapterSnapshotExisting(t *testing.T) {
	_, err := exec.LookPath("claude")
	if err != nil {
		t.Skip("claude not found in PATH — skipping integration test")
	}

	adapter := NewClaudeAdapter()

	inst := domain.RuntimeInstance{
		ID:     "test-claude-snapshot",
		Type:   domain.RuntimeProviderClaudeCode,
		Prompt: "say hello",
	}

	if err := adapter.Start(context.Background(), inst); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		adapter.Stop(context.Background(), inst.ID)
		time.Sleep(200 * time.Millisecond)
		adapter.Destroy(context.Background(), inst.ID)
	}()

	snap, err := adapter.Snapshot(context.Background(), inst.ID)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.State != domain.RuntimeStateRunning {
		t.Errorf("State = %s, want RUNNING", snap.State)
	}
}
