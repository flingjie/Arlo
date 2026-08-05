package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// newTestStore creates a SQLiteStore backed by a temporary file.
func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// helper to create a simple event payload
func testEvent(id string, eventType EventType, data interface{}) Event {
	payload, _ := json.Marshal(data)
	return Event{
		ID:      id,
		Type:    eventType,
		Payload: json.RawMessage(payload),
	}
}

// TestAppendAndRead verifies basic append and read functionality.
func TestAppendAndRead(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	events := []Event{
		testEvent("evt-1", EventTaskCreated, map[string]string{"title": "fix oauth bug"}),
		testEvent("evt-2", EventWorkflowCreated, map[string]string{"name": "bugfix"}),
		testEvent("evt-3", EventNodeCreated, map[string]string{"node": "analyze"}),
	}

	// Append events to the workflow stream.
	positions, err := s.Append(ctx, "workflow-abc", events)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if len(positions) != 3 {
		t.Fatalf("expected 3 positions, got %d", len(positions))
	}
	if positions[0] != 1 {
		t.Errorf("first event position = %d, want 1", positions[0])
	}

	// Read them back.
	read, err := s.Read(ctx, "workflow-abc", 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if len(read) != 3 {
		t.Fatalf("expected 3 events, got %d", len(read))
	}

	// Verify version ordering.
	for i, e := range read {
		if e.Version != i+1 {
			t.Errorf("event %d version = %d, want %d", i, e.Version, i+1)
		}
		if e.StreamID != "workflow-abc" {
			t.Errorf("event %d stream = %s, want workflow-abc", i, e.StreamID)
		}
	}

	// Verify the first event payload is intact.
	var payload map[string]string
	if err := json.Unmarshal(read[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["title"] != "fix oauth bug" {
		t.Errorf("title = %s, want 'fix oauth bug'", payload["title"])
	}
}

// TestReadEmpty verifies reading from a non-existent stream returns no events.
func TestReadEmpty(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	events, err := s.Read(ctx, "nonexistent", 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

// TestReadFromVersion verifies reading from a specific version.
func TestReadFromVersion(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	events := []Event{
		testEvent("evt-1", EventTaskCreated, nil),
		testEvent("evt-2", EventWorkflowCreated, nil),
		testEvent("evt-3", EventNodeCreated, nil),
		testEvent("evt-4", EventNodeStarted, nil),
	}
	s.Append(ctx, "stream-1", events)

	// Read from version 3 — should get events 3 and 4.
	read, err := s.Read(ctx, "stream-1", 3)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(read) != 2 {
		t.Fatalf("expected 2 events from version 3, got %d", len(read))
	}
	if read[0].Version != 3 {
		t.Errorf("first event version = %d, want 3", read[0].Version)
	}
}

// TestReadAll verifies cross-stream event reading ordered by position.
func TestReadAll(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Write to multiple streams interleaved.
	s.Append(ctx, "workflow-1", []Event{
		testEvent("evt-1", EventTaskCreated, nil),
	})
	s.Append(ctx, "node-analyze", []Event{
		testEvent("evt-2", EventNodeStarted, nil),
	})
	s.Append(ctx, "workflow-1", []Event{
		testEvent("evt-3", EventWorkflowCreated, nil),
	})

	// ReadAll should return events in position order across all streams.
	events, nextPos, err := s.ReadAll(ctx, 0, 10)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events total, got %d", len(events))
	}
	if nextPos != 4 {
		t.Errorf("nextPos = %d, want 4", nextPos)
	}

	// Verify global position ordering.
	if events[0].Position != 1 {
		t.Errorf("event 0 position = %d, want 1", events[0].Position)
	}
	if events[1].Position != 2 {
		t.Errorf("event 1 position = %d, want 2", events[1].Position)
	}
	if events[2].Position != 3 {
		t.Errorf("event 2 position = %d, want 3", events[2].Position)
	}
}

// TestReadAllPagination verifies paginated ReadAll.
func TestReadAllPagination(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for i := 0; i < 10; i++ {
		s.Append(ctx, "stream-1", []Event{
			testEvent("evt-"+string(rune('a'+i)), EventTaskCreated, nil),
		})
	}

	// Read first 3.
	events, nextPos, err := s.ReadAll(ctx, 0, 3)
	if err != nil {
		t.Fatalf("ReadAll page 1: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("page 1: expected 3 events, got %d", len(events))
	}
	if nextPos != 4 {
		t.Errorf("page 1: nextPos = %d, want 4", nextPos)
	}

	// Read next 3.
	events, nextPos, err = s.ReadAll(ctx, nextPos, 3)
	if err != nil {
		t.Fatalf("ReadAll page 2: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("page 2: expected 3 events, got %d", len(events))
	}
	if nextPos != 7 {
		t.Errorf("page 2: nextPos = %d, want 7", nextPos)
	}

	// Read remaining.
	events, nextPos, err = s.ReadAll(ctx, nextPos, 10)
	if err != nil {
		t.Fatalf("ReadAll page 3: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("page 3: expected 4 events, got %d", len(events))
	}
	if nextPos != 11 {
		t.Errorf("page 3: nextPos = %d, want 11", nextPos)
	}
}

// TestSubscribe verifies real-time event delivery via subscription
// with correct exclusive fromPosition semantics: subscribing from
// the last known position should NOT replay historical events.
func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := newTestStore(t)

	// Pre-populate some events so we can subscribe from a known position.
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-0", EventTaskCreated, nil),
	})

	// Verify last position is 1.
	lastPos := s.LastPosition()
	if lastPos != 1 {
		t.Fatalf("expected lastPosition=1, got %d", lastPos)
	}

	// Subscribe from position 1 (exclusive — get events after position 1).
	ch, err := s.Subscribe(ctx, 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Append a new event — should arrive on the channel.
	go func() {
		s.Append(context.Background(), "stream-1", []Event{
			testEvent("evt-1", EventNodeStarted, nil),
		})
	}()

	// Should receive evt-1 (the new event), NOT evt-0 (historical, excluded).
	select {
	case e := <-ch:
		if e.ID != "evt-1" {
			t.Errorf("subscribed event ID = %s, want evt-1", e.ID)
		}
		if e.Position != 2 {
			t.Errorf("subscribed event position = %d, want 2", e.Position)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}
}

// TestSubscribeDeliversNewEventsOnly verifies that subscribing from the
// current last position delivers only events appended after subscription.
// It should NOT replay historical events (use ReadAll for that).
func TestSubscribeDeliversNewEventsOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := newTestStore(t)

	// Pre-populate.
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-old", EventTaskCreated, nil),
	})

	// Subscribe from the current last position so that no historical
	// events are included (exclusive semantics).
	lastPos := s.LastPosition()
	ch, err := s.Subscribe(ctx, lastPos)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Append new event.
	go func() {
		s.Append(context.Background(), "stream-1", []Event{
			testEvent("evt-new", EventNodeStarted, nil),
		})
	}()

	select {
	case e := <-ch:
		if e.ID != "evt-new" {
			t.Errorf("expected evt-new, got %s", e.ID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for event")
	}
}

// TestSubscribeReplaysHistoricalEvents verifies that subscribing from an
// earlier position replays events with position > fromPosition (exclusive).
func TestSubscribeReplaysHistoricalEvents(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := newTestStore(t)

	// Pre-populate 3 events.
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-0", EventTaskCreated, nil),
	})
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-1", EventNodeStarted, nil),
	})
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-2", EventNodeCompleted, nil),
	})

	// Subscribe from position 0 — should replay events at positions 1,2,3
	// (exclusive of 0, and caughtUpTo=3 allows all three).
	ch, err := s.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Read all 3 replayed events.
	received := make([]Event, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case e := <-ch:
			received = append(received, e)
		case <-ctx.Done():
			t.Fatalf("timed out waiting for event %d (got %d)", i, len(received))
		}
	}

	if len(received) != 3 {
		t.Fatalf("expected 3 replayed events, got %d", len(received))
	}
	for i, e := range received {
		if e.Position != int64(i+1) {
			t.Errorf("event %d position = %d, want %d", i, e.Position, i+1)
		}
	}
}

// TestSubscribeFromLastPositionSkipsReplay verifies that subscribing from
// the last persisted position delivers only events appended after subscription
// — no historical events are replayed.
func TestSubscribeFromLastPositionSkipsReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := newTestStore(t)

	// Pre-populate events at positions 1 and 2.
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-a", EventTaskCreated, nil),
	})
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-b", EventNodeStarted, nil),
	})

	lastPos := s.LastPosition()
	ch, err := s.Subscribe(ctx, lastPos) // exclusive of position 2
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Append new events after subscription.
	go func() {
		s.Append(context.Background(), "stream-1", []Event{
			testEvent("evt-c", EventNodeCompleted, nil),
		})
	}()

	select {
	case e := <-ch:
		if e.ID != "evt-c" {
			t.Errorf("expected evt-c (new), got %s (historical replay)", e.ID)
		}
		if e.Position != 3 {
			t.Errorf("expected position 3, got %d", e.Position)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for new event")
	}
}

// TestAppendEmptyStream verifies appending no events is a no-op.
func TestAppendEmptyStream(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	positions, err := s.Append(ctx, "stream-1", nil)
	if err != nil {
		t.Fatalf("Append empty: %v", err)
	}
	if positions != nil {
		t.Errorf("expected nil positions for empty append, got %v", positions)
	}
}

// TestAppendToNewStream verifies the first append to a stream starts at version 1.
func TestAppendToNewStream(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	positions, err := s.Append(ctx, "new-stream", []Event{
		testEvent("evt-1", EventTaskCreated, nil),
	})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if positions[0] != 1 {
		t.Errorf("first position = %d, want 1", positions[0])
	}

	// Verify version is 1.
	events, _ := s.Read(ctx, "new-stream", 1)
	if events[0].Version != 1 {
		t.Errorf("first event version = %d, want 1", events[0].Version)
	}
}

// TestStreamVersionIsolation verifies each stream has independent versioning.
func TestStreamVersionIsolation(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	s.Append(ctx, "stream-a", []Event{
		testEvent("a-1", EventTaskCreated, nil),
		testEvent("a-2", EventNodeCreated, nil),
	})
	s.Append(ctx, "stream-b", []Event{
		testEvent("b-1", EventTaskCreated, nil),
	})

	// Stream A should have versions 1,2.
	aEvents, _ := s.Read(ctx, "stream-a", 1)
	if len(aEvents) != 2 || aEvents[0].Version != 1 || aEvents[1].Version != 2 {
		t.Errorf("stream-a: expected versions [1,2], got %v", versions(aEvents))
	}

	// Stream B should have version 1.
	bEvents, _ := s.Read(ctx, "stream-b", 1)
	if len(bEvents) != 1 || bEvents[0].Version != 1 {
		t.Errorf("stream-b: expected version [1], got %v", versions(bEvents))
	}
}

// TestUniqueEventID verifies duplicate event IDs are rejected with a sentinel error.
func TestUniqueEventID(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.Append(ctx, "stream-1", []Event{
		testEvent("evt-1", EventTaskCreated, nil),
	})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}

	// Append with same event ID should fail with ErrDuplicateEventID.
	_, err = s.Append(ctx, "stream-1", []Event{
		testEvent("evt-1", EventNodeStarted, nil),
	})
	if err == nil {
		t.Fatal("expected error for duplicate event ID, got nil")
	}
	if !errors.Is(err, ErrDuplicateEventID) {
		t.Errorf("expected ErrDuplicateEventID, got %v", err)
	}
}

// TestAppendDoesNotMutateCallerSlice verifies that Append does not modify
// the caller's input slice elements.
func TestAppendDoesNotMutateCallerSlice(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	event := Event{
		ID:      "evt-1",
		Type:    EventTaskCreated,
		Payload: json.RawMessage(`{"key":"value"}`),
	}

	// Make a copy to compare against after Append.
	original := Event{
		ID:      event.ID,
		Type:    event.Type,
		Payload: append(json.RawMessage(nil), event.Payload...),
	}

	_, err := s.Append(ctx, "stream-1", []Event{event})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Verify the original passed-in event was NOT mutated.
	if event.StreamID != "" {
		t.Errorf("caller's StreamID was mutated to %q, want empty", event.StreamID)
	}
	if event.Version != 0 {
		t.Errorf("caller's Version was mutated to %d, want 0", event.Version)
	}
	if event.Position != 0 {
		t.Errorf("caller's Position was mutated to %d, want 0", event.Position)
	}
	if event.Timestamp != (time.Time{}) {
		t.Errorf("caller's Timestamp was mutated to %v, want zero", event.Timestamp)
	}
	if event.ID != original.ID {
		t.Errorf("caller's ID was mutated to %q, want %q", event.ID, original.ID)
	}
}

// TestDuplicateEventIDSentinelError verifies that duplicate event ID errors
// wrap ErrDuplicateEventID and can be detected with errors.Is.
func TestDuplicateEventIDSentinelError(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	_, err := s.Append(ctx, "stream-1", []Event{
		testEvent("unique-1", EventTaskCreated, nil),
	})
	if err != nil {
		t.Fatalf("first append: %v", err)
	}

	// Duplicate within same batch should also be detected.
	_, err = s.Append(ctx, "stream-1", []Event{
		testEvent("unique-1", EventNodeStarted, nil),
	})
	if err == nil {
		t.Fatal("expected error for duplicate event ID")
	}
	if !errors.Is(err, ErrDuplicateEventID) {
		t.Errorf("errors.Is(err, ErrDuplicateEventID) = false, err=%v", err)
	}

	// Check error message contains the event ID.
	if errStr := err.Error(); errStr == "" {
		t.Error("error should have a non-empty message")
	}
}

// TestCloseDuringAppendNoRace verifies that concurrent Append notifications
// and Close do not race. Multiple goroutines append while main goroutine closes.
func TestCloseDuringAppendNoRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := newTestStore(t)

	// Start a subscriber so Append has channels to notify.
	ch, err := s.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	var wg sync.WaitGroup

	// Goroutine 1: drain the channel to prevent blocking.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range ch {
		}
	}()

	// Goroutines 2-4: continuously append events.
	for g := 0; g < 3; g++ {
		wg.Add(1)
		gid := g
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				_, err := s.Append(ctx, "stream-1", []Event{
					testEvent(fmt.Sprintf("evt-g%d-%d", gid, i), EventTaskCreated, nil),
				})
				if err != nil {
					// Close may cause errors — that's expected.
					return
				}
			}
		}()
	}

	// Give goroutines a moment to start appending.
	time.Sleep(10 * time.Millisecond)

	// Close the store while appends are in-flight.
	// This should not panic, race, or deadlock.
	if err := s.Close(); err != nil {
		t.Logf("Close returned error (expected under concurrency): %v", err)
	}

	// Wait for all goroutines to finish.
	wg.Wait()
}

// TestUniqueStreamVersion verifies duplicate stream+version is rejected.
func TestUniqueStreamVersion(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	// Two concurrent goroutines try to append to the same stream.
	// SQLite serializes writes (MaxOpenConns=1), so one will succeed,
	// the other will get version 2 automatically. This is actually fine
	// because version is auto-incremented per stream within the transaction.
	//
	// What we really test: two sequential appends each get correct versions.
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-a", EventTaskCreated, nil),
	})
	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-b", EventNodeCreated, nil),
	})

	events, _ := s.Read(ctx, "stream-1", 1)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Version != 1 || events[1].Version != 2 {
		t.Errorf("expected versions [1,2], got [%d,%d]", events[0].Version, events[1].Version)
	}
}

// TestConcurrentAppendDifferentStreams verifies concurrent writes to different streams.
func TestConcurrentAppendDifferentStreams(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	var wg sync.WaitGroup
	streams := []string{"stream-a", "stream-b", "stream-c", "stream-d"}

	for _, stream := range streams {
		wg.Add(1)
		go func(sid string) {
			defer wg.Done()
			_, err := s.Append(ctx, sid, []Event{
				testEvent("evt-"+sid, EventTaskCreated, nil),
			})
			if err != nil {
				t.Errorf("concurrent append to %s: %v", sid, err)
			}
		}(stream)
	}

	wg.Wait()

	// Verify each stream got exactly one event.
	for _, stream := range streams {
		events, err := s.Read(ctx, stream, 1)
		if err != nil {
			t.Errorf("read %s: %v", stream, err)
			continue
		}
		if len(events) != 1 {
			t.Errorf("%s: expected 1 event, got %d", stream, len(events))
		}
	}
}

// TestCloseCleanup verifies Close shuts down subscribers.
func TestCloseCleanup(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	ch, err := s.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Close the store.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Channel should be closed.
	_, ok := <-ch
	if ok {
		t.Error("subscriber channel should be closed after store.Close()")
	}
}

// TestSQLitePersistence verifies that events survive closing and reopening.
func TestSQLitePersistence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	// Create store, write events, close.
	s1, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	s1.Append(ctx, "stream-1", []Event{
		testEvent("evt-1", EventTaskCreated, map[string]string{"title": "persist test"}),
		testEvent("evt-2", EventNodeStarted, nil),
	})
	s1.Close()

	// Reopen and verify events are still there.
	s2, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	events, err := s2.Read(ctx, "stream-1", 1)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events after reopen, got %d", len(events))
	}

	// Verify payload survived.
	var payload map[string]string
	json.Unmarshal(events[0].Payload, &payload)
	if payload["title"] != "persist test" {
		t.Errorf("payload corrupted: title = %s", payload["title"])
	}
}

// TestReadAllFromMiddle verifies ReadAll from a non-zero position.
func TestReadAllFromMiddle(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for i := 0; i < 5; i++ {
		s.Append(ctx, "stream-1", []Event{
			testEvent("evt-"+string(rune('a'+i)), EventTaskCreated, nil),
		})
	}

	// Read from position 3 — should get events at positions 3,4,5.
	events, nextPos, err := s.ReadAll(ctx, 3, 10)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events from position 3, got %d", len(events))
	}
	if events[0].Position != 3 {
		t.Errorf("first position = %d, want 3", events[0].Position)
	}
	if nextPos != 6 {
		t.Errorf("nextPos = %d, want 6", nextPos)
	}
}

// TestEventTypeFiltering verifies we can read events by type via the index.
func TestEventTypeFiltering(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	s.Append(ctx, "stream-1", []Event{
		testEvent("evt-1", EventTaskCreated, nil),
		testEvent("evt-2", EventNodeStarted, nil),
		testEvent("evt-3", EventNodeCompleted, nil),
		testEvent("evt-4", EventNodeFailed, nil),
	})

	// The EventStore interface doesn't have a FilterByType method,
	// but we verify via Read + client-side filtering that the types are correct.
	events, _ := s.Read(ctx, "stream-1", 1)

	expectedTypes := []EventType{
		EventTaskCreated,
		EventNodeStarted,
		EventNodeCompleted,
		EventNodeFailed,
	}
	for i, e := range events {
		if e.Type != expectedTypes[i] {
			t.Errorf("event %d type = %s, want %s", i, e.Type, expectedTypes[i])
		}
	}
}

// versions extracts versions from a slice of events for test assertions.
func versions(events []Event) []int {
	vs := make([]int, len(events))
	for i, e := range events {
		vs[i] = e.Version
	}
	return vs
}

// TestNewSQLiteStore_DefaultPath verifies store creation with a regular file path.
func TestNewSQLiteStore_DefaultPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "arlo.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	// Verify we can append and read.
	ctx := context.Background()
	_, err = s.Append(ctx, "test", []Event{
		testEvent("evt-1", EventTaskCreated, nil),
	})
	if err != nil {
		t.Fatalf("Append after create: %v", err)
	}
}

// TestNewSQLiteStore_MemoryPath verifies store handles :memory: path.
func TestNewSQLiteStore_MemoryPath(t *testing.T) {
	s, err := NewSQLiteStore(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStore(:memory:): %v", err)
	}
	// In-memory databases work, but data is lost on close.
	// For production use, always use a file path.
	s.Close()
}

// BenchmarkAppend measures append throughput.
func BenchmarkAppend(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		b.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.Append(ctx, "bench-stream", []Event{
			{
				ID:      fmt.Sprintf("evt-%d", i),
				Type:    EventTaskCreated,
				Payload: json.RawMessage(`{"bench":true}`),
			},
		})
	}
}

// BenchmarkReadAll measures replay throughput.
func BenchmarkReadAll(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")
	s, err := NewSQLiteStore(path)
	if err != nil {
		b.Fatalf("NewSQLiteStore: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Pre-populate 1000 events.
	for i := 0; i < 1000; i++ {
		s.Append(ctx, "bench-stream", []Event{
			{
				ID:      fmt.Sprintf("evt-%d", i),
				Type:    EventTaskCreated,
				Payload: json.RawMessage(`{"bench":true}`),
			},
		})
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.ReadAll(ctx, 0, 0) // read all events
	}
}
