package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// subscriber wraps a subscriber channel with safe concurrent send/close.
type subscriber struct {
	ch     chan Event
	mu     sync.Mutex
	closed bool
}

// send delivers an event to the subscriber. Returns false if the subscriber is closed.
func (s *subscriber) send(e Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.ch <- e:
	default:
		// Channel full, drop event to avoid blocking the writer.
	}
	return true
}

// close safely closes the subscriber channel. Safe to call multiple times.
func (s *subscriber) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
}

// SQLiteStore implements EventStore using SQLite with WAL mode.
// It is the v0.1 event store — simple, embedded, zero-configuration.
type SQLiteStore struct {
	db *sql.DB

	mu          sync.RWMutex
	subscribers map[int64]*subscriber
	nextSubID   int64
	lastPosition int64
}

// SQLiteStoreOption configures the SQLite event store.
type SQLiteStoreOption func(*SQLiteStore)

// NewSQLiteStore opens (or creates) a SQLite database at the given path.
// It enables WAL mode and creates the events table if it doesn't exist.
func NewSQLiteStore(path string, opts ...SQLiteStoreOption) (*SQLiteStore, error) {
	dsn := path + "?_journal_mode=WAL&_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	// Configure connection pool. SQLite only supports one writer at a time,
	// but WAL mode allows concurrent readers.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}

	s := &SQLiteStore{
		db:          db,
		subscribers: make(map[int64]*subscriber),
	}

	for _, opt := range opts {
		opt(s)
	}

	if err := s.migrate(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	// Determine the current max position.
	if err := db.QueryRow("SELECT COALESCE(MAX(position), 0) FROM events").Scan(&s.lastPosition); err != nil {
		db.Close()
		return nil, fmt.Errorf("read max position: %w", err)
	}

	return s, nil
}

// migrate creates the events table and indexes if they don't exist.
func (s *SQLiteStore) migrate(ctx context.Context) error {
	query := `
	CREATE TABLE IF NOT EXISTS events (
		position   INTEGER PRIMARY KEY AUTOINCREMENT,
		stream_id  TEXT NOT NULL,
		version    INTEGER NOT NULL,
		event_id   TEXT NOT NULL UNIQUE,
		event_type TEXT NOT NULL,
		payload    BLOB NOT NULL,
		timestamp  TEXT NOT NULL DEFAULT (datetime('now')),
		UNIQUE(stream_id, version)
	);

	CREATE INDEX IF NOT EXISTS idx_events_type
		ON events(event_type);
	`

	_, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create tables: %w", err)
	}
	return nil
}

// Append writes events atomically to a stream.
// Events are assigned auto-incremented version numbers within the stream.
// The caller's input slice is never modified.
func (s *SQLiteStore) Append(ctx context.Context, streamID string, events []Event) ([]int64, error) {
	if len(events) == 0 {
		return nil, nil
	}

	s.mu.Lock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Get the current max version for this stream.
	var maxVersion int
	err = tx.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
		streamID,
	).Scan(&maxVersion)
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("query max version: %w", err)
	}

	now := time.Now().UTC()
	timestampStr := now.Format(time.RFC3339)
	positions := make([]int64, len(events))

	// Build copies of events to avoid mutating caller's slice.
	appended := make([]Event, len(events))

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO events (stream_id, version, event_id, event_type, payload, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for i := range events {
		version := maxVersion + i + 1

		payload, err := json.Marshal(events[i].Payload)
		if err != nil {
			s.mu.Unlock()
			return nil, fmt.Errorf("marshal payload for event %s: %w", events[i].ID, err)
		}

		result, err := stmt.ExecContext(ctx,
			streamID,
			version,
			events[i].ID,
			string(events[i].Type),
			payload,
			timestampStr,
		)
		if err != nil {
			s.mu.Unlock()
			if isUniqueConstraintError(err) {
				return nil, fmt.Errorf("insert event %s (version %d): %w", events[i].ID, version, ErrDuplicateEventID)
			}
			return nil, fmt.Errorf("insert event %s (version %d): %w", events[i].ID, version, err)
		}

		pos, _ := result.LastInsertId()
		positions[i] = pos

		// Build the appended event copy with store-assigned fields.
		appended[i] = Event{
			ID:        events[i].ID,
			Type:      events[i].Type,
			Payload:   events[i].Payload,
			StreamID:  streamID,
			Version:   version,
			Position:  pos,
			Timestamp: now,
		}
	}

	if err := tx.Commit(); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("commit: %w", err)
	}

	s.lastPosition = positions[len(positions)-1]
	s.mu.Unlock()

	// Notify subscribers outside the main lock to avoid deadlock.
	s.mu.RLock()
	subs := make(map[int64]*subscriber, len(s.subscribers))
	for id, sub := range s.subscribers {
		subs[id] = sub
	}
	s.mu.RUnlock()

	for i := range appended {
		for _, sub := range subs {
			sub.send(appended[i])
		}
	}

	return positions, nil
}

// isUniqueConstraintError checks if a SQLite error is a UNIQUE constraint violation.
func isUniqueConstraintError(err error) bool {
	return strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// Read reads events from a stream starting at fromVersion (inclusive).
func (s *SQLiteStore) Read(ctx context.Context, streamID string, fromVersion int) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT position, stream_id, version, event_id, event_type, payload, timestamp
		 FROM events
		 WHERE stream_id = ? AND version >= ?
		 ORDER BY version ASC`,
		streamID, fromVersion,
	)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// ReadAll reads events across all streams starting at fromPosition (inclusive).
// Returns up to limit events and the next position to read from.
// A limit of 0 means no limit (use with caution).
func (s *SQLiteStore) ReadAll(ctx context.Context, fromPosition int64, limit int) ([]Event, int64, error) {
	query := `SELECT position, stream_id, version, event_id, event_type, payload, timestamp
		 FROM events
		 WHERE position >= ?
		 ORDER BY position ASC`
	args := []interface{}{fromPosition}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query all events: %w", err)
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil {
		return nil, 0, err
	}

	nextPos := fromPosition
	if len(events) > 0 {
		nextPos = events[len(events)-1].Position + 1
	}

	return events, nextPos, nil
}

// Subscribe returns a channel that receives new events as they are appended.
// The channel is closed when ctx is cancelled or the store is closed.
// Events are delivered starting from the event after fromPosition (exclusive).
// Historical events from fromPosition+1 onward are replayed first, then live
// events are delivered as they are appended.
func (s *SQLiteStore) Subscribe(ctx context.Context, fromPosition int64) (<-chan Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub := &subscriber{
		ch: make(chan Event, 256), // buffered to avoid blocking writers
	}
	id := s.nextSubID
	s.nextSubID++
	s.subscribers[id] = sub

	// Clean up when context is done.
	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.subscribers, id)
		s.mu.Unlock()
		sub.close()
	}()

	// Snapshot lastPosition BEFORE replaying so events appended during replay
	// are delivered only once — via the Append notification path.
	caughtUpTo := s.lastPosition

	go func() {
		// Start replay from fromPosition+1 because fromPosition is exclusive.
		pos := fromPosition + 1
		for {
			events, nextPos, err := s.ReadAll(ctx, pos, 1000)
			if err != nil || len(events) == 0 {
				return
			}
			for _, e := range events {
				if e.Position > caughtUpTo {
					// This event was appended after subscription — it will
					// arrive via the Append notification channel.
					// Stop replaying to avoid duplicates.
					return
				}
				select {
				case sub.ch <- e:
				case <-ctx.Done():
					return
				}
			}
			pos = nextPos
		}
	}()

	return sub.ch, nil
}

// LastPosition returns the current max global position.
func (s *SQLiteStore) LastPosition() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastPosition
}

// Close gracefully shuts down the event store.
func (s *SQLiteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all subscriber channels.
	for _, sub := range s.subscribers {
		sub.close()
	}
	s.subscribers = make(map[int64]*subscriber)

	return s.db.Close()
}

// scanEvents reads events from database rows into Event structs.
func scanEvents(rows *sql.Rows) ([]Event, error) {
	var events []Event
	for rows.Next() {
		var e Event
		var timestamp string
		var payload []byte

		err := rows.Scan(&e.Position, &e.StreamID, &e.Version, &e.ID, &e.Type, &payload, &timestamp)
		if err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}

		e.Payload = json.RawMessage(payload)
		e.Timestamp, err = time.Parse(time.RFC3339, timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse timestamp for event %s: %w", e.ID, err)
		}
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	return events, nil
}
