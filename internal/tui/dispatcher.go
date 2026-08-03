package tui

import (
	"sync"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// InternalEvent is a marker interface for events dispatched within the TUI.
type InternalEvent interface {
	internalEventMarker()
}

// NodeChangedEvent is emitted when a node's state changes.
type NodeChangedEvent struct {
	NodeID    string
	NewStatus string
}

func (NodeChangedEvent) internalEventMarker() {}

// EventAppendedEvent is emitted when a new timeline item arrives from gRPC.
type EventAppendedEvent struct {
	Item TimelineItem
}

func (EventAppendedEvent) internalEventMarker() {}

// WorkflowUpdatedEvent is emitted after a snapshot reconciliation.
type WorkflowUpdatedEvent struct {
	Status  string
	Version uint64
	Nodes   []*arlov1.NodeState
}

func (WorkflowUpdatedEvent) internalEventMarker() {}

// ReconnectedEvent is emitted when the gRPC stream reconnects.
type ReconnectedEvent struct{}

func (ReconnectedEvent) internalEventMarker() {}

// Subscriber is a channel that receives InternalEvents.
type Subscriber chan InternalEvent

// Dispatcher is an internal pub/sub bus for TUI panels.
type Dispatcher struct {
	mu          sync.RWMutex
	subscribers map[Subscriber]struct{}
}

// NewDispatcher creates a new event dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		subscribers: make(map[Subscriber]struct{}),
	}
}

// Subscribe registers a new subscriber channel.
func (d *Dispatcher) Subscribe() Subscriber {
	d.mu.Lock()
	defer d.mu.Unlock()
	ch := make(Subscriber, 32)
	d.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a subscriber channel.
func (d *Dispatcher) Unsubscribe(ch Subscriber) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.subscribers, ch)
	close(ch)
}

// Emit sends an event to all subscribers. Non-blocking — slow subscribers are skipped.
func (d *Dispatcher) Emit(event InternalEvent) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	for ch := range d.subscribers {
		select {
		case ch <- event:
		default:
			// Drop event if subscriber buffer is full (non-blocking).
		}
	}
}
