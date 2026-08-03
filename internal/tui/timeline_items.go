package tui

import (
	"fmt"
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// Level represents the severity of a timeline item.
type Level int

const (
	INFO Level = iota
	WARN
	ERROR
	DEBUG
)

func (l Level) String() string {
	switch l {
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case DEBUG:
		return "DEBUG"
	default:
		return "INFO"
	}
}

// Color returns the lipgloss color code for this level.
func (l Level) Color() string {
	switch l {
	case INFO:
		return "42"
	case WARN:
		return "226"
	case ERROR:
		return "196"
	case DEBUG:
		return "244"
	default:
		return "42"
	}
}

// TimelineItem is the interface for items displayed in the timeline panel.
type TimelineItem interface {
	Time() time.Time
	Level() Level
	Render() string
}

// NodeStartedItem represents a NODE_STARTED event.
type NodeStartedItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeStartedItem) Time() time.Time { return i.Timestamp }
func (i NodeStartedItem) Level() Level    { return INFO }
func (i NodeStartedItem) Render() string {
	return fmt.Sprintf("%s started", i.NodeID)
}

// NodeCompletedItem represents a NODE_COMPLETED event.
type NodeCompletedItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeCompletedItem) Time() time.Time { return i.Timestamp }
func (i NodeCompletedItem) Level() Level    { return INFO }
func (i NodeCompletedItem) Render() string {
	return fmt.Sprintf("%s completed", i.NodeID)
}

// NodeFailedItem represents a NODE_FAILED event.
type NodeFailedItem struct {
	Timestamp time.Time
	NodeID    string
	Reason    string
}

func (i NodeFailedItem) Time() time.Time { return i.Timestamp }
func (i NodeFailedItem) Level() Level    { return ERROR }
func (i NodeFailedItem) Render() string {
	return fmt.Sprintf("%s failed: %s", i.NodeID, i.Reason)
}

// NodeWaitingItem represents a NODE_WAITING event.
type NodeWaitingItem struct {
	Timestamp time.Time
	NodeID    string
	Reason    string
}

func (i NodeWaitingItem) Time() time.Time { return i.Timestamp }
func (i NodeWaitingItem) Level() Level    { return WARN }
func (i NodeWaitingItem) Render() string {
	return fmt.Sprintf("%s waiting: %s", i.NodeID, i.Reason)
}

// GenericEventItem wraps a gRPC event into a timeline item.
type GenericEventItem struct {
	Timestamp time.Time
	EventType string
}

func (i GenericEventItem) Time() time.Time { return i.Timestamp }
func (i GenericEventItem) Level() Level {
	switch i.EventType {
	case "NODE_FAILED", "TASK_FAILED":
		return ERROR
	case "NODE_WAITING":
		return WARN
	default:
		return INFO
	}
}
func (i GenericEventItem) Render() string {
	return i.EventType
}

// EventToItem converts a gRPC event to a more specific TimelineItem when possible.
func EventToItem(event *arlov1.Event) TimelineItem {
	t, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		t = time.Now()
	}

	switch event.Type {
	case "NODE_STARTED":
		return NodeStartedItem{Timestamp: t}
	case "NODE_COMPLETED":
		return NodeCompletedItem{Timestamp: t}
	case "NODE_FAILED":
		return NodeFailedItem{Timestamp: t}
	case "NODE_WAITING":
		return NodeWaitingItem{Timestamp: t}
	default:
		return GenericEventItem{Timestamp: t, EventType: event.Type}
	}
}
