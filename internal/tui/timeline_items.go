package tui

import (
	"encoding/json"
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

// NodeAnnotatedItem represents a NODE_ANNOTATED event.
type NodeAnnotatedItem struct {
	Timestamp time.Time
	NodeID    string
	Key       string
	Value     string
}

func (i NodeAnnotatedItem) Time() time.Time { return i.Timestamp }
func (i NodeAnnotatedItem) Level() Level    { return INFO }
func (i NodeAnnotatedItem) Render() string {
	return fmt.Sprintf("%s annotated: %s=%s", i.NodeID, i.Key, i.Value)
}

// NodeHeartbeatItem represents a NODE_HEARTBEAT event.
type NodeHeartbeatItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeHeartbeatItem) Time() time.Time { return i.Timestamp }
func (i NodeHeartbeatItem) Level() Level    { return DEBUG }
func (i NodeHeartbeatItem) Render() string {
	return fmt.Sprintf("%s heartbeat", i.NodeID)
}

// MetricsSnapshotItem represents a METRICS_SNAPSHOT event.
type MetricsSnapshotItem struct {
	Timestamp time.Time
	NodeID    string
	TokensIn  int64
	TokensOut int64
	CostUSD   float64
}

func (i MetricsSnapshotItem) Time() time.Time { return i.Timestamp }
func (i MetricsSnapshotItem) Level() Level    { return INFO }
func (i MetricsSnapshotItem) Render() string {
	return fmt.Sprintf("%s metrics: %d↑/%d↓ tokens, $%.4f", i.NodeID, i.TokensIn, i.TokensOut, i.CostUSD)
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
	case "NODE_HEARTBEAT":
		return DEBUG
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

	// Extract node ID from stream or payload.
	nodeID := extractNodeID(event)

	switch event.Type {
	case "NODE_STARTED":
		return NodeStartedItem{Timestamp: t, NodeID: nodeID}
	case "NODE_COMPLETED":
		return NodeCompletedItem{Timestamp: t, NodeID: nodeID}
	case "NODE_FAILED":
		return NodeFailedItem{Timestamp: t, NodeID: nodeID}
	case "NODE_WAITING":
		return NodeWaitingItem{Timestamp: t, NodeID: nodeID}
	case "NODE_ANNOTATED":
		key, val := extractAnnotation(event)
		return NodeAnnotatedItem{Timestamp: t, NodeID: nodeID, Key: key, Value: val}
	case "NODE_HEARTBEAT":
		return NodeHeartbeatItem{Timestamp: t, NodeID: nodeID}
	case "METRICS_SNAPSHOT":
		tokensIn, tokensOut, cost := extractMetrics(event)
		return MetricsSnapshotItem{Timestamp: t, NodeID: nodeID, TokensIn: tokensIn, TokensOut: tokensOut, CostUSD: cost}
	default:
		return GenericEventItem{Timestamp: t, EventType: event.Type}
	}
}

// extractNodeID extracts the node ID from the event's StreamId.
// StreamId format: "node-{nodeID}".
func extractNodeID(event *arlov1.Event) string {
	sid := event.StreamId
	if len(sid) > 5 && sid[:5] == "node-" {
		return sid[5:]
	}
	return ""
}

// extractAnnotation extracts key/value from a NODE_ANNOTATED event payload.
func extractAnnotation(event *arlov1.Event) (string, string) {
	// Payload is JSON with "key" and "value" fields.
	// Use a simple JSON decode.
	var payload struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.Key, payload.Value
}

// extractMetrics extracts token/cost info from a METRICS_SNAPSHOT event payload.
func extractMetrics(event *arlov1.Event) (int64, int64, float64) {
	var payload struct {
		TokensIn  int64   `json:"tokens_in"`
		TokensOut int64   `json:"tokens_out"`
		CostUSD   float64 `json:"cost_usd"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.TokensIn, payload.TokensOut, payload.CostUSD
}
