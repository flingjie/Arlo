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

// ── Task / Workflow items ──────────────────────────

type TaskCreatedItem struct {
	Timestamp time.Time
	Title     string
}

func (i TaskCreatedItem) Time() time.Time { return i.Timestamp }
func (i TaskCreatedItem) Level() Level    { return INFO }
func (i TaskCreatedItem) Render() string  { return fmt.Sprintf("task created: %s", i.Title) }

type WorkflowCreatedItem struct {
	Timestamp time.Time
	Name      string
	Version   int
}

func (i WorkflowCreatedItem) Time() time.Time { return i.Timestamp }
func (i WorkflowCreatedItem) Level() Level    { return INFO }
func (i WorkflowCreatedItem) Render() string {
	return fmt.Sprintf("workflow created: %s (v%d)", i.Name, i.Version)
}

type TaskCompletedItem struct {
	Timestamp time.Time
}

func (i TaskCompletedItem) Time() time.Time { return i.Timestamp }
func (i TaskCompletedItem) Level() Level    { return INFO }
func (i TaskCompletedItem) Render() string  { return "workflow completed" }

type TaskFailedItem struct {
	Timestamp time.Time
	Reason    string
}

func (i TaskFailedItem) Time() time.Time { return i.Timestamp }
func (i TaskFailedItem) Level() Level    { return ERROR }
func (i TaskFailedItem) Render() string  { return fmt.Sprintf("workflow failed: %s", i.Reason) }

// ── Node items ─────────────────────────────────────

type NodeCreatedItem struct {
	Timestamp time.Time
	NodeID    string
	Skill     string
}

func (i NodeCreatedItem) Time() time.Time { return i.Timestamp }
func (i NodeCreatedItem) Level() Level    { return INFO }
func (i NodeCreatedItem) Render() string {
	if i.Skill != "" {
		return fmt.Sprintf("%s created (skill: %s)", i.NodeID, i.Skill)
	}
	return fmt.Sprintf("%s created", i.NodeID)
}

type NodeStartedItem struct {
	Timestamp time.Time
	NodeID    string
	SessionID string
}

func (i NodeStartedItem) Time() time.Time { return i.Timestamp }
func (i NodeStartedItem) Level() Level    { return INFO }
func (i NodeStartedItem) Render() string {
	if i.SessionID != "" {
		return fmt.Sprintf("%s started [%s]", i.NodeID, i.SessionID)
	}
	return fmt.Sprintf("%s started", i.NodeID)
}

type NodeCompletedItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeCompletedItem) Time() time.Time { return i.Timestamp }
func (i NodeCompletedItem) Level() Level    { return INFO }
func (i NodeCompletedItem) Render() string {
	return fmt.Sprintf("%s completed ✓", i.NodeID)
}

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

type NodeWaitingItem struct {
	Timestamp time.Time
	NodeID    string
	Reason    string
}

func (i NodeWaitingItem) Time() time.Time { return i.Timestamp }
func (i NodeWaitingItem) Level() Level    { return WARN }
func (i NodeWaitingItem) Render() string {
	if i.Reason != "" {
		return fmt.Sprintf("%s waiting: %s", i.NodeID, i.Reason)
	}
	return fmt.Sprintf("%s waiting", i.NodeID)
}

type NodeAnnotatedItem struct {
	Timestamp time.Time
	NodeID    string
	Key       string
	Value     string
}

func (i NodeAnnotatedItem) Time() time.Time { return i.Timestamp }
func (i NodeAnnotatedItem) Level() Level    { return INFO }
func (i NodeAnnotatedItem) Render() string {
	return fmt.Sprintf("%s annotated: %s = %s", i.NodeID, i.Key, i.Value)
}

type NodeHeartbeatItem struct {
	Timestamp time.Time
	NodeID    string
}

func (i NodeHeartbeatItem) Time() time.Time { return i.Timestamp }
func (i NodeHeartbeatItem) Level() Level    { return DEBUG }
func (i NodeHeartbeatItem) Render() string  { return fmt.Sprintf("%s heartbeat", i.NodeID) }

type MetricsSnapshotItem struct {
	Timestamp time.Time
	NodeID    string
	TokensIn  int64
	TokensOut int64
	ToolCalls int
	DurationMs int64
}

func (i MetricsSnapshotItem) Time() time.Time { return i.Timestamp }
func (i MetricsSnapshotItem) Level() Level    { return INFO }
func (i MetricsSnapshotItem) Render() string {
	return fmt.Sprintf("%s: %d↑/%d↓ tokens, %d tools, %s",
		i.NodeID, i.TokensIn, i.TokensOut, i.ToolCalls, formatDur(i.DurationMs))
}

// ── Artifact items ─────────────────────────────────

type ArtifactCreatedItem struct {
	Timestamp  time.Time
	NodeID     string
	ArtifactID string
	Name       string
}

func (i ArtifactCreatedItem) Time() time.Time { return i.Timestamp }
func (i ArtifactCreatedItem) Level() Level    { return INFO }
func (i ArtifactCreatedItem) Render() string {
	if i.Name != "" {
		return fmt.Sprintf("%s artifact created: %s (%s)", i.NodeID, i.Name, truncateID(i.ArtifactID))
	}
	return fmt.Sprintf("%s artifact created: %s", i.NodeID, truncateID(i.ArtifactID))
}

// ── Generic fallback ──────────────────────────────

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
func (i GenericEventItem) Render() string { return i.EventType }

// ── EventToItem ───────────────────────────────────

func EventToItem(event *arlov1.Event) TimelineItem {
	t, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		t = time.Now()
	}

	nodeID := extractNodeID(event)

	switch event.Type {
	case "TASK_CREATED":
		return TaskCreatedItem{Timestamp: t, Title: extractString(event, "title")}
	case "WORKFLOW_CREATED":
		name, ver := extractWorkflowCreated(event)
		return WorkflowCreatedItem{Timestamp: t, Name: name, Version: ver}
	case "TASK_COMPLETED":
		return TaskCompletedItem{Timestamp: t}
	case "TASK_FAILED":
		return TaskFailedItem{Timestamp: t, Reason: extractString(event, "reason")}
	case "NODE_CREATED":
		return NodeCreatedItem{Timestamp: t, NodeID: nodeID, Skill: extractString(event, "skill_name")}
	case "NODE_STARTED":
		return NodeStartedItem{Timestamp: t, NodeID: nodeID, SessionID: extractString(event, "session_id")}
	case "NODE_COMPLETED":
		return NodeCompletedItem{Timestamp: t, NodeID: nodeID}
	case "NODE_FAILED":
		return NodeFailedItem{Timestamp: t, NodeID: nodeID, Reason: extractString(event, "reason")}
	case "NODE_WAITING":
		return NodeWaitingItem{Timestamp: t, NodeID: nodeID, Reason: extractString(event, "reason")}
	case "NODE_ANNOTATED":
		key, val := extractAnnotation(event)
		return NodeAnnotatedItem{Timestamp: t, NodeID: nodeID, Key: key, Value: val}
	case "NODE_HEARTBEAT":
		return NodeHeartbeatItem{Timestamp: t, NodeID: nodeID}
	case "METRICS_SNAPSHOT":
		ti, to, tc, dur := extractMetrics(event)
		return MetricsSnapshotItem{Timestamp: t, NodeID: nodeID, TokensIn: ti, TokensOut: to, ToolCalls: tc, DurationMs: dur}
	case "ARTIFACT_CREATED":
		artID, artName := extractArtifact(event)
		return ArtifactCreatedItem{Timestamp: t, NodeID: nodeID, ArtifactID: artID, Name: artName}
	default:
		return GenericEventItem{Timestamp: t, EventType: event.Type}
	}
}

// ── Extractors ─────────────────────────────────────

func extractNodeID(event *arlov1.Event) string {
	sid := event.StreamId
	if len(sid) > 5 && sid[:5] == "node-" {
		return sid[5:]
	}
	return ""
}

func extractString(event *arlov1.Event, key string) string {
	var payload map[string]any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return ""
	}
	if v, ok := payload[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func extractWorkflowCreated(event *arlov1.Event) (string, int) {
	var payload struct {
		GraphName string `json:"graph_name"`
		Version   int    `json:"version"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.GraphName, payload.Version
}

func extractAnnotation(event *arlov1.Event) (string, string) {
	var payload struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.Key, payload.Value
}

func extractMetrics(event *arlov1.Event) (tokensIn, tokensOut int64, toolCalls int, durationMs int64) {
	var payload struct {
		TokensIn   int64 `json:"tokens_in"`
		TokensOut  int64 `json:"tokens_out"`
		ToolCalls  int   `json:"tool_calls"`
		DurationMs int64 `json:"duration_ms"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.TokensIn, payload.TokensOut, payload.ToolCalls, payload.DurationMs
}

func extractArtifact(event *arlov1.Event) (artifactID, name string) {
	var payload struct {
		ArtifactID string `json:"artifact_id"`
		Name       string `json:"name"`
	}
	_ = json.Unmarshal(event.Payload, &payload)
	return payload.ArtifactID, payload.Name
}

func truncateID(id string) string {
	if len(id) > 12 {
		return id[:12] + "..."
	}
	return id
}

func formatDur(ms int64) string {
	if ms < 1000 {
		return fmt.Sprintf("%dms", ms)
	}
	if ms < 60000 {
		return fmt.Sprintf("%.1fs", float64(ms)/1000)
	}
	m := ms / 60000
	s := (ms % 60000) / 1000
	return fmt.Sprintf("%dm%ds", m, s)
}
