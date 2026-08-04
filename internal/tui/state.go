package tui

import (
	"time"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
)

// FocusTarget represents which panel has keyboard focus.
type FocusTarget int

const (
	FocusWorkflow FocusTarget = iota
	FocusTimeline
	FocusInspector
)

// InspectorTab represents the active tab in the Node Inspector.
type InspectorTab int

const (
	TabSummary InspectorTab = iota
	TabLogs
	TabPrompt
	TabArtifacts
	TabMetrics
)

func (t InspectorTab) String() string {
	switch t {
	case TabSummary:
		return "Summary"
	case TabLogs:
		return "Logs"
	case TabPrompt:
		return "Prompt"
	case TabArtifacts:
		return "Artifacts"
	case TabMetrics:
		return "Metrics"
	default:
		return "Summary"
	}
}

// UIState holds all UI-only state, separate from workflow data.
type UIState struct {
	Focus         FocusTarget
	SelectedNode  string
	InspectorOpen bool
	InspectorTab  InspectorTab
	FilterOpen    bool
	FilterState   FilterState
	HelpOpen      bool
	CommandMode   bool
	CommandInput  string
	Width         int
	Height        int
}

// WorkflowState holds the current snapshot of workflow data.
type WorkflowState struct {
	ID        string
	Status    string
	Version   uint64
	Nodes     []*arlov1.NodeState
	StartedAt time.Time
}

// FilterState controls which event categories are visible.
type FilterState struct {
	WorkflowEvents bool
	NodeEvents     bool
	ToolCalls      bool
	Errors         bool
	TokenStream    bool
}

// DefaultFilter returns the default filter (all categories on except token stream).
func DefaultFilter() FilterState {
	return FilterState{
		WorkflowEvents: true,
		NodeEvents:     true,
		ToolCalls:      true,
		Errors:         true,
		TokenStream:    false,
	}
}
