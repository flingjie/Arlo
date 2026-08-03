package domain

// WorkspaceStatus represents the lifecycle state of a workspace.
type WorkspaceStatus string

const (
	WorkspaceStatusCreating   WorkspaceStatus = "CREATING"
	WorkspaceStatusRunning    WorkspaceStatus = "RUNNING"
	WorkspaceStatusDestroying WorkspaceStatus = "DESTROYING"
	WorkspaceStatusDestroyed  WorkspaceStatus = "DESTROYED"
	WorkspaceStatusFailed     WorkspaceStatus = "FAILED"
)

// Workspace is an execution environment that houses RuntimeInstances.
// It is a logical resource scheduled onto a physical Worker.
type Workspace struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Type          string          `json:"type"` // "tmux", "docker", "k8s"
	Status        WorkspaceStatus `json:"status"`
	DesiredWorker string          `json:"desired_worker,omitempty"`
	ActualWorker  string          `json:"actual_worker,omitempty"`
	Slots         []ExecutionSlot `json:"slots,omitempty"`
}

// ExecutionSlot is a provider-agnostic container for one RuntimeInstance.
//
// Provider mapping:
//   TmuxProvider:  ExecutionSlot = tmux window/pane
//   DockerProvider: ExecutionSlot = container exec session
//   K8sProvider:    ExecutionSlot = pod
type ExecutionSlot struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	RuntimeID string `json:"runtime_id,omitempty"` // "" if no runtime bound
}

// WorkspaceSpec defines the desired configuration for creating a workspace.
type WorkspaceSpec struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"` // "tmux", "docker"
	Config map[string]string `json:"config,omitempty"`
}

// SlotSpec defines the desired configuration for creating an execution slot.
type SlotSpec struct {
	Name string            `json:"name"`
	Dir  string            `json:"dir,omitempty"` // working directory
	Env  map[string]string `json:"env,omitempty"`
}
