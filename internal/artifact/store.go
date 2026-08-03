// Package artifact stores agent outputs as versioned, content-addressable files.
// Artifacts are the product of agent sessions — code, docs, diffs, logs.
package artifact

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/lingjiefan/arlo/internal/domain"
)

// Store persists and retrieves artifacts.
type Store struct {
	mu   sync.RWMutex
	root string                     // filesystem root for artifact storage
	meta map[string]*domain.Artifact // artifactID → metadata
}

// NewStore creates a new artifact store.
func NewStore(root string) (*Store, error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("create artifact root: %w", err)
	}
	return &Store{
		root: root,
		meta: make(map[string]*domain.Artifact),
	}, nil
}

// Save writes an artifact's content to the store and returns metadata.
func (s *Store) Save(ctx context.Context, artifact *domain.Artifact, content io.Reader) (*domain.Artifact, error) {
	// Hash content for integrity.
	hasher := sha256.New()
	reader := io.TeeReader(content, hasher)

	// Write to disk: <root>/<workflowID>/<nodeID>/<name>
	dir := filepath.Join(s.root, artifact.WorkflowID, artifact.NodeID)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create artifact dir: %w", err)
	}

	artifactPath := filepath.Join(dir, artifact.Name)
	f, err := os.Create(artifactPath)
	if err != nil {
		return nil, fmt.Errorf("create artifact file: %w", err)
	}
	defer f.Close()

	size, err := io.Copy(f, reader)
	if err != nil {
		return nil, fmt.Errorf("write artifact: %w", err)
	}

	artifact.Path = artifactPath
	artifact.Size = size
	artifact.ContentHash = fmt.Sprintf("%x", hasher.Sum(nil))

	s.mu.Lock()
	if artifact.ID == "" {
		artifact.ID = fmt.Sprintf("art-%s-%s-%s", artifact.WorkflowID, artifact.NodeID, artifact.Name)
	}
	s.meta[artifact.ID] = artifact
	s.mu.Unlock()

	return artifact, nil
}

// Get returns artifact metadata.
func (s *Store) Get(ctx context.Context, id string) (*domain.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	art, ok := s.meta[id]
	if !ok {
		return nil, fmt.Errorf("artifact not found: %s", id)
	}
	return art, nil
}

// Open returns a reader for an artifact's content.
func (s *Store) Open(ctx context.Context, id string) (io.ReadCloser, error) {
	s.mu.RLock()
	art, ok := s.meta[id]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("artifact not found: %s", id)
	}

	f, err := os.Open(art.Path)
	if err != nil {
		return nil, fmt.Errorf("open artifact: %w", err)
	}
	return f, nil
}

// ListByNode returns all artifacts produced by a node.
func (s *Store) ListByNode(ctx context.Context, nodeID string) ([]*domain.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var arts []*domain.Artifact
	for _, a := range s.meta {
		if a.NodeID == nodeID {
			arts = append(arts, a)
		}
	}
	return arts, nil
}

// ListByWorkflow returns all artifacts for a workflow.
func (s *Store) ListByWorkflow(ctx context.Context, workflowID string) ([]*domain.Artifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var arts []*domain.Artifact
	for _, a := range s.meta {
		if a.WorkflowID == workflowID {
			arts = append(arts, a)
		}
	}
	return arts, nil
}
