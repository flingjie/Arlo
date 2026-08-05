package artifact

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lingjiefan/arlo/internal/domain"
)

func tempRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	return root
}

func makeArtifact(workflowID, nodeID, name, artifactType string) *domain.Artifact {
	return &domain.Artifact{
		WorkflowID: workflowID,
		NodeID:     nodeID,
		Name:       name,
		Type:       artifactType,
	}
}

func contentReader(content string) io.Reader {
	return strings.NewReader(content)
}

// ── TestNewStore ───────────────────────────────────────

func TestNewStore(t *testing.T) {
	root := tempRoot(t)
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("NewStore() returned nil store")
	}
	if store.root != root {
		t.Errorf("store.root = %q, want %q", store.root, root)
	}

	// Verify root directory was created.
	fi, err := os.Stat(root)
	if err != nil {
		t.Fatalf("root directory not created: %v", err)
	}
	if !fi.IsDir() {
		t.Errorf("root is not a directory")
	}

	// Verify meta map is initialised and empty.
	store.mu.RLock()
	metaLen := len(store.meta)
	store.mu.RUnlock()
	if metaLen != 0 {
		t.Errorf("meta map has %d entries, want 0", metaLen)
	}
}

func TestNewStoreCreatesNestedRoot(t *testing.T) {
	root := filepath.Join(tempRoot(t), "deep", "nested", "path")
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("NewStore() returned nil store")
	}
	fi, err := os.Stat(root)
	if err != nil || !fi.IsDir() {
		t.Fatal("nested root directory was not created")
	}
}

// ── TestSave ───────────────────────────────────────────

func TestSave(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-1", "node-1", "plan.md", "markdown")
	content := "# Plan\n\nDo the thing."

	saved, err := store.Save(ctx, art, contentReader(content))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Verify metadata fields populated.
	if saved.ID == "" {
		t.Error("saved.ID is empty")
	}
	if saved.Path == "" {
		t.Error("saved.Path is empty")
	}
	if saved.Size == 0 {
		t.Error("saved.Size is 0")
	}
	if saved.ContentHash == "" {
		t.Error("saved.ContentHash is empty")
	}

	// Verify file exists on disk.
	if _, err := os.Stat(saved.Path); err != nil {
		t.Fatalf("artifact file not found on disk: %v", err)
	}

	// Verify content on disk matches.
	diskContent, err := os.ReadFile(saved.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(diskContent) != content {
		t.Errorf("disk content = %q, want %q", string(diskContent), content)
	}

	// Verify size matches.
	if saved.Size != int64(len(content)) {
		t.Errorf("Size = %d, want %d", saved.Size, len(content))
	}
}

func TestSaveEmptyContent(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-1", "node-1", "empty.txt", "text")
	saved, err := store.Save(ctx, art, contentReader(""))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if saved.Size != 0 {
		t.Errorf("Size of empty artifact = %d, want 0", saved.Size)
	}
}

func TestSavePreservesExistingID(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-1", "node-1", "output.log", "log")
	art.ID = "custom-id-123"

	saved, err := store.Save(ctx, art, contentReader("log line"))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if saved.ID != "custom-id-123" {
		t.Errorf("ID = %q, want %q", saved.ID, "custom-id-123")
	}
}

func TestSaveSetsContentHash(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-1", "node-1", "data.txt", "text")
	content := "hello world"

	saved, err := store.Save(ctx, art, contentReader(content))
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	// Content hash of same content must be deterministic.
	saved2, err := store.Save(ctx, makeArtifact("wf-2", "node-2", "data2.txt", "text"), contentReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if saved.ContentHash != saved2.ContentHash {
		t.Errorf("content hashes differ for same content: %q vs %q", saved.ContentHash, saved2.ContentHash)
	}

	// Different content produces different hash.
	saved3, err := store.Save(ctx, makeArtifact("wf-3", "node-3", "data3.txt", "text"), contentReader("different content"))
	if err != nil {
		t.Fatal(err)
	}
	if saved.ContentHash == saved3.ContentHash {
		t.Error("content hashes should differ for different content")
	}
}

// ── TestSaveDuplicateID ────────────────────────────────

func TestSaveDuplicateID(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-1", "node-1", "output.log", "log")
	art.ID = "fixed-id"

	// Save first version.
	saved1, err := store.Save(ctx, art, contentReader("version 1"))
	if err != nil {
		t.Fatalf("first Save() error = %v", err)
	}

	// Save second version with same ID — overwrites in meta map.
	saved2, err := store.Save(ctx, art, contentReader("version 2"))
	if err != nil {
		t.Fatalf("second Save() error = %v", err)
	}

	// Retrieved artifact should have version 2 content.
	retrieved, err := store.Get(ctx, "fixed-id")
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.Size != saved2.Size {
		t.Errorf("retrieved Size = %d, want %d", retrieved.Size, saved2.Size)
	}

	// File on disk should contain version 2 content.
	diskContent, err := os.ReadFile(retrieved.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(diskContent) != "version 2" {
		t.Errorf("disk content = %q, want %q", string(diskContent), "version 2")
	}

	// Verify only one entry in meta.
	store.mu.RLock()
	metaLen := len(store.meta)
	store.mu.RUnlock()
	if metaLen != 1 {
		t.Errorf("meta map has %d entries after duplicate ID save, want 1", metaLen)
	}

	_ = saved1
}

// ── TestGet ────────────────────────────────────────────

func TestGet(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-1", "node-1", "plan.md", "markdown")
	art.ID = "art-wf-1-node-1-plan.md"

	saved, err := store.Save(ctx, art, contentReader("# Plan"))
	if err != nil {
		t.Fatal(err)
	}

	retrieved, err := store.Get(ctx, "art-wf-1-node-1-plan.md")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	// Verify all fields match.
	if retrieved.ID != saved.ID {
		t.Errorf("ID = %q, want %q", retrieved.ID, saved.ID)
	}
	if retrieved.Name != saved.Name {
		t.Errorf("Name = %q, want %q", retrieved.Name, saved.Name)
	}
	if retrieved.Type != saved.Type {
		t.Errorf("Type = %q, want %q", retrieved.Type, saved.Type)
	}
	if retrieved.Path != saved.Path {
		t.Errorf("Path = %q, want %q", retrieved.Path, saved.Path)
	}
	if retrieved.NodeID != saved.NodeID {
		t.Errorf("NodeID = %q, want %q", retrieved.NodeID, saved.NodeID)
	}
	if retrieved.WorkflowID != saved.WorkflowID {
		t.Errorf("WorkflowID = %q, want %q", retrieved.WorkflowID, saved.WorkflowID)
	}
	if retrieved.Size != saved.Size {
		t.Errorf("Size = %d, want %d", retrieved.Size, saved.Size)
	}
	if retrieved.ContentHash != saved.ContentHash {
		t.Errorf("ContentHash = %q, want %q", retrieved.ContentHash, saved.ContentHash)
	}
}

// ── TestGetNotFound ────────────────────────────────────

func TestGetNotFound(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Get(ctx, "non-existent-id")
	if err == nil {
		t.Fatal("Get() expected error for non-existent ID, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want error containing 'not found'", err.Error())
	}
}

// ── TestOpen ───────────────────────────────────────────

func TestOpen(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	content := "artifact content\nline 2\n"
	art := makeArtifact("wf-1", "node-1", "output.txt", "text")
	saved, err := store.Save(ctx, art, contentReader(content))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := store.Open(ctx, saved.ID)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(readContent) != content {
		t.Errorf("read content = %q, want %q", string(readContent), content)
	}
}

func TestOpenNotFound(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.Open(ctx, "non-existent-id")
	if err == nil {
		t.Fatal("Open() expected error for non-existent ID, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want error containing 'not found'", err.Error())
	}
}

// ── TestListByNode ─────────────────────────────────────

func TestListByNode(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	// Save artifacts for multiple nodes.
	save := func(workflow, node, name, kind, content string) {
		t.Helper()
		_, err := store.Save(ctx, makeArtifact(workflow, node, name, kind), contentReader(content))
		if err != nil {
			t.Fatalf("Save(%s/%s/%s) error = %v", workflow, node, name, err)
		}
	}

	save("wf-1", "node-a", "plan.md", "markdown", "# Plan")
	save("wf-1", "node-a", "output.log", "log", "log data")
	save("wf-1", "node-b", "diff.patch", "diff", "--- a\n+++ b")
	save("wf-2", "node-a", "notes.md", "markdown", "## Notes")

	arts, err := store.ListByNode(ctx, "node-a")
	if err != nil {
		t.Fatalf("ListByNode() error = %v", err)
	}
	if len(arts) != 3 {
		t.Fatalf("ListByNode(node-a) returned %d artifacts, want 3", len(arts))
	}

	// Make sure all returned artifacts belong to node-a.
	for _, a := range arts {
		if a.NodeID != "node-a" {
			t.Errorf("unexpected NodeID %q in results", a.NodeID)
		}
	}

	// Node-b has only one artifact.
	arts, err = store.ListByNode(ctx, "node-b")
	if err != nil {
		t.Fatalf("ListByNode() error = %v", err)
	}
	if len(arts) != 1 {
		t.Errorf("ListByNode(node-b) returned %d artifacts, want 1", len(arts))
	}
	if arts[0].Name != "diff.patch" {
		t.Errorf("unexpected artifact name %q", arts[0].Name)
	}
}

func TestListByNodeEmpty(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	arts, err := store.ListByNode(ctx, "nonexistent-node")
	if err != nil {
		t.Fatalf("ListByNode() error = %v", err)
	}
	if len(arts) != 0 {
		t.Errorf("ListByNode() returned %d artifacts, want 0", len(arts))
	}
}

// ── TestListByWorkflow ─────────────────────────────────

func TestListByWorkflow(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	save := func(workflow, node, name, kind, content string) {
		t.Helper()
		_, err := store.Save(ctx, makeArtifact(workflow, node, name, kind), contentReader(content))
		if err != nil {
			t.Fatalf("Save(%s/%s/%s) error = %v", workflow, node, name, err)
		}
	}

	save("wf-1", "node-a", "plan.md", "markdown", "# Plan")
	save("wf-1", "node-b", "diff.patch", "diff", "--- a\n+++ b")
	save("wf-1", "node-c", "output.log", "log", "done")
	save("wf-2", "node-a", "notes.md", "markdown", "## Notes")
	save("wf-2", "node-b", "code.go", "code", "package main")

	arts, err := store.ListByWorkflow(ctx, "wf-1")
	if err != nil {
		t.Fatalf("ListByWorkflow() error = %v", err)
	}
	if len(arts) != 3 {
		t.Fatalf("ListByWorkflow(wf-1) returned %d artifacts, want 3", len(arts))
	}
	for _, a := range arts {
		if a.WorkflowID != "wf-1" {
			t.Errorf("unexpected WorkflowID %q in results", a.WorkflowID)
		}
	}

	arts, err = store.ListByWorkflow(ctx, "wf-2")
	if err != nil {
		t.Fatalf("ListByWorkflow() error = %v", err)
	}
	if len(arts) != 2 {
		t.Errorf("ListByWorkflow(wf-2) returned %d artifacts, want 2", len(arts))
	}
}

func TestListByWorkflowEmpty(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	arts, err := store.ListByWorkflow(ctx, "nonexistent-workflow")
	if err != nil {
		t.Fatalf("ListByWorkflow() error = %v", err)
	}
	if len(arts) != 0 {
		t.Errorf("ListByWorkflow() returned %d artifacts, want 0", len(arts))
	}
}

// ── Concurrent tests ───────────────────────────────────

func TestConcurrentSave(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			art := makeArtifact(
				fmt.Sprintf("wf-%d", idx),
				fmt.Sprintf("node-%d", idx),
				fmt.Sprintf("file-%d.txt", idx),
				"text",
			)
			content := fmt.Sprintf("content from goroutine %d", idx)
			_, err := store.Save(ctx, art, contentReader(content))
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent Save() error: %v", err)
	}

	// Verify all artifacts were saved.
	store.mu.RLock()
	metaLen := len(store.meta)
	store.mu.RUnlock()
	if metaLen != numGoroutines {
		t.Errorf("meta has %d entries after concurrent saves, want %d", metaLen, numGoroutines)
	}
}

func TestConcurrentSaveSameNode(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	const numGoroutines = 10
	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			art := makeArtifact("wf-shared", "node-shared", fmt.Sprintf("file-%d.txt", idx), "text")
			content := fmt.Sprintf("content %d", idx)
			_, err := store.Save(ctx, art, contentReader(content))
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent Save() error: %v", err)
	}

	// All artifacts should be retrievable.
	for i := 0; i < numGoroutines; i++ {
		id := fmt.Sprintf("art-wf-shared-node-shared-file-%d.txt", i)
		_, err := store.Get(ctx, id)
		if err != nil {
			t.Errorf("Get(%q) after concurrent save error: %v", id, err)
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	// Pre-populate some artifacts.
	for i := 0; i < 5; i++ {
		art := makeArtifact("wf-readwrite", fmt.Sprintf("node-%d", i), "data.txt", "text")
		_, err := store.Save(ctx, art, contentReader(fmt.Sprintf("initial content %d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	const writers = 5
	const readers = 10
	var wg sync.WaitGroup

	// Writers: continually save new artifacts.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				art := makeArtifact(
					"wf-readwrite",
					fmt.Sprintf("writer-%d", idx),
					fmt.Sprintf("file-%d.txt", j),
					"text",
				)
				_, err := store.Save(ctx, art, contentReader(fmt.Sprintf("writer %d, iteration %d", idx, j)))
				if err != nil {
					t.Errorf("concurrent Save() error: %v", err)
					return
				}
			}
		}(i)
	}

	// Readers: list by workflow and by node while writes happen.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = store.ListByWorkflow(ctx, "wf-readwrite")
				_, _ = store.ListByNode(ctx, fmt.Sprintf("node-%d", j%5))
				_, _ = store.ListByNode(ctx, fmt.Sprintf("writer-%d", j%writers))
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentGetAndSave(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	// Pre-populate with a known artifact.
	art := makeArtifact("wf-getsave", "node-gs", "shared.txt", "text")
	art.ID = "shared-id"
	saved, err := store.Save(ctx, art, contentReader("original"))
	if err != nil {
		t.Fatal(err)
	}
	_ = saved

	var wg sync.WaitGroup

	// Writer goroutine: repeatedly overwrite the same ID.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			art := makeArtifact("wf-getsave", "node-gs", "shared.txt", "text")
			art.ID = "shared-id"
			_, err := store.Save(ctx, art, contentReader(fmt.Sprintf("version-%d", i)))
			if err != nil {
				t.Errorf("Save() error: %v", err)
				return
			}
		}
	}()

	// Reader goroutines: repeatedly Get and Open the shared artifact.
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				art, err := store.Get(ctx, "shared-id")
				if err != nil {
					t.Errorf("Get() error: %v", err)
					return
				}
				// Verify path is set (means the artifact is consistent).
				if art.Path == "" {
					t.Error("Get() returned artifact with empty Path")
				}
			}
		}()
	}

	wg.Wait()
}

// ── TestSaveDirectoryStructure ─────────────────────────

func TestSaveDirectoryStructure(t *testing.T) {
	ctx := context.Background()
	root := tempRoot(t)
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-dir", "node-dir", "file.txt", "text")
	saved, err := store.Save(ctx, art, contentReader("content"))
	if err != nil {
		t.Fatal(err)
	}

	expectedDir := filepath.Join(root, "wf-dir", "node-dir")
	expectedPath := filepath.Join(expectedDir, "file.txt")

	if saved.Path != expectedPath {
		t.Errorf("Path = %q, want %q", saved.Path, expectedPath)
	}

	// Verify directory exists.
	fi, err := os.Stat(expectedDir)
	if err != nil || !fi.IsDir() {
		t.Fatal("artifact directory was not created correctly")
	}

	// Verify file exists in correct location.
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatal("artifact file not at expected path")
	}
}

// ── TestMultipleNodesSameWorkflow ──────────────────────

func TestMultipleNodesSameWorkflow(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	nodes := []string{"plan", "implement", "review", "test"}
	for _, node := range nodes {
		art := makeArtifact("wf-multi", node, "output.log", "log")
		_, err := store.Save(ctx, art, contentReader(fmt.Sprintf("log for %s", node)))
		if err != nil {
			t.Fatal(err)
		}
	}

	// Each node should have exactly 1 artifact.
	for _, node := range nodes {
		arts, err := store.ListByNode(ctx, node)
		if err != nil {
			t.Fatal(err)
		}
		if len(arts) != 1 {
			t.Errorf("ListByNode(%s) returned %d artifacts, want 1", node, len(arts))
		}
	}

	// Workflow should have all 4.
	arts, err := store.ListByWorkflow(ctx, "wf-multi")
	if err != nil {
		t.Fatal(err)
	}
	if len(arts) != 4 {
		t.Errorf("ListByWorkflow(wf-multi) returned %d artifacts, want 4", len(arts))
	}
}

// ── TestAutoGeneratedID ────────────────────────────────

func TestAutoGeneratedID(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	art := makeArtifact("wf-auto", "node-auto", "report.json", "json")
	// art.ID is intentionally empty.

	saved, err := store.Save(ctx, art, contentReader(`{"status":"ok"}`))
	if err != nil {
		t.Fatal(err)
	}

	expectedID := "art-wf-auto-node-auto-report.json"
	if saved.ID != expectedID {
		t.Errorf("auto-generated ID = %q, want %q", saved.ID, expectedID)
	}

	// Verify it's retrievable by that ID.
	retrieved, err := store.Get(ctx, expectedID)
	if err != nil {
		t.Fatalf("Get() with auto-generated ID error = %v", err)
	}
	if retrieved.Name != "report.json" {
		t.Errorf("retrieved Name = %q, want %q", retrieved.Name, "report.json")
	}
}

// ── TestSaveLargeContent ───────────────────────────────

func TestSaveLargeContent(t *testing.T) {
	ctx := context.Background()
	store, err := NewStore(tempRoot(t))
	if err != nil {
		t.Fatal(err)
	}

	// Create 1 MB of content.
	largeContent := bytes.Repeat([]byte("ABCDEFGHIJ"), 1024*100) // ~1 MB
	art := makeArtifact("wf-large", "node-large", "big.bin", "binary")

	saved, err := store.Save(ctx, art, bytes.NewReader(largeContent))
	if err != nil {
		t.Fatalf("Save() large content error = %v", err)
	}
	if saved.Size != int64(len(largeContent)) {
		t.Errorf("Size = %d, want %d", saved.Size, len(largeContent))
	}

	// Verify content integrity via Open.
	reader, err := store.Open(ctx, saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	readContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readContent, largeContent) {
		t.Error("large content read does not match written content")
	}
}

// ── TestNilContextBehavior ─────────────────────────────

func TestSaveAndGetWithBackgroundContext(t *testing.T) {
	// The current implementation does not check context cancellation,
	// but verify it works with context.Background() and context.TODO().
	for _, name := range []string{"Background", "TODO"} {
		t.Run(name, func(t *testing.T) {
			var ctx context.Context
			if name == "Background" {
				ctx = context.Background()
			} else {
				ctx = context.TODO()
			}

			store, err := NewStore(tempRoot(t))
			if err != nil {
				t.Fatal(err)
			}

			art := makeArtifact("wf-ctx", "node-ctx", "ctx.txt", "text")
			saved, err := store.Save(ctx, art, contentReader("test"))
			if err != nil {
				t.Fatalf("Save() error = %v", err)
			}

			_, err = store.Get(ctx, saved.ID)
			if err != nil {
				t.Errorf("Get() error = %v", err)
			}
		})
	}
}
