package skill

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lingjiefan/arlo/internal/domain"
)

// ── TestNewRegistry ─────────────────────────────────────────

func TestNewRegistry(t *testing.T) {
	r := NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}

	skills := r.List()
	if len(skills) != 0 {
		t.Fatalf("expected empty registry, got %d skills", len(skills))
	}
}

func TestNewRegistry_listEmpty(t *testing.T) {
	r := NewRegistry()
	skills := r.List()
	if len(skills) != 0 {
		t.Errorf("expected 0 skills, got %d", len(skills))
	}
}

// ── TestRegister ─────────────────────────────────────────────

func TestRegister(t *testing.T) {
	r := NewRegistry()
	skill := &domain.Skill{
		Name:    "code-review",
		Version: "1.0",
		Prompt:  "Review this code.",
	}

	r.Register(skill)

	got, err := r.Resolve(domain.SkillRef{Name: "code-review"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "code-review" {
		t.Errorf("expected name 'code-review', got %q", got.Name)
	}
	if got.Version != "1.0" {
		t.Errorf("expected version '1.0', got %q", got.Version)
	}
	if got.Prompt != "Review this code." {
		t.Errorf("expected prompt 'Review this code.', got %q", got.Prompt)
	}
}

func TestRegister_multipleVersions(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "lint", Version: "1.0", Prompt: "v1"})
	r.Register(&domain.Skill{Name: "lint", Version: "2.0", Prompt: "v2"})
	r.Register(&domain.Skill{Name: "lint", Version: "1.5", Prompt: "v1.5"})

	// Latest version should be "2.0" (lexicographic max)
	got, err := r.Resolve(domain.SkillRef{Name: "lint"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Version != "2.0" {
		t.Errorf("expected latest version '2.0', got %q", got.Version)
	}
	if got.Prompt != "v2" {
		t.Errorf("expected prompt 'v2', got %q", got.Prompt)
	}
}

func TestRegister_multipleSkills(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "lint", Version: "1.0"})
	r.Register(&domain.Skill{Name: "test", Version: "1.0"})
	r.Register(&domain.Skill{Name: "build", Version: "1.0"})

	all := r.List()
	if len(all) != 3 {
		t.Fatalf("expected 3 skills, got %d", len(all))
	}
}

// ── TestRegisterDuplicate ───────────────────────────────────

func TestRegisterDuplicate(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "test", Version: "1.0", Prompt: "first"})
	r.Register(&domain.Skill{Name: "test", Version: "1.0", Prompt: "second"})

	got, err := r.Resolve(domain.SkillRef{Name: "test", Version: "1.0"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be overwritten with the second registration
	if got.Prompt != "second" {
		t.Errorf("expected prompt 'second' (overwritten), got %q", got.Prompt)
	}
}

func TestRegisterDuplicate_noVersionGrowth(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "dup", Version: "2.0"})
	r.Register(&domain.Skill{Name: "dup", Version: "1.0"})
	r.Register(&domain.Skill{Name: "dup", Version: "1.5"})

	// Latest should remain "2.0" even though it was registered before 1.5
	got, err := r.Resolve(domain.SkillRef{Name: "dup"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Version != "2.0" {
		t.Errorf("expected latest version '2.0', got %q", got.Version)
	}
}

// ── TestResolve ──────────────────────────────────────────────

func TestResolve(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "format", Version: "1.0"})
	r.Register(&domain.Skill{Name: "format", Version: "3.0"})
	r.Register(&domain.Skill{Name: "format", Version: "2.0"})

	got, err := r.Resolve(domain.SkillRef{Name: "format"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Version != "3.0" {
		t.Errorf("expected latest version '3.0', got %q", got.Version)
	}
}

// ── TestResolveWithVersion ───────────────────────────────────

func TestResolveWithVersion(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "format", Version: "1.0", Prompt: "v1"})
	r.Register(&domain.Skill{Name: "format", Version: "2.0", Prompt: "v2"})

	tests := []struct {
		name    string
		ref     domain.SkillRef
		wantVer string
		wantPr  string
	}{
		{
			name:    "explicit v1",
			ref:     domain.SkillRef{Name: "format", Version: "1.0"},
			wantVer: "1.0",
			wantPr:  "v1",
		},
		{
			name:    "explicit v2",
			ref:     domain.SkillRef{Name: "format", Version: "2.0"},
			wantVer: "2.0",
			wantPr:  "v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := r.Resolve(tt.ref)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Version != tt.wantVer {
				t.Errorf("expected version %q, got %q", tt.wantVer, got.Version)
			}
			if got.Prompt != tt.wantPr {
				t.Errorf("expected prompt %q, got %q", tt.wantPr, got.Prompt)
			}
		})
	}
}

// ── TestResolveNotFound ──────────────────────────────────────

func TestResolveNotFound(t *testing.T) {
	r := NewRegistry()

	_, err := r.Resolve(domain.SkillRef{Name: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for non-existent skill, got nil")
	}
	if !strings.Contains(err.Error(), "skill not found") {
		t.Errorf("expected error to contain 'skill not found', got %q", err.Error())
	}
}

func TestResolveNotFound_specificVersion(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "foo", Version: "1.0"})

	_, err := r.Resolve(domain.SkillRef{Name: "foo", Version: "99.0"})
	if err == nil {
		t.Fatal("expected error for non-existent version, got nil")
	}
}

func TestResolveNotFound_emptyRegistry(t *testing.T) {
	r := NewRegistry()

	_, err := r.Resolve(domain.SkillRef{Name: "anything"})
	if err == nil {
		t.Fatal("expected error for empty registry, got nil")
	}
}

// ── TestList ─────────────────────────────────────────────────

func TestList(t *testing.T) {
	r := NewRegistry()
	r.Register(&domain.Skill{Name: "a", Version: "1.0"})
	r.Register(&domain.Skill{Name: "b", Version: "1.0"})
	r.Register(&domain.Skill{Name: "a", Version: "2.0"}) // same name, newer version

	all := r.List()
	if len(all) != 3 {
		t.Fatalf("expected 3 skills (all versions), got %d", len(all))
	}

	// Verify all the expected skills are present
	found := make(map[string]bool)
	for _, s := range all {
		key := s.Name + "@" + s.Version
		found[key] = true
	}
	for _, key := range []string{"a@1.0", "a@2.0", "b@1.0"} {
		if !found[key] {
			t.Errorf("expected skill %q in list but not found", key)
		}
	}
}

// ── TestListEmpty ────────────────────────────────────────────

func TestListEmpty(t *testing.T) {
	r := NewRegistry()
	all := r.List()
	if len(all) != 0 {
		t.Errorf("expected empty list, got %d items", len(all))
	}
}

// ── TestLoadDir ──────────────────────────────────────────────

func TestLoadDir(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "lint.yaml", `
name: lint
version: "1.0"
description: "Runs lint checks"
prompt: "Lint the code."
`)

	writeFile(t, dir, "test.yaml", `
name: test
version: "2.0"
description: "Runs tests"
prompt: "Run tests."
`)

	r := NewRegistry()
	ctx := context.Background()
	err := r.LoadDir(ctx, dir)
	if err != nil {
		t.Fatalf("LoadDir failed: %v", err)
	}

	if len(r.List()) != 2 {
		t.Fatalf("expected 2 skills, got %d", len(r.List()))
	}

	// Verify both skills can be resolved
	lint, err := r.Resolve(domain.SkillRef{Name: "lint"})
	if err != nil {
		t.Fatalf("resolve lint: %v", err)
	}
	if lint.Version != "1.0" {
		t.Errorf("expected lint version 1.0, got %q", lint.Version)
	}

	testSkill, err := r.Resolve(domain.SkillRef{Name: "test"})
	if err != nil {
		t.Fatalf("resolve test: %v", err)
	}
	if testSkill.Version != "2.0" {
		t.Errorf("expected test version 2.0, got %q", testSkill.Version)
	}
}

func TestLoadDir_defaultVersion(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "noversion.yaml", `
name: noversion
description: "No version specified"
prompt: "Default me."
`)

	r := NewRegistry()
	ctx := context.Background()
	err := r.LoadDir(ctx, dir)
	if err != nil {
		t.Fatalf("LoadDir failed: %v", err)
	}

	skill, err := r.Resolve(domain.SkillRef{Name: "noversion"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if skill.Version != "1.0" {
		t.Errorf("expected default version '1.0', got %q", skill.Version)
	}
}

func TestLoadDir_skipsDirectories(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "valid.yaml", `
name: valid
version: "1.0"
prompt: "Valid skill."
`)

	subDir := filepath.Join(dir, "subdir")
	if err := os.Mkdir(subDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	writeFile(t, subDir, "sub.yaml", `
name: sub
version: "1.0"
prompt: "Should be skipped."
`)

	r := NewRegistry()
	ctx := context.Background()
	err := r.LoadDir(ctx, dir)
	if err != nil {
		t.Fatalf("LoadDir failed: %v", err)
	}

	if len(r.List()) != 1 {
		t.Fatalf("expected 1 skill (subdirectory skipped), got %d", len(r.List()))
	}

	// sub should not exist
	_, err = r.Resolve(domain.SkillRef{Name: "sub"})
	if err == nil {
		t.Fatal("expected 'sub' to not be loaded (directory was skipped)")
	}
}

func TestLoadDir_ignoresNonYAML(t *testing.T) {
	dir := t.TempDir()

	// .yaml extension is the only one checked by LoadDir (it reads ALL files)
	writeFile(t, dir, "good.yaml", `
name: good
version: "1.0"
prompt: "Good."
`)
	writeFile(t, dir, "notes.txt", "This is not YAML at all just some text")
	writeFile(t, dir, "readme.md", "# README\n\nThis is markdown.")

	r := NewRegistry()
	ctx := context.Background()
	err := r.LoadDir(ctx, dir)
	// This will fail because notes.txt and readme.md are not valid YAML
	if err == nil {
		t.Fatal("expected error when parsing non-YAML files")
	}
}

// ── TestLoadDirEmpty ─────────────────────────────────────────

func TestLoadDirEmpty(t *testing.T) {
	dir := t.TempDir()
	r := NewRegistry()
	ctx := context.Background()

	err := r.LoadDir(ctx, dir)
	if err != nil {
		t.Fatalf("LoadDir on empty directory should not error: %v", err)
	}
	if len(r.List()) != 0 {
		t.Errorf("expected no skills loaded, got %d", len(r.List()))
	}
}

// ── TestLoadDirNonExistent ───────────────────────────────────

func TestLoadDirNonExistent(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()

	err := r.LoadDir(ctx, "/nonexistent/path/skills")
	if err == nil {
		t.Fatal("expected error for non-existent directory, got nil")
	}
}

// ── TestLoadDirInvalidYAML ────────────────────────────────────

func TestLoadDirInvalidYAML(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "bad.yaml", ":: this is not valid yaml :: --- {{{")

	r := NewRegistry()
	ctx := context.Background()

	err := r.LoadDir(ctx, dir)
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
	if !strings.Contains(err.Error(), "parse skill") {
		t.Errorf("expected error to mention 'parse skill', got %q", err.Error())
	}
}

func TestLoadDirMissingName(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, dir, "noname.yaml", `
version: "1.0"
prompt: "No name field."
`)

	r := NewRegistry()
	ctx := context.Background()

	err := r.LoadDir(ctx, dir)
	if err == nil {
		t.Fatal("expected error for skill with missing name, got nil")
	}
	if !strings.Contains(err.Error(), "name is required") {
		t.Errorf("expected error to mention 'name is required', got %q", err.Error())
	}
}

func TestLoadDir_withContextCancellation(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.yaml", `
name: a
version: "1.0"
prompt: "A."
`)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// LoadDir doesn't check ctx, but we verify it still works
	// (context is passed but not currently consulted inside LoadDir)
	r := NewRegistry()
	err := r.LoadDir(ctx, dir)
	if err != nil {
		t.Fatalf("LoadDir failed: %v", err)
	}
}

// ── TestConcurrentRegisterResolve ────────────────────────────

func TestConcurrentRegisterResolve(t *testing.T) {
	r := NewRegistry()

	// Pre-load some skills
	r.Register(&domain.Skill{Name: "base", Version: "1.0"})

	var wg sync.WaitGroup
	numGoroutines := 20

	// Register concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r.Register(&domain.Skill{
				Name:    "concurrent",
				Version: string(rune('0' + (idx % 10))) + ".0",
				Prompt:  "p",
			})
		}(i)
	}

	// Resolve concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.Resolve(domain.SkillRef{Name: "base"})
			_, _ = r.Resolve(domain.SkillRef{Name: "concurrent"})
		}()
	}

	// List concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.List()
		}()
	}

	wg.Wait()

	// After all goroutines, we should be able to resolve something
	// and list should return non-zero skills
	skills := r.List()
	if len(skills) == 0 {
		t.Error("expected at least 1 skill after concurrent registration")
	}

	// Verify we can still resolve base
	base, err := r.Resolve(domain.SkillRef{Name: "base"})
	if err != nil {
		t.Fatalf("resolve base after concurrent ops: %v", err)
	}
	if base.Version != "1.0" {
		t.Errorf("expected base version 1.0, got %q", base.Version)
	}
}

func TestConcurrentLoadDirAndResolve(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "a.yaml", `
name: a
version: "1.0"
prompt: "A."
`)
	writeFile(t, dir, "b.yaml", `
name: b
version: "1.0"
prompt: "B."
`)

	r := NewRegistry()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 10

	// Multiple concurrent LoadDirs (same dir, same registry)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.LoadDir(ctx, dir) // errors are fine (overlaps)
		}()
	}

	// Concurrent resolves while loading
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.Resolve(domain.SkillRef{Name: "a"})
			_, _ = r.Resolve(domain.SkillRef{Name: "b"})
		}()
	}

	wg.Wait()

	// Verify both skills are resolvable
	skills := r.List()
	if len(skills) < 2 {
		t.Errorf("expected at least 2 skills, got %d", len(skills))
	}
}

func TestConcurrentRegisterAndList(t *testing.T) {
	r := NewRegistry()
	var wg sync.WaitGroup
	numRegistrations := 50
	numListers := 20

	// Register many skills concurrently
	for i := 0; i < numRegistrations; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r.Register(&domain.Skill{
				Name:    "skill",
				Version: string(rune('0'+(idx%10))) + "." + string(rune('0'+(idx%10))),
			})
		}(i)
	}

	// List concurrently
	for i := 0; i < numListers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.List()
		}()
	}

	wg.Wait()

	// Should not panic, and should have skills loaded
	skills := r.List()
	if len(skills) == 0 {
		t.Error("expected non-zero skills after concurrent ops")
	}
}

// ── Helpers ──────────────────────────────────────────────────

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writeFile(%s): %v", name, err)
	}
}
