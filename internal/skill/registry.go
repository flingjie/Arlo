// Package skill manages agent capabilities as versioned, platform-level assets.
// Skills are reusable across workflows and can be updated independently.
package skill

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/lingjiefan/arlo/internal/domain"
	"gopkg.in/yaml.v3"
)

// Registry resolves skill references to full skill definitions.
type Registry struct {
	mu     sync.RWMutex
	skills map[string]*domain.Skill // "name@version" → skill
	latest map[string]string        // "name" → "name@version"
}

// NewRegistry creates a new skill registry.
func NewRegistry() *Registry {
	return &Registry{
		skills: make(map[string]*domain.Skill),
		latest: make(map[string]string),
	}
}

// LoadDir loads all .yaml skill files from a directory.
func (r *Registry) LoadDir(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read skill dir %s: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := dir + "/" + entry.Name()
		data, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read skill %s: %w", path, err)
		}

		var skill domain.Skill
		if err := yaml.Unmarshal(data, &skill); err != nil {
			return fmt.Errorf("parse skill %s: %w", path, err)
		}

		if skill.Name == "" {
			return fmt.Errorf("skill %s: name is required", path)
		}
		if skill.Version == "" {
			skill.Version = "1.0"
		}

		r.Register(&skill)
	}

	return nil
}

// Register adds a skill to the registry.
func (r *Registry) Register(skill *domain.Skill) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := skill.Name + "@" + skill.Version
	r.skills[key] = skill

	// Track latest version (lexicographic — semver would be better for v1).
	currentLatest, ok := r.latest[skill.Name]
	if !ok || skill.Version > currentLatest {
		r.latest[skill.Name] = skill.Version
	}
}

// Resolve returns a skill by name and optional version.
// If version is empty, returns the latest version.
func (r *Registry) Resolve(ref domain.SkillRef) (*domain.Skill, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	version := ref.Version
	if version == "" {
		v, ok := r.latest[ref.Name]
		if !ok {
			return nil, fmt.Errorf("skill not found: %s (no versions registered)", ref.Name)
		}
		version = v
	}

	key := ref.Name + "@" + version
	skill, ok := r.skills[key]
	if !ok {
		return nil, fmt.Errorf("skill not found: %s", key)
	}
	return skill, nil
}

// List returns all registered skills.
func (r *Registry) List() []*domain.Skill {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var skills []*domain.Skill
	for _, s := range r.skills {
		skills = append(skills, s)
	}
	return skills
}
