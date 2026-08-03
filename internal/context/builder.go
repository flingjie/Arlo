// Package context builds optimized context windows for agent nodes.
// It answers: "what should this agent see when it starts?"
package context

import (
	"context"
	"fmt"

	"github.com/lingjiefan/arlo/internal/artifact"
	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/skill"
)

// Builder assembles context for a node by collecting upstream artifacts
// and applying the skill's context policy.
type Builder struct {
	skillRegistry *skill.Registry
	artifactStore *artifact.Store
}

// NewBuilder creates a new ContextBuilder.
func NewBuilder(sr *skill.Registry, as *artifact.Store) *Builder {
	return &Builder{
		skillRegistry: sr,
		artifactStore: as,
	}
}

// Build assembles the full context for a node.
func (b *Builder) Build(ctx context.Context, spec domain.ContextSpec) (*domain.Context, error) {
	// Resolve the skill to get the system prompt.
	skill, err := b.skillRegistry.Resolve(skillRefForNode(spec.NodeID))
	if err != nil {
		return nil, fmt.Errorf("resolve skill: %w", err)
	}

	bc := &domain.Context{
		SystemPrompt: skill.Prompt,
	}

	// Collect artifacts from upstream nodes.
	for _, dep := range spec.DependsOn {
		arts, err := b.artifactStore.ListByNode(ctx, dep)
		if err != nil {
			return nil, fmt.Errorf("list artifacts for %s: %w", dep, err)
		}
		for _, a := range arts {
			bc.Artifacts = append(bc.Artifacts, domain.ContextArtifact{
				Artifact: *a,
				Priority: 1, // direct upstream
			})
		}
	}

	// Include files matching the context policy.
	if skill.ContextPolicy.Strategy != "" {
		for _, pattern := range skill.ContextPolicy.IncludeFiles {
			bc.Files = append(bc.Files, domain.ContextFile{
				Path: pattern,
			})
		}
	}

	return bc, nil
}

// skillRefForNode maps node IDs to skill names. In v0.2, the node's SkillRef
// is stored in the ExecutableGraph. We retrieve it via the workflow engine.
// For now, we use a simple convention: nodeID == skill name.
func skillRefForNode(nodeID string) domain.SkillRef {
	return domain.SkillRef{Name: nodeID}
}

// Optimizer fits a context into a token budget by applying three-tier degradation:
//   P0: system prompt (always keep)
//   P1: upstream artifacts — keep if fits, compress if large
//   P2: reference files — summarize or omit
type Optimizer struct {
	// maxTokensPerArtifact is the threshold above which artifacts get compressed.
	maxTokensPerArtifact int
}

// NewOptimizer creates a context optimizer.
func NewOptimizer() *Optimizer {
	return &Optimizer{
		maxTokensPerArtifact: 4000,
	}
}

// Fit takes a built context and fits it into the token budget.
func (o *Optimizer) Fit(ctx context.Context, c *domain.Context, budget int) (*domain.AssembledPrompt, error) {
	ap := &domain.AssembledPrompt{
		System: c.SystemPrompt,
	}

	tokensUsed := estimateTokens(c.SystemPrompt)

	// Add artifacts in priority order.
	for i := range c.Artifacts {
		a := &c.Artifacts[i]

		// Read content to estimate tokens.
		content, tokenCount, err := o.readArtifactContent(ctx, &a.Artifact)
		if err != nil {
			a.Included = false
			a.OmittedReason = fmt.Sprintf("read error: %v", err)
			continue
		}

		if tokensUsed+tokenCount <= budget {
			ap.Context += fmt.Sprintf("\n--- %s ---\n%s\n", a.Name, string(content))
			tokensUsed += tokenCount
			a.Included = true
			a.TokenCount = tokenCount
		} else if tokenCount > o.maxTokensPerArtifact {
			// Compress: just include a summary reference.
			summary := fmt.Sprintf("[%s: %d bytes, content at %s]", a.Name, a.Size, a.Path)
			ap.Context += "\n" + summary + "\n"
			tokensUsed += estimateTokens(summary)
			a.Included = false
			a.OmittedReason = "budget_exhausted: compressed to reference"
			ap.Omitted = append(ap.Omitted, a.Name+" (compressed)")
		} else {
			a.Included = false
			a.OmittedReason = "budget_exhausted"
			ap.Omitted = append(ap.Omitted, a.Name+" (no budget)")
		}
	}

	// Add context files.
	for _, cf := range c.Files {
		if tokensUsed < budget {
			ap.Context += fmt.Sprintf("\n[file: %s]\n", cf.Path)
			tokensUsed += estimateTokens(cf.Path) + 10
		}
	}

	ap.Tokens = tokensUsed
	ap.Budget = budget

	return ap, nil
}

func (o *Optimizer) readArtifactContent(ctx context.Context, a *domain.Artifact) ([]byte, int, error) {
	// In v0.2, the artifact store provides an Open method.
	// For now, return a placeholder.
	return nil, 0, nil // Will be connected when artifact.Store.Open is wired in
}

// estimateTokens gives a rough token count (4 chars ≈ 1 token).
func estimateTokens(s string) int {
	return len(s) / 4
}

// Ensure unused imports don't cause issues.
