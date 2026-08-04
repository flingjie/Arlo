# TUI Best-Practices Redesign — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evolve the Arlo Bubble Tea TUI to keyboard-first lazygit/k9s feel in three shippable phases: interaction, visuals, density.

**Architecture:** Keep `Model` + three panels + `CommandRegistry` + `Dispatcher`. Phase 1 single-keys call the same `Command.Execute` paths as colon commands. Phase 2 centralizes semantics in `styles.go`. Phase 3 adds layout/density flags on `UIState` without changing gRPC contracts.

**Tech Stack:** Go, Bubble Tea, Lipgloss, existing `internal/tui` package, `go test -race ./internal/tui/...`

**Spec:** `docs/superpowers/specs/2026-08-04-tui-best-practices-redesign.md`

## Global Constraints

- TUI remains a view over arlod; no new daemon RPCs unless unavoidable.
- Colon command mode (`:`) stays as power-user escape hatch.
- `a` = attach; Timeline auto-follow resume = `s` (Phase 3 only).
- Help = `?` only; do not bind `h` to help.
- Each phase independently shippable and testable.
- Do not commit unless the user asks.

## File map

| File | Phase 1 | Phase 2 | Phase 3 |
|------|---------|---------|---------|
| `internal/tui/state.go` | `HelpOpen` | — | collapse/follow/compact/layout flags |
| `internal/tui/command.go` | attach falls back to selected node; help text mentions keys | — | optional |
| `internal/tui/app.go` | single-key dispatch, status bar, help overlay, Esc | focus border colors | layout widths, new keys |
| `internal/tui/styles.go` | — | palette, glyphs, cursor | — |
| `internal/tui/workflow_panel.go` | focus border if owned here | glyphs/cursor | Space detail, column collapse, tree |
| `internal/tui/timeline_panel.go` | — | borders | follow/`s`/`c`/truncate |
| `internal/tui/inspector_panel.go` | — | borders | scroll/highlight/dedupe |
| `internal/tui/tui_test.go` | key + overlay + status bar tests | icon tests | density/layout tests |

---

## Phase 1 — Interaction

### Task 1: Attach falls back to selected node

**Files:**
- Modify: `internal/tui/command.go` (`AttachCommand.Execute`)
- Test: `internal/tui/tui_test.go`

**Interfaces:**
- Consumes: `resolveNodeID(args, ctx) string`
- Produces: `AttachCommand.Execute` works with empty args when `ctx.UIState.SelectedNode` is set

- [x] **Step 1: Write failing test**
- [x] **Step 2: Run test — expect FAIL**
- [x] **Step 3: Implement**
- [x] **Step 4: Run test — expect PASS**

---

### Task 2: Single-key dispatch for a / p / r / R / ?

**Files:**
- Modify: `internal/tui/state.go` — add `HelpOpen bool`
- Modify: `internal/tui/app.go` — `handleKeyMsg`, `appContext()`, `runRegistryCommand(name string) tea.Cmd`
- Test: `internal/tui/tui_test.go`

**Interfaces:**
- Produces: `(*Model).runRegistryCommand(name string) tea.Cmd`
- Produces: keys `a`,`p`,`r`,`R`,`?` handled outside command mode; `Esc`/`q` close help first

- [x] **Step 1: Write failing tests** for `runRegistryCommand("approve")`, `?` toggle `HelpOpen`, `Esc` closes help without quit
- [x] **Step 2: Run — expect FAIL**
- [x] **Step 3: Implement** `HelpOpen`, `appContext`, `runRegistryCommand`, key cases; when help open, `q` closes help only (`ctrl+c` still quits)
- [x] **Step 4: Run — expect PASS**

---

### Task 3: Status bar single-key hints + help overlay + focus mark

**Files:**
- Modify: `internal/tui/app.go` — `renderCommandBar`, `renderHelpOverlay`, `View`
- Modify: `internal/tui/command.go` — `:help` mentions single keys
- Modify: panel titles for focus `*`
- Test: `internal/tui/tui_test.go`

- [x] **Step 1: Failing tests** — View contains `a:attach`, not `:a[ttach]`; `HelpOpen` shows overlay with attach binding
- [x] **Step 2: Implement** status bar hints; brighten `p`/`r` when `isBlocked(selected)`; help overlay box; wire into View like filter overlay; title `*` on focused panel
- [x] **Step 3:** `go test -race ./internal/tui/` PASS

---

## Phase 2 — Visuals

### Task 4: Semantic palette + glyphs + borders

**Files:** `styles.go`, `workflow_panel.go`, `timeline_panel.go`, `inspector_panel.go`, tests

**Glyph map:** RUNNING `●`, WAITING/PENDING `○`, BLOCKED `■`, COMPLETED `✓`, FAILED `✗`, READY `↻`, selection cursor `▶`

- [x] Update `TestStatusIconAllStatuses`; implement icons + row cursor; Yellow for gate (no Purple/Orange on status paths); `NormalBorder` / single-line; focus Cyan vs dim Gray borders
- [x] `go test -race ./internal/tui/` PASS

---

## Phase 3 — Density

### Task 5: Workflow detail toggle + column collapse

- [ ] `Space` toggles session/gate detail; `-`/`+` (and Workflow-focused `←`/`→`) collapse column; tests

### Task 6: Timeline follow / compact / expand

- [ ] Follow default on; scroll pauses; `s` resumes; `c` compact; truncate + `→` expand; tests

### Task 7: Inspector polish + responsive layout

- [ ] Width &lt;100 hide inspector (`i` overlay); &lt;70 single pane; node-scoped inspector; `Launching…` empty state; tests

### Task 8: Regression

- [ ] `go test -race ./internal/tui/`
- [ ] Manual `make run-cli` when daemon available

---

## Spec coverage

| Spec item | Task |
|-----------|------|
| Single keys a/p/r/R/f/? | 2, 3 |
| Status bar + gate highlight | 3 |
| Help overlay; Esc/q close | 2, 3 |
| Attach selected-node fallback | 1 |
| Keep `:` mode | 2 |
| Palette/glyphs/cursor/borders | 4 |
| Space / column collapse | 5 |
| Timeline s/c/truncate | 6 |
| Responsive + inspector | 7 |

## Deferred

- Persist layout/filter; virtual scroll; mouse
