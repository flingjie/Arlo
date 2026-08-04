# TUI Best-Practices Redesign

Date: 2026-08-04  
Status: Approved (design dialogue)  
Approach: Incremental phases (A) — interaction → visuals → density  
Related: [2026-08-03-tui-v1-design.md](./2026-08-03-tui-v1-design.md)

### Relationship to v1 design

v1 principle “command bar, not key soup” remains for **power/rare** actions via `:`. This redesign **adds** single-key bindings for the high-frequency set (attach / approve / reject / retry / filter / help), matching lazygit/k9s practice. Colon mode is not removed.

## Goal

Evolve the existing three-panel Bubble Tea TUI toward the interaction feel of lazygit / k9s / htop: keyboard-first, high information density without clutter, glanceable status, terminal-friendly, and responsive to width.

Preserve existing domain wiring (`Client`, `Dispatcher`, `CommandRegistry`, event stream). Prefer thin adapters over rewrites.

## Non-goals (this redesign)

- Mouse-first interaction
- Persisting layout ratios / filter prefs to disk (optional follow-up)
- Virtual scrolling for huge logs (defer until compact + filter prove insufficient)
- Replacing `:` command mode (kept as power-user escape hatch)
- Changing gRPC / daemon contracts

## Current baseline

| Area | Today |
|------|--------|
| Layout | Three panels (Workflow / Timeline / Inspector) + status + command bar |
| Keys | `j/k`, `Tab`, `Enter`, `f`, `1–5`, `q`; actions mostly via `:attach` / `:approve` / … |
| Colors | Green, Yellow, Red, Blue, Cyan, Gray, Purple, Orange — too many |
| Borders | Workflow uses single-line box; `PanelStyle` uses rounded borders |
| Status icons | `▶ RUNNING`, `⏸ WAITING`, `✓` / `✗` / `○` / `↻` |
| Help | `:help` dumps to command-bar message |

## Architecture constraints

```
cmd/arlo → internal/tui (Model + panels)
                ├── CommandRegistry  (colon commands; Phase 1 single-keys call same Execute paths)
                ├── WorkflowPanel / TimelinePanel / InspectorPanel
                └── styles.go       (semantic palette; Phase 2 owns this)
```

- Single-key bindings in `Model.handleKeyMsg` dispatch into shared helpers or `CommandRegistry.Execute`, not duplicated RPC logic.
- Focus remains `FocusWorkflow | FocusTimeline | FocusInspector`.
- Tests: extend `internal/tui/tui_test.go` per phase; no daemon required for key/view unit tests.

---

## Phase 1 — Interaction layer

**Outcome:** Operators can drive attach / approve / reject / retry / filter / help without colon mode; status bar always shows single-key hints; focus is visible.

### Keymap

| Key | Action | Notes |
|-----|--------|--------|
| `↑↓` / `j k` | Navigate focused panel | Existing |
| `Tab` | Cycle focus Workflow → Timeline → Inspector | Existing; do **not** bind `h`/`l` to focus (tree collapse keeps them) |
| `Enter` | Sync selection → Inspector | Existing |
| `a` | Attach selected node | Same as `:attach` with selected node |
| `p` | Approve | Same as `:approve` |
| `r` | Reject | Same as `:reject` |
| `R` | Retry | Same as `:retry` |
| `f` | Toggle filter overlay | Existing |
| `?` | Toggle help overlay | New; **not** `h` |
| `q` | Quit | Existing |
| `Esc` | Close overlay if open, else quit | Refine existing |
| `1`–`5` | Inspector tabs | Existing |
| `:` | Command mode | Kept |

**Resolved conflicts (vs original prose design):**

- Attach owns `a`. Timeline auto-follow resume is **not** `a` (Phase 3 uses `s`).
- Help owns `?`. `h` stays available for tree / vim-left later; not help.

### Status bar

Always visible bottom row (non-command-mode):

```
NORMAL  workflow  │  a:attach  p:approve  r:reject  R:retry  f:filter  ?:help  q:quit  │  ↑↓ Tab Enter
```

- Left: mode + focus panel name  
- Center: global single-key hints; when selected node is gate-blocked, brighten `p` and `r`  
- Right: navigation chrome  

Command mode still shows `:` input. Transient `cmdMsg` may briefly replace the hint row.

### Focus indicator

Brighten focused panel border (or title suffix `*`). Prefer border highlight if cheap against existing box drawing.

### Help overlay

`?` opens a half-screen (or centered) keymap list covering Phase 1 keys. Close with `?`, `Esc`, or `q` (close only — `q` while overlay open does not quit the app). Does not remove `:help`.

### Files (expected)

- `internal/tui/app.go` — key dispatch, status bar, help overlay  
- `internal/tui/command.go` — shared execute helpers if needed; help text updated to mention single keys  
- `internal/tui/state.go` — `HelpOpen` (or reuse a generic overlay flag)  
- `internal/tui/tui_test.go` — key → command dispatch tests  

### Acceptance

- Pressing `a`/`p`/`r`/`R` with a selected node invokes the same code paths as the colon commands.  
- Status bar shows single-key hints, not `:attach`-style hints.  
- `?` overlay open/close works; Esc closes overlay before quitting.

---

## Phase 2 — Visual layer

**Outcome:** ≤6 semantic colors; status dual-coded (glyph + color); uniform single-line borders; selection cursor separated from status glyph.

### Palette

| Role | Color | Usage |
|------|--------|--------|
| Title / focused border | Cyan / Bright White | Panel titles, focus border |
| RUNNING | Bright Green or Cyan | `● RUNNING` |
| WAITING / secondary | Dim Gray | `○ WAITING`, timestamps, session ids |
| BLOCKED / warn | Bright Yellow | `■ BLOCKED`, gate lines |
| ERROR / FAILED | Bright Red | `✗ FAILED`, error log lines |
| Selection | Reverse / Bright Blue bg | Selected node row |

Remove **Purple** and **Orange** from the default semantic set.

### Status glyphs

| State | Glyph | Notes |
|-------|-------|--------|
| RUNNING | `●` | Replaces status-as-`▶` |
| WAITING / PENDING | `○` | Replaces `⏸` |
| BLOCKED (gate active) | `■` | Via `displayStatus` path |
| COMPLETED | `✓` | Unchanged |
| FAILED | `✗` | Unchanged |
| READY | `↻` | Cyan label; distinct from WAITING `○` |
| Selection cursor | `▶` | Prefix on selected row only; not a status icon |

### Borders

Unify on single-line `┌─┐│└┘`. Retire rounded `PanelStyle` borders for main panels.

### Focus

Focused panel: bright Cyan border. Unfocused: Dim Gray border. Compatible with Phase 1.

### Files (expected)

- `internal/tui/styles.go` — palette + `StatusIcon` + selection cursor helper  
- `internal/tui/workflow_panel.go` — cursor vs status, gate styling  
- `internal/tui/timeline_panel.go` / `inspector_panel.go` — border/styles  
- `internal/tui/tui_test.go` — update `TestStatusIconAllStatuses` and view assertions  

### Acceptance

- No Purple/Orange required for status readability.  
- Blind (no-color) distinction works via ●○■✓✗.  
- Selected row shows `▶` cursor distinct from RUNNING `●`.

---

## Phase 3 — Density & responsiveness

**Outcome:** Denser tree and timeline without clutter; follow/filter/compact; narrow terminals degrade gracefully.

### Workflow panel

- Tree connectors `│ ├ └` where hierarchy exists.  
- Session / gate sublines collapsed by default; `Space` toggles detail on the selected node.  
- Collapse panel width: `-` or (when Workflow focused) `←` → icon + short name only; `+` or `→` expands.  
- Keep RUNNING / selected node scrolled into view when list exceeds height.

### Timeline panel

- Columns: fixed-width time `15:04:05`, fixed-width level `INFO`/`WARN`/`ERRO`, message.  
- Auto-follow on by default; manual scroll pauses; `s` resumes follow (**not** `a`).  
- `f` filter overlay unchanged in role.  
- `c` toggles compact mode (critical events only: failed, waiting, annotated, completed — exact set documented in implementation).  
- Long lines truncated; with Timeline focused, `→` expands the current line (one-line detail or inline unwrap).

### Inspector panel

- Keep numeric tabs `1`–`5`.  
- Section rules like `── events ──`.  
- Node-scoped only; do not mirror global task/workflow timeline noise.  
- Scroll long content; highlight important annotation keys (`runtime.launch`, workdir-like keys).

### Responsive layout

| Width | Behavior |
|-------|----------|
| ≥ ~100 | Three columns (current) |
| < ~100 | Hide Inspector; `Tab` cycles Workflow ↔ Timeline; `i` opens Inspector as full-width overlay |
| < ~70 | Single pane; `Tab` cycles Workflow / Timeline / Inspector views |

Thresholds may be tuned ±10 cols during implementation; document final constants in code.

### Empty states

Show skeleton / `Launching…` / `no nodes yet` copy instead of an empty void while waiting for snapshot.

### Deferred

- Persist layout + filter to disk  
- Virtualized timeline rendering  
- Mouse click-to-select  

### Files (expected)

- `internal/tui/workflow_panel.go`, `timeline_panel.go`, `inspector_panel.go`, `app.go` (layout widths, overlays, new keys)  
- `internal/tui/state.go` — collapse / follow / compact / layout mode flags  
- Tests for compact filter, follow pause/resume, narrow layout mode  

### Acceptance

- Compact + filter keep a busy timeline readable.  
- Narrow terminal does not clip/overlap unusable UI.  
- `s` restores auto-follow after scroll; `a` still attaches.

---

## Cross-phase keymap (final)

| Key | Phase | Action |
|-----|-------|--------|
| `j k` / arrows | 1 | Navigate |
| `Tab` | 1 | Focus / pane cycle (mode-dependent in Phase 3) |
| `Enter` | 1 | Select → inspector |
| `a` | 1 | Attach |
| `p` / `r` | 1 | Approve / reject |
| `R` | 1 | Retry |
| `f` | 1 | Filter |
| `?` | 1 | Help overlay |
| `q` / `Esc` | 1 | Quit / dismiss |
| `1`–`5` | 1 | Inspector tabs |
| `:` | 1 | Command mode |
| `Space` | 3 | Toggle node detail lines |
| `-` / `+` | 3 | Collapse / expand workflow column |
| `s` | 3 | Resume timeline auto-follow |
| `c` | 3 | Compact timeline |
| `i` | 3 | Open inspector overlay (narrow layout) |
| `→` | 3 | Expand truncated timeline line (when Timeline focused) |

---

## Testing strategy

- Unit tests per phase for keys, icons, status bar substrings, overlay open/close.  
- Prefer table-driven cases for keymap dispatch.  
- Manual smoke: `make run-cli` against a running workflow for attach/approve paths when daemon available.  
- Do not require full e2e daemon for merging Phase 1–2 visual/key changes.

## Rollout

1. Land Phase 1 behind normal merge; verify operators can drop colon habits.  
2. Land Phase 2; update any docs/screenshots that show old glyphs.  
3. Land Phase 3; tune width breakpoints if needed.

Each phase should be independently shippable and not leave the TUI unusable mid-migration.
