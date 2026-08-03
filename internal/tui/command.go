package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
)

// AppContext provides commands access to the TUI state.
type AppContext struct {
	Socket     string
	WorkflowID string
	Client     *Client
	UIState    *UIState
	Workflow   *WorkflowState
	Dispatch   func(InternalEvent)
}

// Command is an executable command in the command bar.
type Command interface {
	Name() string
	Aliases() []string
	Description() string
	Usage() string
	Execute(args []string, ctx *AppContext) tea.Cmd
}

// CommandRegistry holds all registered commands.
type CommandRegistry struct {
	commands map[string]Command
}

// NewCommandRegistry creates a new registry and registers built-in commands.
func NewCommandRegistry() *CommandRegistry {
	r := &CommandRegistry{commands: make(map[string]Command)}
	r.Register(&QuitCommand{})
	r.Register(&HelpCommand{})
	r.Register(&FilterCommand{})
	r.Register(&RefreshCommand{})
	r.Register(&AttachCommand{})
	return r
}

// Register adds a command.
func (r *CommandRegistry) Register(cmd Command) {
	r.commands[cmd.Name()] = cmd
	for _, alias := range cmd.Aliases() {
		r.commands[alias] = cmd
	}
}

// Execute finds a command by name and executes it.
func (r *CommandRegistry) Execute(input string, ctx *AppContext) tea.Cmd {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return nil
	}
	name := parts[0]
	args := parts[1:]

	cmd, ok := r.commands[name]
	if !ok {
		return func() tea.Msg { return commandMsg{output: fmt.Sprintf("unknown command: %s", name)} }
	}
	return cmd.Execute(args, ctx)
}

type commandMsg struct {
	output string
}

// ── QuitCommand ────────────────────────────────────

type QuitCommand struct{}

func (c *QuitCommand) Name() string        { return "quit" }
func (c *QuitCommand) Aliases() []string   { return []string{"q"} }
func (c *QuitCommand) Description() string { return "Exit the TUI" }
func (c *QuitCommand) Usage() string       { return ":quit" }

func (c *QuitCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return tea.Quit
}

// ── HelpCommand ────────────────────────────────────

type HelpCommand struct{}

func (c *HelpCommand) Name() string        { return "help" }
func (c *HelpCommand) Aliases() []string   { return []string{"h"} }
func (c *HelpCommand) Description() string { return "Show available commands" }
func (c *HelpCommand) Usage() string       { return ":help" }

func (c *HelpCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return func() tea.Msg {
		lines := []string{
			"Available commands:",
			"  :quit, :q        Exit the TUI",
			"  :help, :h        Show this help",
			"  :filter, :f      Toggle event filter",
			"  :refresh, :rf    Reconnect stream and fetch snapshot",
			"  :attach, :a      Attach to a node's session (usage: :attach <node-id>)",
		}
		return commandMsg{output: strings.Join(lines, "\n")}
	}
}

// ── FilterCommand ──────────────────────────────────

type FilterCommand struct{}

func (c *FilterCommand) Name() string        { return "filter" }
func (c *FilterCommand) Aliases() []string   { return []string{"f"} }
func (c *FilterCommand) Description() string { return "Toggle event filter overlay" }
func (c *FilterCommand) Usage() string       { return ":filter" }

func (c *FilterCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return func() tea.Msg {
		ctx.UIState.FilterOpen = !ctx.UIState.FilterOpen
		return nil
	}
}

// ── RefreshCommand ─────────────────────────────────

type RefreshCommand struct{}

func (c *RefreshCommand) Name() string        { return "refresh" }
func (c *RefreshCommand) Aliases() []string   { return []string{"rf"} }
func (c *RefreshCommand) Description() string { return "Reconnect stream and fetch snapshot" }
func (c *RefreshCommand) Usage() string       { return ":refresh" }

func (c *RefreshCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	return tea.Batch(
		ctx.Client.SubscribeEvents(ctx.WorkflowID),
		ctx.Client.GetSnapshot(ctx.WorkflowID),
	)
}

// ── AttachCommand ──────────────────────────────────

type AttachCommand struct{}

func (c *AttachCommand) Name() string        { return "attach" }
func (c *AttachCommand) Aliases() []string   { return []string{"a"} }
func (c *AttachCommand) Description() string { return "Attach to a node's workspace session" }
func (c *AttachCommand) Usage() string       { return ":attach <node-id>" }

func (c *AttachCommand) Execute(args []string, ctx *AppContext) tea.Cmd {
	if len(args) < 1 {
		return func() tea.Msg { return commandMsg{output: "usage: :attach <node-id>"} }
	}
	nodeID := args[0]
	return func() tea.Msg {
		for _, n := range ctx.Workflow.Nodes {
			if n.NodeId == nodeID && n.SessionId != "" {
				return attachMsg{nodeID: nodeID, sessionID: n.SessionId}
			}
		}
		return commandMsg{output: fmt.Sprintf("no session found for node %s", nodeID)}
	}
}

type attachMsg struct {
	nodeID    string
	sessionID string
}
