package runtime

import (
	"encoding/json"
	"time"
)

// StreamEvent is a parsed observation from a runtime's output stream.
type StreamEvent struct {
	Timestamp time.Time

	// Fields from Claude stream-json.
	Type    string `json:"type"`    // "assistant", "user", "system", "result"
	Subtype string `json:"subtype"` // "init", "success", "error"

	// Text content, if any.
	Text string `json:"text,omitempty"`

	// Tool call, if any.
	ToolName  string         `json:"tool_name,omitempty"`
	ToolInput map[string]any `json:"tool_input,omitempty"`
	ToolID    string         `json:"tool_id,omitempty"`

	// Tool result, if any.
	ToolResult string `json:"tool_result,omitempty"`

	// Token usage, if reported.
	TokensIn  int64 `json:"tokens_in,omitempty"`
	TokensOut int64 `json:"tokens_out,omitempty"`
}

// ParseStreamJSON reads one line of Claude stream-json output and returns
// a StreamEvent. Returns false if the line could not be parsed.
func ParseStreamJSON(line []byte) (StreamEvent, bool) {
	var raw struct {
		Type       string          `json:"type"`
		Subtype    string          `json:"subtype"`
		Message    json.RawMessage `json:"message"`
		Result     string          `json:"result,omitempty"`
		Usage      json.RawMessage `json:"usage,omitempty"`
		ModelUsage json.RawMessage `json:"modelUsage,omitempty"`
	}
	if err := json.Unmarshal(line, &raw); err != nil {
		return StreamEvent{}, false
	}

	event := StreamEvent{
		Timestamp: time.Now(),
		Type:      raw.Type,
		Subtype:   raw.Subtype,
	}

	switch raw.Type {
	case "assistant":
		event.parseAssistant(raw.Message)
	case "user":
		event.parseUser(raw.Message)
	case "result":
		event.Text = raw.Result
		event.parseResultUsage(raw.Usage, raw.ModelUsage)
	}

	return event, true
}

// parseResultUsage extracts token totals from a Claude stream-json "result" event.
// Prefer modelUsage (camelCase per-model totals) when present and non-zero;
// otherwise fall back to usage.input_tokens / usage.output_tokens.
func (e *StreamEvent) parseResultUsage(usageRaw, modelUsageRaw json.RawMessage) {
	if len(modelUsageRaw) > 0 {
		var models map[string]struct {
			InputTokens  int64 `json:"inputTokens"`
			OutputTokens int64 `json:"outputTokens"`
		}
		if err := json.Unmarshal(modelUsageRaw, &models); err == nil {
			var in, out int64
			for _, m := range models {
				in += m.InputTokens
				out += m.OutputTokens
			}
			if in > 0 || out > 0 {
				e.TokensIn = in
				e.TokensOut = out
				return
			}
		}
	}

	if len(usageRaw) == 0 {
		return
	}
	var usage struct {
		InputTokens  int64 `json:"input_tokens"`
		OutputTokens int64 `json:"output_tokens"`
	}
	if err := json.Unmarshal(usageRaw, &usage); err != nil {
		return
	}
	e.TokensIn = usage.InputTokens
	e.TokensOut = usage.OutputTokens
}

func (e *StreamEvent) parseAssistant(raw json.RawMessage) {
	var msg struct {
		Model   string `json:"model"`
		Content []struct {
			Type  string          `json:"type"`
			Text  string          `json:"text"`
			Name  string          `json:"name"`
			ID    string          `json:"id"`
			Input map[string]any  `json:"input"`
		} `json:"content"`
		Usage struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	e.TokensIn = msg.Usage.InputTokens
	e.TokensOut = msg.Usage.OutputTokens

	for _, c := range msg.Content {
		switch c.Type {
		case "text":
			e.Text = c.Text
		case "tool_use":
			e.ToolName = c.Name
			e.ToolInput = c.Input
			e.ToolID = c.ID
		}
	}
}

func (e *StreamEvent) parseUser(raw json.RawMessage) {
	var msg struct {
		Content []struct {
			Type      string `json:"type"`
			ToolUseID string `json:"tool_use_id"`
			Content   string `json:"content"`
		} `json:"content"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	for _, c := range msg.Content {
		if c.Type == "tool_result" {
			e.ToolID = c.ToolUseID
			e.ToolResult = truncate(c.Content, 200)
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ParsePiJSON reads one line of Pi stream-json output and returns a StreamEvent.
// Pi uses a different event schema from Claude:
//
//	message_start (role=assistant) → content may include tool_use blocks
//	message_end (role=assistant) → usage.input, usage.output, usage.totalTokens
//	turn_end → same usage as final assistant message_end
//	agent_end → final event, no usage
//
// Returns false if the line could not be parsed.
func ParsePiJSON(line []byte) (StreamEvent, bool) {
	var raw struct {
		Type    string          `json:"type"`
		Message json.RawMessage `json:"message"`
	}
	if err := json.Unmarshal(line, &raw); err != nil {
		return StreamEvent{}, false
	}

	event := StreamEvent{
		Timestamp: time.Now(),
		Type:      raw.Type,
	}

	switch raw.Type {
	case "message_start":
		event.parsePiMessageEnd(raw.Message)
	case "message_end":
		event.parsePiMessageEnd(raw.Message)
	case "turn_end":
		event.parsePiMessageEnd(raw.Message)
	case "agent_end":
		event.Type = "result" // normalize to Claude-equivalent type
	}

	return event, true
}

// parsePiMessageEnd extracts token usage and tool calls from Pi's message/turn end events.
// Pi's usage object: {"input": N, "output": N, "totalTokens": N, "cacheRead": N, ...}
// Pi's content array may contain tool_use items: {"type":"tool_use","name":"Bash","id":"...","input":{...}}
func (e *StreamEvent) parsePiMessageEnd(raw json.RawMessage) {
	var msg struct {
		Role    string `json:"role"`
		Content []struct {
			Type  string         `json:"type"`
			Text  string         `json:"text"`
			Name  string         `json:"name"`
			ID    string         `json:"id"`
			Input map[string]any `json:"input"`
		} `json:"content"`
		Usage struct {
			Input       int64 `json:"input"`
			Output      int64 `json:"output"`
			TotalTokens int64 `json:"totalTokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}

	// Only track assistant message usage (skip user/toolResult).
	if msg.Role != "assistant" {
		// Detect tool invocations even for non-assistant roles.
		// Pi's tool execution creates message_start/message_end with role="toolResult".
		if msg.Role == "toolResult" {
			e.ToolName = "agent-tool"
		}
		return
	}

	e.TokensIn = msg.Usage.Input
	e.TokensOut = msg.Usage.Output

	for _, c := range msg.Content {
		switch c.Type {
		case "text":
			e.Text = c.Text
		case "tool_use":
			e.ToolName = c.Name
			e.ToolID = c.ID
			e.ToolInput = c.Input
		}
	}
}
