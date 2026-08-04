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
		Type    string          `json:"type"`
		Subtype string          `json:"subtype"`
		Message json.RawMessage `json:"message"`
		Result  string          `json:"result,omitempty"`
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
	}

	return event, true
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
