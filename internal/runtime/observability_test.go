package runtime

import (
	"testing"
)

func TestParseStreamJSON_ResultModelUsage(t *testing.T) {
	// Claude CLI reports real token totals in result.modelUsage (camelCase),
	// while result.usage.input_tokens/output_tokens are often 0.
	line := []byte(`{
		"type":"result",
		"subtype":"success",
		"result":"done",
		"usage":{"input_tokens":0,"output_tokens":0},
		"modelUsage":{
			"claude-haiku-4-5":{"inputTokens":430,"outputTokens":197},
			"claude-sonnet-4":{"inputTokens":100,"outputTokens":50}
		}
	}`)

	ev, ok := ParseStreamJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.Type != "result" {
		t.Fatalf("type = %q, want result", ev.Type)
	}
	if ev.TokensIn != 530 {
		t.Errorf("TokensIn = %d, want 530 (sum of modelUsage)", ev.TokensIn)
	}
	if ev.TokensOut != 247 {
		t.Errorf("TokensOut = %d, want 247 (sum of modelUsage)", ev.TokensOut)
	}
	if ev.Text != "done" {
		t.Errorf("Text = %q, want done", ev.Text)
	}
}

func TestParseStreamJSON_ResultUsageFallback(t *testing.T) {
	// When modelUsage is absent, fall back to result.usage snake_case fields.
	line := []byte(`{
		"type":"result",
		"subtype":"success",
		"result":"ok",
		"usage":{"input_tokens":120,"output_tokens":40}
	}`)

	ev, ok := ParseStreamJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.TokensIn != 120 {
		t.Errorf("TokensIn = %d, want 120", ev.TokensIn)
	}
	if ev.TokensOut != 40 {
		t.Errorf("TokensOut = %d, want 40", ev.TokensOut)
	}
}

func TestParseStreamJSON_AssistantUsageStillParsed(t *testing.T) {
	line := []byte(`{
		"type":"assistant",
		"message":{
			"model":"x",
			"content":[{"type":"text","text":"hi"}],
			"usage":{"input_tokens":10,"output_tokens":5}
		}
	}`)

	ev, ok := ParseStreamJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.TokensIn != 10 || ev.TokensOut != 5 {
		t.Errorf("tokens = %d/%d, want 10/5", ev.TokensIn, ev.TokensOut)
	}
}

func TestParseStreamJSON_PreferModelUsageOverZeroUsage(t *testing.T) {
	line := []byte(`{
		"type":"result",
		"result":"x",
		"usage":{"input_tokens":0,"output_tokens":0},
		"modelUsage":{"m":{"inputTokens":99,"outputTokens":11}}
	}`)

	ev, ok := ParseStreamJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.TokensIn != 99 || ev.TokensOut != 11 {
		t.Errorf("tokens = %d/%d, want 99/11 from modelUsage", ev.TokensIn, ev.TokensOut)
	}
}

func TestParsePiJSON_MessageEnd(t *testing.T) {
	// Pi's message_end for assistant carries usage.input/output.
	line := []byte(`{
		"type":"message_end",
		"message":{
			"role":"assistant",
			"content":[{"type":"text","text":"Hi!"}],
			"usage":{"input":64,"output":29,"cacheRead":3968,"cacheWrite":0,"reasoning":14,"totalTokens":4061}
		}
	}`)

	ev, ok := ParsePiJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.Type != "message_end" {
		t.Errorf("type = %q, want message_end", ev.Type)
	}
	if ev.TokensIn != 64 {
		t.Errorf("TokensIn = %d, want 64", ev.TokensIn)
	}
	if ev.TokensOut != 29 {
		t.Errorf("TokensOut = %d, want 29", ev.TokensOut)
	}
}

func TestParsePiJSON_SkipsUserMessage(t *testing.T) {
	// Pi's message_end for user/toolResult roles should not count tokens.
	line := []byte(`{
		"type":"message_end",
		"message":{"role":"user","content":[{"type":"text","text":"hi"}],"usage":null}
	}`)

	ev, ok := ParsePiJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.TokensIn != 0 || ev.TokensOut != 0 {
		t.Errorf("user message_end should not count tokens, got %d/%d", ev.TokensIn, ev.TokensOut)
	}
}

func TestParsePiJSON_AgentEnd(t *testing.T) {
	// Pi's agent_end normalizes to 'result' type.
	line := []byte(`{"type":"agent_end","messages":[],"willRetry":false}`)

	ev, ok := ParsePiJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.Type != "result" {
		t.Errorf("type = %q, want result (normalized)", ev.Type)
	}
}

func TestParsePiJSON_TurnEnd(t *testing.T) {
	// Pi's turn_end carries cumulative usage from the last assistant message.
	line := []byte(`{
		"type":"turn_end",
		"message":{
			"role":"assistant",
			"usage":{"input":200,"output":150,"totalTokens":5000}
		}
	}`)

	ev, ok := ParsePiJSON(line)
	if !ok {
		t.Fatal("expected parse ok")
	}
	if ev.TokensIn != 200 || ev.TokensOut != 150 {
		t.Errorf("tokens = %d/%d, want 200/150", ev.TokensIn, ev.TokensOut)
	}
}

func TestParsePiJSON_InvalidJSON(t *testing.T) {
	_, ok := ParsePiJSON([]byte(`not json`))
	if ok {
		t.Error("expected parse failure for invalid JSON")
	}
}
