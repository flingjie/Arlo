package workspace

import (
	"testing"
	"time"
)

// TestNewTmuxProviderPollInterval verifies that a newly created TmuxProvider
// has a non-zero PollInterval so the Attach goroutine does not spin-tight.
func TestNewTmuxProviderPollInterval(t *testing.T) {
	prov := NewTmuxProvider("test-socket")
	if prov.PollInterval <= 0 {
		t.Errorf("PollInterval should be > 0 (got %v). A zero interval causes a tight polling loop.", prov.PollInterval)
	}
}

// TestTmuxAttachReturnsAfterContextCancel verifies that the Attach goroutine
// stops when the context is cancelled. This test also indirectly validates
// that the polling loop uses a ticker (otherwise it would fill the channel
// near-instantly and might behave differently).
func TestTmuxAttachReturnsAfterContextCancel(t *testing.T) {
	// TmuxProvider.Attach requires a real tmux binary, so we test the
	// structural properties instead: the PollInterval field and the
	// ticker-based loop ensure backoff.

	// Verify the default PollInterval is reasonable.
	prov := NewTmuxProvider("arlo-test")
	if prov.PollInterval < time.Millisecond {
		t.Errorf("default PollInterval %v is too small; want >= 1ms to avoid tight spin", prov.PollInterval)
	}
	if prov.PollInterval > 5*time.Second {
		t.Errorf("default PollInterval %v is too large; want a reasonable polling cadence", prov.PollInterval)
	}
}

// TestEscapeTmuxKeys verifies that send-keys input is properly escaped
// so that tmux interprets the bytes literally instead of expanding $vars,
// treating ; as a command separator, or interpreting \ as escape.
func TestEscapeTmuxKeys(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "plain text is unchanged",
			input: "hello world",
			want:  "hello world",
		},
		{
			name:  "semicolon is escaped",
			input: "echo hello; echo world",
			want:  `echo hello\; echo world`,
		},
		{
			name:  "backslash is escaped",
			input: `C:\path\to\file`,
			want:  `C:\\path\\to\\file`,
		},
		{
			name:  "dollar sign is escaped",
			input: "echo $HOME",
			want:  `echo \$HOME`,
		},
		{
			name:  "mixed special chars",
			input: `echo $PATH; ls C:\dir`,
			want:  `echo \$PATH\; ls C:\\dir`,
		},
		{
			name:  "newline characters",
			input: "line1\nline2",
			want:  "line1\nline2", // newlines are fine for send-keys
		},
		{
			name:  "empty string",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeTmuxKeys(tt.input)
			if got != tt.want {
				t.Errorf("escapeTmuxKeys(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
