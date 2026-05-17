package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRunHook_Success(t *testing.T) {
	res, err := RunHook(context.Background(), "echo hi", nil, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode=%d want 0", res.ExitCode)
	}
	if res.Stdout != "hi\n" {
		t.Errorf("Stdout=%q want %q", res.Stdout, "hi\n")
	}
	if res.TimedOut {
		t.Errorf("TimedOut=true; should be false on success")
	}
	if res.Duration <= 0 {
		t.Errorf("Duration=%v should be > 0", res.Duration)
	}
}

func TestRunHook_NonZeroExit(t *testing.T) {
	res, err := RunHook(context.Background(), "false", nil, time.Second)
	if err == nil {
		t.Fatalf("expected error for non-zero exit")
	}
	if res.ExitCode != 1 {
		t.Errorf("ExitCode=%d want 1", res.ExitCode)
	}
	if res.TimedOut {
		t.Errorf("TimedOut should be false for plain non-zero exit")
	}
}

func TestRunHook_Timeout(t *testing.T) {
	start := time.Now()
	res, err := RunHook(context.Background(), "sleep 10", nil, 100*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected timeout error")
	}
	if !res.TimedOut {
		t.Errorf("TimedOut=false; want true")
	}
	if res.ExitCode != -1 {
		t.Errorf("ExitCode=%d want -1 on timeout", res.ExitCode)
	}
	// Killed within reasonable slack of the timeout.
	if elapsed > 5*time.Second {
		t.Errorf("hook took %v, should die well under 5s", elapsed)
	}
}

func TestRunHook_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res HookResult
	var err error
	start := time.Now()
	go func() {
		res, err = RunHook(ctx, "sleep 30", nil, 30*time.Second)
		close(done)
	}()
	// Let the process actually start before we cancel.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("hook did not exit within 2s of cancel")
	}
	if err == nil {
		t.Errorf("expected error on cancel")
	}
	if res.TimedOut {
		t.Errorf("cancel should NOT be reported as TimedOut")
	}
	if res.ExitCode != -1 {
		t.Errorf("ExitCode=%d want -1 on cancel", res.ExitCode)
	}
	// Sanity: we cancelled fast.
	if time.Since(start) > 5*time.Second {
		t.Errorf("cancel took too long: %v", time.Since(start))
	}
}

func TestRunHook_StdoutTruncation(t *testing.T) {
	// Produce ~100 KB to stdout; ring buffer keeps last 8 KB.
	// `yes "X" | head -c 102400` is portable across BSD + GNU userland.
	cmd := `yes X | head -c 102400`
	res, err := RunHook(context.Background(), cmd, nil, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(res.Stdout) != HookOutputBufBytes {
		t.Errorf("stdout length=%d want %d (ring buffer cap)", len(res.Stdout), HookOutputBufBytes)
	}
	// Every byte in the buffer should be a captured 'X' or '\n' (from yes).
	for i, c := range res.Stdout {
		if c != 'X' && c != '\n' {
			t.Errorf("unexpected byte at offset %d: %q", i, c)
			break
		}
	}
}

func TestRunHook_EnvPassedThrough(t *testing.T) {
	res, err := RunHook(context.Background(), `echo "$BACKUPY_TEST_VAR"`, []string{"BACKUPY_TEST_VAR=hello"}, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.TrimSpace(res.Stdout) != "hello" {
		t.Errorf("stdout=%q want %q", res.Stdout, "hello")
	}
}

func TestRunHook_EmptyCommandRejected(t *testing.T) {
	if _, err := RunHook(context.Background(), "", nil, time.Second); err == nil {
		t.Error("empty command must be rejected")
	}
}

func TestRunHook_StderrCaptured(t *testing.T) {
	res, err := RunHook(context.Background(), `echo "oops" 1>&2; exit 1`, nil, time.Second)
	if err == nil {
		t.Fatalf("expected non-zero exit error")
	}
	if res.ExitCode != 1 {
		t.Errorf("ExitCode=%d want 1", res.ExitCode)
	}
	if strings.TrimSpace(res.Stderr) != "oops" {
		t.Errorf("stderr=%q want %q", res.Stderr, "oops")
	}
}

func TestHookSet_BudgetExhaustion(t *testing.T) {
	hs := NewHookSet()
	// Pre-charge past the budget so the next Run is immediately denied.
	hs.consumed = HooksTotalBudget + time.Second
	_, err := hs.Run(context.Background(), "true", nil, time.Second)
	if err == nil {
		t.Error("expected ErrHooksBudgetExceeded after budget consumed")
	}
}

func TestHookSet_NormalRunCharges(t *testing.T) {
	hs := NewHookSet()
	res, err := hs.Run(context.Background(), "true", nil, time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ExitCode != 0 {
		t.Errorf("ExitCode=%d want 0", res.ExitCode)
	}
	if hs.consumed <= 0 {
		t.Errorf("consumed=%v should be > 0 after a run", hs.consumed)
	}
}

func TestHookRingBuffer_Truncation(t *testing.T) {
	buf := newHookRingBuffer(8)
	// Write more than capacity in chunks.
	for _, chunk := range []string{"abcd", "efgh", "ijkl"} {
		if _, err := buf.Write([]byte(chunk)); err != nil {
			t.Fatalf("Write: %v", err)
		}
	}
	got := buf.String()
	if got != "efghijkl" {
		t.Errorf("ring buffer kept %q, want %q", got, "efghijkl")
	}
}

func TestHookRingBuffer_SingleHugeWrite(t *testing.T) {
	buf := newHookRingBuffer(8)
	// Single write much larger than capacity.
	src := []byte("0123456789ABCDEF")
	if _, err := buf.Write(src); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := buf.String()
	if got != "89ABCDEF" {
		t.Errorf("ring buffer kept %q, want %q", got, "89ABCDEF")
	}
}

func TestHookRingBuffer_SmallWriteUnderCap(t *testing.T) {
	buf := newHookRingBuffer(16)
	if _, err := buf.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != "hello" {
		t.Errorf("got %q want %q", got, "hello")
	}
}
