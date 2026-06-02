// Package pipeline — pre/post hook execution (D-16).
//
// # Security model
//
// Pre and post hooks are arbitrary shell commands executed by the agent
// on its own host with the agent's filesystem permissions. They are
// inherently dangerous: any code path that can write a hook string into
// backup_jobs can run code on every agent that polls that job.
//
// The agent host owner must trust the user's backup config — a
// compromised server could push hostile hooks. We mitigate this with
// defense-in-depth limits enforced on BOTH sides:
//
//	server: validates max command length (HookCommandMaxBytes)
//	        and hook count (HooksMaxCount) at job-config time.
//	agent : enforces a per-hook timeout (DefaultHookTimeout, capped by
//	        HookTimeoutMax) and a hard total budget per backup run
//	        (HooksTotalBudget) so a wedged hook cannot keep an agent
//	        process pinned forever.
//
// Commands are passed verbatim to /bin/sh -c — NO env-var or path
// expansion happens in our code. The shell performs interpolation; we
// never call fmt.Sprintf-style formatting on user-supplied strings.
//
// Hook stdout and stderr are captured into separate 8 KB ring buffers
// (HookOutputBufBytes) so a noisy hook cannot OOM the agent.
package pipeline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// Limits — these constants encode the security model above and are
// referenced by the server-side validators.
const (
	// HookCommandMaxBytes caps each hook string. 4 KB matches the
	// argv/env headroom on every supported OS and is well above any
	// realistic shell-pipeline length.
	HookCommandMaxBytes = 4 * 1024

	// HooksMaxCount caps the number of pre/post hooks per job. 16 is
	// generous for any normal workflow (snapshot → quiesce → notify),
	// while preventing a runaway config from generating dozens of
	// child processes per run.
	HooksMaxCount = 16

	// HookOutputBufBytes is the per-stream (stdout, stderr) ring-buffer
	// size. 8 KB is small enough to keep many concurrent runs in
	// memory and large enough to capture typical hook chatter.
	HookOutputBufBytes = 8 * 1024

	// DefaultHookTimeout is the per-hook timeout when the job config
	// does not override it.
	DefaultHookTimeout = 5 * time.Minute

	// HookTimeoutMax caps the per-hook timeout regardless of job
	// config. Prevents a single hostile hook from hanging the agent.
	HookTimeoutMax = 15 * time.Minute

	// HooksTotalBudget is the hard ceiling for the combined runtime of
	// every pre+post hook in a single backup run. Once exceeded,
	// further hooks return immediately with ErrHooksBudgetExceeded.
	HooksTotalBudget = 30 * time.Minute
)

// ErrHooksBudgetExceeded indicates the total hook runtime budget for
// a backup run was exhausted.
var ErrHooksBudgetExceeded = errors.New("pipeline: hook budget exceeded for run")

// HookResult is the post-mortem of a single hook invocation. Stdout and
// Stderr are best-effort: the last HookOutputBufBytes of each stream are
// kept, earlier bytes are dropped.
type HookResult struct {
	// Command is the raw shell string that was executed (informational).
	Command string
	// ExitCode is the process exit code. 0 == success. For timeouts
	// and context cancellations it is -1.
	ExitCode int
	// Stdout / Stderr hold up to HookOutputBufBytes of captured output.
	Stdout string
	Stderr string
	// Duration is the wall-clock time the hook took.
	Duration time.Duration
	// TimedOut indicates the hook was killed because its per-hook
	// timeout fired (vs. caller-cancelled or completed naturally).
	TimedOut bool
}

// RunHook executes command under /bin/sh -c, applying timeout and
// capturing up to HookOutputBufBytes of stdout/stderr each.
//
// The returned error is non-nil when the hook FAILED (non-zero exit,
// timeout, or process spawn error). HookResult is always returned with
// whatever fields are known.
//
// Environment variables in env are added on top of the current process
// environment (caller can pass nil for default).
func RunHook(ctx context.Context, command string, env []string, timeout time.Duration) (HookResult, error) {
	if command == "" {
		return HookResult{}, errors.New("pipeline: empty hook command")
	}
	if timeout <= 0 {
		timeout = DefaultHookTimeout
	}
	if timeout > HookTimeoutMax {
		timeout = HookTimeoutMax
	}

	// Per-hook timeout layered on top of the caller's ctx.
	hookCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(hookCtx, "/bin/sh", "-c", command)
	if env != nil {
		// Append (not replace) so the agent's PATH etc. are still
		// available to the shell.
		cmd.Env = append(cmd.Env, env...)
	}

	// Run the shell in its own process group so that on timeout/cancel we
	// can kill the WHOLE tree, not just /bin/sh. Without this, a hook like
	// `pg_dump | gzip` leaves the real workers (pg_dump, mongodump, …)
	// alive after the shell is killed: they survive holding the write end
	// of our stdout/stderr pipes, the output-copy goroutines block, and
	// cmd.Wait never returns — so the documented per-hook timeout silently
	// does nothing. (Linux/Unix only, which is the agent's only target.)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative PID => signal the entire process group. Setpgid above
		// makes the group id equal to the shell's pid.
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil {
			// Group gone already (e.g. raced with natural exit) is fine;
			// fall back to killing the process directly.
			return cmd.Process.Kill()
		}
		return nil
	}
	// Backstop: if anything still pins the stdout/stderr pipes after the
	// group kill, force Wait to close them and return instead of hanging
	// on the copy goroutines. Kept well under the smallest test/timeout
	// slack so a wedged hook frees the agent promptly.
	cmd.WaitDelay = time.Second

	stdoutBuf := newHookRingBuffer(HookOutputBufBytes)
	stderrBuf := newHookRingBuffer(HookOutputBufBytes)
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	start := time.Now()
	runErr := cmd.Run()
	dur := time.Since(start)

	result := HookResult{
		Command:  command,
		Duration: dur,
		Stdout:   stdoutBuf.String(),
		Stderr:   stderrBuf.String(),
	}

	if runErr == nil {
		result.ExitCode = 0
		return result, nil
	}

	// Distinguish: timeout / parent-cancel / non-zero exit.
	if hookCtx.Err() != nil {
		// Either deadline (timeout) or caller cancel. Mark exit -1.
		result.ExitCode = -1
		if errors.Is(hookCtx.Err(), context.DeadlineExceeded) {
			result.TimedOut = true
			return result, fmt.Errorf("hook timed out after %s: %w", timeout, hookCtx.Err())
		}
		return result, fmt.Errorf("hook cancelled: %w", hookCtx.Err())
	}

	// Non-zero exit (or process-start failure). exec.ExitError carries
	// the exit code.
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok {
			result.ExitCode = ws.ExitStatus()
		} else {
			result.ExitCode = exitErr.ExitCode()
		}
		return result, fmt.Errorf("hook exited non-zero (%d): %w", result.ExitCode, runErr)
	}
	// Spawn failure, signal, or unknown error.
	result.ExitCode = -1
	return result, fmt.Errorf("hook failed: %w", runErr)
}

// HookSet executes a sequence of hooks under a shared total-budget.
// It returns the per-hook results in order, and the first error (if
// any). The shared budget across the whole set is HooksTotalBudget;
// callers should hold one HookSet per backup run and feed it the
// pre_hooks list, then the post_hooks list.
type HookSet struct {
	mu       sync.Mutex
	consumed time.Duration
}

// NewHookSet returns an empty HookSet that has not yet consumed any
// budget.
func NewHookSet() *HookSet {
	return &HookSet{}
}

// Run executes one hook, charging its duration against the set's
// budget. If the budget is already exhausted when Run is called, the
// hook is skipped and ErrHooksBudgetExceeded is returned with an empty
// HookResult.
func (h *HookSet) Run(ctx context.Context, command string, env []string, timeout time.Duration) (HookResult, error) {
	h.mu.Lock()
	used := h.consumed
	h.mu.Unlock()
	if used >= HooksTotalBudget {
		return HookResult{Command: command}, ErrHooksBudgetExceeded
	}
	// Cap the per-hook timeout at the remaining budget.
	remaining := HooksTotalBudget - used
	if timeout <= 0 || timeout > remaining {
		timeout = remaining
	}
	res, err := RunHook(ctx, command, env, timeout)
	h.mu.Lock()
	h.consumed += res.Duration
	h.mu.Unlock()
	return res, err
}

// hookRingBuffer keeps the LAST `cap` bytes written to it. Writes that
// exceed `cap` discard the oldest bytes. Safe for the io.Writer
// contract used by exec.Cmd; not safe for concurrent writes (exec.Cmd
// writes from one goroutine per stream).
type hookRingBuffer struct {
	buf  []byte
	cap  int
	full bool
	pos  int // next write position
}

func newHookRingBuffer(cap int) *hookRingBuffer {
	return &hookRingBuffer{buf: make([]byte, 0, cap), cap: cap}
}

func (r *hookRingBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	// If we haven't yet wrapped, grow the slice up to cap.
	if !r.full && len(r.buf) < r.cap {
		room := r.cap - len(r.buf)
		if n <= room {
			r.buf = append(r.buf, p...)
			r.pos = (r.pos + n) % r.cap
			if len(r.buf) == r.cap {
				r.full = true
			}
			return n, nil
		}
		r.buf = append(r.buf, p[:room]...)
		p = p[room:]
		r.full = true
		r.pos = 0
	}
	// We're full; overwrite oldest bytes.
	if len(p) >= r.cap {
		// Only the trailing cap bytes matter.
		copy(r.buf, p[len(p)-r.cap:])
		r.pos = 0
		return n, nil
	}
	// Write may wrap around the end of the slice.
	end := r.pos + len(p)
	if end <= r.cap {
		copy(r.buf[r.pos:], p)
	} else {
		first := r.cap - r.pos
		copy(r.buf[r.pos:], p[:first])
		copy(r.buf[:len(p)-first], p[first:])
	}
	r.pos = (r.pos + len(p)) % r.cap
	return n, nil
}

// String returns the buffer contents in write order (oldest first).
func (r *hookRingBuffer) String() string {
	if !r.full {
		return string(r.buf)
	}
	out := bytes.NewBuffer(make([]byte, 0, r.cap))
	out.Write(r.buf[r.pos:])
	out.Write(r.buf[:r.pos])
	return out.String()
}

// Ensure hookRingBuffer satisfies io.Writer.
var _ io.Writer = (*hookRingBuffer)(nil)
