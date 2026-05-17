package main

import (
	"context"
	"fmt"
	"log/slog"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/backupy/backupy/apps/agent/internal/config"
	"github.com/backupy/backupy/apps/agent/internal/discovery"
	"github.com/backupy/backupy/apps/agent/internal/logging"
	"github.com/backupy/backupy/apps/agent/internal/metrics"
	"github.com/backupy/backupy/apps/agent/internal/pipeline"
	agentproto "github.com/backupy/backupy/apps/agent/internal/proto"
	"github.com/backupy/backupy/apps/agent/internal/queue"
	"github.com/backupy/backupy/apps/agent/internal/state"
	"github.com/backupy/backupy/apps/agent/internal/version"
	"github.com/backupy/backupy/apps/agent/internal/wss"
	backupv1 "github.com/backupy/backupy/packages/proto/gen/go/backupv1"
)

// discoveryInterval is the period for the auto-rescan loop. The spec
// pins this to "once an hour" — see docs/03-agent-spec.md.
const discoveryInterval = time.Hour

func newRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Start the agent service loop",
		Long: `Run loads bootstrap config from env, opens the persistent state DB,
opens the WSS connection to the control plane, and blocks until SIGINT or
SIGTERM is received.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runAgent()
		},
	}
}

func runAgent() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	logger := logging.New(cfg.LogLevel)
	slog.SetDefault(logger)

	logger.Info("backupy agent starting",
		slog.String("version", version.Version),
		slog.String("commit", version.Commit),
		slog.String("server_url", cfg.ServerURL),
		slog.String("state_dir", cfg.StateDir),
	)

	// Persistent state — required to even start.
	store, err := state.Open(cfg.StateDBPath(), state.Options{AgentKey: cfg.AgentKey})
	if err != nil {
		return fmt.Errorf("state: %w", err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			logger.Warn("state: close error", slog.Any("err", cerr))
		}
	}()

	q := queue.NewBolt(store)
	if depth, derr := q.Depth(); derr == nil && depth > 0 {
		logger.Info("recovered pending jobs from queue", slog.Int("depth", depth))
	}

	// Auto-discovery + backup pipeline runner.
	scanner := discovery.NewScanner(discovery.Config{
		DockerSocket: cfg.DockerSocket,
		Logger:       logger,
	})

	// In-memory AgentConfig snapshot — the WSS client pushes a fresh
	// ConfigUpdate at register-time and on every change, and the
	// pipeline runner needs the current Targets/Jobs to resolve a
	// RunBackup.job_id to a Connection spec.
	snap := newConfigSnapshot()

	runner := pipeline.NewRunner(
		map[string]pipeline.Driver{
			"postgresql": pipeline.NewPgDump(),
			"mysql":      pipeline.NewMysqldump(),
			"mariadb":    pipeline.NewMysqldump(),
			// --- B14 BEGIN
			"mongodb": pipeline.NewMongoDump(),
			"redis":   pipeline.NewRedisDriver(),
			"sqlite":  pipeline.NewSqliteDriver(),
			// --- B14 END
		},
		pipeline.NewUploader(),
		pipeline.WithLogger(logger),
		pipeline.WithTargetLookup(snap),
		pipeline.WithJobLookup(snap),
	)

	// The handlers struct is supplied by the WSS agent side; we plug
	// the runner into OnRunBackup so an inbound RunBackup envelope
	// drives an end-to-end backup.
	var client *wss.Client // forward-declared for handler closures
	handlers := &wss.Handlers{
		OnConfigUpdate: func(_ context.Context, msg *agentproto.ConfigUpdate) error {
			c := msg.GetConfig()
			if c == nil {
				return nil
			}
			snap.Set(c)
			logger.Info("wss: config update applied",
				slog.Uint64("version", c.Version),
				slog.Int("targets", len(c.Targets)),
				slog.Int("jobs", len(c.Jobs)))
			return nil
		},
		OnRunBackup: func(ctx context.Context, msg *agentproto.RunBackup) error {
			// Execute the backup pipeline in a goroutine so the read
			// loop is not blocked while a multi-minute upload runs.
			jobID := msg.JobId
			runID := msg.RunId
			go func() {
				completed, runErr := runner.Run(ctx, msg)
				if runErr != nil {
					logger.Error("pipeline: run failed",
						slog.String("job_id", jobID),
						slog.String("run_id", runID),
						slog.Any("err", runErr))
					if client != nil {
						_ = client.Send(buildJobFailed(jobID, runID, runErr))
					}
					return
				}
				if client != nil {
					_ = client.Send(buildBackupCompleted(completed))
				}
			}()
			return nil
		},
		OnCancelJob: func(_ context.Context, msg *agentproto.CancelJob) error {
			logger.Info("wss: cancel job", slog.String("job_id", msg.JobId))
			return nil
		},
		OnRunHealthCheck: func(_ context.Context, msg *agentproto.RunHealthCheck) error {
			logger.Info("wss: run health check", slog.String("check_id", msg.CheckId))
			return nil
		},
		OnSelfUpdate: func(_ context.Context, msg *agentproto.SelfUpdate) error {
			logger.Info("wss: self-update requested",
				slog.String("target_version", msg.TargetVersion))
			return nil
		},
	}

	client = wss.NewClient(wss.Config{
		ServerURL:     cfg.ServerURL,
		AgentKey:      cfg.AgentKey,
		AgentVersion:  version.Version,
		AllowInsecure: cfg.DevAllowInsecure,
		Capabilities:  []string{"pg_dump", "mysqldump", "docker_discovery"},
	}, store, q, handlers, nil, logger)

	// Graceful shutdown on SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- D-19 BEGIN: Prometheus metrics endpoint (loopback by default).
	metrics.SetBuildInfo(version.Version, version.Commit)
	metrics.SetWSSState("disconnected")
	if cfg.MetricsListenAddr != "" {
		go func() {
			if err := metrics.ListenAndServe(ctx, cfg.MetricsListenAddr); err != nil {
				logger.Error("metrics: server error",
					slog.String("addr", cfg.MetricsListenAddr),
					slog.Any("err", err))
			}
		}()
		logger.Info("metrics endpoint enabled",
			slog.String("addr", cfg.MetricsListenAddr))
	}
	// --- D-19 END

	// Periodic discovery loop. Publishes DiscoveryReport envelopes
	// through the WSS client once it is connected; while disconnected,
	// Send buffers to the persistent queue automatically.
	go runDiscoveryLoop(ctx, scanner, client, logger)

	if err := client.Start(ctx); err != nil {
		return fmt.Errorf("wss: %w", err)
	}
	logger.Info("backupy agent stopped cleanly")
	return nil
}

// runDiscoveryLoop runs an immediate scan plus a periodic rescan every
// discoveryInterval, publishing DiscoveryReport envelopes through the
// supplied client.
func runDiscoveryLoop(ctx context.Context, scanner discovery.Scanner, client *wss.Client, logger *slog.Logger) {
	scan := func() {
		scanCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		containers, err := scanner.Scan(scanCtx)
		if err != nil {
			logger.Warn("discovery: scan failed", slog.Any("err", err))
			return
		}
		logger.Info("discovery: scan complete", slog.Int("containers", len(containers)))
		report := discovery.BuildReport(containers)
		if client == nil {
			return
		}
		env := agentproto.NewEnvelope()
		env.Payload = &backupv1.Envelope_Discovery{Discovery: report}
		if err := client.Send(env); err != nil {
			logger.Warn("discovery: send report", slog.Any("err", err))
		}
	}

	scan() // run once on startup
	ticker := time.NewTicker(discoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan()
		}
	}
}

// buildBackupCompleted wraps a BackupCompleted in an outbound envelope.
func buildBackupCompleted(c *backupv1.BackupCompleted) *agentproto.Envelope {
	env := agentproto.NewEnvelope()
	env.Payload = &backupv1.Envelope_BackupCompleted{BackupCompleted: c}
	return env
}

// buildJobFailed wraps a FAILED JobUpdate in an outbound envelope.
// Both job_id and run_id are populated so the scheduler can correlate
// the failure to its BackupRun row and apply the retry policy.
func buildJobFailed(jobID, runID string, runErr error) *agentproto.Envelope {
	env := agentproto.NewEnvelope()
	env.Payload = &backupv1.Envelope_JobUpdate{
		JobUpdate: &backupv1.JobUpdate{
			JobId:        jobID,
			RunId:        runID,
			Status:       backupv1.JobStatus_FAILED,
			ErrorMessage: runErr.Error(),
		},
	}
	return env
}

// configSnapshot is the in-process projection of the latest AgentConfig
// the agent has received. It satisfies both pipeline.TargetLookup and
// pipeline.JobLookup so the runner can resolve job_id -> connection.
type configSnapshot struct {
	mu      sync.RWMutex
	targets map[string]*backupv1.Target
	jobs    map[string]*backupv1.BackupJobSpec
}

func newConfigSnapshot() *configSnapshot {
	return &configSnapshot{
		targets: map[string]*backupv1.Target{},
		jobs:    map[string]*backupv1.BackupJobSpec{},
	}
}

func (s *configSnapshot) Set(cfg *backupv1.AgentConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.targets = map[string]*backupv1.Target{}
	for _, t := range cfg.Targets {
		s.targets[t.Id] = t
	}
	s.jobs = map[string]*backupv1.BackupJobSpec{}
	for _, j := range cfg.Jobs {
		s.jobs[j.Id] = j
	}
}

func (s *configSnapshot) Target(id string) (*backupv1.Target, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.targets[id]
	return t, ok
}

func (s *configSnapshot) Job(id string) (*backupv1.BackupJobSpec, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}
