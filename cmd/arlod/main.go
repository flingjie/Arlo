// arlod is the Arlo daemon — the AgentOS Control Plane.
//
// It runs a gRPC server over a Unix socket and manages agent lifecycles
// through event sourcing, reconciliation, runtime adapters, and workspace providers.
//
// Usage:
//
//	arlod [flags]
//
// Flags:
//
//	-socket  string   Unix socket path (default: ~/.arlo/arlo.sock)
//	-db      string   SQLite database path (default: ~/.arlo/arlod.db)
//	-debug           Enable debug logging
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	arlov1 "github.com/lingjiefan/arlo/api/gen/arlo/v1"
	"github.com/lingjiefan/arlo/internal/domain"
	"github.com/lingjiefan/arlo/internal/reconciler"
	"github.com/lingjiefan/arlo/internal/runtime"
	"github.com/lingjiefan/arlo/internal/service"
	"github.com/lingjiefan/arlo/internal/skill"
	"github.com/lingjiefan/arlo/internal/state"
	"github.com/lingjiefan/arlo/internal/store"
	"github.com/lingjiefan/arlo/internal/workflow"
	"github.com/lingjiefan/arlo/internal/workspace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	socketPath := flag.String("socket", defaultSocketPath(), "Unix socket path")
	dbPath := flag.String("db", defaultDBPath(), "SQLite database path")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	if *debug {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	slog.Info("arlod starting", "socket", *socketPath, "db", *dbPath)

	// ── Step 1: Event Store ──────────────────────
	es, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		slog.Error("failed to open event store", "error", err)
		os.Exit(1)
	}
	defer es.Close()

	// ── Step 3: State Store ──────────────────────
	ss := state.NewInMemoryStateStore(es)
	if err := ss.Rebuild(context.Background()); err != nil {
		slog.Error("failed to rebuild projections", "error", err)
		os.Exit(1)
	}
	slog.Info("projections rebuilt")

	// ── Step 4: Workflow Engine ──────────────────
	eng := workflow.NewEngine()

	// ── Step 4.5: Skill Registry ─────────────────
	skillReg := skill.NewRegistry()
	if err := skillReg.LoadDir(context.Background(), "skills"); err != nil {
		slog.Warn("failed to load skills", "error", err)
	} else {
		slog.Info("skills loaded", "count", len(skillReg.List()))
	}

	// ── Step 6: Runtime & Workspace ──────────────
	rtMgr := runtime.NewManager()
	claudeAdapter := runtime.NewClaudeAdapter()
	claudeAdapter.SetManager(rtMgr)
	rtMgr.RegisterAdapter(domain.RuntimeProviderClaudeCode, claudeAdapter)

	piAdapter := runtime.NewPiAdapter()
	piAdapter.SetManager(rtMgr)
	rtMgr.RegisterAdapter(domain.RuntimeProviderPi, piAdapter)

	wsMgr := workspace.NewManager()
	_ = wsMgr

	// ── Step 5: Reconciler ───────────────────────
	rec := reconciler.New(ss, es, eng, rtMgr, wsMgr, skillReg)

	// ── gRPC Service ─────────────────────────────
	svc := service.New(es, ss, eng, rec, rtMgr)

	// ── Start Reconciler background loop ─────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := rec.Start(ctx); err != nil && err != context.Canceled {
			slog.Error("reconciler stopped with error", "error", err)
		}
	}()

	// ── gRPC Server ──────────────────────────────
	// Remove stale socket file from a previous run.
	if err := os.Remove(*socketPath); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove stale socket", "path", *socketPath, "error", err)
	}

	listener, err := net.Listen("unix", *socketPath)
	if err != nil {
		slog.Error("failed to listen on socket", "error", err)
		os.Exit(1)
	}
	defer listener.Close()

	// Make socket accessible.
	os.Chmod(*socketPath, 0700)

	srv := grpc.NewServer()
	arlov1.RegisterArloServiceServer(srv, svc)
	reflection.Register(srv)

	// Graceful shutdown on SIGINT/SIGTERM.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		slog.Info("shutting down...")
		srv.GracefulStop()
		cancel()
	}()

	slog.Info("arlod listening", "socket", *socketPath)
	if err := srv.Serve(listener); err != nil {
		slog.Error("gRPC server stopped", "error", err)
		os.Exit(1)
	}
}

func defaultSocketPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".arlo", "arlo.sock")
}

func defaultDBPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".arlo", "arlod.db")
}
