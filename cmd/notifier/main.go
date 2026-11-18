// Command notifier runs the notification system. Usage:
//
//	notifier api      # HTTP API
//	notifier worker   # queue workers delivering notifications
//	notifier migrate  # apply database migrations and exit
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Yigidosantos9/insider-notification-system/internal/config"
	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/logging"
	"github.com/Yigidosantos9/insider-notification-system/internal/metrics"
	"github.com/Yigidosantos9/insider-notification-system/internal/provider"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
	"github.com/Yigidosantos9/insider-notification-system/internal/worker"
	"github.com/Yigidosantos9/insider-notification-system/migrations"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: notifier <api|worker|migrate>")
		os.Exit(2)
	}
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	log := logging.New(cfg.LogLevel)

	// SIGINT/SIGTERM cancel ctx; every component shuts down gracefully from it.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch os.Args[1] {
	case "migrate":
		err = migrations.Up(cfg.DatabaseURL)
		if err == nil {
			log.Info("migrations applied")
		}
	case "worker":
		err = runWorker(ctx, cfg, log)
	case "api":
		err = runAPI(ctx, cfg, log)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Error("exiting with error", "command", os.Args[1], "error", err)
		os.Exit(1)
	}
}

func connect(ctx context.Context, cfg config.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect database: %w", err)
	}
	return pool, nil
}

func newRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

func runWorker(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	pool, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	st := store.New(pool)

	reg := newRegistry()
	m := metrics.New(reg)
	go m.RunQueueDepthPoller(ctx, st, 2*time.Second, log)

	// The worker has no API, so it exposes its own /metrics and /health.
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	msrv := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := msrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server failed", "error", err)
		}
	}()
	defer msrv.Close()

	w := worker.New(st, provider.NewClient(cfg.ProviderURL, cfg.ProviderTimeout), worker.Config{
		Channels:         domain.Channels,
		RatePerSecond:    cfg.RatePerSecond,
		Concurrency:      cfg.WorkerConcurrency,
		ClaimBatch:       cfg.ClaimBatch,
		Lease:            30 * time.Second,
		PollInterval:     200 * time.Millisecond,
		ReapInterval:     10 * time.Second,
		Retry:            worker.RetryPolicy{Base: cfg.RetryBase, Cap: cfg.RetryCap, MaxAttempts: cfg.MaxAttempts},
		BreakerThreshold: cfg.BreakerThreshold,
		BreakerCooldown:  cfg.BreakerCooldown,
	}, m, log)
	return w.Run(ctx)
}

func runAPI(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	return errors.New("api not implemented yet")
}
