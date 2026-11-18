// Package api implements the HTTP API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/metrics"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
)

// Store is what the API needs from persistence (implemented by *store.Store).
type Store interface {
	Create(ctx context.Context, n domain.NewNotification, correlationID string, maxAttempts int) (domain.Notification, bool, error)
	CreateBatch(ctx context.Context, items []domain.NewNotification, correlationID string, maxAttempts int) (uuid.UUID, []domain.Notification, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Attempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error)
	BatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int, error)
	List(ctx context.Context, f store.ListFilter) ([]domain.Notification, string, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Requeue(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	Ping(ctx context.Context) error
	CreateTemplate(ctx context.Context, t domain.NewTemplate) (domain.Template, error)
	GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error)
	Summary(ctx context.Context, window time.Duration) (store.Summary, error)
}

type server struct {
	store       Store
	metrics     *metrics.Metrics
	log         *slog.Logger
	maxAttempts int
}

// NewServer builds the HTTP handler with all routes and middleware.
func NewServer(st Store, m *metrics.Metrics, gatherer prometheus.Gatherer, log *slog.Logger, maxAttempts int) http.Handler {
	s := &server{store: st, metrics: m, log: log, maxAttempts: maxAttempts}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/notifications", s.createNotification)
	mux.HandleFunc("POST /api/v1/notifications/batch", s.createBatch)
	mux.HandleFunc("GET /api/v1/notifications", s.listNotifications)
	mux.HandleFunc("GET /api/v1/notifications/{id}", s.getNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", s.cancelNotification)
	mux.HandleFunc("POST /api/v1/notifications/{id}/requeue", s.requeueNotification)
	mux.HandleFunc("GET /api/v1/batches/{id}", s.getBatch)
	mux.HandleFunc("POST /api/v1/templates", s.createTemplate)
	mux.HandleFunc("GET /api/v1/templates/{id}", s.getTemplate)
	mux.HandleFunc("GET /api/v1/metrics/summary", s.metricsSummary)

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", s.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	registerDocs(mux)

	return s.chain(mux)
}

// chain wraps the routes with middleware. Outermost first:
// correlation ID → logging/metrics → panic recovery → routes. Recovery sits
// inside observability so a panic is still logged and counted as a 500.
func (s *server) chain(h http.Handler) http.Handler {
	return withCorrelationID(s.withObservability(s.withRecover(h)))
}

const maxSummaryWindow = 24 * time.Hour

// metricsSummary is a human-friendly JSON view of the system: queue depth,
// success/failure rates and latency percentiles over ?window= (default 5m).
// Prometheus scrapes /metrics; this endpoint answers the same questions
// without needing a Prometheus server.
func (s *server) metricsSummary(w http.ResponseWriter, r *http.Request) {
	window := 5 * time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 || d > maxSummaryWindow {
			badRequest(w, "window must be a duration between 1s and 24h, e.g. 5m or 1h")
			return
		}
		window = d
	}
	sum, err := s.store.Summary(r.Context(), window)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		// The detail goes to the log only: it can contain hosts and user names.
		s.logger(r).Error("readiness check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": "unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
