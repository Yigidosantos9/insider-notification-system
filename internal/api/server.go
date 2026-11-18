// Package api implements the HTTP API.
package api

import (
	"context"
	"log/slog"
	"net/http"

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

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", s.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	registerDocs(mux)

	// Outermost first: correlation ID → recover → logging/metrics → routes.
	return withCorrelationID(s.withRecover(s.withObservability(mux)))
}

func (s *server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "database": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
