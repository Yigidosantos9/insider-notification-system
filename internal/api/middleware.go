package api

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Yigidosantos9/insider-notification-system/internal/logging"
)

const correlationHeader = "X-Correlation-ID"

// statusRecorder captures the response status for logging and metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withCorrelationID reads X-Correlation-ID (or generates one), stores it in the
// request context and echoes it on the response. Notifications created by the
// request carry it, so worker logs for those messages share the same ID.
func withCorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set(correlationHeader, id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelationID(r.Context(), id)))
	})
}

// withObservability logs every request and records HTTP metrics.
func (s *server) withObservability(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		// r.Pattern is filled in by ServeMux; using it (not the raw path) keeps
		// metric label cardinality bounded.
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		method := metricMethod(r.Method)
		s.metrics.HTTPRequests.WithLabelValues(method, route, strconv.Itoa(rec.status)).Inc()
		s.metrics.HTTPDuration.WithLabelValues(method, route).Observe(elapsed.Seconds())

		level := slog.LevelInfo
		if rec.status >= 500 {
			level = slog.LevelError
		}
		s.logger(r).Log(r.Context(), level, "http request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status, "duration_ms", elapsed.Milliseconds())
	})
}

// metricMethod maps arbitrary client-supplied methods to a fixed set, so a
// client cannot create unbounded metric series.
func metricMethod(m string) string {
	switch m {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
		http.MethodHead, http.MethodOptions:
		return m
	}
	return "OTHER"
}

// withRecover turns panics into 500 responses instead of dropping the connection.
func (s *server) withRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				s.logger(r).Error("panic", "panic", v)
				writeError(w, http.StatusInternalServerError, "internal", "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *server) logger(r *http.Request) *slog.Logger {
	return logging.FromContext(r.Context(), s.log)
}
