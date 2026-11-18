package api

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Yigidosantos9/insider-notification-system/internal/metrics"
)

func newTestServer() *server {
	return &server{metrics: metrics.New(prometheus.NewRegistry()), log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestPanicIsRecoveredAndMeasured(t *testing.T) {
	s := newTestServer()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("kaboom") })
	h := s.chain(mux)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/boom", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if got := testutil.ToFloat64(s.metrics.HTTPRequests.WithLabelValues("GET", "GET /boom", "500")); got != 1 {
		t.Fatalf("panic not counted in http_requests_total (got %v)", got)
	}
}

func TestUnknownMethodsShareOneLabel(t *testing.T) {
	s := newTestServer()
	h := s.chain(http.NewServeMux())
	for _, m := range []string{"FOO1", "FOO2", "FOO3"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(m, "/x", nil))
	}
	if got := testutil.ToFloat64(s.metrics.HTTPRequests.WithLabelValues("OTHER", "unmatched", "404")); got != 3 {
		t.Fatalf("custom methods should collapse into OTHER, got %v", got)
	}
}
