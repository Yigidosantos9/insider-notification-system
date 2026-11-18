package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Yigidosantos9/insider-notification-system/internal/metrics"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
	"github.com/Yigidosantos9/insider-notification-system/internal/testutil"
)

type env struct {
	t   *testing.T
	srv *httptest.Server
	st  *store.Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	st := store.New(testutil.Postgres(t))
	reg := prometheus.NewRegistry()
	h := NewServer(st, metrics.New(reg), reg, slog.New(slog.NewTextHandler(io.Discard, nil)), 5)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &env{t: t, srv: srv, st: st}
}

type resp struct {
	code   int
	header http.Header
	body   map[string]any
	raw    string
}

func (e *env) do(method, path, body string, headers ...string) resp {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer r.Body.Close()
	raw, _ := io.ReadAll(r.Body)
	out := resp{code: r.StatusCode, header: r.Header, raw: string(raw)}
	_ = json.Unmarshal(raw, &out.body)
	return out
}

func (r resp) errField(i int) string {
	details, _ := r.body["error"].(map[string]any)["details"].([]any)
	if len(details) <= i {
		return ""
	}
	return details[i].(map[string]any)["field"].(string)
}

func (r resp) errCode() string {
	e, _ := r.body["error"].(map[string]any)
	s, _ := e["code"].(string)
	return s
}

const validSMS = `{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}`

func TestCreateNotification(t *testing.T) {
	e := newEnv(t)
	r := e.do("POST", "/api/v1/notifications", validSMS, "X-Correlation-ID", "abc-123")
	if r.code != http.StatusCreated {
		t.Fatalf("status %d: %s", r.code, r.raw)
	}
	if r.body["id"] == "" || r.body["status"] != "pending" || r.body["priority"] != "high" {
		t.Fatalf("body: %s", r.raw)
	}
	if r.header.Get("X-Correlation-ID") != "abc-123" || r.body["correlation_id"] != "abc-123" {
		t.Fatalf("correlation id not propagated: header=%q body=%v", r.header.Get("X-Correlation-ID"), r.body["correlation_id"])
	}

	r = e.do("POST", "/api/v1/notifications", validSMS)
	if r.header.Get("X-Correlation-ID") == "" {
		t.Fatal("correlation id should be generated when absent")
	}
}

func TestCreateIdempotentReplay(t *testing.T) {
	e := newEnv(t)
	first := e.do("POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k-1")
	second := e.do("POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k-1")
	if first.code != http.StatusCreated || second.code != http.StatusOK || first.body["id"] != second.body["id"] {
		t.Fatalf("first %d %v, second %d %v", first.code, first.body["id"], second.code, second.body["id"])
	}
	conflict := e.do("POST", "/api/v1/notifications",
		`{"channel":"sms","recipient":"+905551234567","content":"other"}`, "Idempotency-Key", "k-1")
	if conflict.code != http.StatusConflict || conflict.errCode() != "idempotency_conflict" {
		t.Fatalf("conflict: %d %s", conflict.code, conflict.raw)
	}
}

func TestCreateValidation422(t *testing.T) {
	e := newEnv(t)
	r := e.do("POST", "/api/v1/notifications", `{"channel":"sms","recipient":"123","content":"x"}`)
	if r.code != http.StatusUnprocessableEntity || r.errCode() != "validation_failed" || r.errField(0) != "recipient" {
		t.Fatalf("got %d %s", r.code, r.raw)
	}
}

func TestCreateMalformed400(t *testing.T) {
	e := newEnv(t)
	for _, body := range []string{
		`{`,
		`{"channel":"sms","recipient":"+905551234567","content":"x","foo":1}`,
		`{"channel":"sms","recipient":"+905551234567","content":"x","scheduled_at":"tomorrow"}`,
	} {
		if r := e.do("POST", "/api/v1/notifications", body); r.code != http.StatusBadRequest || r.errCode() != "bad_request" {
			t.Fatalf("body %s: got %d %s", body, r.code, r.raw)
		}
	}
}

func batchBody(n int, badIndex int) string {
	items := make([]string, n)
	for i := range items {
		rec := "+905551234567"
		if i == badIndex {
			rec = "nope"
		}
		items[i] = fmt.Sprintf(`{"channel":"sms","recipient":"%s","content":"m%d"}`, rec, i)
	}
	return `{"notifications":[` + strings.Join(items, ",") + `]}`
}

func TestBatch(t *testing.T) {
	e := newEnv(t)
	r := e.do("POST", "/api/v1/notifications/batch", batchBody(3, -1))
	if r.code != http.StatusCreated || r.body["count"].(float64) != 3 || len(r.body["ids"].([]any)) != 3 || r.body["batch_id"] == "" {
		t.Fatalf("batch: %d %s", r.code, r.raw)
	}

	if r := e.do("POST", "/api/v1/notifications/batch", batchBody(1001, -1)); r.code != http.StatusUnprocessableEntity || r.errField(0) != "items" {
		t.Fatalf("oversized: %d %.200s", r.code, r.raw)
	}

	if _, err := testutil.Postgres(t).Exec(context.Background(), `SELECT 1`); err != nil { // truncates
		t.Fatal(err)
	}
	r = e.do("POST", "/api/v1/notifications/batch", batchBody(10, 7))
	if r.code != http.StatusUnprocessableEntity || r.errField(0) != "items[7].recipient" {
		t.Fatalf("bad item: %d %s", r.code, r.raw)
	}
	items, _, _ := e.st.List(context.Background(), store.ListFilter{Limit: 100})
	if len(items) != 0 {
		t.Fatalf("%d rows inserted from a rejected batch", len(items))
	}
}

func TestGetAndBatchSummary(t *testing.T) {
	e := newEnv(t)
	b := e.do("POST", "/api/v1/notifications/batch", batchBody(2, -1))
	id := b.body["ids"].([]any)[0].(string)

	g := e.do("GET", "/api/v1/notifications/"+id, "")
	n, _ := g.body["notification"].(map[string]any)
	if g.code != http.StatusOK || n["id"] != id || g.body["attempts"] == nil {
		t.Fatalf("get: %d %s", g.code, g.raw)
	}

	s := e.do("GET", "/api/v1/batches/"+b.body["batch_id"].(string), "")
	counts, _ := s.body["counts"].(map[string]any)
	if s.code != http.StatusOK || s.body["total"].(float64) != 2 || counts["pending"].(float64) != 2 {
		t.Fatalf("summary: %d %s", s.code, s.raw)
	}
	l := e.do("GET", "/api/v1/notifications?batch_id="+b.body["batch_id"].(string), "")
	if len(l.body["items"].([]any)) != 2 {
		t.Fatalf("list by batch: %s", l.raw)
	}
}

func TestGet404And400(t *testing.T) {
	e := newEnv(t)
	if r := e.do("GET", "/api/v1/notifications/7b0c7c1e-2f0a-4a8e-9a43-3a3e7a1f0000", ""); r.code != http.StatusNotFound || r.errCode() != "not_found" {
		t.Fatalf("404: %d %s", r.code, r.raw)
	}
	if r := e.do("GET", "/api/v1/notifications/not-a-uuid", ""); r.code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", r.code)
	}
	if r := e.do("GET", "/api/v1/batches/7b0c7c1e-2f0a-4a8e-9a43-3a3e7a1f0000", ""); r.code != http.StatusNotFound {
		t.Fatalf("batch 404: %d", r.code)
	}
}

func TestCancel200Then409(t *testing.T) {
	e := newEnv(t)
	c := e.do("POST", "/api/v1/notifications", validSMS)
	id := c.body["id"].(string)
	r := e.do("POST", "/api/v1/notifications/"+id+"/cancel", "")
	if r.code != http.StatusOK || r.body["status"] != "cancelled" {
		t.Fatalf("cancel: %d %s", r.code, r.raw)
	}
	r = e.do("POST", "/api/v1/notifications/"+id+"/cancel", "")
	if r.code != http.StatusConflict || r.errCode() != "invalid_state" {
		t.Fatalf("second cancel: %d %s", r.code, r.raw)
	}
	if r := e.do("POST", "/api/v1/notifications/"+id+"/requeue", ""); r.code != http.StatusConflict {
		t.Fatalf("requeue cancelled: %d", r.code)
	}
}

func TestListPagination(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 5; i++ {
		e.do("POST", "/api/v1/notifications", validSMS)
	}
	seen := 0
	path := "/api/v1/notifications?limit=2&channel=sms&status=pending"
	for pages := 0; ; pages++ {
		r := e.do("GET", path, "")
		if r.code != http.StatusOK {
			t.Fatalf("list: %d %s", r.code, r.raw)
		}
		seen += len(r.body["items"].([]any))
		next, _ := r.body["next_cursor"].(string)
		if next == "" {
			break
		}
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		path = "/api/v1/notifications?limit=2&channel=sms&status=pending&cursor=" + next
	}
	if seen != 5 {
		t.Fatalf("saw %d items across pages, want 5", seen)
	}

	for i := 0; i < 100; i++ {
		e.do("POST", "/api/v1/notifications", validSMS)
	}
	if r := e.do("GET", "/api/v1/notifications?limit=500", ""); len(r.body["items"].([]any)) != 100 {
		t.Fatalf("limit not clamped to 100: %d", len(r.body["items"].([]any)))
	}
	for _, q := range []string{"limit=abc", "status=nope", "channel=fax", "from=yesterday", "cursor=not-base64!!", "batch_id=x"} {
		if r := e.do("GET", "/api/v1/notifications?"+q, ""); r.code != http.StatusBadRequest {
			t.Fatalf("%s: got %d", q, r.code)
		}
	}
}

func TestHealthReady(t *testing.T) {
	e := newEnv(t)
	if r := e.do("GET", "/health", ""); r.code != http.StatusOK {
		t.Fatalf("health %d", r.code)
	}
	if r := e.do("GET", "/ready", ""); r.code != http.StatusOK {
		t.Fatalf("ready %d", r.code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	e := newEnv(t)
	e.do("POST", "/api/v1/notifications", validSMS)
	r := e.do("GET", "/metrics", "")
	if r.code != http.StatusOK || !strings.Contains(r.raw, `notifications_created_total{channel="sms"} 1`) ||
		!strings.Contains(r.raw, "http_requests_total") {
		t.Fatalf("metrics missing: %.500s", r.raw)
	}
}

func TestBodyTooLarge(t *testing.T) {
	e := newEnv(t)
	big := `{"notifications":"` + string(bytes.Repeat([]byte("a"), 11<<20)) + `"}`
	if r := e.do("POST", "/api/v1/notifications/batch", big); r.code != http.StatusRequestEntityTooLarge {
		t.Fatalf("got %d", r.code)
	}
}

func TestDocsServed(t *testing.T) {
	e := newEnv(t)
	if r := e.do("GET", "/docs", ""); r.code != http.StatusOK || !strings.Contains(r.raw, "swagger-ui") {
		t.Fatalf("/docs: %d", r.code)
	}
	if r := e.do("GET", "/openapi.yaml", ""); r.code != http.StatusOK || !strings.Contains(r.raw, "openapi: 3.0.3") {
		t.Fatalf("/openapi.yaml: %d %.100s", r.code, r.raw)
	}
}
