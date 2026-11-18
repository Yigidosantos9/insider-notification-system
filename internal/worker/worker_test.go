package worker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/metrics"
	"github.com/Yigidosantos9/insider-notification-system/internal/provider"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
	"github.com/Yigidosantos9/insider-notification-system/internal/testutil"
)

// fakeProvider records requests and answers with a scripted status code.
type fakeProvider struct {
	mu      sync.Mutex
	srv     *httptest.Server
	reqs    []provider.Request
	times   map[string][]time.Time // per channel
	respond func(n int) int        // n = 1-based request count → status code
	hits    atomic.Int64
}

func newFakeProvider(t *testing.T, respond func(n int) int) *fakeProvider {
	f := &fakeProvider{respond: respond, times: map[string][]time.Time{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req provider.Request
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		n := int(f.hits.Add(1))
		f.mu.Lock()
		f.reqs = append(f.reqs, req)
		f.times[req.Channel] = append(f.times[req.Channel], time.Now())
		f.mu.Unlock()
		code := f.respond(n)
		if code == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "10")
		}
		w.WriteHeader(code)
		if code == http.StatusAccepted {
			_, _ = w.Write([]byte(`{"messageId":"m","status":"accepted","timestamp":"2026-11-18T00:00:00Z"}`))
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func always(code int) func(int) int { return func(int) int { return code } }

func testConfig() Config {
	return Config{
		Channels:         domain.Channels,
		RatePerSecond:    1000,
		Concurrency:      10,
		ClaimBatch:       50,
		Lease:            30 * time.Second,
		PollInterval:     10 * time.Millisecond,
		ReapInterval:     time.Second,
		Retry:            RetryPolicy{Base: 10 * time.Millisecond, Cap: 20 * time.Millisecond, MaxAttempts: 5, Rand: func() float64 { return 1 }},
		BreakerThreshold: 100,
		BreakerCooldown:  time.Hour,
	}
}

// startWorker runs a worker until the test ends.
func startWorker(t *testing.T, s *store.Store, f *fakeProvider, cfg Config) {
	t.Helper()
	w := New(s, provider.NewClient(f.srv.URL, time.Second), cfg,
		metrics.New(prometheus.NewRegistry()), slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", msg)
}

func create(t *testing.T, s *store.Store, n domain.NewNotification) domain.Notification {
	t.Helper()
	got, _, err := s.Create(context.Background(), n, "corr", 5)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func smsN(content string, p domain.Priority) domain.NewNotification {
	return domain.NewNotification{Channel: domain.ChannelSMS, Priority: p, Recipient: "+905551234567", Content: content}
}

func status(t *testing.T, s *store.Store, n domain.Notification) domain.Notification {
	t.Helper()
	got, err := s.Get(context.Background(), n.ID)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestWorkerDeliversInPriorityOrder(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusAccepted))
	create(t, s, smsN("low", domain.PriorityLow))
	create(t, s, smsN("high", domain.PriorityHigh))
	create(t, s, smsN("normal", domain.PriorityNormal))

	cfg := testConfig()
	cfg.Concurrency = 1
	startWorker(t, s, f, cfg)

	eventually(t, 5*time.Second, func() bool { return f.hits.Load() == 3 }, "3 deliveries")
	f.mu.Lock()
	defer f.mu.Unlock()
	got := []string{f.reqs[0].Content, f.reqs[1].Content, f.reqs[2].Content}
	if got[0] != "high" || got[1] != "normal" || got[2] != "low" {
		t.Fatalf("delivery order = %v, want [high normal low]", got)
	}
}

func TestWorkerRecordsActualSuccessStatusCode(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusOK)) // provider answers 200, not 202
	n := create(t, s, smsN("ok", domain.PriorityNormal))
	startWorker(t, s, f, testConfig())

	eventually(t, 5*time.Second, func() bool { return status(t, s, n).Status == domain.StatusSent }, "sent")
	atts, _ := s.Attempts(context.Background(), n.ID)
	if len(atts) != 1 || atts[0].StatusCode == nil || *atts[0].StatusCode != http.StatusOK {
		t.Fatalf("attempt status code = %v, want 200", atts)
	}
}

func TestWorkerHonoursRetryAfter(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusTooManyRequests))
	n := create(t, s, smsN("throttled", domain.PriorityNormal))
	startWorker(t, s, f, testConfig()) // backoff alone would be ≤ 20ms

	eventually(t, 5*time.Second, func() bool { return status(t, s, n).Attempts == 1 }, "first attempt")
	got := status(t, s, n)
	if wait := got.NextAttemptAt.Sub(got.UpdatedAt); got.Status != domain.StatusPending || wait < 10*time.Second {
		t.Fatalf("status %s, next attempt in %s; want pending and >= 10s (Retry-After)", got.Status, wait)
	}
}

func TestWorkerRetryThenSuccess(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, func(n int) int {
		if n <= 2 {
			return http.StatusInternalServerError
		}
		return http.StatusAccepted
	})
	n := create(t, s, smsN("flaky", domain.PriorityNormal))
	startWorker(t, s, f, testConfig())

	eventually(t, 5*time.Second, func() bool { return status(t, s, n).Status == domain.StatusSent }, "sent after retries")
	got := status(t, s, n)
	atts, _ := s.Attempts(context.Background(), n.ID)
	if got.Attempts != 3 || len(atts) != 3 {
		t.Fatalf("attempts=%d attempt rows=%d, want 3/3", got.Attempts, len(atts))
	}
}

func TestWorkerRetryExhausted(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusInternalServerError))
	n, _, err := s.Create(context.Background(), smsN("doomed", domain.PriorityNormal), "corr", 3)
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, s, f, testConfig())

	eventually(t, 5*time.Second, func() bool { return status(t, s, n).Status == domain.StatusFailed }, "failed after exhausting retries")
	if got := status(t, s, n); got.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", got.Attempts)
	}
}

func TestWorkerPermanentFailure(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusBadRequest))
	n := create(t, s, smsN("bad", domain.PriorityNormal))
	startWorker(t, s, f, testConfig())

	eventually(t, 5*time.Second, func() bool { return status(t, s, n).Status == domain.StatusFailed }, "failed")
	if got := status(t, s, n); got.Attempts != 1 || f.hits.Load() != 1 {
		t.Fatalf("attempts=%d hits=%d, want 1/1", got.Attempts, f.hits.Load())
	}
}

func TestWorkerRateLimit(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusAccepted))
	for i := 0; i < 150; i++ {
		create(t, s, smsN("bulk", domain.PriorityNormal))
	}
	for i := 0; i < 100; i++ {
		create(t, s, domain.NewNotification{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Recipient: "a@b.co", Content: "e"})
	}
	cfg := testConfig()
	cfg.RatePerSecond = 100
	cfg.Concurrency = 20
	startWorker(t, s, f, cfg)

	eventually(t, 10*time.Second, func() bool { return f.hits.Load() == 250 }, "all 250 delivered")
	f.mu.Lock()
	defer f.mu.Unlock()
	span := func(ts []time.Time) time.Duration { return ts[len(ts)-1].Sub(ts[0]) }
	smsSpan, emailSpan := span(f.times["sms"]), span(f.times["email"])
	// "Max 100 per second" must hold in every 1-second window, not just on average.
	if w := maxInWindow(f.times["sms"], time.Second); w > 102 {
		t.Fatalf("%d sms sent within one second; limit is 100/s", w)
	}
	if smsSpan < 1300*time.Millisecond {
		t.Fatalf("150 sms delivered in %s; expected ~1.5s at 100/s", smsSpan)
	}
	// Email has its own bucket: its 100 fit in the burst and must not wait behind sms.
	if emailSpan >= smsSpan {
		t.Fatalf("email span %s >= sms span %s; channels share a limiter", emailSpan, smsSpan)
	}
}

// maxInWindow returns the largest number of timestamps falling within any window of length d.
func maxInWindow(ts []time.Time, d time.Duration) int {
	best, j := 0, 0
	for i := range ts {
		for ts[i].Sub(ts[j]) >= d {
			j++
		}
		best = max(best, i-j+1)
	}
	return best
}

func TestWorkerNeverSendsCancelled(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusAccepted))
	keep := create(t, s, smsN("keep", domain.PriorityNormal))
	drop := create(t, s, smsN("drop", domain.PriorityHigh))
	if _, err := s.Cancel(context.Background(), drop.ID); err != nil {
		t.Fatal(err)
	}
	startWorker(t, s, f, testConfig())

	eventually(t, 5*time.Second, func() bool { return status(t, s, keep).Status == domain.StatusSent }, "keep sent")
	time.Sleep(100 * time.Millisecond)
	if f.hits.Load() != 1 || status(t, s, drop).Status != domain.StatusCancelled {
		t.Fatalf("hits=%d drop status=%s", f.hits.Load(), status(t, s, drop).Status)
	}
}

func TestWorkerBreakerOpens(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusInternalServerError))
	var rows []domain.Notification
	for i := 0; i < 5; i++ {
		rows = append(rows, create(t, s, smsN("down", domain.PriorityNormal)))
	}
	cfg := testConfig()
	cfg.Concurrency = 1
	cfg.BreakerThreshold = 2
	startWorker(t, s, f, cfg)

	eventually(t, 5*time.Second, func() bool { return f.hits.Load() >= 2 }, "two failures")
	time.Sleep(300 * time.Millisecond) // retries are due after 20ms; an open breaker must hold them back
	if h := f.hits.Load(); h != 2 {
		t.Fatalf("provider hit %d times, want 2 (breaker should stop traffic)", h)
	}
	var untouched int
	for _, n := range rows {
		got := status(t, s, n)
		if got.Status != domain.StatusPending {
			t.Fatalf("row %s status %s, want pending", n.ID, got.Status)
		}
		if got.Attempts == 0 {
			untouched++
			// While the breaker is open the worker must not claim (and reschedule)
			// rows at all, so untouched rows keep their original due time.
			if !got.NextAttemptAt.Equal(n.NextAttemptAt) {
				t.Fatalf("row %s was rescheduled from %s to %s while breaker open", n.ID, n.NextAttemptAt, got.NextAttemptAt)
			}
		}
	}
	if untouched != 3 {
		t.Fatalf("%d rows without attempts, want 3 (rescheduled without spending attempts)", untouched)
	}
}

func TestWorkerHonoursPerNotificationMaxAttempts(t *testing.T) {
	s := store.New(testutil.Postgres(t))
	f := newFakeProvider(t, always(http.StatusInternalServerError))
	// Stored with max_attempts=2 while the worker's own default is 5:
	// the value on the notification must win.
	n, _, err := s.Create(context.Background(), smsN("two tries", domain.PriorityNormal), "corr", 2)
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, s, f, testConfig())

	eventually(t, 5*time.Second, func() bool { return status(t, s, n).Status == domain.StatusFailed }, "failed")
	if got := status(t, s, n); got.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", got.Attempts)
	}
}
