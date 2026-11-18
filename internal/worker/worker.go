// Package worker delivers queued notifications to the provider.
//
// For every channel the worker runs an independent loop:
//
//	claim due rows (priority order) → wait for a rate-limit token → check the
//	circuit breaker → send → record sent / retry later / failed
//
// Channels never share a rate limiter or a breaker, so a slow or broken SMS
// provider does not hold back email or push.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/metrics"
	"github.com/Yigidosantos9/insider-notification-system/internal/provider"
)

// Queue is the worker's view of the notification store. It is an interface so
// PostgreSQL could be swapped for a broker (Kafka, RabbitMQ) without touching
// the delivery logic.
type Queue interface {
	Claim(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error)
	MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string, a domain.Attempt) error
	MarkRetry(ctx context.Context, id uuid.UUID, delay time.Duration, a domain.Attempt) error
	MarkFailed(ctx context.Context, id uuid.UUID, a domain.Attempt) error
	Reschedule(ctx context.Context, id uuid.UUID, delay time.Duration) error
	ReapExpiredLeases(ctx context.Context) (int64, error)
}

// Sender delivers one notification to the provider (implemented by *provider.Client).
type Sender interface {
	Send(ctx context.Context, idempotencyKey string, r provider.Request) (provider.Response, error)
}

// Config tunes the worker.
type Config struct {
	Channels         []domain.Channel
	RatePerSecond    int           // max sends per second, per channel
	Concurrency      int           // max in-flight sends, per channel
	ClaimBatch       int           // max rows claimed per query
	Lease            time.Duration // how long a claimed row stays reserved
	PollInterval     time.Duration // sleep when the queue is empty
	ReapInterval     time.Duration // how often expired leases are recovered
	Retry            RetryPolicy
	BreakerThreshold int
	BreakerCooldown  time.Duration
}

// Worker processes the queue until its context is cancelled.
type Worker struct {
	q       Queue
	sender  Sender
	cfg     Config
	metrics *metrics.Metrics
	log     *slog.Logger
}

// New creates a worker.
func New(q Queue, s Sender, cfg Config, m *metrics.Metrics, log *slog.Logger) *Worker {
	return &Worker{q: q, sender: s, cfg: cfg, metrics: m, log: log}
}

// channelRunner holds the per-channel state.
type channelRunner struct {
	*Worker
	ch      domain.Channel
	limiter *rate.Limiter
	breaker *provider.Breaker
	slots   chan struct{} // semaphore bounding in-flight sends
	wg      sync.WaitGroup
}

// Run blocks until ctx is cancelled, then waits for in-flight sends to finish.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, ch := range w.cfg.Channels {
		r := &channelRunner{
			Worker: w,
			ch:     ch,
			// Burst 1 spaces sends evenly (every 10ms at 100/s), so no 1-second
			// window ever exceeds the limit. A burst of N would allow up to 2N in
			// the first second (N instantly + N refilled).
			limiter: rate.NewLimiter(rate.Limit(w.cfg.RatePerSecond), 1),
			breaker: provider.NewBreaker(w.cfg.BreakerThreshold, w.cfg.BreakerCooldown, time.Now),
			slots:   make(chan struct{}, w.cfg.Concurrency),
		}
		w.metrics.BreakerState.WithLabelValues(string(ch)).Set(0)
		wg.Add(1)
		go func() { defer wg.Done(); r.loop(ctx) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); w.reapLoop(ctx) }()

	w.log.Info("worker started", "channels", w.cfg.Channels, "rate_per_second", w.cfg.RatePerSecond)
	wg.Wait()
	w.log.Info("worker stopped")
	return nil
}

func (w *Worker) reapLoop(ctx context.Context) {
	t := time.NewTicker(w.cfg.ReapInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := w.q.ReapExpiredLeases(ctx); err != nil && ctx.Err() == nil {
				w.log.Error("lease reaper failed", "error", err)
			} else if n > 0 {
				w.log.Warn("recovered notifications with expired leases", "count", n)
			}
		}
	}
}

func (r *channelRunner) loop(ctx context.Context) {
	defer r.wg.Wait() // graceful shutdown: let in-flight sends finish
	for {
		// Provider down: leave the queue alone until the breaker lets a probe through.
		if r.breaker.Blocked() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.cfg.PollInterval):
			}
			continue
		}
		// Block until at least one send slot is free, then grab any other free ones
		// so a single claim query can fill them all.
		select {
		case <-ctx.Done():
			return
		case r.slots <- struct{}{}:
		}
		free := 1
		for free < r.cfg.ClaimBatch {
			select {
			case r.slots <- struct{}{}:
				free++
				continue
			default:
			}
			break
		}

		// Re-check after waiting for a slot: the send that just freed it may have
		// tripped the breaker.
		if r.breaker.Blocked() {
			for i := 0; i < free; i++ {
				<-r.slots
			}
			continue
		}

		claimed, err := r.q.Claim(ctx, r.ch, free, r.cfg.Lease)
		if err != nil && ctx.Err() == nil {
			r.log.Error("claim failed", "channel", r.ch, "error", err)
		}
		for i := len(claimed); i < free; i++ { // return unused slots
			<-r.slots
		}
		if len(claimed) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(r.cfg.PollInterval):
			}
			continue
		}
		for _, n := range claimed {
			r.wg.Add(1)
			go func(n domain.Notification) {
				defer func() { <-r.slots; r.wg.Done() }()
				r.handle(ctx, n)
			}(n)
		}
	}
}

// handle delivers one claimed notification and records the outcome.
func (r *channelRunner) handle(ctx context.Context, n domain.Notification) {
	log := r.log.With("notification_id", n.ID, "channel", n.Channel, "correlation_id", n.CorrelationID)
	// Finish the bookkeeping even if shutdown starts mid-send.
	dbCtx := context.WithoutCancel(ctx)

	// Rate limit first: a token must be available before we may even probe the breaker.
	if err := r.limiter.Wait(ctx); err != nil {
		r.reschedule(dbCtx, log, n, 0) // shutting down; hand the row back
		return
	}
	if !r.breaker.Allow() {
		r.reschedule(dbCtx, log, n, r.breaker.Cooldown())
		return
	}

	attemptNo := n.Attempts + 1
	start := time.Now()
	resp, err := r.sender.Send(dbCtx, n.ID.String(), provider.Request{
		To: n.Recipient, Channel: string(n.Channel), Content: n.Content,
	})
	latency := time.Since(start)
	r.metrics.DeliveryLatency.WithLabelValues(string(n.Channel)).Observe(latency.Seconds())
	attempt := domain.Attempt{AttemptNo: attemptNo, LatencyMs: int(latency.Milliseconds())}

	defer r.updateBreakerMetric()

	if err == nil {
		r.breaker.Success()
		code := resp.StatusCode
		attempt.StatusCode = &code
		if err := r.q.MarkSent(dbCtx, n.ID, resp.MessageID, attempt); err != nil {
			log.Error("mark sent failed", "error", err)
			return
		}
		r.metrics.Sent.WithLabelValues(string(n.Channel)).Inc()
		log.Info("notification sent", "attempt", attemptNo, "latency_ms", attempt.LatencyMs, "provider_message_id", resp.MessageID)
		return
	}

	var se *provider.SendError
	if !errors.As(err, &se) {
		se = &provider.SendError{Retryable: true, Err: err}
	}
	if se.StatusCode != 0 {
		code := se.StatusCode
		attempt.StatusCode = &code
	}
	attempt.Error = se.Error()

	switch {
	case !se.Retryable:
		// The provider answered, so it is healthy; the request itself is bad.
		r.breaker.Success()
		r.fail(dbCtx, log, n, attempt, "permanent")
	case r.exhausted(n, attemptNo):
		r.breaker.Failure()
		r.fail(dbCtx, log, n, attempt, "exhausted")
	default:
		r.breaker.Failure()
		delay := r.cfg.Retry.Delay(attemptNo, se.RetryAfter)
		if err := r.q.MarkRetry(dbCtx, n.ID, delay, attempt); err != nil {
			log.Error("mark retry failed", "error", err)
			return
		}
		r.metrics.Retried.WithLabelValues(string(n.Channel)).Inc()
		log.Warn("delivery failed, will retry", "attempt", attemptNo, "error", attempt.Error, "retry_in", delay.String())
	}
}

// exhausted uses the attempt budget stored on the notification (set at
// creation, reset by requeue), falling back to the worker's policy.
func (r *channelRunner) exhausted(n domain.Notification, attempts int) bool {
	if n.MaxAttempts > 0 {
		return attempts >= n.MaxAttempts
	}
	return r.cfg.Retry.Exhausted(attempts)
}

func (r *channelRunner) fail(ctx context.Context, log *slog.Logger, n domain.Notification, a domain.Attempt, reason string) {
	if err := r.q.MarkFailed(ctx, n.ID, a); err != nil {
		log.Error("mark failed failed", "error", err)
		return
	}
	r.metrics.Failed.WithLabelValues(string(n.Channel), reason).Inc()
	log.Error("notification failed", "reason", reason, "attempt", a.AttemptNo, "error", a.Error)
}

func (r *channelRunner) reschedule(ctx context.Context, log *slog.Logger, n domain.Notification, delay time.Duration) {
	if err := r.q.Reschedule(ctx, n.ID, delay); err != nil {
		log.Error("reschedule failed", "error", err)
	}
}

func (r *channelRunner) updateBreakerMetric() {
	v := map[provider.BreakerState]float64{provider.BreakerClosed: 0, provider.BreakerHalfOpen: 1, provider.BreakerOpen: 2}
	r.metrics.BreakerState.WithLabelValues(string(r.ch)).Set(v[r.breaker.State()])
}
