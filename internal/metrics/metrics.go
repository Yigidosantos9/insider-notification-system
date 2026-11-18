// Package metrics defines the Prometheus metrics exposed on /metrics.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Yigidosantos9/insider-notification-system/internal/store"
)

// Metrics groups every collector. Create it once per process with New.
type Metrics struct {
	Created         *prometheus.CounterVec
	Sent            *prometheus.CounterVec
	Failed          *prometheus.CounterVec
	Retried         *prometheus.CounterVec
	DeliveryLatency *prometheus.HistogramVec
	QueueDepth      *prometheus.GaugeVec
	BreakerState    *prometheus.GaugeVec
	HTTPRequests    *prometheus.CounterVec
	HTTPDuration    *prometheus.HistogramVec
}

// New creates the collectors and registers them on reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total", Help: "Notifications accepted by the API.",
		}, []string{"channel"}),
		Sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_sent_total", Help: "Notifications delivered to the provider.",
		}, []string{"channel"}),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_failed_total", Help: "Notifications moved to failed (dead letter). reason=permanent|exhausted.",
		}, []string{"channel", "reason"}),
		Retried: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_retried_total", Help: "Delivery attempts that failed transiently and were rescheduled.",
		}, []string{"channel"}),
		DeliveryLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notification_delivery_latency_seconds", Help: "Provider call latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel"}),
		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notification_queue_depth", Help: "Due pending notifications.",
		}, []string{"channel", "priority"}),
		BreakerState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "provider_circuit_state", Help: "Circuit breaker state per channel: 0 closed, 1 half-open, 2 open.",
		}, []string{"channel"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status code.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "HTTP request latency.", Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.Created, m.Sent, m.Failed, m.Retried, m.DeliveryLatency, m.QueueDepth,
		m.BreakerState, m.HTTPRequests, m.HTTPDuration)
	return m
}

// DepthSource provides queue depth rows (implemented by *store.Store).
type DepthSource interface {
	QueueDepth(ctx context.Context) ([]store.DepthRow, error)
}

// RunQueueDepthPoller refreshes the queue depth gauge every interval until ctx ends.
func (m *Metrics) RunQueueDepthPoller(ctx context.Context, src DepthSource, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		rows, err := src.QueueDepth(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("queue depth poll failed", "error", err)
		} else {
			m.QueueDepth.Reset() // drained series must read 0, not their last value
			for _, r := range rows {
				m.QueueDepth.WithLabelValues(string(r.Channel), string(r.Priority)).Set(float64(r.Count))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
