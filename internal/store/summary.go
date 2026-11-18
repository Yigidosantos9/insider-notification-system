package store

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

// Summary is a point-in-time view of the system, computed from the database so
// it is correct no matter how many api/worker replicas run.
type Summary struct {
	Window      string                    `json:"window"`
	GeneratedAt time.Time                 `json:"generated_at"`
	QueueDepth  map[string]map[string]int `json:"queue_depth"` // due pending: channel → priority → count
	Scheduled   int                       `json:"scheduled"`   // pending but not yet due (scheduled or backing off)
	Processing  int                       `json:"processing"`  // claimed by a worker right now
	Channels    map[string]ChannelStats   `json:"channels"`    // delivery stats within the window
}

// ChannelStats summarises delivery attempts of one channel within the window.
type ChannelStats struct {
	Attempts     int          `json:"attempts"`
	Succeeded    int          `json:"succeeded"`     // attempts the provider accepted (2xx)
	Failed       int          `json:"failed"`        // attempts that errored (retried or dead-lettered)
	SuccessRate  float64      `json:"success_rate"`  // succeeded / attempts
	DeadLettered int          `json:"dead_lettered"` // notifications that ended in failed
	Latency      LatencyStats `json:"latency"`
}

// LatencyStats are provider call latencies in milliseconds.
type LatencyStats struct {
	AvgMs int `json:"avg_ms"`
	P50Ms int `json:"p50_ms"`
	P95Ms int `json:"p95_ms"`
	P99Ms int `json:"p99_ms"`
}

// Summary computes queue depth and per-channel delivery stats for the last `window`.
func (s *Store) Summary(ctx context.Context, window time.Duration) (Summary, error) {
	out := Summary{
		Window:     window.String(),
		QueueDepth: map[string]map[string]int{},
		Channels:   map[string]ChannelStats{},
	}
	if err := s.pool.QueryRow(ctx, `SELECT now()`).Scan(&out.GeneratedAt); err != nil {
		return out, err
	}

	depth, err := s.QueueDepth(ctx)
	if err != nil {
		return out, err
	}
	for _, d := range depth {
		if out.QueueDepth[string(d.Channel)] == nil {
			out.QueueDepth[string(d.Channel)] = map[string]int{}
		}
		out.QueueDepth[string(d.Channel)][string(d.Priority)] = d.Count
	}

	if err := s.pool.QueryRow(ctx, `SELECT
			count(*) FILTER (WHERE status = 'pending' AND next_attempt_at > now()),
			count(*) FILTER (WHERE status = 'processing')
		FROM notifications WHERE status IN ('pending', 'processing')`).Scan(&out.Scheduled, &out.Processing); err != nil {
		return out, fmt.Errorf("summary counts: %w", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT n.channel,
		       count(*),
		       count(*) FILTER (WHERE a.status_code BETWEEN 200 AND 299),
		       COALESCE(avg(a.latency_ms), 0),
		       COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY a.latency_ms), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY a.latency_ms), 0),
		       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY a.latency_ms), 0)
		FROM notification_attempts a
		JOIN notifications n ON n.id = a.notification_id
		WHERE a.created_at >= now() - $1::interval
		GROUP BY n.channel`, window)
	if err != nil {
		return out, fmt.Errorf("summary attempts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ch domain.Channel
		var c ChannelStats
		var avg, p50, p95, p99 float64
		if err := rows.Scan(&ch, &c.Attempts, &c.Succeeded, &avg, &p50, &p95, &p99); err != nil {
			return out, err
		}
		c.Failed = c.Attempts - c.Succeeded
		if c.Attempts > 0 {
			c.SuccessRate = math.Round(float64(c.Succeeded)/float64(c.Attempts)*10000) / 10000
		}
		c.Latency = LatencyStats{AvgMs: int(math.Round(avg)), P50Ms: int(math.Round(p50)), P95Ms: int(math.Round(p95)), P99Ms: int(math.Round(p99))}
		out.Channels[string(ch)] = c
	}
	if err := rows.Err(); err != nil {
		return out, err
	}

	dl, err := s.pool.Query(ctx, `SELECT channel, count(*) FROM notifications
		WHERE status = 'failed' AND updated_at >= now() - $1::interval GROUP BY channel`, window)
	if err != nil {
		return out, fmt.Errorf("summary dead letters: %w", err)
	}
	defer dl.Close()
	for dl.Next() {
		var ch domain.Channel
		var n int
		if err := dl.Scan(&ch, &n); err != nil {
			return out, err
		}
		c := out.Channels[string(ch)]
		c.DeadLettered = n
		out.Channels[string(ch)] = c
	}
	return out, dl.Err()
}
