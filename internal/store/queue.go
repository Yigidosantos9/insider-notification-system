package store

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

// Claim atomically takes up to `limit` due notifications of one channel,
// highest priority first, and leases them to the caller for `lease`.
//
// FOR UPDATE SKIP LOCKED lets many workers run this concurrently: rows locked
// by another worker's in-flight claim are skipped instead of waited on, so no
// row is handed out twice. If a worker dies, its lease expires and
// ReapExpiredLeases returns the rows to the queue.
func (s *Store) Claim(ctx context.Context, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE notifications
		SET status = 'processing', locked_until = now() + $3::interval, updated_at = now()
		WHERE id IN (
			SELECT id FROM notifications
			WHERE channel = $1 AND status = 'pending' AND next_attempt_at <= now()
			ORDER BY priority, next_attempt_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING `+columns, ch, limit, lease)
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	out, err := scanAll(rows)
	if err != nil {
		return nil, err
	}
	// UPDATE ... RETURNING does not preserve the subquery's order.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Priority.Rank() != out[j].Priority.Rank() {
			return out[i].Priority.Rank() < out[j].Priority.Rank()
		}
		return out[i].NextAttemptAt.Before(out[j].NextAttemptAt)
	})
	return out, nil
}

// finish records an attempt and applies the status update in one transaction.
func (s *Store) finish(ctx context.Context, id uuid.UUID, a domain.Attempt, update string, args ...any) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO notification_attempts
			(notification_id, attempt_no, status_code, error, latency_ms) VALUES ($1, $2, $3, NULLIF($4, ''), $5)`,
			id, a.AttemptNo, a.StatusCode, a.Error, a.LatencyMs); err != nil {
			return fmt.Errorf("insert attempt: %w", err)
		}
		if _, err := tx.Exec(ctx, update, append([]any{id}, args...)...); err != nil {
			return fmt.Errorf("update notification: %w", err)
		}
		return nil
	})
}

// MarkSent records a successful delivery.
func (s *Store) MarkSent(ctx context.Context, id uuid.UUID, providerMessageID string, a domain.Attempt) error {
	return s.finish(ctx, id, a, `UPDATE notifications
		SET status = 'sent', attempts = attempts + 1, sent_at = now(), locked_until = NULL,
		    provider_message_id = NULLIF($2, ''), last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, providerMessageID)
}

// MarkRetry records a transient failure and puts the row back in the queue, due at nextAt.
func (s *Store) MarkRetry(ctx context.Context, id uuid.UUID, nextAt time.Time, a domain.Attempt) error {
	return s.finish(ctx, id, a, `UPDATE notifications
		SET status = 'pending', attempts = attempts + 1, next_attempt_at = $2, last_error = $3,
		    locked_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, nextAt, a.Error)
}

// MarkFailed records a permanent failure (or exhausted retries): the dead-letter state.
func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, a domain.Attempt) error {
	return s.finish(ctx, id, a, `UPDATE notifications
		SET status = 'failed', attempts = attempts + 1, last_error = $2, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, a.Error)
}

// Reschedule returns a claimed row to the queue without spending an attempt
// (used while the provider's circuit breaker is open).
func (s *Store) Reschedule(ctx context.Context, id uuid.UUID, nextAt time.Time) error {
	_, err := s.pool.Exec(ctx, `UPDATE notifications
		SET status = 'pending', next_attempt_at = $2, locked_until = NULL, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, id, nextAt)
	return err
}

// ReapExpiredLeases returns rows whose worker lease expired (crashed or stuck
// worker) to the queue, and reports how many were recovered.
func (s *Store) ReapExpiredLeases(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE notifications
		SET status = 'pending', locked_until = NULL, updated_at = now()
		WHERE status = 'processing' AND locked_until < now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// DepthRow is the number of due, pending notifications for a channel and priority.
type DepthRow struct {
	Channel  domain.Channel
	Priority domain.Priority
	Count    int
}

// QueueDepth counts due pending notifications per channel and priority.
func (s *Store) QueueDepth(ctx context.Context) ([]DepthRow, error) {
	rows, err := s.pool.Query(ctx, `SELECT channel, priority, count(*) FROM notifications
		WHERE status = 'pending' AND next_attempt_at <= now() GROUP BY channel, priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DepthRow
	for rows.Next() {
		var r DepthRow
		var p int16
		if err := rows.Scan(&r.Channel, &p, &r.Count); err != nil {
			return nil, err
		}
		r.Priority = domain.PriorityFromRank(p)
		out = append(out, r)
	}
	return out, rows.Err()
}
