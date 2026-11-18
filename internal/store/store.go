// Package store persists notifications in PostgreSQL. The same table doubles as
// the work queue (see queue.go).
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

var (
	// ErrNotFound means no notification (or batch) has the given ID.
	ErrNotFound = errors.New("not found")
	// ErrIdempotencyConflict means the idempotency key was already used with a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrInvalidState means the requested transition is not allowed from the current status.
	ErrInvalidState = errors.New("operation not allowed in current status")
)

// Store is the PostgreSQL repository.
type Store struct {
	pool *pgxpool.Pool
}

// New wraps a connection pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping checks database connectivity (used by the readiness probe).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const columns = `id, batch_id, idempotency_key, payload_hash, correlation_id, channel, priority,
	recipient, content, status, attempts, max_attempts, next_attempt_at, scheduled_at, locked_until,
	last_error, provider_message_id, created_at, updated_at, sent_at`

func scanNotification(row pgx.Row) (domain.Notification, error) {
	var n domain.Notification
	var payloadHash *string
	var priority int16
	err := row.Scan(&n.ID, &n.BatchID, &n.IdempotencyKey, &payloadHash, &n.CorrelationID, &n.Channel,
		&priority, &n.Recipient, &n.Content, &n.Status, &n.Attempts, &n.MaxAttempts, &n.NextAttemptAt,
		&n.ScheduledAt, &n.LockedUntil, &n.LastError, &n.ProviderMessageID, &n.CreatedAt, &n.UpdatedAt, &n.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return n, ErrNotFound
	}
	if payloadHash != nil {
		n.PayloadHash = *payloadHash
	}
	n.Priority = domain.PriorityFromRank(priority)
	return n, err
}

func scanAll(rows pgx.Rows) ([]domain.Notification, error) {
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const insertSQL = `
INSERT INTO notifications (id, batch_id, idempotency_key, payload_hash, correlation_id, channel, priority,
	recipient, content, max_attempts, scheduled_at, next_attempt_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, COALESCE($11, now()))
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ` + columns

// insertOne inserts n, or — when its idempotency key already exists — returns
// the existing row (created=false), provided the payload matches.
//
// ON CONFLICT DO NOTHING makes concurrent duplicates safe: Postgres serialises
// them on the unique index, exactly one insert wins, and the others fall
// through to the lookup below.
func insertOne(ctx context.Context, q querier, n domain.NewNotification, batchID *uuid.UUID, correlationID string, maxAttempts int) (domain.Notification, bool, error) {
	var key *string
	if n.IdempotencyKey != "" {
		key = &n.IdempotencyKey
	}
	hash := n.PayloadHash()
	got, err := scanNotification(q.QueryRow(ctx, insertSQL, uuid.New(), batchID, key, hash, correlationID,
		n.Channel, n.Priority.Rank(), n.Recipient, n.Content, maxAttempts, n.ScheduledAt))
	if err == nil {
		return got, true, nil
	}
	if !errors.Is(err, ErrNotFound) || key == nil {
		return got, false, fmt.Errorf("insert notification: %w", err)
	}

	existing, err := scanNotification(q.QueryRow(ctx,
		`SELECT `+columns+` FROM notifications WHERE idempotency_key = $1`, *key))
	if err != nil {
		return existing, false, fmt.Errorf("load idempotent notification: %w", err)
	}
	if existing.PayloadHash != hash {
		return existing, false, ErrIdempotencyConflict
	}
	return existing, false, nil
}

// Create stores a single notification. The bool is false when an existing row
// was returned because of a repeated idempotency key.
func (s *Store) Create(ctx context.Context, n domain.NewNotification, correlationID string, maxAttempts int) (domain.Notification, bool, error) {
	return insertOne(ctx, s.pool, n, nil, correlationID, maxAttempts)
}

// CreateBatch stores all items atomically under a new batch ID.
func (s *Store) CreateBatch(ctx context.Context, items []domain.NewNotification, correlationID string, maxAttempts int) (uuid.UUID, []domain.Notification, error) {
	batchID := uuid.New()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op after commit

	out := make([]domain.Notification, 0, len(items))
	for _, it := range items {
		n, _, err := insertOne(ctx, tx, it, &batchID, correlationID, maxAttempts)
		if err != nil {
			return uuid.Nil, nil, err
		}
		out = append(out, n)
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, nil, err
	}
	return batchID, out, nil
}

// Get returns one notification.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return scanNotification(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE id = $1`, id))
}

// Attempts returns the delivery attempts of a notification, oldest first.
func (s *Store) Attempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error) {
	rows, err := s.pool.Query(ctx, `SELECT attempt_no, status_code, COALESCE(error, ''), latency_ms, created_at
		FROM notification_attempts WHERE notification_id = $1 ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Attempt{}
	for rows.Next() {
		var a domain.Attempt
		if err := rows.Scan(&a.AttemptNo, &a.StatusCode, &a.Error, &a.LatencyMs, &a.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// BatchSummary counts the batch's notifications per status.
func (s *Store) BatchSummary(ctx context.Context, batchID uuid.UUID) (map[domain.Status]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.Status]int{}
	for rows.Next() {
		var st domain.Status
		var c int
		if err := rows.Scan(&st, &c); err != nil {
			return nil, err
		}
		out[st] = c
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

// ListFilter narrows List results. Nil fields are ignored. From is inclusive, To exclusive.
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	BatchID *uuid.UUID
	From    *time.Time
	To      *time.Time
	Limit   int
	Cursor  string
}

// List returns one page of notifications, newest first, and the cursor of the
// next page ("" when there are no more).
func (s *Store) List(ctx context.Context, f ListFilter) ([]domain.Notification, string, error) {
	var where []string
	var args []any
	add := func(cond string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.Status != nil {
		add("status = $%d", *f.Status)
	}
	if f.Channel != nil {
		add("channel = $%d", *f.Channel)
	}
	if f.BatchID != nil {
		add("batch_id = $%d", *f.BatchID)
	}
	if f.From != nil {
		add("created_at >= $%d", *f.From)
	}
	if f.To != nil {
		add("created_at < $%d", *f.To)
	}
	if f.Cursor != "" {
		t, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, t, id)
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}

	q := `SELECT ` + columns + ` FROM notifications`
	if len(where) > 0 {
		q += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit+1) // one extra row tells us whether another page exists
	q += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	items, err := scanAll(rows)
	if err != nil {
		return nil, "", err
	}
	if items == nil {
		items = []domain.Notification{}
	}
	next := ""
	if len(items) > f.Limit {
		items = items[:f.Limit]
		last := items[len(items)-1]
		next = encodeCursor(last.CreatedAt, last.ID)
	}
	return items, next, nil
}

// transition runs a conditional UPDATE and maps "no row changed" to
// ErrNotFound or ErrInvalidState.
func (s *Store) transition(ctx context.Context, id uuid.UUID, sql string) (domain.Notification, error) {
	n, err := scanNotification(s.pool.QueryRow(ctx, sql, id))
	if !errors.Is(err, ErrNotFound) {
		return n, err
	}
	if _, err := s.Get(ctx, id); err != nil {
		return n, err
	}
	return n, ErrInvalidState
}

// Cancel cancels a pending (including scheduled) notification. Rows already
// being processed or finished cannot be cancelled.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.transition(ctx, id, `UPDATE notifications SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status = 'pending' RETURNING `+columns)
}

// Requeue moves a failed (dead-lettered) notification back to the queue with a fresh attempt budget.
func (s *Store) Requeue(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return s.transition(ctx, id, `UPDATE notifications
		SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'failed' RETURNING `+columns)
}
