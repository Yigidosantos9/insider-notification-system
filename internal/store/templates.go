package store

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

// ErrTemplateNameTaken means a template with that name already exists.
var ErrTemplateNameTaken = errors.New("template name already exists")

// CreateTemplate stores a message template.
func (s *Store) CreateTemplate(ctx context.Context, t domain.NewTemplate) (domain.Template, error) {
	out := domain.Template{ID: uuid.New(), Name: t.Name, Channel: t.Channel, Body: t.Body}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO templates (id, name, channel, body) VALUES ($1, $2, $3, $4) RETURNING created_at`,
		out.ID, out.Name, out.Channel, out.Body).Scan(&out.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return out, ErrTemplateNameTaken
	}
	return out, err
}

// GetTemplate returns one template.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (domain.Template, error) {
	var t domain.Template
	err := s.pool.QueryRow(ctx, `SELECT id, name, channel, body, created_at FROM templates WHERE id = $1`, id).
		Scan(&t.ID, &t.Name, &t.Channel, &t.Body, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return t, ErrNotFound
	}
	return t, err
}
