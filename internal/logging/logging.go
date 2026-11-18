// Package logging configures structured JSON logging and carries the
// correlation ID through context.Context.
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey struct{}

// New returns a JSON slog logger writing to stdout at the given level (debug|info|warn|error).
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}

// WithCorrelationID stores the correlation ID in the context.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the correlation ID from the context, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// FromContext returns base enriched with the context's correlation_id, if any.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := CorrelationID(ctx); id != "" {
		return base.With("correlation_id", id)
	}
	return base
}
