// Package migrations holds the versioned SQL schema and applies it with golang-migrate.
package migrations

import (
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// FS contains every *.sql migration file, embedded into the binary.
//
//go:embed *.sql
var FS embed.FS

// Up applies all pending migrations. Running it on an up-to-date database is a no-op.
func Up(databaseURL string) error {
	src, err := iofs.New(FS, ".")
	if err != nil {
		return fmt.Errorf("open migration source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, toPgx5URL(databaseURL))
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}

// toPgx5URL converts postgres:// URLs to the scheme golang-migrate's pgx/v5 driver expects.
func toPgx5URL(u string) string {
	for _, prefix := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(u, prefix) {
			return "pgx5://" + strings.TrimPrefix(u, prefix)
		}
	}
	return u
}
