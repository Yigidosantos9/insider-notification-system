// Package testutil provides shared helpers for integration tests.
package testutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Yigidosantos9/insider-notification-system/migrations"
)

var (
	once    sync.Once
	pool    *pgxpool.Pool
	dsn     string
	initErr error
)

// Postgres returns a pool connected to a migrated, empty PostgreSQL database.
//
// One container is started per test binary (package) and reused; each call
// truncates all tables so tests start from a clean state. Tests using it must
// not run in parallel with each other.
func Postgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	once.Do(start)
	if initErr != nil {
		t.Fatalf("start postgres: %v", initErr)
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE notifications, notification_attempts, templates RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return pool
}

// DSN returns the connection string of the shared test database (after Postgres was called).
func DSN() string { return dsn }

func start() {
	ctx := context.Background()
	c, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("notifier"),
		postgres.WithUsername("notifier"),
		postgres.WithPassword("notifier"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		initErr = err
		return
	}
	dsn, err = c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		initErr = err
		return
	}
	if err = migrations.Up(dsn); err != nil {
		initErr = err
		return
	}
	pool, initErr = pgxpool.New(ctx, dsn)
}
