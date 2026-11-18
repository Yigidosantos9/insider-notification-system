package testutil

import (
	"context"
	"testing"
)

func TestPostgresMigrated(t *testing.T) {
	pool := Postgres(t)

	var n int
	err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_name IN ('notifications','notification_attempts')`).Scan(&n)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 tables, got %d", n)
	}
}
