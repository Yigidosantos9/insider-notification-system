package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/testutil"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	return New(testutil.Postgres(t))
}

func sms(content string) domain.NewNotification {
	return domain.NewNotification{Channel: domain.ChannelSMS, Priority: domain.PriorityNormal, Recipient: "+905551234567", Content: content}
}

func mustCreate(t *testing.T, s *Store, n domain.NewNotification) domain.Notification {
	t.Helper()
	got, _, err := s.Create(context.Background(), n, "corr-1", 5)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	return got
}

func setStatus(t *testing.T, s *Store, id uuid.UUID, st domain.Status) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), `UPDATE notifications SET status=$2 WHERE id=$1`, id, st); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAndGet(t *testing.T) {
	s := newStore(t)
	n := sms("hello")
	n.Priority = domain.PriorityHigh

	created, isNew, err := s.Create(context.Background(), n, "corr-1", 5)
	if err != nil || !isNew {
		t.Fatalf("create: isNew=%v err=%v", isNew, err)
	}
	got, err := s.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != domain.StatusPending || got.Priority != domain.PriorityHigh || got.Content != "hello" ||
		got.CorrelationID != "corr-1" || got.MaxAttempts != 5 {
		t.Fatalf("unexpected row: %+v", got)
	}
	if d := time.Since(got.NextAttemptAt); d < -time.Second || d > 5*time.Second {
		t.Fatalf("next_attempt_at should be ~now, got %s", got.NextAttemptAt)
	}

	if _, err := s.Get(context.Background(), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing id: got %v, want ErrNotFound", err)
	}
}

func TestCreateScheduled(t *testing.T) {
	s := newStore(t)
	at := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	n := sms("later")
	n.ScheduledAt = &at
	got := mustCreate(t, s, n)
	if !got.NextAttemptAt.Equal(at) || got.ScheduledAt == nil || !got.ScheduledAt.Equal(at) {
		t.Fatalf("next_attempt_at=%s scheduled_at=%v, want %s", got.NextAttemptAt, got.ScheduledAt, at)
	}
}

func TestIdempotency(t *testing.T) {
	s := newStore(t)
	n := sms("once")
	n.IdempotencyKey = "order-42"

	first, isNew1, err := s.Create(context.Background(), n, "", 5)
	if err != nil || !isNew1 {
		t.Fatalf("first: %v %v", isNew1, err)
	}
	second, isNew2, err := s.Create(context.Background(), n, "", 5)
	if err != nil || isNew2 || second.ID != first.ID {
		t.Fatalf("replay: isNew=%v id=%s err=%v (want %s)", isNew2, second.ID, err, first.ID)
	}

	n.Content = "changed"
	if _, _, err := s.Create(context.Background(), n, "", 5); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different payload: got %v, want ErrIdempotencyConflict", err)
	}
}

func TestIdempotencyConcurrent(t *testing.T) {
	s := newStore(t)
	n := sms("race")
	n.IdempotencyKey = "same-key"

	const workers = 10
	ids := make([]uuid.UUID, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got, _, err := s.Create(context.Background(), n, "", 5)
			ids[i], errs[i] = got.ID, err
		}(i)
	}
	wg.Wait()

	for i := range ids {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("worker %d got id %s, want %s", i, ids[i], ids[0])
		}
	}
	var count int
	_ = s.pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&count)
	if count != 1 {
		t.Fatalf("rows = %d, want 1", count)
	}
}

func TestCreateBatch(t *testing.T) {
	s := newStore(t)
	items := make([]domain.NewNotification, 1000)
	for i := range items {
		items[i] = sms("bulk")
	}
	batchID, created, err := s.CreateBatch(context.Background(), items, "corr", 5)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(created) != 1000 {
		t.Fatalf("created %d, want 1000", len(created))
	}
	for _, n := range created {
		if n.BatchID == nil || *n.BatchID != batchID {
			t.Fatalf("row %s missing batch id", n.ID)
		}
	}
	summary, err := s.BatchSummary(context.Background(), batchID)
	if err != nil {
		t.Fatal(err)
	}
	if summary[domain.StatusPending] != 1000 {
		t.Fatalf("summary = %v", summary)
	}
	if _, err := s.BatchSummary(context.Background(), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown batch: %v", err)
	}
}

func TestListFiltersAndPagination(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 20; i++ {
		mustCreate(t, s, sms("s"))
	}
	for i := 0; i < 5; i++ {
		e := domain.NewNotification{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Recipient: "a@b.co", Content: "e"}
		got := mustCreate(t, s, e)
		setStatus(t, s, got.ID, domain.StatusSent)
	}

	email := domain.ChannelEmail
	items, _, err := s.List(context.Background(), ListFilter{Channel: &email, Limit: 100})
	if err != nil || len(items) != 5 {
		t.Fatalf("channel filter: %d items, err %v", len(items), err)
	}
	for _, it := range items {
		if it.Channel != domain.ChannelEmail {
			t.Fatalf("channel filter leaked %s", it.Channel)
		}
	}
	sent := domain.StatusSent
	items, _, _ = s.List(context.Background(), ListFilter{Status: &sent, Limit: 100})
	if len(items) != 5 {
		t.Fatalf("status filter: %d items", len(items))
	}
	future := time.Now().Add(time.Hour)
	items, _, _ = s.List(context.Background(), ListFilter{From: &future, Limit: 100})
	if len(items) != 0 {
		t.Fatalf("date filter: %d items", len(items))
	}

	seen := map[uuid.UUID]bool{}
	var sizes []int
	cursor := ""
	for {
		page, next, err := s.List(context.Background(), ListFilter{Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, len(page))
		for _, it := range page {
			if seen[it.ID] {
				t.Fatalf("duplicate %s across pages", it.ID)
			}
			seen[it.ID] = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(sizes) != 3 || sizes[0] != 10 || sizes[1] != 10 || sizes[2] != 5 {
		t.Fatalf("page sizes = %v, want [10 10 5]", sizes)
	}

	if _, _, err := s.List(context.Background(), ListFilter{Limit: 10, Cursor: "garbage!!"}); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("bad cursor: %v", err)
	}
}

func TestCancel(t *testing.T) {
	s := newStore(t)
	n := mustCreate(t, s, sms("x"))

	got, err := s.Cancel(context.Background(), n.ID)
	if err != nil || got.Status != domain.StatusCancelled {
		t.Fatalf("cancel: %v %v", got.Status, err)
	}
	if _, err := s.Cancel(context.Background(), n.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("second cancel: %v", err)
	}
	if _, err := s.Cancel(context.Background(), uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown: %v", err)
	}

	p := mustCreate(t, s, sms("y"))
	setStatus(t, s, p.ID, domain.StatusProcessing)
	if _, err := s.Cancel(context.Background(), p.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("processing: %v", err)
	}
}

func TestRequeue(t *testing.T) {
	s := newStore(t)
	n := mustCreate(t, s, sms("x"))
	if _, err := s.Requeue(context.Background(), n.ID); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("requeue pending: %v", err)
	}
	if _, err := s.pool.Exec(context.Background(),
		`UPDATE notifications SET status='failed', attempts=5, last_error='boom' WHERE id=$1`, n.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.Requeue(context.Background(), n.ID)
	if err != nil || got.Status != domain.StatusPending || got.Attempts != 0 || got.LastError != nil {
		t.Fatalf("requeue: %+v %v", got, err)
	}
}
