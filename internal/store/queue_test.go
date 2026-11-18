package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

func withPriority(p domain.Priority) domain.NewNotification {
	n := sms(string(p))
	n.Priority = p
	return n
}

func TestClaimPriorityOrder(t *testing.T) {
	s := newStore(t)
	low := mustCreate(t, s, withPriority(domain.PriorityLow))
	normal := mustCreate(t, s, withPriority(domain.PriorityNormal))
	high := mustCreate(t, s, withPriority(domain.PriorityHigh))

	got, err := s.Claim(context.Background(), domain.ChannelSMS, 3, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []uuid.UUID{high.ID, normal.ID, low.ID}
	if len(got) != 3 {
		t.Fatalf("claimed %d, want 3", len(got))
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("position %d: got %s (%s), want %s", i, got[i].ID, got[i].Priority, want[i])
		}
		if got[i].Status != domain.StatusProcessing || got[i].LockedUntil == nil {
			t.Fatalf("claimed row not processing/leased: %+v", got[i])
		}
	}
}

func TestClaimSkipsOtherChannelsFutureAndCancelled(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, domain.NewNotification{Channel: domain.ChannelEmail, Priority: domain.PriorityNormal, Recipient: "a@b.co", Content: "x"})
	later := time.Now().Add(time.Hour)
	scheduled := sms("later")
	scheduled.ScheduledAt = &later
	mustCreate(t, s, scheduled)
	c := mustCreate(t, s, sms("cancel me"))
	if _, err := s.Cancel(context.Background(), c.ID); err != nil {
		t.Fatal(err)
	}

	got, err := s.Claim(context.Background(), domain.ChannelSMS, 10, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("claimed %d rows, want 0: %+v", len(got), got)
	}
}

func TestClaimConcurrentNoDoubleClaim(t *testing.T) {
	s := newStore(t)
	for i := 0; i < 100; i++ {
		mustCreate(t, s, sms("c"))
	}

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.Claim(context.Background(), domain.ChannelSMS, 30, 30*time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			for _, n := range got {
				seen[n.ID]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) != 100 {
		t.Fatalf("claimed %d unique rows, want 100", len(seen))
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("row %s claimed %d times", id, c)
		}
	}
}

func claimOne(t *testing.T, s *Store) domain.Notification {
	t.Helper()
	got, err := s.Claim(context.Background(), domain.ChannelSMS, 1, 30*time.Second)
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %d rows, err %v", len(got), err)
	}
	return got[0]
}

func TestMarkTransitions(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	code := 202

	sent := mustCreate(t, s, sms("a"))
	claimOne(t, s)
	if err := s.MarkSent(ctx, sent.ID, "prov-1", domain.Attempt{AttemptNo: 1, StatusCode: &code, LatencyMs: 12}); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, sent.ID)
	if got.Status != domain.StatusSent || got.Attempts != 1 || got.SentAt == nil ||
		got.ProviderMessageID == nil || *got.ProviderMessageID != "prov-1" || got.LockedUntil != nil {
		t.Fatalf("after MarkSent: %+v", got)
	}
	atts, _ := s.Attempts(ctx, sent.ID)
	if len(atts) != 1 || atts[0].StatusCode == nil || *atts[0].StatusCode != 202 {
		t.Fatalf("attempts: %+v", atts)
	}

	retry := mustCreate(t, s, sms("b"))
	claimOne(t, s)
	next := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	if err := s.MarkRetry(ctx, retry.ID, next, domain.Attempt{AttemptNo: 1, Error: "timeout"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, retry.ID)
	if got.Status != domain.StatusPending || got.Attempts != 1 || !got.NextAttemptAt.Equal(next) ||
		got.LastError == nil || *got.LastError != "timeout" {
		t.Fatalf("after MarkRetry: %+v", got)
	}

	failed := mustCreate(t, s, sms("c"))
	claimOne(t, s)
	if err := s.MarkFailed(ctx, failed.ID, domain.Attempt{AttemptNo: 1, Error: "bad request"}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, failed.ID)
	if got.Status != domain.StatusFailed || got.Attempts != 1 || got.LastError == nil || *got.LastError != "bad request" {
		t.Fatalf("after MarkFailed: %+v", got)
	}
}

func TestReschedule(t *testing.T) {
	s := newStore(t)
	n := mustCreate(t, s, sms("r"))
	claimOne(t, s)
	at := time.Now().Add(30 * time.Second).UTC().Truncate(time.Microsecond)
	if err := s.Reschedule(context.Background(), n.ID, at); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(context.Background(), n.ID)
	if got.Status != domain.StatusPending || got.Attempts != 0 || !got.NextAttemptAt.Equal(at) {
		t.Fatalf("after Reschedule: %+v", got)
	}
}

func TestReapExpiredLeases(t *testing.T) {
	s := newStore(t)
	n := mustCreate(t, s, sms("crash"))
	if _, err := s.Claim(context.Background(), domain.ChannelSMS, 1, -time.Second); err != nil {
		t.Fatal(err)
	}
	reaped, err := s.ReapExpiredLeases(context.Background())
	if err != nil || reaped != 1 {
		t.Fatalf("reaped %d, err %v", reaped, err)
	}
	got, _ := s.Get(context.Background(), n.ID)
	if got.Status != domain.StatusPending {
		t.Fatalf("status %s, want pending", got.Status)
	}
}

func TestQueueDepth(t *testing.T) {
	s := newStore(t)
	mustCreate(t, s, withPriority(domain.PriorityHigh))
	mustCreate(t, s, withPriority(domain.PriorityHigh))
	mustCreate(t, s, domain.NewNotification{Channel: domain.ChannelEmail, Priority: domain.PriorityLow, Recipient: "a@b.co", Content: "x"})

	rows, err := s.QueueDepth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, r := range rows {
		got[string(r.Channel)+"/"+string(r.Priority)] = r.Count
	}
	if len(got) != 2 || got["sms/high"] != 2 || got["email/low"] != 1 {
		t.Fatalf("depth = %v", got)
	}
}
