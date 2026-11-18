package store

import (
	"context"
	"testing"
	"time"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

func TestSummary(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	ok, bad := 202, 500

	// 3 successful deliveries (10, 20, 30 ms), 1 retry (100 ms), 1 dead letter (200 ms) on sms.
	for _, lat := range []int{10, 20, 30} {
		n := mustCreate(t, s, sms("ok"))
		claimOne(t, s)
		if err := s.MarkSent(ctx, n.ID, "m", domain.Attempt{AttemptNo: 1, StatusCode: &ok, LatencyMs: lat}); err != nil {
			t.Fatal(err)
		}
	}
	r := mustCreate(t, s, sms("retry"))
	claimOne(t, s)
	if err := s.MarkRetry(ctx, r.ID, time.Hour, domain.Attempt{AttemptNo: 1, StatusCode: &bad, Error: "500", LatencyMs: 100}); err != nil {
		t.Fatal(err)
	}
	f := mustCreate(t, s, sms("fail"))
	claimOne(t, s)
	if err := s.MarkFailed(ctx, f.ID, domain.Attempt{AttemptNo: 1, Error: "400", LatencyMs: 200}); err != nil {
		t.Fatal(err)
	}
	// Queue: 2 due high-priority email, 1 scheduled for later.
	e := domain.NewNotification{Channel: domain.ChannelEmail, Priority: domain.PriorityHigh, Recipient: "a@b.co", Content: "x"}
	mustCreate(t, s, e)
	mustCreate(t, s, e)
	later := time.Now().Add(time.Hour)
	e.ScheduledAt = &later
	mustCreate(t, s, e)

	sum, err := s.Summary(ctx, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if sum.QueueDepth["email"]["high"] != 2 || sum.Scheduled != 2 { // retry row (due in 1h) counts as scheduled too
		t.Fatalf("queue depth %v scheduled %d", sum.QueueDepth, sum.Scheduled)
	}
	c, found := sum.Channels["sms"]
	if !found {
		t.Fatalf("no sms stats: %+v", sum.Channels)
	}
	if c.Succeeded != 3 || c.Failed != 2 || c.DeadLettered != 1 {
		t.Fatalf("counts: %+v", c)
	}
	if c.SuccessRate < 0.59 || c.SuccessRate > 0.61 {
		t.Fatalf("success rate %v, want 0.6", c.SuccessRate)
	}
	if c.Latency.AvgMs != 72 || c.Latency.P50Ms != 30 || c.Latency.P99Ms < 190 {
		t.Fatalf("latency %+v", c.Latency)
	}
	if _, found := sum.Channels["push"]; found {
		t.Fatal("channels without attempts should be omitted")
	}
}
