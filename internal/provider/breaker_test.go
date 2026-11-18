package provider

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestBreaker(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	b := NewBreaker(5, 30*time.Second, clk.now)

	for i := 0; i < 4; i++ {
		b.Failure()
	}
	if b.State() != BreakerClosed || !b.Allow() {
		t.Fatalf("4 failures: state %s, want closed", b.State())
	}

	b.Failure()
	if b.State() != BreakerOpen || b.Allow() {
		t.Fatalf("5 failures: state %s, want open and blocking", b.State())
	}

	clk.advance(30 * time.Second)
	if !b.Allow() {
		t.Fatal("after cooldown the first request must be allowed as a probe")
	}
	if b.State() != BreakerHalfOpen {
		t.Fatalf("state %s, want half_open", b.State())
	}
	if b.Allow() {
		t.Fatal("only one probe may be in flight while half-open")
	}

	b.Success()
	if b.State() != BreakerClosed || !b.Allow() {
		t.Fatalf("probe success: state %s, want closed", b.State())
	}

	for i := 0; i < 5; i++ {
		b.Failure()
	}
	clk.advance(30 * time.Second)
	if !b.Allow() {
		t.Fatal("probe should be allowed")
	}
	b.Failure()
	if b.State() != BreakerOpen || b.Allow() {
		t.Fatalf("probe failure: state %s, want open", b.State())
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	b := NewBreaker(3, time.Second, time.Now)
	b.Failure()
	b.Failure()
	b.Success()
	b.Failure()
	b.Failure()
	if b.State() != BreakerClosed {
		t.Fatalf("non-consecutive failures opened the breaker")
	}
}
