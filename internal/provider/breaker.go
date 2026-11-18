package provider

import (
	"sync"
	"time"
)

// BreakerState is the state of a circuit breaker.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"    // requests flow normally
	BreakerOpen     BreakerState = "open"      // provider considered down; requests are held back
	BreakerHalfOpen BreakerState = "half_open" // cooldown over; one probe request tests recovery
)

// Breaker stops hammering a failing provider. After `threshold` consecutive
// transient failures it opens for `cooldown`; then a single probe is let
// through and its result closes or re-opens the circuit.
type Breaker struct {
	mu        sync.Mutex
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	state         BreakerState
	failures      int
	openedAt      time.Time
	probeInFlight bool
}

// NewBreaker creates a closed breaker. `now` is injectable for tests.
func NewBreaker(threshold int, cooldown time.Duration, now func() time.Time) *Breaker {
	if now == nil {
		now = time.Now
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, now: now, state: BreakerClosed}
}

// Allow reports whether a request may be sent now.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case BreakerOpen:
		if b.now().Sub(b.openedAt) < b.cooldown {
			return false
		}
		b.state = BreakerHalfOpen
		b.probeInFlight = true
		return true
	case BreakerHalfOpen:
		if b.probeInFlight {
			return false
		}
		b.probeInFlight = true
		return true
	default:
		return true
	}
}

// Blocked reports, without changing state, whether no request could be sent
// right now: open and still cooling down, or half-open with the probe in
// flight. The worker checks it before claiming work, so an outage does not
// make it churn through the queue rescheduling rows it cannot send.
func (b *Breaker) Blocked() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case BreakerOpen:
		return b.now().Sub(b.openedAt) < b.cooldown
	case BreakerHalfOpen:
		return b.probeInFlight
	default:
		return false
	}
}

// Success records a successful request and closes the circuit.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = BreakerClosed
	b.failures = 0
	b.probeInFlight = false
}

// Failure records a transient failure.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == BreakerHalfOpen {
		b.trip()
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.trip()
	}
}

// State returns the current state (it does not advance open → half_open).
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Cooldown returns how long the breaker stays open.
func (b *Breaker) Cooldown() time.Duration { return b.cooldown }

func (b *Breaker) trip() {
	b.state = BreakerOpen
	b.openedAt = b.now()
	b.failures = 0
	b.probeInFlight = false
}
