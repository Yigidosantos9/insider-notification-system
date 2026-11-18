package worker

import (
	"math/rand/v2"
	"time"
)

// RetryPolicy computes when a failed delivery is tried again.
//
// It uses exponential backoff with "full jitter": the n-th retry waits a random
// time in [0, min(Cap, Base·2^(n-1))]. Randomising the whole window spreads out
// retries from a burst of failures so they don't hit the provider in lockstep.
type RetryPolicy struct {
	Base        time.Duration
	Cap         time.Duration
	MaxAttempts int
	Rand        func() float64 // returns [0,1); injectable for tests
}

// Delay returns the wait before the next try. attempt is the number of attempts
// already made (1 after the first failure). A provider Retry-After is honoured
// as a lower bound.
func (p RetryPolicy) Delay(attempt int, retryAfter time.Duration) time.Duration {
	window := p.Cap
	if attempt < 1 {
		attempt = 1
	}
	// Compare in float space so large attempt counts can't overflow the shift.
	if exp := float64(p.Base) * float64(uint64(1)<<min(attempt-1, 62)); exp < float64(p.Cap) {
		window = time.Duration(exp)
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	return max(time.Duration(r()*float64(window)), retryAfter)
}

// Exhausted reports whether no attempts are left.
func (p RetryPolicy) Exhausted(attempts int) bool {
	return attempts >= p.MaxAttempts
}
