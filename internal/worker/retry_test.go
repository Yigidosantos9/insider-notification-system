package worker

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	max := RetryPolicy{Base: 2 * time.Second, Cap: 5 * time.Minute, MaxAttempts: 5, Rand: func() float64 { return 1 }}
	cases := []struct {
		attempt    int
		retryAfter time.Duration
		want       time.Duration
	}{
		{1, 0, 2 * time.Second},
		{3, 0, 8 * time.Second},
		{20, 0, 5 * time.Minute}, // capped, and no overflow
		{1, 10 * time.Second, 10 * time.Second},
	}
	for _, tc := range cases {
		if got := max.Delay(tc.attempt, tc.retryAfter); got != tc.want {
			t.Errorf("Delay(%d, %s) = %s, want %s", tc.attempt, tc.retryAfter, got, tc.want)
		}
	}

	half := max
	half.Rand = func() float64 { return 0.5 }
	if got := half.Delay(2, 0); got != 2*time.Second {
		t.Errorf("jittered Delay(2) = %s, want 2s", got)
	}
}

func TestRetryExhausted(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 5}
	if !p.Exhausted(5) || p.Exhausted(4) {
		t.Fatalf("Exhausted(5)=%v Exhausted(4)=%v", p.Exhausted(5), p.Exhausted(4))
	}
}
