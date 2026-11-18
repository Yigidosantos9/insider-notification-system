// Package config loads runtime configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds every tunable of the api and worker processes. All fields have defaults.
type Config struct {
	DatabaseURL       string
	HTTPAddr          string
	MetricsAddr       string // worker-only Prometheus listener
	ProviderURL       string
	ProviderTimeout   time.Duration
	RatePerSecond     int
	WorkerConcurrency int
	ClaimBatch        int
	MaxAttempts       int
	RetryBase         time.Duration
	RetryCap          time.Duration
	BreakerThreshold  int
	BreakerCooldown   time.Duration
	LogLevel          string
}

// Load reads the configuration from the environment, falling back to defaults.
func Load() (Config, error) {
	l := loader{}
	c := Config{
		DatabaseURL:       l.str("DATABASE_URL", "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"),
		HTTPAddr:          l.str("HTTP_ADDR", ":8080"),
		MetricsAddr:       l.str("METRICS_ADDR", ":9091"),
		ProviderURL:       l.str("PROVIDER_URL", "http://localhost:9090/send"),
		ProviderTimeout:   l.dur("PROVIDER_TIMEOUT", 5*time.Second),
		RatePerSecond:     l.int("RATE_LIMIT_PER_SECOND", 100),
		WorkerConcurrency: l.int("WORKER_CONCURRENCY", 20),
		ClaimBatch:        l.int("CLAIM_BATCH", 50),
		MaxAttempts:       l.int("MAX_ATTEMPTS", 5),
		RetryBase:         l.dur("RETRY_BASE", 2*time.Second),
		RetryCap:          l.dur("RETRY_CAP", 5*time.Minute),
		BreakerThreshold:  l.int("BREAKER_THRESHOLD", 5),
		BreakerCooldown:   l.dur("BREAKER_COOLDOWN", 30*time.Second),
		LogLevel:          l.str("LOG_LEVEL", "info"),
	}
	return c, l.err
}

// loader remembers the first parse error so Load can report it once.
type loader struct{ err error }

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func (l *loader) int(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil && l.err == nil {
		l.err = fmt.Errorf("%s: invalid integer %q", key, v)
	}
	return n
}

func (l *loader) dur(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil && l.err == nil {
		l.err = fmt.Errorf("%s: invalid duration %q", key, v)
	}
	return d
}
