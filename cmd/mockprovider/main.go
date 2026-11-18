// Command mockprovider is a local stand-in for the external notification
// provider (webhook.site). It answers like the configured webhook:
//
//	POST /send  →  202 {"messageId": "<uuid>", "status": "accepted", "timestamp": "<RFC3339>"}
//
// Failure injection, to watch retries and the circuit breaker in action:
//
//	FAIL_RATE   fraction of requests answered with 500 (0..1, default 0)
//	RATE_429    fraction answered with 429 + Retry-After: 1 (0..1, default 0)
//	LATENCY_MS  artificial delay per request (default 0)
//	PORT        listen port (default 9090)
package main

import (
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	failRate := envFloat("FAIL_RATE", 0)
	rate429 := envFloat("RATE_429", 0)
	latency := time.Duration(envFloat("LATENCY_MS", 0)) * time.Millisecond
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		time.Sleep(latency)

		roll := rand.Float64()
		switch {
		case roll < failRate:
			log.Info("mock: 500", "idempotency_key", r.Header.Get("Idempotency-Key"))
			http.Error(w, "simulated provider failure", http.StatusInternalServerError)
			return
		case roll < failRate+rate429:
			log.Info("mock: 429", "idempotency_key", r.Header.Get("Idempotency-Key"))
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}

		log.Info("mock: accepted", "idempotency_key", r.Header.Get("Idempotency-Key"),
			"to", body["to"], "channel", body["channel"])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"messageId": uuid.NewString(),
			"status":    "accepted",
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})

	log.Info("mock provider listening", "port", port, "fail_rate", failRate, "rate_429", rate429)
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func envFloat(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}
