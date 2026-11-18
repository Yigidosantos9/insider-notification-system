// Package provider talks to the external notification provider (webhook.site
// or the local mock) and classifies its failures.
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// Request is the payload the provider expects.
type Request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Response is the provider's acceptance receipt.
type Response struct {
	MessageID string `json:"messageId"`
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`

	StatusCode int `json:"-"` // HTTP status the provider answered with
}

// SendError is returned for every non-2xx outcome, including transport errors (StatusCode 0).
type SendError struct {
	StatusCode int
	Retryable  bool
	RetryAfter time.Duration // from a Retry-After header, if any
	Err        error
}

func (e *SendError) Error() string {
	if e.StatusCode == 0 {
		return fmt.Sprintf("provider unreachable: %v", e.Err)
	}
	return fmt.Sprintf("provider returned %d: %v", e.StatusCode, e.Err)
}

func (e *SendError) Unwrap() error { return e.Err }

// Classify decides whether a failed send is worth retrying.
// Transport errors/timeouts (status 0), 429 and 5xx are transient; other 4xx
// mean the request itself is wrong and will never succeed.
func Classify(statusCode int) bool {
	return statusCode == 0 || statusCode == http.StatusTooManyRequests || statusCode >= 500
}

// Client sends notifications to the provider over HTTP.
type Client struct {
	url  string
	http *http.Client
}

// NewClient returns a client with the given per-request timeout.
func NewClient(url string, timeout time.Duration) *Client {
	return &Client{url: url, http: &http.Client{Timeout: timeout}}
}

// Send posts the notification. The idempotency key (our notification ID) lets a
// provider that supports it drop duplicates caused by our at-least-once retries.
//
// Any 2xx is success even if the body is unreadable: the provider has accepted
// the message, and retrying would send it twice.
func (c *Client) Send(ctx context.Context, idempotencyKey string, r Request) (Response, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return Response{}, &SendError{Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return Response{}, &SendError{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return Response{}, &SendError{Retryable: true, Err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var out Response
		_ = json.Unmarshal(raw, &out) // best effort; see doc comment
		out.StatusCode = resp.StatusCode
		return out, nil
	}

	return Response{}, &SendError{
		StatusCode: resp.StatusCode,
		Retryable:  Classify(resp.StatusCode),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After")),
		Err:        fmt.Errorf("%s", truncate(string(raw), 200)),
	}
}

// parseRetryAfter supports the delta-seconds form of Retry-After.
func parseRetryAfter(v string) time.Duration {
	if s, err := strconv.Atoi(v); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return 0
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
