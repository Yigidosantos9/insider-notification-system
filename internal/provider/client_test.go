package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSendSuccess(t *testing.T) {
	var got Request
	var gotKey, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotKey = r.Header.Get("Idempotency-Key")
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m-1","status":"accepted","timestamp":"2026-11-18T10:00:00Z"}`))
	}))
	defer srv.Close()

	resp, err := NewClient(srv.URL, time.Second).Send(context.Background(), "key-1",
		Request{To: "+905551234567", Channel: "sms", Content: "hi"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotMethod != http.MethodPost || gotKey != "key-1" {
		t.Fatalf("method=%s key=%s", gotMethod, gotKey)
	}
	if got != (Request{To: "+905551234567", Channel: "sms", Content: "hi"}) {
		t.Fatalf("body = %+v", got)
	}
	if resp.MessageID != "m-1" || resp.Status != "accepted" {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestSend2xxMalformedBodyIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`oops`))
	}))
	defer srv.Close()

	resp, err := NewClient(srv.URL, time.Second).Send(context.Background(), "k", Request{})
	if err != nil {
		t.Fatalf("2xx with bad body must count as success, got %v", err)
	}
	if resp.MessageID != "" {
		t.Fatalf("expected empty message id, got %q", resp.MessageID)
	}
}

func TestSendClassification(t *testing.T) {
	cases := []struct {
		code       int
		retryAfter string
		retryable  bool
		wantAfter  time.Duration
	}{
		{500, "", true, 0},
		{503, "", true, 0},
		{400, "", false, 0},
		{404, "", false, 0},
		{429, "10", true, 10 * time.Second},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if tc.retryAfter != "" {
				w.Header().Set("Retry-After", tc.retryAfter)
			}
			w.WriteHeader(tc.code)
		}))
		_, err := NewClient(srv.URL, time.Second).Send(context.Background(), "k", Request{})
		srv.Close()

		var se *SendError
		if !errors.As(err, &se) {
			t.Fatalf("%d: expected *SendError, got %v", tc.code, err)
		}
		if se.StatusCode != tc.code || se.Retryable != tc.retryable || se.RetryAfter != tc.wantAfter {
			t.Fatalf("%d: got %+v", tc.code, se)
		}
	}
}

func TestSendTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL, 50*time.Millisecond).Send(context.Background(), "k", Request{})
	var se *SendError
	if !errors.As(err, &se) {
		t.Fatalf("expected *SendError, got %v", err)
	}
	if !se.Retryable || se.StatusCode != 0 {
		t.Fatalf("timeout must be retryable with status 0, got %+v", se)
	}
}
