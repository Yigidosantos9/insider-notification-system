package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func fieldsOf(t *testing.T, err error) []string {
	t.Helper()
	if err == nil {
		return nil
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	out := make([]string, 0, len(ve.Fields))
	for _, f := range ve.Fields {
		out = append(out, f.Field)
	}
	return out
}

func TestValidate(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	cases := []struct {
		name string
		n    NewNotification
		want string // expected single invalid field, "" for valid
	}{
		{"valid sms", NewNotification{Channel: ChannelSMS, Recipient: "+905551234567", Content: "hi"}, ""},
		{"sms not e164", NewNotification{Channel: ChannelSMS, Recipient: "05551234567", Content: "hi"}, "recipient"},
		{"sms too long", NewNotification{Channel: ChannelSMS, Recipient: "+905551234567", Content: strings.Repeat("a", 161)}, "content"},
		{"sms 160 runes multibyte", NewNotification{Channel: ChannelSMS, Recipient: "+905551234567", Content: strings.Repeat("ğ", 160)}, ""},
		{"valid email", NewNotification{Channel: ChannelEmail, Recipient: "a@b.co", Content: "hello"}, ""},
		{"email with display name", NewNotification{Channel: ChannelEmail, Recipient: "Bob <a@b.co>", Content: "hello"}, "recipient"},
		{"push empty recipient", NewNotification{Channel: ChannelPush, Recipient: "", Content: "hello"}, "recipient"},
		{"push too long", NewNotification{Channel: ChannelPush, Recipient: "device-token", Content: strings.Repeat("a", 257)}, "content"},
		{"unknown channel", NewNotification{Channel: "fax", Recipient: "x", Content: "hello"}, "channel"},
		{"unknown priority", NewNotification{Channel: ChannelSMS, Priority: "urgent", Recipient: "+905551234567", Content: "hi"}, "priority"},
		{"empty content", NewNotification{Channel: ChannelSMS, Recipient: "+905551234567", Content: ""}, "content"},
		{"scheduled in past is fine", NewNotification{Channel: ChannelSMS, Recipient: "+905551234567", Content: "hi", ScheduledAt: &past}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.n.Normalize()
			got := fieldsOf(t, tc.n.Validate())
			if tc.want == "" {
				if len(got) != 0 {
					t.Fatalf("expected valid, got errors on %v", got)
				}
				return
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("expected error on [%s], got %v", tc.want, got)
			}
		})
	}
}

func TestNormalizeDefaultsPriority(t *testing.T) {
	n := NewNotification{Channel: ChannelSMS, Recipient: "  +905551234567 ", Content: "hi"}
	n.Normalize()
	if n.Priority != PriorityNormal {
		t.Fatalf("priority = %q, want normal", n.Priority)
	}
	if n.Recipient != "+905551234567" {
		t.Fatalf("recipient not trimmed: %q", n.Recipient)
	}
}

func validSMS() NewNotification {
	return NewNotification{Channel: ChannelSMS, Priority: PriorityNormal, Recipient: "+905551234567", Content: "hi"}
}

func TestValidateBatch(t *testing.T) {
	if got := fieldsOf(t, ValidateBatch(nil)); len(got) != 1 || got[0] != "items" {
		t.Fatalf("empty batch: got %v", got)
	}

	big := make([]NewNotification, MaxBatchSize+1)
	for i := range big {
		big[i] = validSMS()
	}
	if got := fieldsOf(t, ValidateBatch(big)); len(got) != 1 || got[0] != "items" {
		t.Fatalf("oversized batch: got %v", got)
	}

	items := make([]NewNotification, 10)
	for i := range items {
		items[i] = validSMS()
	}
	items[7].Recipient = "not-a-phone"
	if got := fieldsOf(t, ValidateBatch(items)); len(got) != 1 || got[0] != "items[7].recipient" {
		t.Fatalf("bad item: got %v", got)
	}

	if err := ValidateBatch(items[:7]); err != nil {
		t.Fatalf("valid batch rejected: %v", err)
	}
}

func TestPayloadHashStable(t *testing.T) {
	a, b := validSMS(), validSMS()
	if a.PayloadHash() != b.PayloadHash() {
		t.Fatal("same payload produced different hashes")
	}
	b.Content = "different"
	if a.PayloadHash() == b.PayloadHash() {
		t.Fatal("different payloads produced the same hash")
	}
}

func TestPriorityRank(t *testing.T) {
	for p, r := range map[Priority]int16{PriorityHigh: 0, PriorityNormal: 1, PriorityLow: 2} {
		if p.Rank() != r {
			t.Fatalf("%s rank = %d, want %d", p, p.Rank(), r)
		}
		if PriorityFromRank(r) != p {
			t.Fatalf("PriorityFromRank(%d) = %s, want %s", r, PriorityFromRank(r), p)
		}
	}
}

func TestValidateBatchDuplicateIdempotencyKeys(t *testing.T) {
	items := []NewNotification{validSMS(), validSMS(), validSMS()}
	items[0].IdempotencyKey = "k"
	items[2].IdempotencyKey = "k"
	if got := fieldsOf(t, ValidateBatch(items)); len(got) != 1 || got[0] != "items[2].idempotency_key" {
		t.Fatalf("got %v, want [items[2].idempotency_key]", got)
	}
}
