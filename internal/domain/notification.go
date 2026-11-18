// Package domain defines the notification model and its validation rules.
package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Channel is a delivery channel.
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every supported channel.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// Valid reports whether c is a supported channel.
func (c Channel) Valid() bool {
	return c == ChannelSMS || c == ChannelEmail || c == ChannelPush
}

// Priority decides processing order: high before normal before low.
type Priority string

const (
	PriorityHigh   Priority = "high"
	PriorityNormal Priority = "normal"
	PriorityLow    Priority = "low"
)

// Rank is the numeric value stored in the database; lower ranks are processed first.
func (p Priority) Rank() int16 {
	switch p {
	case PriorityHigh:
		return 0
	case PriorityLow:
		return 2
	default:
		return 1
	}
}

// Valid reports whether p is a supported priority.
func (p Priority) Valid() bool {
	return p == PriorityHigh || p == PriorityNormal || p == PriorityLow
}

// PriorityFromRank is the inverse of Priority.Rank.
func PriorityFromRank(r int16) Priority {
	switch r {
	case 0:
		return PriorityHigh
	case 2:
		return PriorityLow
	default:
		return PriorityNormal
	}
}

// Status is the lifecycle state of a notification.
//
//	pending ──claim──▶ processing ──2xx──▶ sent
//	   ▲                    │
//	   └──retryable error───┤ (attempts left)
//	                        └──permanent / exhausted──▶ failed ──requeue──▶ pending
//	pending ──cancel──▶ cancelled
type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusSent       Status = "sent"
	StatusFailed     Status = "failed"
	StatusCancelled  Status = "cancelled"
)

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusProcessing, StatusSent, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

// NewNotification is the client's request to send one notification.
type NewNotification struct {
	Channel        Channel    `json:"channel"`
	Priority       Priority   `json:"priority,omitempty"`
	Recipient      string     `json:"recipient"`
	Content        string     `json:"content"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	ScheduledAt    *time.Time `json:"scheduled_at,omitempty"`

	// Optional template: when set, Content is rendered from the template with
	// Variables (and must not be supplied by the client).
	TemplateID *uuid.UUID        `json:"template_id,omitempty"`
	Variables  map[string]string `json:"variables,omitempty"`
}

// Normalize trims the recipient and applies the default priority.
func (n *NewNotification) Normalize() {
	n.Recipient = strings.TrimSpace(n.Recipient)
	n.IdempotencyKey = strings.TrimSpace(n.IdempotencyKey)
	if n.Priority == "" {
		n.Priority = PriorityNormal
	}
}

// PayloadHash fingerprints the request so a reused idempotency key with a
// different payload can be detected.
func (n NewNotification) PayloadHash() string {
	sched := ""
	if n.ScheduledAt != nil {
		sched = n.ScheduledAt.UTC().Format(time.RFC3339)
	}
	sum := sha256.Sum256([]byte(strings.Join(
		[]string{string(n.Channel), string(n.Priority), n.Recipient, n.Content, sched}, "|")))
	return hex.EncodeToString(sum[:])
}

// Notification is a stored notification.
type Notification struct {
	ID                uuid.UUID  `json:"id"`
	BatchID           *uuid.UUID `json:"batch_id,omitempty"`
	IdempotencyKey    *string    `json:"idempotency_key,omitempty"`
	PayloadHash       string     `json:"-"`
	CorrelationID     string     `json:"correlation_id,omitempty"`
	Channel           Channel    `json:"channel"`
	Priority          Priority   `json:"priority"`
	Recipient         string     `json:"recipient"`
	Content           string     `json:"content"`
	TemplateID        *uuid.UUID `json:"template_id,omitempty"`
	Status            Status     `json:"status"`
	Attempts          int        `json:"attempts"`
	MaxAttempts       int        `json:"max_attempts"`
	NextAttemptAt     time.Time  `json:"next_attempt_at"`
	ScheduledAt       *time.Time `json:"scheduled_at,omitempty"`
	LockedUntil       *time.Time `json:"-"`
	LastError         *string    `json:"last_error,omitempty"`
	ProviderMessageID *string    `json:"provider_message_id,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
	SentAt            *time.Time `json:"sent_at,omitempty"`
}

// Attempt records one delivery try against the provider.
type Attempt struct {
	AttemptNo  int       `json:"attempt_no"`
	StatusCode *int      `json:"status_code,omitempty"`
	Error      string    `json:"error,omitempty"`
	LatencyMs  int       `json:"latency_ms"`
	CreatedAt  time.Time `json:"created_at"`
}
