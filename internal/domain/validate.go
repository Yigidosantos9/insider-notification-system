package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Content and batch limits.
const (
	MaxBatchSize      = 1000
	MaxSMSLength      = 160 // one GSM segment
	MaxPushLength     = 256
	MaxEmailLength    = 10000
	MaxPushTokenLen   = 512
	MaxIdempotencyLen = 255
)

var e164 = regexp.MustCompile(`^\+[1-9]\d{7,14}$`)

// FieldError describes one invalid field.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ValidationError lists every invalid field of a request.
type ValidationError struct {
	Fields []FieldError
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Fields))
	for i, f := range e.Fields {
		parts[i] = f.Field + ": " + f.Message
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

func (e *ValidationError) add(field, msg string) { e.Fields = append(e.Fields, FieldError{field, msg}) }

func (e *ValidationError) orNil() error {
	if len(e.Fields) == 0 {
		return nil
	}
	return e
}

// Validate checks required fields, channel-specific recipient formats and content limits.
// Call Normalize first so the default priority is applied.
func (n NewNotification) Validate() error {
	ve := &ValidationError{}

	if !n.Channel.Valid() {
		ve.add("channel", "must be one of sms, email, push")
	}
	if !n.Priority.Valid() {
		ve.add("priority", "must be one of high, normal, low")
	}
	if len(n.IdempotencyKey) > MaxIdempotencyLen {
		ve.add("idempotency_key", fmt.Sprintf("must be at most %d characters", MaxIdempotencyLen))
	}

	contentLen := utf8.RuneCountInString(n.Content)
	if strings.TrimSpace(n.Content) == "" {
		ve.add("content", "is required")
	}

	switch n.Channel {
	case ChannelSMS:
		if !e164.MatchString(n.Recipient) {
			ve.add("recipient", "must be a phone number in E.164 format, e.g. +905551234567")
		}
		if contentLen > MaxSMSLength {
			ve.add("content", fmt.Sprintf("must be at most %d characters for sms", MaxSMSLength))
		}
	case ChannelEmail:
		addr, err := mail.ParseAddress(n.Recipient)
		if err != nil || addr.Address != n.Recipient {
			ve.add("recipient", "must be a plain email address, e.g. user@example.com")
		}
		if contentLen > MaxEmailLength {
			ve.add("content", fmt.Sprintf("must be at most %d characters for email", MaxEmailLength))
		}
	case ChannelPush:
		if n.Recipient == "" || len(n.Recipient) > MaxPushTokenLen {
			ve.add("recipient", fmt.Sprintf("must be a device token of 1-%d characters", MaxPushTokenLen))
		}
		if contentLen > MaxPushLength {
			ve.add("content", fmt.Sprintf("must be at most %d characters for push", MaxPushLength))
		}
	}
	return ve.orNil()
}

// ValidateBatch checks the batch size and every item. Field names are prefixed
// with the item index, e.g. "items[7].recipient".
func ValidateBatch(items []NewNotification) error {
	ve := &ValidationError{}
	if len(items) == 0 || len(items) > MaxBatchSize {
		ve.add("items", fmt.Sprintf("must contain between 1 and %d notifications", MaxBatchSize))
		return ve
	}
	firstByKey := map[string]int{}
	for i, it := range items {
		if err := it.Validate(); err != nil {
			for _, f := range err.(*ValidationError).Fields {
				ve.add(fmt.Sprintf("items[%d].%s", i, f.Field), f.Message)
			}
		}
		// The same key twice in one batch is almost certainly a client bug; it
		// would silently collapse into one notification.
		if it.IdempotencyKey != "" {
			if j, dup := firstByKey[it.IdempotencyKey]; dup {
				ve.add(fmt.Sprintf("items[%d].idempotency_key", i), fmt.Sprintf("duplicates items[%d]", j))
			} else {
				firstByKey[it.IdempotencyKey] = i
			}
		}
	}
	return ve.orNil()
}
