CREATE TYPE channel AS ENUM ('sms', 'email', 'push');
CREATE TYPE notification_status AS ENUM ('pending', 'processing', 'sent', 'failed', 'cancelled');

CREATE TABLE notifications (
    id                  uuid PRIMARY KEY,
    batch_id            uuid,
    idempotency_key     text UNIQUE,
    payload_hash        text,
    correlation_id      text NOT NULL DEFAULT '',
    channel             channel NOT NULL,
    priority            smallint NOT NULL,            -- 0 high, 1 normal, 2 low
    recipient           text NOT NULL,
    content             text NOT NULL,
    status              notification_status NOT NULL DEFAULT 'pending',
    attempts            int NOT NULL DEFAULT 0,
    max_attempts        int NOT NULL DEFAULT 5,
    next_attempt_at     timestamptz NOT NULL DEFAULT now(),
    scheduled_at        timestamptz,
    locked_until        timestamptz,
    last_error          text,
    provider_message_id text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz
);

-- Worker claim query: pending rows per channel ordered by priority, then due time.
CREATE INDEX notifications_claim_idx ON notifications (channel, priority, next_attempt_at) WHERE status = 'pending';
CREATE INDEX notifications_batch_idx ON notifications (batch_id);
CREATE INDEX notifications_created_idx ON notifications (created_at DESC, id DESC);
-- Lease reaper: find processing rows whose lease expired.
CREATE INDEX notifications_processing_idx ON notifications (locked_until) WHERE status = 'processing';

CREATE TABLE notification_attempts (
    id              bigserial PRIMARY KEY,
    notification_id uuid NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no      int NOT NULL,
    status_code     int,
    error           text,
    latency_ms      int NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notification_attempts_nid_idx ON notification_attempts (notification_id);
