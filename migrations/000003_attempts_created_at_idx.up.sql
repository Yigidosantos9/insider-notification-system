-- Supports time-window queries over delivery attempts (GET /api/v1/metrics/summary).
CREATE INDEX notification_attempts_created_idx ON notification_attempts (created_at);
