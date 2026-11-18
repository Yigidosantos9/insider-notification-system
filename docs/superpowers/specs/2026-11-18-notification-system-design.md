# Event-Driven Notification System — Design

Date: 2026-11-18 · Context: Insider One Software Engineer Assessment (Go)

## 1. Intent

Build a take-home submission that a reviewer can clone, run with one command, and read
easily. It must meet every functional requirement and deliverable in the brief, and
make the open "Delivery & Retry" section a showcase of deliberate design. The author
will not have to defend the code in depth, but must understand the main mechanics.
Code and docs should therefore stay plain and well-commented, with no clever abstractions.

## 2. Goals / Non-goals / Anti-goals

**Goals**
- All functional requirements (API, processing engine, retry, observability).
- All 6 deliverables: public GitHub repo with clean incremental commits, README, `docker-compose up`,
  OpenAPI spec, versioned migrations, single-command test suite.
- Bonuses, in order: Failure Handling (circuit breaker + dead-letter/requeue), Scheduled
  Notifications, GitHub Actions CI. Template System only if time remains.

**Non-goals**
- WebSocket updates, distributed tracing (OpenTelemetry). These are listed in README "Future work".
- Auth/multi-tenancy, a real SMS/email provider, a UI.

**Anti-goals** (a failure even if it "works")
- `docker-compose up` fails on a clean machine, or tests need manual setup.
- Over-engineering (Kafka, microservice sprawl) that the author can't explain.
- A single giant commit.

## 3. Constraints

- Go 1.2x (use Go 1.23+; stdlib `net/http` routing with method patterns).
- Provider is simulated with webhook.site (`POST {to, channel, content}` → `202 {messageId, status, timestamp}`).
  The free tier has request limits, so the compose stack ships a **local mock provider** as the default.
  `PROVIDER_URL` switches to the real webhook.site URL.
- Rate limit: max 100 msg/s **per channel**.
- Batch: max 1000 notifications per request.
- Budget ~12h.

## 4. Approaches considered

| | Approach | Trade-off |
|---|---|---|
| **A (chosen)** | **PostgreSQL as store + queue** (`SELECT … FOR UPDATE SKIP LOCKED`) | One component. Writes are transactional, so no dual-write and no outbox. Priority, scheduling and retry delay are all one indexed query. Ceiling is ~thousands msg/s, well above "millions/day" (~12/s avg, bursts handled by queueing). |
| B | Postgres + Redis (queues + distributed rate limit) | Faster and the rate limit stays correct across replicas, but needs an outbox for PG↔Redis consistency. More parts. |
| C | Postgres + Kafka/RabbitMQ | Most "event-driven" on paper, but priority is awkward in Kafka. Highest setup/failure risk for 12h. |

Chosen: **A**, picked by the assistant (the author delegated the how). The queue sits behind a Go
interface so B or C is a documented scaling path in the README, not a rewrite.

## 5. Architecture

```
            ┌────────────┐   insert (tx)    ┌──────────────┐
 client ──▶ │  API (Go)  │ ───────────────▶ │  PostgreSQL  │
            └────────────┘ ◀── query ────── │ notifications│
                                            └──────┬───────┘
                                  claim (SKIP LOCKED, priority order)
                                                   ▼
            ┌───────────────────────────────────────────────┐
            │ Worker (Go)                                   │
            │  per-channel loop: rate limiter (100/s) ─▶    │
            │  circuit breaker ─▶ provider HTTP client ─▶   │
            │  result → sent | retry(backoff) | failed(DLQ) │
            └───────────────────────────────────────────────┘
                                                   ▼
                                  mock-provider  /  webhook.site
```

- **One binary, two modes**: `notifier api` and `notifier worker` (same image, two compose services).
  They can scale independently.
- Compose services: `postgres`, `migrate` (runs golang-migrate, then exits), `api` (:8080), `worker`, `mock-provider` (:9090).

### Repo layout
```
cmd/notifier/          main: subcommands api | worker
cmd/mockprovider/      tiny fake provider (configurable latency / failure rate / 429s)
internal/domain/       Notification, Channel, Priority, Status, validation rules
internal/store/        Postgres repository (pgx/v5, plain SQL)
internal/api/          HTTP handlers, middleware (correlation ID, logging, recovery)
internal/worker/       dispatcher, claim loop, retry policy
internal/provider/     HTTP client + circuit breaker
internal/metrics/      Prometheus collectors
migrations/            golang-migrate versioned .up/.down.sql
api/openapi.yaml       OpenAPI 3 spec, served with Swagger UI at /docs
.github/workflows/     CI: lint (golangci-lint) + test
```

## 6. Data model (migration 0001)

`notifications`
- `id uuid pk`, `batch_id uuid null`, `idempotency_key text null unique`
- `channel` enum(sms, email, push), `priority` enum(high, normal, low) stored as smallint (high=0) for ordering
- `recipient text`, `content text`
- `status` enum(pending, processing, sent, failed, cancelled)
- `attempts int`, `max_attempts int default 5`, `next_attempt_at timestamptz` (= scheduled_at or now)
- `locked_until timestamptz null` (worker lease), `last_error text`, `provider_message_id text`
- `scheduled_at`, `created_at`, `updated_at`, `sent_at`
- Index: `(channel, status, priority, next_attempt_at) WHERE status='pending'`; plus `batch_id`, `created_at`.

`notification_attempts` (audit of each delivery try: status code, error, latency). Makes retry behaviour visible.

## 7. API (`/api/v1`)

| Method | Path | Notes |
|---|---|---|
| POST | `/notifications` | single; optional `Idempotency-Key` header or `idempotency_key` field; optional `scheduled_at` |
| POST | `/notifications/batch` | 1–1000 items, one transaction, returns `batch_id` + ids; per-item validation errors reported (all-or-nothing reject) |
| GET | `/notifications/{id}` | status + attempts |
| GET | `/batches/{id}` | summary counts per status + items (paginated) |
| POST | `/notifications/{id}/cancel` | only `pending` (incl. scheduled); else 409 |
| GET | `/notifications` | filters: `status, channel, from, to`; cursor pagination (`limit`, `cursor`) |
| POST | `/notifications/{id}/requeue` | bonus: move `failed` (dead-letter) back to pending |
| GET | `/health`, `/ready` | liveness / DB-ping readiness |
| GET | `/metrics` | Prometheus |
| GET | `/docs` | Swagger UI |

Idempotency: a repeated key returns the **original** notification with `200` (first call `201`).
Same key with a different payload → `409`.

Validation (assistant's calls): required fields; channel ∈ enum; SMS recipient E.164, content ≤ 160 chars;
email recipient RFC-ish format, content ≤ 10,000; push recipient = non-empty device token, content ≤ 256.
Errors use one JSON shape: `{error:{code,message,details[]}}`.

## 8. Processing engine

- Each worker runs **one goroutine group per channel**. Each group claims work in small batches:
  ```sql
  UPDATE notifications SET status='processing', locked_until=now()+'30s'
  WHERE id IN (SELECT id FROM notifications
               WHERE channel=$1 AND status='pending' AND next_attempt_at<=now()
               ORDER BY priority, next_attempt_at LIMIT $2
               FOR UPDATE SKIP LOCKED)
  RETURNING …
  ```
  Priority is strict: high drains before normal before low. Starvation is acceptable here and documented.
- **Rate limit**: `golang.org/x/time/rate` token bucket per channel, 100/s, burst 100, waited
  before every send. This is per worker process, and compose runs one worker. The README notes that
  multiple worker replicas need a shared limiter (Redis GCRA), which is the scaling path.
- **Lease recovery**: a reaper resets `processing` rows whose `locked_until` has passed back to `pending`,
  so a crashed worker loses no work.
- Sends run on a bounded goroutine pool per channel (e.g. 20), so slow provider calls don't cap throughput.

## 9. Delivery & retry (the candidate-designed section)

- **Classification**: retryable = network error, timeout (5s client timeout), 5xx, 429 (honour `Retry-After`).
  Permanent = other 4xx. Permanent goes straight to `failed`. Any 2xx counts as sent, even if the body
  is malformed: retrying a message the provider already accepted would duplicate it. The `messageId` is stored when present.
- **Backoff**: exponential with full jitter: `delay = rand(0, min(cap, base·2^attempt))`, base 2s,
  cap 5m, `max_attempts` 5. Implemented by setting `status=pending, next_attempt_at=now()+delay`.
  The same claim query picks it up, so no separate retry queue is needed.
- **Dead letter**: when attempts are exhausted, `status=failed` with `last_error`. Visible via the list filter
  and requeue-able via the API.
- **Circuit breaker per channel** (bonus "Failure Handling"): opens after N consecutive failures and stays open
  for a cool-down. While open, claimed items are re-scheduled without spending attempts. Half-open probes one request.
- **Idempotent send**: the notification `id` is sent as an `Idempotency-Key` header to the provider. Delivery is
  at-least-once (a crash after the provider accepts but before the DB commit can resend). This is stated
  honestly in the README.
- Every attempt is written to `notification_attempts`.

## 10. Observability

- `log/slog` JSON logs. Middleware reads `X-Correlation-ID` or generates one, puts it in the context and
  echoes it in the response. The ID is stored on the notification row so worker logs for that message carry the same ID.
- Prometheus metrics: `notifications_created_total{channel}`, `notifications_sent_total{channel}`,
  `notifications_failed_total{channel,reason}`, `delivery_latency_seconds{channel}` histogram,
  `queue_depth{channel,priority}` gauge (refreshed every few seconds from a `count(*) group by`),
  `circuit_state{channel}`, HTTP request metrics.
- `/health` (process up), `/ready` (DB reachable).

## 11. Testing

- Unit: validation, retry classification/backoff, circuit breaker, rate limiter behaviour.
- Integration: store + worker against real Postgres via **testcontainers-go**. Provider is an `httptest.Server`
  that can fail on demand: covers priority order, retry→sent, retry→failed, cancel, idempotency, batch.
- API handler tests with `httptest`.
- One command: `make test` (= `go test -race ./...`; needs Docker running).

## 12. Delivery plan (commit-sized phases)

0. Tooling: install Go, Docker (Docker Desktop or Colima), `gh`, golangci-lint; `gh auth login`; create public repo.
1. Skeleton: go mod, layout, config, slog, compose with Postgres, migration 0001.
2. Domain + validation (+ tests).
3. Store layer + create/get/list/cancel/batch/idempotency (+ integration tests).
4. API handlers + middleware + OpenAPI + Swagger UI.
5. Worker: claim loop, rate limiter, provider client, mock provider.
6. Retry/backoff, dead letter, circuit breaker, lease reaper (+ tests).
7. Metrics + health.
8. Scheduled notifications (mostly free via `next_attempt_at`) + requeue.
9. CI workflow, README (architecture, API curl examples, design decisions, scaling path), final polish.
10. (If time) Template system.

## 13. Calls left to the builder

Exact package-internal naming, the batch size of claims, pool sizes, and the circuit breaker thresholds
(start: 5 failures / 30s), as long as they are documented in config with sane defaults.
