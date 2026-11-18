# Event-Driven Notification System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go notification service (API + queue workers) that accepts SMS/Email/Push notifications, delivers them through a webhook provider with priority, rate limiting, retry and observability. Runs with `docker-compose up` and tests run with `make test`.

**Architecture:** A single binary `notifier` with subcommands `api`, `worker`, `migrate`. PostgreSQL is both the store and the queue: workers claim rows with `FOR UPDATE SKIP LOCKED` in priority order, and retries and scheduling are expressed through `next_attempt_at`. A local mock provider stands in for webhook.site by default.

**Tech Stack:** Go 1.23+, stdlib `net/http` (method routing), `log/slog`, `jackc/pgx/v5` (pgxpool), `golang-migrate/migrate/v4` (iofs + pgx5 driver), `golang.org/x/time/rate`, `prometheus/client_golang`, `google/uuid`, `testcontainers-go` (postgres module), `stretchr/testify`, golangci-lint, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-11-18-notification-system-design.md`

## Global Constraints

- Module path: `github.com/Yigidosantos9/insider-notification-system` (`<gh-user>` known after Task 0).
- Channels: `sms`, `email`, `push`. Priorities: `high`, `normal` (default), `low`. Statuses: `pending`, `processing`, `sent`, `failed`, `cancelled`.
- Rate limit: 100 msg/s per channel (token bucket, burst 100).
- Batch size: 1–1000 items; all-or-nothing validation.
- Content limits: SMS ≤ 160 chars (runes), push ≤ 256, email ≤ 10,000. SMS recipient E.164 `^\+[1-9]\d{7,14}$`. Email recipient must parse with `net/mail.ParseAddress` and equal the bare address. Push recipient non-empty, ≤ 512.
- Retry: max_attempts 5, base 2s, cap 5m, full jitter. Retryable = network error, timeout, 5xx, 429 (honour `Retry-After` seconds). Other 4xx = permanent. Any 2xx = sent.
- Provider request: `POST {PROVIDER_URL}` body `{"to","channel","content"}`, header `Idempotency-Key: <notification id>`, client timeout 5s.
- Breaker defaults: open after 5 consecutive failures, cool-down 30s, one half-open probe.
- Worker lease 30s. Reaper every 10s.
- Error JSON shape everywhere: `{"error":{"code":"...","message":"...","details":[{"field":"...","message":"..."}]}}`.
- Pagination: `limit` default 20, max 100 (clamped). Opaque `cursor`, ordered by `created_at DESC, id DESC`.
- Correlation header: `X-Correlation-ID` (generated as UUID if absent, echoed on every response, stored on the notification).
- All config from env with defaults (see Task 1). No secrets in repo.
- Every task ends with a commit whose message follows Conventional Commits and ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Concurrent duplicate `Idempotency-Key`**: two simultaneous creates with the same key and payload give exactly one row, and both responses carry the same id. → test in Task 3.
2. **Cancel vs. worker claim race**: cancelling a row that is already `processing` returns 409, and a `cancelled` row is never claimed or sent. → tests in Tasks 3 and 4.
3. **Bad batches**: 0 items, 1001 items, or one invalid item at index 7 → 422, nothing inserted, and the error details name `items[7].recipient`. → tests in Tasks 3 and 7.
4. **Scheduled time edge cases**: `scheduled_at` in the past → accepted and due immediately. Malformed timestamp → 400. Future → not claimed before its time. → tests in Tasks 2, 4 and 7.
5. **Provider oddities**: 202 with non-JSON body → sent (no duplicate retry). 429 with `Retry-After: 10` → next attempt ≥ 10s. Hung provider → timeout → retry. → tests in Tasks 5 and 6.

---

## File Structure

```
cmd/notifier/main.go            subcommand dispatch: api | worker | migrate
cmd/mockprovider/main.go        fake provider (env: FAIL_RATE, LATENCY_MS, RATE_429)
internal/config/config.go       env → Config
internal/logging/logging.go     slog setup, ctx helpers for correlation id
internal/domain/notification.go types, enums, NewNotification
internal/domain/validate.go     Validate, ValidationError
internal/store/store.go         Store, errors, Create/Batch/Get/List/Cancel/Requeue
internal/store/queue.go         Claim/MarkSent/MarkRetry/MarkFailed/Reschedule/Reap/QueueDepth
internal/store/cursor.go        cursor encode/decode
internal/provider/client.go     HTTP client, SendError, Classify
internal/provider/breaker.go    circuit breaker
internal/worker/retry.go        RetryPolicy
internal/worker/worker.go       Worker, per-channel loops, handle()
internal/metrics/metrics.go     Prometheus collectors + queue depth poller
internal/api/server.go          routes, NewServer
internal/api/middleware.go      correlation id, logging, recover, http metrics
internal/api/handlers.go        notification handlers
internal/api/errors.go          writeError, JSON helpers
internal/api/docs.go            embedded openapi.yaml + Swagger UI page
internal/testutil/postgres.go   testcontainers Postgres + migrations
migrations/embed.go             //go:embed *.sql
migrations/000001_init.up.sql / .down.sql
api/openapi.yaml
Dockerfile, docker-compose.yml, Makefile, .golangci.yml, .github/workflows/ci.yml, README.md, .gitignore, .env.example
scripts/smoke.sh                end-to-end curl check against running compose
```

---

### Task 0: Tooling and repository bootstrap

**Files:** Create `.gitignore`, `go.mod`

- [ ] **Step 1:** Verify tools: `go version` (≥1.23), `gh --version`, `golangci-lint --version`, `docker --version`, `docker compose version`.
- [ ] **Step 2:** `colima start --cpu 4 --memory 6`, then `docker ps` → no error. For testcontainers on Colima, export in `~/.zshrc`: `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock`, `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`, `TESTCONTAINERS_RYUK_DISABLED=true`.
- [ ] **Step 3:** The user runs `! gh auth login` (interactive). Then `gh api user -q .login` → their username.
- [ ] **Step 4:** In the project dir: `git init -b main`, `go mod init github.com/Yigidosantos9/insider-notification-system`, `.gitignore` (bin/, coverage.out, .env).
- [ ] **Step 5:** Commit spec + plan: `docs: add design spec and implementation plan`.
- [ ] **Step 6:** `gh repo create insider-notification-system --public --source . --push` (confirm with the user right before running, since it publishes).

### Task 1: Skeleton — config, logging, migrations, compose with Postgres

**Files:** Create `internal/config/config.go`, `internal/logging/logging.go`, `migrations/embed.go`, `migrations/000001_init.{up,down}.sql`, `internal/testutil/postgres.go`, `internal/testutil/postgres_test.go`, `cmd/notifier/main.go`, `Dockerfile`, `docker-compose.yml`, `Makefile`, `.env.example`

**Interfaces:**
- Produces: `config.Load() (config.Config, error)` with fields `DatabaseURL` (`DATABASE_URL`, default `postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable`), `HTTPAddr` (`HTTP_ADDR` `:8080`), `ProviderURL` (`PROVIDER_URL` `http://localhost:9090/send`), `ProviderTimeout` (5s), `RatePerSecond` (100), `WorkerConcurrency` (20), `ClaimBatch` (50), `MaxAttempts` (5), `RetryBase` (2s), `RetryCap` (5m), `BreakerThreshold` (5), `BreakerCooldown` (30s), `LogLevel` (`info`).
- Produces: `logging.New(level string) *slog.Logger` (JSON handler); `logging.WithCorrelationID(ctx, id string) context.Context`; `logging.CorrelationID(ctx) string`; `logging.FromContext(ctx, base *slog.Logger) *slog.Logger` (adds `correlation_id` attr).
- Produces: `migrations.FS embed.FS`; `migrations.Up(databaseURL string) error` (golang-migrate iofs source + `pgx5://` driver; `ErrNoChange` is not an error).
- Produces: `testutil.Postgres(t *testing.T) *pgxpool.Pool`. Starts `postgres:16-alpine` once per package (sync.Once + TestMain-friendly), runs `migrations.Up`, and `TRUNCATE notifications, notification_attempts` per call.

Schema (`000001_init.up.sql`), exact:
```sql
CREATE TYPE channel AS ENUM ('sms','email','push');
CREATE TYPE notification_status AS ENUM ('pending','processing','sent','failed','cancelled');
CREATE TABLE notifications (
  id uuid PRIMARY KEY,
  batch_id uuid,
  idempotency_key text UNIQUE,
  payload_hash text,
  correlation_id text NOT NULL DEFAULT '',
  channel channel NOT NULL,
  priority smallint NOT NULL,            -- 0 high, 1 normal, 2 low
  recipient text NOT NULL,
  content text NOT NULL,
  status notification_status NOT NULL DEFAULT 'pending',
  attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL DEFAULT 5,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  scheduled_at timestamptz,
  locked_until timestamptz,
  last_error text,
  provider_message_id text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  sent_at timestamptz
);
CREATE INDEX notifications_claim_idx ON notifications (channel, priority, next_attempt_at) WHERE status = 'pending';
CREATE INDEX notifications_batch_idx ON notifications (batch_id);
CREATE INDEX notifications_created_idx ON notifications (created_at DESC, id DESC);
CREATE INDEX notifications_processing_idx ON notifications (locked_until) WHERE status = 'processing';
CREATE TABLE notification_attempts (
  id bigserial PRIMARY KEY,
  notification_id uuid NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
  attempt_no int NOT NULL,
  status_code int,
  error text,
  latency_ms int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX notification_attempts_nid_idx ON notification_attempts (notification_id);
```
The down migration drops both tables and both types.

- [ ] **Step 1: Failing test** `TestPostgresMigrated` in `internal/testutil/postgres_test.go`: query `SELECT count(*) FROM information_schema.tables WHERE table_name IN ('notifications','notification_attempts')` → `2`.
- [ ] **Step 2:** `go test ./internal/testutil/...` → FAIL (package missing).
- [ ] **Step 3:** Implement config, logging, migrations, testutil, and `cmd/notifier/main.go` (`os.Args[1]` ∈ api|worker|migrate. For now `migrate` calls `migrations.Up`. api/worker print "not implemented" and exit 1).
- [ ] **Step 4:** `go test ./internal/testutil/...` → PASS.
- [ ] **Step 5:** Dockerfile: multi-stage `golang:1.23-alpine` build of `./cmd/notifier` and `./cmd/mockprovider` (mockprovider can be a stub `main` for now) → `gcr.io/distroless/static` or `alpine`. Compose: `postgres` (healthcheck `pg_isready`), `migrate` (`command: ["migrate"]`, depends_on postgres healthy). Makefile targets: `up` (`docker compose up --build`), `down`, `test` (`go test -race -count=1 ./...`), `lint` (`golangci-lint run`), `run-api`, `run-worker`.
- [ ] **Step 6:** `docker compose up --build migrate` → exits 0. `make test` → PASS.
- [ ] **Step 7:** Commit `chore: project skeleton with config, logging, migrations and compose`.

### Task 2: Domain types and validation

**Files:** Create `internal/domain/notification.go`, `internal/domain/validate.go`, `internal/domain/validate_test.go`

**Interfaces:**
- Produces: `type Channel string` (`ChannelSMS`, `ChannelEmail`, `ChannelPush`); `type Priority string` (`PriorityHigh`, `PriorityNormal`, `PriorityLow`) with `Rank() int16` (0/1/2) and `PriorityFromRank(int16) Priority`; `type Status string` (5 consts).
- Produces: `type NewNotification struct { Channel Channel; Priority Priority; Recipient string; Content string; IdempotencyKey string; ScheduledAt *time.Time }` with JSON tags `channel, priority, recipient, content, idempotency_key, scheduled_at`.
- Produces: `type Notification struct` mirroring the table (pointers for nullable columns; `Priority Priority`; JSON snake_case; omit `payload_hash` and `locked_until` from JSON).
- Produces: `type Attempt struct { AttemptNo int; StatusCode *int; Error string; LatencyMs int; CreatedAt time.Time }`.
- Produces: `func (n *NewNotification) Normalize()` (trim recipient; empty priority → normal); `func (n NewNotification) Validate() error` returns `*ValidationError` or nil; `type ValidationError struct { Fields []FieldError }`, `type FieldError struct { Field, Message string }`, with `Error() string`; `func ValidateBatch(items []NewNotification) error` (prefixes fields `items[i].`); consts `MaxBatchSize = 1000`, `MaxSMSLength = 160`, `MaxPushLength = 256`, `MaxEmailLength = 10000`.
- Produces: `func (n NewNotification) PayloadHash() string`: sha256 hex of `channel|priority|recipient|content|scheduled_at(RFC3339 or "")`.

- [ ] **Step 1: Failing table test** `TestValidate` with cases (field → expected error field):
  valid sms `+905551234567`/"hi" → nil; sms `05551234567` → `recipient`; sms 161×"a" → `content`; sms 160×"ğ" → nil (rune count); email `a@b.co` → nil; email `Bob <a@b.co>` → `recipient`; push "" recipient → `recipient`; push 257 chars → `content`; channel `fax` → `channel`; priority `urgent` → `priority`; empty content → `content`; scheduled_at 1h in the past → nil.
- [ ] **Step 2: Failing test** `TestValidateBatch`: 0 items → field `items`; 1001 items → field `items`; 10 valid items with item 7 bad recipient → exactly one FieldError with field `items[7].recipient`.
- [ ] **Step 3: Failing test** `TestPayloadHashStable`: same input → same hash; different content → different hash.
- [ ] **Step 4:** `go test ./internal/domain/...` → FAIL. Implement. → PASS.
- [ ] **Step 5:** Commit `feat(domain): notification types and content validation`.

### Task 3: Store — create, idempotency, batch, get, list, cancel, requeue

**Files:** Create `internal/store/store.go`, `internal/store/cursor.go`, `internal/store/store_test.go`

**Interfaces:**
- Consumes: `testutil.Postgres`, domain types.
- Produces: `store.New(pool *pgxpool.Pool) *Store`; errors `ErrNotFound`, `ErrIdempotencyConflict`, `ErrInvalidState`.
- `func (s *Store) Create(ctx context.Context, n domain.NewNotification, correlationID string, maxAttempts int) (domain.Notification, bool, error)`. The bool is "created". Uses `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`. With no row returned, select by key: same `payload_hash` → (existing, false, nil), else `ErrIdempotencyConflict`. Empty key → NULL. `next_attempt_at = COALESCE(scheduled_at, now())`.
- `func (s *Store) CreateBatch(ctx, items []domain.NewNotification, correlationID string, maxAttempts int) (uuid.UUID, []domain.Notification, error)`: one tx, `CopyFrom` or multi-row insert, shared `batch_id`. Items with idempotency keys still go through ON CONFLICT semantics (insert-per-row inside the tx is acceptable).
- `func (s *Store) Get(ctx, id uuid.UUID) (domain.Notification, error)`; `func (s *Store) Attempts(ctx, id uuid.UUID) ([]domain.Attempt, error)`.
- `func (s *Store) BatchSummary(ctx, batchID uuid.UUID) (map[domain.Status]int, error)`. `ErrNotFound` if zero rows.
- `type ListFilter struct { Status *domain.Status; Channel *domain.Channel; BatchID *uuid.UUID; From, To *time.Time; Limit int; Cursor string }`; `func (s *Store) List(ctx, f ListFilter) ([]domain.Notification, string, error)`. Returns next cursor ("" when done). Fetch `limit+1` to detect more. Filter on `created_at >= From AND created_at < To`.
- Cursor: `encodeCursor(t time.Time, id uuid.UUID) string` (base64url of `RFC3339Nano|uuid`) / `decodeCursor(string) (time.Time, uuid.UUID, error)`. A bad cursor wraps `ErrInvalidCursor`.
- `func (s *Store) Cancel(ctx, id) (domain.Notification, error)`: `UPDATE … SET status='cancelled' WHERE id=$1 AND status='pending' RETURNING *`. No row → Get. Missing → `ErrNotFound`, else `ErrInvalidState`.
- `func (s *Store) Requeue(ctx, id) (domain.Notification, error)`: only from `failed`. Sets pending, attempts=0, next_attempt_at=now(), last_error NULL.

Tests (all against `testutil.Postgres`):
- [ ] **Step 1:** `TestCreateAndGet` (created=true, status pending, priority round-trips, next_attempt_at≈now). `TestCreateScheduled` (next_attempt_at == scheduled_at).
- [ ] **Step 2:** `TestIdempotency`: same key+payload twice → same id, second created=false. Same key, different content → `ErrIdempotencyConflict`. `TestIdempotencyConcurrent`: 10 goroutines, same key → 1 row (`count(*)`), all returned ids equal.
- [ ] **Step 3:** `TestCreateBatch`: 1000 items → 1000 rows sharing batch_id, `BatchSummary` → `{pending:1000}`.
- [ ] **Step 4:** `TestListFiltersAndPagination`: 25 rows (mixed channels/status). Channel filter returns only that channel. Paging with limit 10 → pages of 10, 10, 5, then cursor "", no duplicates. Garbage cursor → `ErrInvalidCursor`.
- [ ] **Step 5:** `TestCancel`: pending → cancelled. Cancel again → `ErrInvalidState`. Unknown id → `ErrNotFound`. Row forced to `processing` via SQL → `ErrInvalidState`. `TestRequeue`: failed → pending with attempts 0. Pending → `ErrInvalidState`.
- [ ] **Step 6:** Run → FAIL. Implement. `go test -race ./internal/store/...` → PASS.
- [ ] **Step 7:** Commit `feat(store): notification persistence with idempotency, batches, listing and cancel`.

### Task 4: Store — queue operations

**Files:** Create `internal/store/queue.go`, `internal/store/queue_test.go`

**Interfaces:**
- `func (s *Store) Claim(ctx, ch domain.Channel, limit int, lease time.Duration) ([]domain.Notification, error)`: the spec §8 query (`ORDER BY priority, next_attempt_at`, `FOR UPDATE SKIP LOCKED`). Sets `status='processing', locked_until=now()+lease, updated_at=now()`. Returns rows in priority order (re-sort in Go after RETURNING, since UPDATE…RETURNING order is not guaranteed).
- `func (s *Store) MarkSent(ctx, id uuid.UUID, providerMessageID string, a domain.Attempt) error`: tx. Inserts attempt; `status='sent', attempts=attempts+1, sent_at=now(), locked_until=NULL WHERE id=$1 AND status='processing'`.
- `func (s *Store) MarkRetry(ctx, id, nextAt time.Time, a domain.Attempt) error`: attempt insert. `status='pending', attempts+1, next_attempt_at=$2, last_error=a.Error, locked_until=NULL`.
- `func (s *Store) MarkFailed(ctx, id, a domain.Attempt) error`: attempt insert. `status='failed', attempts+1, last_error, locked_until=NULL`.
- `func (s *Store) Reschedule(ctx, id, nextAt time.Time) error`: back to pending without an attempt (breaker open).
- `func (s *Store) ReapExpiredLeases(ctx) (int64, error)`: `processing` with `locked_until < now()` → pending.
- `type DepthRow struct { Channel domain.Channel; Priority domain.Priority; Count int }`; `func (s *Store) QueueDepth(ctx) ([]DepthRow, error)`: pending rows with `next_attempt_at <= now()`.

- [ ] **Step 1: Failing tests:**
  `TestClaimPriorityOrder`: insert low, normal, high (sms) → Claim(limit 3) returns high, normal, low; all now `processing`.
  `TestClaimSkipsOtherChannelsFutureAndCancelled`: email row, scheduled +1h row, cancelled row → Claim(sms) returns none.
  `TestClaimConcurrentNoDoubleClaim`: 100 rows, 5 goroutines Claim(limit 30) concurrently → union has 100 unique ids, no duplicates.
  `TestMarkTransitions`: sent → status sent, attempts 1, one attempt row. Retry → pending, next_attempt_at as given, attempts 1. Failed → failed, last_error set.
  `TestReapExpiredLeases`: Claim with lease -1s → Reap returns 1 → row pending.
  `TestQueueDepth`: 2 high sms + 1 low email pending → matching rows.
- [ ] **Step 2:** Run → FAIL. Implement. Run → PASS.
- [ ] **Step 3:** Commit `feat(store): queue claim, delivery state transitions and lease reaper`.

### Task 5: Provider client, failure classification, circuit breaker, mock provider

**Files:** Create `internal/provider/client.go`, `internal/provider/client_test.go`, `internal/provider/breaker.go`, `internal/provider/breaker_test.go`, `cmd/mockprovider/main.go`

**Interfaces:**
- `type Request struct { To, Channel, Content string }` (JSON `to, channel, content`); `type Response struct { MessageID string `json:"messageId"`; Status string; Timestamp string }`.
- `type SendError struct { StatusCode int; Retryable bool; RetryAfter time.Duration; Err error }` implementing `error` and `Unwrap`.
- `func NewClient(url string, timeout time.Duration) *Client`; `func (c *Client) Send(ctx context.Context, idempotencyKey string, r Request) (Response, error)`. On 2xx → (parsed or zero Response, nil). Otherwise `*SendError`.
- `func Classify(statusCode int, err error) bool`: true for transport error/timeout (`statusCode==0`), 429, ≥500.
- `type BreakerState string` (`closed`, `open`, `half_open`); `func NewBreaker(threshold int, cooldown time.Duration, now func() time.Time) *Breaker`; methods `Allow() bool`, `Success()`, `Failure()`, `State() BreakerState`. Half-open admits exactly one in-flight probe. A probe success → closed, a probe failure → open again. Mutex-guarded.
- Mock provider: `POST /send` → `202 {"messageId":<uuid>,"status":"accepted","timestamp":<RFC3339>}`. Env `FAIL_RATE` (0–1, returns 500), `RATE_429` (0–1, returns 429 with `Retry-After: 1`), `LATENCY_MS`, `PORT` (9090). Logs each request as JSON.

- [ ] **Step 1: Failing tests** (`httptest.Server`):
  `TestSendSuccess` (asserts method POST, body fields, `Idempotency-Key` header, parsed messageId).
  `TestSend2xxMalformedBodyIsSuccess` (202 `"oops"` → nil error).
  `TestSendClassification` table: 500 → retryable; 503 → retryable; 400 → permanent; 404 → permanent; 429 with `Retry-After: 10` → retryable, RetryAfter 10s.
  `TestSendTimeout`: handler sleeps 200ms, client timeout 50ms → `*SendError` retryable, StatusCode 0.
  `TestBreaker`: fake clock. 4 failures → still closed. 5th → open, Allow false. Advance 30s → Allow true once (half_open), second Allow false. Success → closed. Repeat to half-open, then Failure → open.
- [ ] **Step 2:** Run → FAIL. Implement. Run → PASS.
- [ ] **Step 3:** Commit `feat(provider): webhook client with failure classification, circuit breaker and mock provider`.

### Task 6: Retry policy, metrics, worker

**Files:** Create `internal/worker/retry.go`, `internal/worker/retry_test.go`, `internal/worker/worker.go`, `internal/worker/worker_test.go`, `internal/metrics/metrics.go`. Modify `cmd/notifier/main.go` (worker subcommand).

**Interfaces:**
- `type RetryPolicy struct { Base, Cap time.Duration; MaxAttempts int; Rand func() float64 }`; `func (p RetryPolicy) Delay(attempt int, retryAfter time.Duration) time.Duration`: `attempt` is the number of attempts already made, starting at 1. `delay = Rand() * min(Cap, Base * 2^(attempt-1))`, then `max(delay, retryAfter)`. `func (p RetryPolicy) Exhausted(attempts int) bool` → `attempts >= MaxAttempts`.
- `metrics.New(reg prometheus.Registerer) *Metrics` with: `Created *CounterVec{channel}`, `Sent *CounterVec{channel}`, `Failed *CounterVec{channel,reason}` (reason ∈ `permanent`, `exhausted`), `Retried *CounterVec{channel}`, `DeliveryLatency *HistogramVec{channel}`, `QueueDepth *GaugeVec{channel,priority}`, `BreakerState *GaugeVec{channel}` (0 closed / 1 half_open / 2 open), `HTTPRequests *CounterVec{method,route,code}`, `HTTPDuration *HistogramVec{method,route}`. `func (m *Metrics) RunQueueDepthPoller(ctx, src interface{ QueueDepth(context.Context) ([]store.DepthRow, error) }, every time.Duration)`. This resets the gauge each tick so drained series read 0.
- `type Queue interface { Claim(...); MarkSent(...); MarkRetry(...); MarkFailed(...); Reschedule(...); ReapExpiredLeases(...) }` (signatures from Task 4). `type Sender interface { Send(ctx, key string, r provider.Request) (provider.Response, error) }`.
- `type Config struct { Channels []domain.Channel; RatePerSecond int; Concurrency int; ClaimBatch int; Lease time.Duration; PollInterval time.Duration (200ms); ReapInterval time.Duration (10s); Retry RetryPolicy; BreakerThreshold int; BreakerCooldown time.Duration }`.
- `func New(q Queue, s Sender, cfg Config, m *metrics.Metrics, log *slog.Logger) *Worker`; `func (w *Worker) Run(ctx context.Context) error` blocks until ctx is cancelled, then drains in-flight sends (graceful shutdown).
- Per channel: one `rate.Limiter(rate.Limit(RatePerSecond), RatePerSecond)`, one `Breaker`, and a semaphore of `Concurrency`. Loop: if the semaphore has room, Claim(min(free slots, ClaimBatch)). If nothing is claimed, sleep PollInterval. For each item, `go handle`.
- `handle`: logger with `notification_id`, `channel`, `correlation_id`. If `!breaker.Allow()` → `Reschedule(now+cooldown)`. Then `limiter.Wait`, Send, and time it. Success → MarkSent, breaker.Success, metrics. Retryable and not exhausted → MarkRetry(now+Delay), breaker.Failure. Retryable and exhausted → MarkFailed (reason exhausted). Permanent → MarkFailed (reason permanent). A 4xx permanent error does **not** count as a breaker failure.

- [ ] **Step 1: Failing tests** `retry_test.go`: with `Rand=func()float64{return 1}`, Delay(1)=2s, Delay(3)=8s, Delay(20)=5m. With Rand=0.5, Delay(2)=2s. Delay(1, retryAfter=10s)=10s. Exhausted(5)=true, Exhausted(4)=false.
- [ ] **Step 2: Failing tests** `worker_test.go`, integration (real store via testutil + `httptest` provider; small Base like 10ms):
  `TestWorkerDeliversInPriorityOrder`: Concurrency 1. Insert low, high, normal before start → provider receives high, normal, low.
  `TestWorkerRetryThenSuccess`: provider 500 twice then 202 → status sent, attempts 3, 3 attempt rows.
  `TestWorkerRetryExhausted`: always 500, MaxAttempts 3 → failed, attempts 3.
  `TestWorkerPermanentFailure`: 400 → failed after 1 attempt.
  `TestWorkerRateLimit`: rate 100, 150 sms rows, fast provider → all sent, and elapsed between first and last request ≥ 400ms. 100 email rows sent concurrently don't slow sms (per-channel limiter).
  `TestWorkerNeverSendsCancelled`: insert 1 pending + 1 cancelled → provider sees only the pending id.
  `TestWorkerBreakerOpens`: threshold 2, always 500 → after the breaker opens, provider hit count stops increasing within the cooldown window. Rows go back to pending without extra attempts.
- [ ] **Step 3:** Run → FAIL. Implement. `go test -race ./internal/worker/...` → PASS.
- [ ] **Step 4:** Wire the `worker` subcommand: config → pool → store → provider client → metrics (registry + `/metrics` on `:9091` via `promhttp` for the worker process) → queue-depth poller → `Worker.Run`, with SIGINT/SIGTERM cancellation. Add `worker` + `mock-provider` to compose.
- [ ] **Step 5:** Commit `feat(worker): priority queue workers with rate limiting, retry backoff and circuit breaker`.

### Task 7: HTTP API

**Files:** Create `internal/api/server.go`, `internal/api/middleware.go`, `internal/api/handlers.go`, `internal/api/errors.go`, `internal/api/api_test.go`. Modify `cmd/notifier/main.go` (api subcommand), `docker-compose.yml`.

**Interfaces:**
- `type Store interface` with the Task 3 method signatures plus `Ping(ctx) error` (add `func (s *Store) Ping` → `pool.Ping`).
- `func NewServer(st Store, m *metrics.Metrics, reg prometheus.Gatherer, log *slog.Logger, maxAttempts int) http.Handler`.
- Routes (Go 1.22 patterns): `POST /api/v1/notifications`, `POST /api/v1/notifications/batch`, `GET /api/v1/notifications/{id}`, `GET /api/v1/notifications`, `POST /api/v1/notifications/{id}/cancel`, `POST /api/v1/notifications/{id}/requeue`, `GET /api/v1/batches/{id}`, `GET /health`, `GET /ready`, `GET /metrics`.
- Middleware chain: Recover → CorrelationID → Logging (method, path, status, duration_ms, correlation_id) → HTTP metrics (route = pattern from `r.Pattern`).
- Status codes: create 201 (idempotent replay 200), batch 201 `{batch_id, count, ids}` (`ids` only, not full objects, to keep 1000-item responses small), get 200 `{notification, attempts}`, batch get 200 `{batch_id, counts, total}` (items via `GET /notifications?batch_id=`), cancel/requeue 200, `ErrNotFound` 404 `not_found`, `ErrInvalidState` 409 `invalid_state`, `ErrIdempotencyConflict` 409 `idempotency_conflict`, `ValidationError` 422 `validation_failed`, malformed JSON / unknown fields (`DisallowUnknownFields`) / bad uuid / bad time query / bad cursor 400 `bad_request`, body > 10MB 413. `/ready` 503 when Ping fails.
- `Idempotency-Key` header overrides the body field for single create. List query params: `status, channel, batch_id, from, to` (RFC3339), `limit, cursor`. Response `{items, next_cursor}`.
- `metrics.Created` is incremented on create.

- [ ] **Step 1: Failing tests** (`httptest` + real store via testutil):
  `TestCreateNotification` (201, body has id/status pending, `X-Correlation-ID` echoed when sent and generated when absent).
  `TestCreateIdempotentReplay` (201 then 200, same id; header key works).
  `TestCreateValidation422` (bad sms → 422, details[0].field `recipient`).
  `TestCreateMalformed400` (`{` body; unknown field `foo`; `scheduled_at: "tomorrow"`).
  `TestBatch` (3 items → 201 with 3 ids. 1001 → 422. Item 7 invalid → 422 with `items[7].recipient` and DB count 0).
  `TestGetAndBatchSummary`, `TestGet404`, `TestCancel200Then409`, `TestListPagination` (limit 2 over 5 rows + next_cursor walk; limit 500 clamped to 100), `TestHealthReady`, `TestMetricsEndpoint` (contains `notifications_created_total`).
- [ ] **Step 2:** Run → FAIL. Implement. Run → PASS.
- [ ] **Step 3:** Wire the `api` subcommand (graceful shutdown, `http.Server` timeouts read 10s/write 30s). Compose service `api` on 8080 depends on migrate completed. `docker compose up --build` → `curl localhost:8080/health` 200.
- [ ] **Step 4:** Commit `feat(api): notification management REST API with validation, idempotency and pagination`.

### Task 8: OpenAPI spec, Swagger UI, end-to-end smoke

**Files:** Create `api/openapi.yaml`, `internal/api/docs.go`, `scripts/smoke.sh`. Modify `internal/api/server.go`.

- [ ] **Step 1:** Write `api/openapi.yaml` (OpenAPI 3.0.3) covering every Task 7 route, request/response schemas, error schema, `Idempotency-Key` and `X-Correlation-ID` headers, and examples. Embed it via a Go file in `api/` (`package apispec; //go:embed openapi.yaml`).
- [ ] **Step 2:** `GET /docs` serves an HTML page loading `swagger-ui-dist@5.17.14` from jsDelivr pointing at `/openapi.yaml`. `GET /openapi.yaml` serves the spec. Test `TestDocsServed` (both 200; yaml contains `openapi: 3.0.3`).
- [ ] **Step 3:** Validate the spec: `docker run --rm -v $PWD/api:/s redocly/cli lint /s/openapi.yaml` → no errors.
- [ ] **Step 4:** `scripts/smoke.sh`: against a running compose, create a single + batch(5) + scheduled(+5s) notification, poll until statuses are `sent`, cancel a scheduled one, check `/metrics`. Exit non-zero on failure. Run it after `docker compose up -d --build` → "SMOKE OK".
- [ ] **Step 5:** Commit `docs(api): OpenAPI spec, Swagger UI and smoke test`.

### Task 9: CI, README, real webhook.site check

**Files:** Create `.github/workflows/ci.yml`, `.golangci.yml`, `README.md`

- [ ] **Step 1:** `.golangci.yml` enabling `govet, staticcheck, errcheck, ineffassign, unused, gofmt, revive`. `make lint` → clean (fix findings).
- [ ] **Step 2:** CI on push/PR: jobs `lint` (golangci-lint-action) and `test` (setup-go 1.23, `go test -race -count=1 ./...`; Docker is available on ubuntu-latest for testcontainers), plus a `docker compose build` job.
- [ ] **Step 3:** README sections: overview, quick start (`docker-compose up`, URLs: API :8080, Swagger :8080/docs, worker metrics :9091, mock provider :9090), switching to webhook.site (`PROVIDER_URL`, configuring the 202 response there), architecture diagram (from spec), data model, delivery & retry design (classification table, backoff formula, DLQ/requeue, breaker, at-least-once statement), API curl examples for every endpoint, observability, testing (`make test`), configuration table, design decisions & trade-offs, scaling path (Kafka/Redis via the `Queue` interface; shared rate limiter for multiple workers), future work (WebSocket, tracing, templates if not done).
- [ ] **Step 4:** With the user's webhook.site URL: `PROVIDER_URL=… docker compose up`, send 1 notification, confirm it arrives at webhook.site and the status is `sent`.
- [ ] **Step 5:** Push. `gh run watch` → CI green.
- [ ] **Step 6:** Commit `ci: lint and test pipeline` and `docs: README` (two commits).

### Task 10 (optional, if time): Template system

**Files:** Create `migrations/000002_templates.{up,down}.sql`, `internal/store/templates.go`, `internal/api/templates.go`, tests; Modify `internal/domain` + create handlers + OpenAPI + README.

- Table `templates(id uuid pk, name text unique, channel channel, body text, created_at)`. Body uses `{{.var}}` via `text/template` with `missingkey=error`.
- `POST /api/v1/templates`, `GET /api/v1/templates/{id}`. NewNotification gains `template_id` + `variables map[string]string`, which are mutually exclusive with `content`. Content is rendered at create time and then validated with the normal limits.
- Tests: render ok, missing variable → 422 `variables`, both content and template → 422.
- Commit `feat: message templates with variable substitution`.
