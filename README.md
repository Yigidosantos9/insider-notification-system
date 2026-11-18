# Event-Driven Notification System

[![CI](https://github.com/Yigidosantos9/insider-notification-system/actions/workflows/ci.yml/badge.svg)](https://github.com/Yigidosantos9/insider-notification-system/actions/workflows/ci.yml)

A Go service that accepts SMS, Email and Push notifications through a REST API and delivers them asynchronously through an external provider. Delivery is ordered by priority, rate limited per channel, retried with exponential backoff, and protected by a per-channel circuit breaker. Delivery status can be tracked at any time.

Built for the Insider One Software Engineer Assessment.

---

## Quick start

```bash
docker-compose up --build      # or: make up
```

| What | URL |
|---|---|
| REST API | http://localhost:8080/api/v1 |
| Swagger UI | http://localhost:8080/docs |
| OpenAPI spec | http://localhost:8080/openapi.yaml ([source](api/openapi.yaml)) |
| Metrics summary (JSON) | http://localhost:8080/api/v1/metrics/summary?window=5m |
| Prometheus metrics / health / readiness | http://localhost:8080/metrics · `/health` · `/ready` |
| Worker metrics / health | http://localhost:9091/metrics · `/health` |
| Mock provider | http://localhost:9090/send |

![Swagger UI listing every endpoint](docs/images/swagger-overview.png)

Send a notification and watch it get delivered:

```bash
curl -s -X POST localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"Hello from Insider!","priority":"high"}'

curl -s localhost:8080/api/v1/notifications/<id> | jq   # status: pending → sent
```

A delivered notification as returned by `GET /api/v1/notifications/{id}`, with its delivery attempts, correlation ID and provider message ID:

![GET notification response in Swagger UI](docs/images/swagger-get.png)

End-to-end check against the running stack: `make smoke`. The database is not published on the host (so it can't clash with a local PostgreSQL); inspect it with `docker compose exec postgres psql -U notifier`.

### Using webhook.site as the provider

By default the stack uses a bundled **mock provider**, because webhook.site's free tier limits how many requests a URL accepts, and load or retry tests would use that up quickly. To deliver to webhook.site instead:

1. Create a URL on https://webhook.site and edit its response: status **202**, body
   `{"messageId": "$request.uuid$", "status": "accepted", "timestamp": "$request.date$"}` (or any static JSON).
2. Start the stack pointing at it:
   ```bash
   PROVIDER_URL=https://webhook.site/<your-uuid> docker-compose up --build
   ```

Each delivery is a `POST` with body `{"to", "channel", "content"}` and an `Idempotency-Key` header holding the notification ID.

A delivery received by webhook.site (source IP and URL masked):

![Request received by webhook.site](docs/images/webhook-site.png)

---

## Architecture

```
            ┌────────────┐   INSERT (tx)    ┌──────────────────┐
 client ──▶ │    api     │ ───────────────▶ │    PostgreSQL    │
            └────────────┘ ◀── queries ──── │  notifications   │ ◀── store + queue
                                            │  attempts        │
                                            └────────┬─────────┘
                       claim: FOR UPDATE SKIP LOCKED │ ORDER BY priority, next_attempt_at
                                                     ▼
            ┌──────────────────────────────────────────────────────────┐
            │ worker   (one independent loop per channel: sms/email/push)│
            │  token bucket 100/s ─▶ circuit breaker ─▶ HTTP send (5s) │
            │  ─▶ sent │ retry later (backoff) │ failed (dead letter)  │
            │  lease reaper: recovers rows from crashed workers        │
            └──────────────────────────────────────────────────────────┘
                                                     ▼
                                   mock-provider  /  webhook.site
```

A single binary, `notifier`, runs in three modes: `api`, `worker`, `migrate`. Docker Compose starts `postgres`, `migrate` (runs once and exits), `api`, `worker` and `mock-provider`. The API and the worker scale independently.

### Why PostgreSQL is both the store and the queue

The `notifications` table is the queue. Workers claim work with:

```sql
UPDATE notifications SET status='processing', locked_until=now()+'30s'
WHERE id IN (SELECT id FROM notifications
             WHERE channel=$1 AND status='pending' AND next_attempt_at <= now()
             ORDER BY priority, next_attempt_at LIMIT $2
             FOR UPDATE SKIP LOCKED)
RETURNING ...
```

- **No dual write.** Creating a notification *is* enqueuing it, in one transaction. There is no window where a row exists but its message was never published (or the reverse), and no outbox is needed.
- **Priority, scheduling and retry delays come from one indexed query.** A scheduled notification and a retry waiting out its backoff are both rows whose `next_attempt_at` is in the future.
- **Safe concurrency.** `SKIP LOCKED` lets any number of workers claim at once without handing a row out twice.
- **Crash recovery.** A claimed row carries a 30s lease. If its worker dies, the reaper puts it back in the queue.

The trade-off is a throughput ceiling. PostgreSQL handles thousands of claims per second, comfortably above "millions per day" (about 12/s on average, with bursts absorbed by the queue). See [Scaling path](#scaling-path) for the next step.

### Project layout

```
cmd/notifier/        entrypoint: api | worker | migrate
cmd/mockprovider/    local fake provider with failure injection
internal/domain/     types, statuses, validation rules
internal/store/      PostgreSQL repository (store.go), queue operations (queue.go), templates, metrics summary
internal/worker/     per-channel delivery loops, retry policy
internal/provider/   HTTP client, failure classification, circuit breaker
internal/api/        handlers (incl. templates), middleware (correlation id, logging, metrics, recover)
internal/metrics/    Prometheus collectors
migrations/          versioned SQL (golang-migrate), embedded in the binary
api/openapi.yaml     OpenAPI 3 specification
```

---

## Processing engine

| Requirement | How |
|---|---|
| Async queue workers | `worker` claims due rows per channel and sends them on a bounded goroutine pool (`WORKER_CONCURRENCY`, default 20 per channel) |
| **100 msg/s per channel** | One token bucket (`golang.org/x/time/rate`, 100/s, burst 1) per channel, so sends are spaced 10ms apart and **no 1-second window ever exceeds 100**. A larger burst would let up to 200 through in the first second. A token is taken before every send, and channels never share a bucket (verified by a sliding-window test) |
| Priority (high/normal/low) | Stored as 0/1/2 and claimed `ORDER BY priority, next_attempt_at`. Strict priority: high drains first |
| Content validation | Per channel: SMS recipient in E.164 and ≤ 160 chars; email a plain address and ≤ 10,000 chars; push a device token and ≤ 256 chars. All errors are returned together |
| Idempotency | `Idempotency-Key` header (or `idempotency_key` field) has a unique index. A replay returns the original with `200`; the same key with a different payload returns `409`. Safe under concurrent duplicates (`INSERT … ON CONFLICT DO NOTHING`) |
| Scheduling (bonus) | `scheduled_at` sets `next_attempt_at`. The row is not claimed before then and can be cancelled while pending |
| Templates (bonus) | `POST /templates` stores a Go-template body (`Hi {{.name}}`). A notification with `template_id` + `variables` gets its content rendered at creation time and then validated like any other content (e.g. the SMS 160-character limit). A missing variable returns 422 rather than an empty substitution |

## Delivery & retry design

Each attempt is recorded in `notification_attempts` (status code, error, latency) and returned by `GET /notifications/{id}`.

**1. Classify the outcome**

| Provider result | Meaning | Action |
|---|---|---|
| any `2xx` | accepted | `sent`. A 2xx with an unreadable body still counts as sent, since retrying would duplicate the message |
| network error, timeout (5s) | transient | retry |
| `5xx` | transient | retry |
| `429` | throttled | retry, waiting **at least** `Retry-After` |
| other `4xx` | the request itself is wrong | `failed` immediately, as retrying cannot help |

**2. Back off exponentially, with full jitter**

```
delay(n) = random(0, min(cap, base · 2^(n-1)))     base = 2s, cap = 5m, max_attempts = 5 (stored per notification)
```

Jitter spreads out retries from a burst of failures (for example a provider blip during a flash sale) so they don't return in lockstep. The row goes back to `pending` with `next_attempt_at = now() + delay`, and the same claim query picks it up later, so no separate retry queue is needed. Due times always come from the **database clock**, which keeps clock skew between app hosts and the DB from reordering the queue.

**3. Dead letter.** When attempts are exhausted, the status becomes `failed` with `last_error`. Failed notifications can be listed (`?status=failed`) and sent again with `POST /notifications/{id}/requeue`, which resets the attempt budget.

**4. Circuit breaker per channel (bonus: failure handling).** After 5 consecutive transient failures the channel's breaker **opens** for 30s. While open, the worker **stops claiming** that channel's notifications. They stay queued untouched, and nothing spends an attempt, so an outage doesn't burn through everyone's retry budget. (Sends already in flight when it opens are rescheduled for after the cool-down, also without spending an attempt.) Then a single **half-open** probe is sent: success closes the circuit, failure reopens it. The state is exported as `provider_circuit_state`.

**5. Delivery guarantee: at-least-once.** If a worker crashes after the provider accepted a message but before the database commit, the lease expires and the message is sent again. The notification ID is sent as an `Idempotency-Key` header so a provider that supports it can drop the duplicate. Exactly-once delivery to a third party isn't achievable without the third party's cooperation, so this design minimises the window and makes duplicates detectable.

**Graceful shutdown.** On `SIGTERM` the worker stops claiming, lets in-flight sends finish and records their results. A row still waiting for a rate-limit token is handed back to the queue.

Try it: `MOCK_FAIL_RATE=0.5 docker-compose up` makes the mock fail half of all requests. Watch `GET /api/v1/metrics/summary` (success rate drops, dead letters appear) and `provider_circuit_state` on :9091/metrics.

---

## API examples

The full contract, with schemas and error codes, is in [Swagger UI](http://localhost:8080/docs). Every response carries `X-Correlation-ID`.

```bash
# Create (idempotent)
curl -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-1234-shipped' \
  -d '{"channel":"email","recipient":"user@example.com","content":"Your order has shipped"}'

# Schedule for later
curl -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -d '{"channel":"push","recipient":"device-token-abc","content":"Sale starts!","scheduled_at":"2026-12-01T09:00:00Z"}'

# Batch (1–1000, all-or-nothing validation)
curl -X POST localhost:8080/api/v1/notifications/batch -H 'Content-Type: application/json' \
  -d '{"notifications":[{"channel":"sms","recipient":"+905551234567","content":"Hi"},
                        {"channel":"push","recipient":"device-token-abc","content":"Breaking news","priority":"high"}]}'
# → 201 {"batch_id":"…","count":2,"ids":["…","…"]}

# Status
curl localhost:8080/api/v1/notifications/<id>          # notification + delivery attempts
curl localhost:8080/api/v1/batches/<batch_id>          # {"total":2,"counts":{"sent":2}}

# List with filters and cursor pagination
curl 'localhost:8080/api/v1/notifications?status=failed&channel=sms&from=2026-11-18T00:00:00Z&limit=50'
curl 'localhost:8080/api/v1/notifications?cursor=<next_cursor>'

# Templates
curl -X POST localhost:8080/api/v1/templates -H 'Content-Type: application/json' \
  -d '{"name":"otp","channel":"sms","body":"Hi {{.name}}, your code is {{.code}}"}'
curl -X POST localhost:8080/api/v1/notifications -H 'Content-Type: application/json' \
  -d '{"recipient":"+905551234567","template_id":"<template_id>","variables":{"name":"Ayşe","code":"1234"}}'

# Cancel a pending/scheduled notification; requeue a failed one
curl -X POST localhost:8080/api/v1/notifications/<id>/cancel
curl -X POST localhost:8080/api/v1/notifications/<id>/requeue
```

| Status | Error code | When |
|---|---|---|
| 400 | `bad_request` | malformed JSON, unknown field, bad query parameter or cursor |
| 404 | `not_found` | unknown notification or batch |
| 409 | `invalid_state` | cancel a non-pending notification, requeue a non-failed one |
| 409 | `idempotency_conflict` | idempotency key reused with a different payload |
| 409 | `template_name_taken` | template name already exists |
| 413 | `body_too_large` | body over 10MB |
| 422 | `validation_failed` | invalid fields, listed in `details` (e.g. `items[7].recipient`) |

## Observability

- **Metrics summary (JSON)**: `GET /api/v1/metrics/summary?window=5m` answers the brief's questions in one call, without needing Prometheus: queue depth per channel and priority, scheduled and processing counts, and per channel the attempts, success/failure counts, **success rate**, dead letters and **latency avg/p50/p95/p99**. It is computed from the database, so it stays correct with any number of replicas.
  ```json
  {"window":"5m0s","queue_depth":{"sms":{"high":3}},"scheduled":12,"processing":20,
   "channels":{"sms":{"attempts":1000,"succeeded":982,"failed":18,"success_rate":0.982,
     "dead_lettered":2,"latency":{"avg_ms":24,"p50_ms":21,"p95_ms":48,"p99_ms":95}}}}
  ```

  Live output after a mixed batch:

  ![Metrics summary response](docs/images/metrics-summary.png)
- **Metrics (Prometheus).**
  - API `:8080/metrics`: `notifications_created_total`, `notification_queue_depth{channel,priority}`, `http_requests_total`, `http_request_duration_seconds`.
  - Worker `:9091/metrics`: `notifications_sent_total`, `notifications_failed_total{reason}`, `notifications_retried_total`, `notification_delivery_latency_seconds`, `notification_queue_depth`, `provider_circuit_state`.
  - Success and failure rates are ratios of these counters, e.g. `rate(notifications_failed_total[5m]) / rate(notifications_sent_total[5m])`.
- **Structured logs.** JSON via `log/slog`. The API reads or generates `X-Correlation-ID`, logs it on every request and stores it on each notification it creates. Worker logs for that notification (`sent`, `will retry`, `failed`) carry the same `correlation_id`, so one ID traces a request from HTTP call to provider delivery.
- **Health.** `/health` is liveness. `/ready` checks the database and returns 503 when it is unreachable.

## Testing

```bash
make test        # go test -race -count=1 ./...
```

Integration tests run against a real PostgreSQL started by [testcontainers](https://golang.testcontainers.org/), so Docker must be running; nothing else needs setting up. They cover priority ordering, concurrent claims (no double delivery), the per-channel rate limit, retry → success, retry exhaustion, permanent failures, the circuit breaker, lease recovery, cancellation races, idempotency under concurrency, batch validation, pagination, and every API status code. `make lint` runs golangci-lint, and `make smoke` runs an end-to-end check against the running stack.

CI (GitHub Actions) runs lint, the test suite, and a `docker compose up` + smoke test on every push.

> **macOS + Colima:** testcontainers needs `~/.testcontainers.properties` containing `docker.host=unix://$HOME/.colima/default/docker.sock`. The Makefile already sets `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE`. Docker Desktop works out of the box.

## Configuration

All settings are environment variables with defaults (see [.env.example](.env.example)). Docker Compose passes each of them through, e.g. `RETRY_BASE=200ms BREAKER_COOLDOWN=10s docker-compose up`.

| Variable | Default | Purpose |
|---|---|---|
| `DATABASE_URL` | `postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable` | PostgreSQL |
| `HTTP_ADDR` / `METRICS_ADDR` | `:8080` / `:9091` | API / worker metrics listeners |
| `PROVIDER_URL` | mock provider | delivery endpoint (webhook.site URL) |
| `PROVIDER_TIMEOUT` | `5s` | per-request timeout |
| `RATE_LIMIT_PER_SECOND` | `100` | per channel |
| `WORKER_CONCURRENCY` / `CLAIM_BATCH` | `20` / `50` | in-flight sends and claim size, per channel |
| `MAX_ATTEMPTS` | `5` | before dead-lettering |
| `RETRY_BASE` / `RETRY_CAP` | `2s` / `5m` | backoff |
| `BREAKER_THRESHOLD` / `BREAKER_COOLDOWN` | `5` / `30s` | circuit breaker |
| `LOG_LEVEL` | `info` | `debug` · `info` · `warn` · `error` |
| `MOCK_FAIL_RATE` / `MOCK_RATE_429` / `MOCK_LATENCY_MS` | `0` / `0` / `20` | mock provider failure injection (compose) |

## Design decisions & trade-offs

- **Strict priority.** High always drains first, so under sustained high-priority load, low priority can starve. That matches "breaking news before newsletters". Weighted fair queuing would be the fix if starvation became a problem.
- **The rate limit is per worker process.** Compose runs one worker, so the 100/s limit is exact. Running several worker replicas would need a shared limiter (for example Redis GCRA), see below.
- **All-or-nothing batches.** One invalid item rejects the batch, so a client never has to work out which half of a request went through.
- **The batch response returns IDs, not full objects**, to keep 1000-item responses small.
- **Cursor pagination** (`created_at, id`) instead of OFFSET: stable while new rows arrive, and fast on deep pages.
- **No authentication.** It is out of scope for the assessment.

## Scaling path

1. **More API replicas.** They are stateless; scale freely.
2. **More worker replicas.** Claiming is already safe with `SKIP LOCKED`. Move the rate limiter to Redis so the 100/s budget is shared across replicas.
3. **Beyond PostgreSQL's queue throughput.** The worker depends only on the `worker.Queue` interface, so a Kafka or RabbitMQ implementation (with an outbox table for transactional publishing) can replace the SQL claim without changing the delivery logic.
4. **Retention.** Partition `notifications` by month and archive delivered rows.

## Future work

WebSocket status updates, OpenTelemetry tracing (the correlation ID is the hook), template listing/versioning, per-tenant rate limits, and provider failover.
