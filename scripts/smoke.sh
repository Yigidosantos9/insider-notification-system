#!/usr/bin/env bash
# End-to-end smoke test against a running stack (docker compose up -d).
# Requires: curl, jq.
set -euo pipefail

API=${API:-http://localhost:8080}
WORKER_METRICS=${WORKER_METRICS:-http://localhost:9091/metrics}

fail() { echo "SMOKE FAILED: $*" >&2; exit 1; }
post() { curl -sf -X POST "$API$1" -H 'Content-Type: application/json' -d "${2:-}"; }
status_of() { curl -sf "$API/api/v1/notifications/$1" | jq -r .notification.status; }

wait_status() { # id expected timeout_s
  for _ in $(seq 1 $(($3 * 4))); do
    [[ "$(status_of "$1")" == "$2" ]] && return 0
    sleep 0.25
  done
  fail "notification $1 not $2 after $3s (is $(status_of "$1"))"
}

echo "→ health"
curl -sf "$API/health" >/dev/null || fail "api not healthy"
curl -sf "$API/ready" >/dev/null || fail "api not ready"

echo "→ single notification"
single=$(post /api/v1/notifications '{"channel":"sms","recipient":"+905551234567","content":"smoke single","priority":"high"}' | jq -r .id)
wait_status "$single" sent 10

echo "→ batch of 5"
batch=$(post /api/v1/notifications/batch '{"notifications":[
  {"channel":"sms","recipient":"+905551234567","content":"b1"},
  {"channel":"email","recipient":"user@example.com","content":"b2"},
  {"channel":"push","recipient":"device-token-1","content":"b3"},
  {"channel":"sms","recipient":"+905551234568","content":"b4","priority":"low"},
  {"channel":"email","recipient":"other@example.com","content":"b5"}]}')
batch_id=$(jq -r .batch_id <<<"$batch")
for _ in $(seq 1 40); do
  sent=$(curl -sf "$API/api/v1/batches/$batch_id" | jq -r '.counts.sent // 0')
  [[ "$sent" == "5" ]] && break
  sleep 0.25
done
[[ "$sent" == "5" ]] || fail "batch: only $sent/5 sent"

echo "→ scheduled notification (+3s)"
at=$(date -u -v+3S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+3 seconds' +%Y-%m-%dT%H:%M:%SZ)
sched=$(post /api/v1/notifications "{\"channel\":\"push\",\"recipient\":\"device-token-2\",\"content\":\"later\",\"scheduled_at\":\"$at\"}" | jq -r .id)
[[ "$(status_of "$sched")" == "pending" ]] || fail "scheduled notification delivered early"
wait_status "$sched" sent 15

echo "→ cancel a scheduled notification"
far=$(post /api/v1/notifications '{"channel":"sms","recipient":"+905551234567","content":"never","scheduled_at":"2099-01-01T00:00:00Z"}' | jq -r .id)
[[ "$(post "/api/v1/notifications/$far/cancel" | jq -r .status)" == "cancelled" ]] || fail "cancel"

echo "→ idempotency"
k1=$(curl -sf -X POST "$API/api/v1/notifications" -H 'Idempotency-Key: smoke-'"$RANDOM$RANDOM" -H 'Content-Type: application/json' \
  -d '{"channel":"sms","recipient":"+905551234567","content":"once"}' -D - -o /dev/null | head -1)
[[ "$k1" == *201* ]] || fail "idempotent create: $k1"

echo "→ metrics"
curl -sf "$API/metrics" | grep -q notifications_created_total || fail "api metrics"
curl -sf "$WORKER_METRICS" | grep -q notifications_sent_total || fail "worker metrics"

echo "SMOKE OK"
