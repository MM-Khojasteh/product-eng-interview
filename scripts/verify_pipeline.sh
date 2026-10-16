#!/usr/bin/env bash
# =============================================================================
# Flight pipeline — END-TO-END smoke test (multi-service edition)
#
#   1. Stack health (kafka, postgres, redis) — dumps compose logs on failure
#   2. Pre-flight probe of FlyToday's gateway (429/blocked-IP triage, headers
#      captured to .verify/, token bodies never persisted)
#   3. Worker --once       supplier → raw_flight_prices (raw bytes + headers)
#   4. Normalizer --idle   raw → canonical + tombstones, DLQ for malformed
#   5. Sink + Liveness     canonical → Postgres + Redis (TTL hash)
#   6. Assertions: DB counts + UTC timestamps; Redis hash populated + TTL
#   7. DLQ injection: garbage on the raw topic must land in scraper.dlq
#   8. Idempotency replay: reset normalizer group, replay raw, counts unchanged
#
# Usage:  bash scripts/verify_pipeline.sh      (Git Bash / WSL / Linux)
# Artifacts: .verify/*.log
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# .env (the file compose also reads): host-run steps below need the supplier
# endpoint matrix (FLYTODAY_SEARCH_PATH, ...), which lives ONLY there — never
# in the codebase. Precedence: real env wins, .env fills the gaps.
if [ -f .env ]; then set -a; . ./.env; set +a; fi

RAW_TOPIC="${RAW_TOPIC:-raw_flight_prices}"
CANONICAL_TOPIC="${CANONICAL_TOPIC:-flight-prices.canonical}"
DELTAS_TOPIC="${DELTAS_TOPIC:-flight-prices.deltas}"
DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-scraper.dlq}"
NORMALIZER_GROUP="${NORMALIZER_GROUP_ID:-flight-normalizer}"
PG_USER="${POSTGRES_USER:-flight}"
PG_DB="${POSTGRES_DB:-flights}"
ROUTE_ORIGIN="$(echo "${ROUTES:-THR-MHD}" | cut -d, -f1 | cut -d- -f1 | tr -d ' ')"
ROUTE_DEST="$(echo "${ROUTES:-THR-MHD}" | cut -d, -f1 | cut -d- -f2 | tr -d ' ')"
DEPARTURE_DATE="${DEPARTURE_DATE:-2026-11-15}"
REDIS_KEY="quote:${ROUTE_ORIGIN}-${ROUTE_DEST}:${DEPARTURE_DATE}"
LOG_DIR="${VERIFY_LOG_DIR:-.verify}"
mkdir -p "$LOG_DIR"

step()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
note()  { printf '   \033[0;33m%s\033[0m\n' "$*"; }
pass()  { printf '\n\033[1;32m[PASS]\033[0m %s\n' "$*"; }
fail()  { printf '\n\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

dump_log() { local file="$1" lines="${2:-25}"
    printf '   ---- last %s line(s) of %s ----\n' "$lines" "$file" >&2
    tail -n "$lines" "$file" >&2 || true; }
dump_compose_logs() { printf '   ---- docker compose logs --tail 25 %s ----\n' "$1" >&2
    docker compose logs --tail 25 "$1" >&2 || true; }

psql_exec() { docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" \
    -v ON_ERROR_STOP=1 -Atc "$1"; }
kafka_in()  { docker compose exec -T kafka "$@"; }
topic_count() {  # topic_count <topic> → total messages ever (sum of end offsets)
    kafka_in kafka-run-class kafka.tools.GetOffsetShell \
        --bootstrap-server localhost:29092 --topic "$1" 2>/dev/null \
        | awk -F: '{s+=$3} END {print s+0}'
}

# -----------------------------------------------------------------------------
step "1/8 — stack health (kafka, postgres, redis)"
docker compose ps kafka postgres redis
kafka_in kafka-broker-api-versions --bootstrap-server localhost:29092 > /dev/null 2>&1 \
    || { dump_compose_logs kafka; fail "kafka broker unreachable — 'docker compose up -d' healthy? (~20-40s)"; }
docker compose exec -T postgres pg_isready -U "$PG_USER" -d "$PG_DB" > /dev/null 2>&1 \
    || { dump_compose_logs postgres; fail "postgres unreachable"; }
docker compose exec -T redis redis-cli ping > /dev/null 2>&1 \
    || { dump_compose_logs redis; fail "redis unreachable"; }
note "kafka, postgres and redis are up"

# -----------------------------------------------------------------------------
step "2/8 — pre-flight probe: FlyToday gateway reachability"
if command -v curl > /dev/null 2>&1; then
    # Probe endpoint = the same token path the worker uses (FLYTODAY_TOKEN_PATH,
    # from .env — never a literal in this script). Without it, probe the base URL.
    PROBE_URL="${FLYTODAY_BASE_URL:-https://www.flytoday.ir}${FLYTODAY_TOKEN_PATH:-}"
    PROBE_UA="${SCRAPER_USER_AGENT:-Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36}"
    HTTP_CODE="$(curl -sS --max-time 20 -A "$PROBE_UA" -H 'accept: */*' \
        -D "$LOG_DIR/flytoday-probe.headers" -o "$LOG_DIR/flytoday-probe.body" \
        -w '%{http_code}' "$PROBE_URL" 2> "$LOG_DIR/flytoday-probe.curlerr" || true)"
    BODY_BYTES="$(wc -c < "$LOG_DIR/flytoday-probe.body" 2> /dev/null || echo '?')"
    rm -f "$LOG_DIR/flytoday-probe.body"   # token body is a credential — never persist
    sed -i '/^[xX]-[tT]oken:/d' "$LOG_DIR/flytoday-probe.headers" 2> /dev/null || true
    note "probe GET $PROBE_URL → HTTP ${HTTP_CODE:-no-response} (body ${BODY_BYTES} bytes; headers → $LOG_DIR/flytoday-probe.headers)"
    case "${HTTP_CODE:-000}" in
        2*)   note "gateway reachable — proceeding" ;;
        429)  note "WARNING: 429 rate-limited (dossier §3.5) — scrape may fail; raise SCRAPE_INTERVAL_SECONDS" ;;
        403|401) note "WARNING: ${HTTP_CODE} blocked (WAF/IP policy, dossier §3.5) — sustained 403s = change approach (§6/§8)" ;;
        000)  note "WARNING: no response/timeout (see $LOG_DIR/flytoday-probe.curlerr) — network egress or DNS" ;;
        *)    note "WARNING: unexpected HTTP ${HTTP_CODE} — see $LOG_DIR/flytoday-probe.headers" ;;
    esac
else
    note "curl not found — skipping probe"
fi

# -----------------------------------------------------------------------------
step "3/8 — worker: supplier → ${RAW_TOPIC} (single pass)"
if ! LOG_LEVEL="${VERIFY_WORKER_LOG_LEVEL:-DEBUG}" python -m pipelines.raw_worker --once 2>&1 | tee "$LOG_DIR/worker.log"; then
    if grep -q "BLOCKED" "$LOG_DIR/worker.log"; then
        note "DIAGNOSIS: blocked (403/auth/geo wall) — circuit breaker engaged; change approach (dossier §6/§8)"
    fi
    if grep -q "RATE-LIMITED" "$LOG_DIR/worker.log"; then
        note "DIAGNOSIS: rate-limited (429 exhausted) — backoff engaged; lower RATE_SEARCHES_PER_MINUTE"
    fi
    if grep -Eq "scrape failed|request failed" "$LOG_DIR/worker.log"; then
        note "DIAGNOSIS: supplier network/scrape failure — see worker.log"
    fi
    if grep -q "cannot reach kafka" "$LOG_DIR/worker.log"; then
        dump_compose_logs kafka
        note "DIAGNOSIS: worker could not bootstrap Kafka"
    fi
    dump_log "$LOG_DIR/worker.log" 30
    fail "worker pass failed — full log: $LOG_DIR/worker.log"
fi
SCAN_ID="$(sed -n 's/.*scan_id=\([0-9a-f]\{32\}\).*/\1/p' "$LOG_DIR/worker.log" | head -1)"
EXPECTED="$(sed -n 's/.*itineraries=\([0-9][0-9]*\).*/\1/p' "$LOG_DIR/worker.log" | head -1)"
[ -n "$SCAN_ID" ]   || { dump_log "$LOG_DIR/worker.log" 15; fail "could not parse scan_id from worker log"; }
[ -n "$EXPECTED" ]  || { dump_log "$LOG_DIR/worker.log" 15; fail "could not parse itineraries count from worker log"; }
note "scan_id=${SCAN_ID}  itineraries=${EXPECTED}"

# -----------------------------------------------------------------------------
step "4/8 — normalizer: ${RAW_TOPIC} → ${CANONICAL_TOPIC} (+tombstones/DLQ)"
python -m pipelines.normalizer --idle-exit-seconds 15 --max-messages 5000 2>&1 | tee "$LOG_DIR/normalizer.log" \
    || { dump_log "$LOG_DIR/normalizer.log" 30; dump_compose_logs kafka; fail "normalizer run failed"; }
CANONICAL_COUNT="$(sed -n 's/.*canonical=\([0-9][0-9]*\).*/\1/p' "$LOG_DIR/normalizer.log" | tail -1)"
note "canonical events produced: ${CANONICAL_COUNT:-0}"

# -----------------------------------------------------------------------------
step "5/8 — sink + liveness: canonical → Postgres + Redis"
python -m pipelines.kafka_to_db --idle-exit-seconds 15 --max-messages 5000 2>&1 | tee "$LOG_DIR/sink.log" \
    || { dump_log "$LOG_DIR/sink.log" 30; dump_compose_logs postgres
         printf '   ---- consumer group state ----\n' >&2
         kafka_in kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group flight-db-sink >&2 || true
         fail "sink run failed"; }
python -m pipelines.liveness --idle-exit-seconds 15 --max-messages 5000 2>&1 | tee "$LOG_DIR/liveness.log" \
    || { dump_log "$LOG_DIR/liveness.log" 30; dump_compose_logs redis; fail "liveness run failed"; }

# -----------------------------------------------------------------------------
step "6/8 — assertions: Postgres counts/timestamps + Redis cache"
ROWS="$(psql_exec "SELECT COUNT(*) FROM flytoday_flights WHERE scan_id = '${SCAN_ID}';")"
DISTINCT_KEYS="$(psql_exec "SELECT COUNT(DISTINCT proposal_key) FROM flytoday_flights WHERE scan_id = '${SCAN_ID}';")"
PHYSICAL_KEYS="$(psql_exec "SELECT COUNT(DISTINCT physical_flight_key) FROM flytoday_flights WHERE scan_id = '${SCAN_ID}';")"
VALID_TS="$(psql_exec "SELECT COUNT(*) FROM flytoday_flights WHERE scan_id = '${SCAN_ID}'
    AND captured_at IS NOT NULL AND dep_utc IS NOT NULL AND arr_utc IS NOT NULL;")"
SANE_ORDER="$(psql_exec "SELECT COUNT(*) FROM flytoday_flights WHERE scan_id = '${SCAN_ID}' AND arr_utc > dep_utc;")"
REDIS_HLEN="$(docker compose exec -T redis redis-cli HLEN "$REDIS_KEY")"
REDIS_TTL="$(docker compose exec -T redis redis-cli TTL "$REDIS_KEY")"

note "rows=${ROWS} distinct_proposals=${DISTINCT_KEYS} physical=${PHYSICAL_KEYS} valid_utc=${VALID_TS} arr_after_dep=${SANE_ORDER}"
note "redis ${REDIS_KEY}: HLEN=${REDIS_HLEN} TTL=${REDIS_TTL}s"
[ "$ROWS" -gt 0 ]                || { dump_log "$LOG_DIR/sink.log" 20; dump_log "$LOG_DIR/normalizer.log" 20; fail "no rows for scan ${SCAN_ID}"; }
[ "$ROWS" -eq "$DISTINCT_KEYS" ] || fail "duplicate (proposal_key, captured_at) rows — upsert broken"
[ "$VALID_TS" -eq "$ROWS" ]      || fail "$((ROWS - VALID_TS)) row(s) missing UTC timestamps"
[ "$SANE_ORDER" -eq "$ROWS" ]    || fail "some rows have arr_utc <= dep_utc — timezone handling broken"
[ "$PHYSICAL_KEYS" -gt 0 ]       || fail "physical_flight_key missing on all rows"
[ "$REDIS_HLEN" -gt 0 ] 2>/dev/null || fail "redis hash ${REDIS_KEY} empty — liveness did not write"
[ "$REDIS_TTL" -gt 0 ] 2>/dev/null || fail "redis key ${REDIS_KEY} has no TTL — liveness cache-first broken"
if [ "$ROWS" -lt "$EXPECTED" ]; then
    note "rows (${ROWS}) < itineraries (${EXPECTED}): identical proposal_keys collapsed by the upsert — expected for multi-fare-bucket flights"
fi

{   echo "# pipeline state snapshot ($(date -u '+%Y-%m-%dT%H:%M:%SZ'))"
    echo; echo "== last 5 scans =="
    docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT scan_id, supplier, COUNT(*) rows, COUNT(DISTINCT physical_flight_key) physical,
                MIN(captured_at) scan_ts, MIN(price_total_rial) min_price
         FROM flytoday_flights GROUP BY 1,2 ORDER BY scan_ts DESC LIMIT 5;"
    echo "== top physical flights (cross-supplier view) =="
    docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DB" -c \
        "SELECT physical_flight_key, airline_code, flight_no, origin, destination, dep_utc,
                COUNT(*) fare_buckets, MIN(price_total_rial) min_price
         FROM flytoday_flights GROUP BY 1,2,3,4,5,6 ORDER BY fare_buckets DESC LIMIT 10;"
    echo "== redis ${REDIS_KEY} (first 3 entries) =="
    docker compose exec -T redis redis-cli HSCAN "$REDIS_KEY" 0 COUNT 3
} > "$LOG_DIR/state-snapshot.log" 2>&1
note "state snapshot → $LOG_DIR/state-snapshot.log"

# -----------------------------------------------------------------------------
step "7/8 — DLQ injection: malformed raw message must dead-letter"
DLQ_BEFORE="$(topic_count "$DLQ_TOPIC")"
printf 'not-json{{' | kafka_in kafka-console-producer \
    --bootstrap-server localhost:29092 --topic "$RAW_TOPIC" > /dev/null 2>&1
python -m pipelines.normalizer --idle-exit-seconds 10 --max-messages 5000 2>&1 | tee "$LOG_DIR/normalizer-dlq.log" \
    || { dump_log "$LOG_DIR/normalizer-dlq.log" 20; fail "DLQ-test normalizer run failed"; }
DLQ_AFTER="$(topic_count "$DLQ_TOPIC")"
note "dlq messages: ${DLQ_BEFORE} → ${DLQ_AFTER}"
[ "$DLQ_AFTER" -gt "$DLQ_BEFORE" ] \
    || { dump_log "$LOG_DIR/normalizer-dlq.log" 20; fail "malformed raw message was NOT dead-lettered"; }

# -----------------------------------------------------------------------------
step "8/8 — idempotency replay: reset normalizer group, replay whole raw topic"
TOTAL_BEFORE="$(psql_exec "SELECT COUNT(*) FROM flytoday_flights;")"
note "total rows before replay: ${TOTAL_BEFORE}"
kafka_in kafka-consumer-groups --bootstrap-server localhost:29092 \
    --group "$NORMALIZER_GROUP" --topic "$RAW_TOPIC" \
    --reset-offsets --to-earliest --execute > "$LOG_DIR/offset-reset.log" 2>&1 \
    || { dump_log "$LOG_DIR/offset-reset.log" 10; fail "offset reset failed — is the normalizer stopped?"; }
python -m pipelines.normalizer --idle-exit-seconds 15 --max-messages 5000 2>&1 | tee "$LOG_DIR/normalizer-replay.log" \
    || { dump_log "$LOG_DIR/normalizer-replay.log" 30; fail "replay normalizer run failed"; }
python -m pipelines.kafka_to_db --idle-exit-seconds 15 --max-messages 5000 2>&1 | tee "$LOG_DIR/sink-replay.log" \
    || { dump_log "$LOG_DIR/sink-replay.log" 30; fail "replay sink run failed"; }
python -m pipelines.liveness --idle-exit-seconds 15 --max-messages 5000 > "$LOG_DIR/liveness-replay.log" 2>&1 \
    || { dump_log "$LOG_DIR/liveness-replay.log" 20; fail "replay liveness run failed"; }
TOTAL_AFTER="$(psql_exec "SELECT COUNT(*) FROM flytoday_flights;")"
note "total rows after replay:  ${TOTAL_AFTER}"
[ "$TOTAL_BEFORE" -eq "$TOTAL_AFTER" ] \
    || fail "replay changed row count (${TOTAL_BEFORE} → ${TOTAL_AFTER}) — upsert NOT idempotent"

pass "END-TO-END SMOKE TEST PASSED — worker → kafka raw → normalizer → canonical → sink+liveness (scan ${SCAN_ID}: ${ROWS} row(s)); DLQ + idempotency verified; artifacts in ${LOG_DIR}/"
