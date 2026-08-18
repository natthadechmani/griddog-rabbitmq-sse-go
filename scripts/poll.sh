#!/usr/bin/env bash
#
# Seasonal traffic generator for the griddog baseline flows.
#
# Fires requests at a target rate that changes each "season" (e.g. 5 rps for 30s, then
# 2 rps for 30s, then 7 rps ...), cycling through the seasons until you Ctrl+C.
# Dependency-free: just curl.
#
# Flows (baseline app — note: no `mqtt` flow on main; that lives on feature/mqtt):
#   http      POST /api/http-call      {"value":N}   (fast request/reply)
#   rabbitmq  POST /api/rabbitmq-call   {"value":N}   (round-trip via RabbitMQ)
#   sse       GET  /api/sse-call                      (streams a 1..20 counter, ~10s/call)
#
# For `sse`, each call is a long-lived (~10s) stream, so it's fired in the BACKGROUND and
# bounded by --max-time; "rps" there means new-streams-per-second (they overlap).
#
# Usage:
#   scripts/poll.sh                                   # default: http flow, default seasons
#   FLOW=rabbitmq scripts/poll.sh                     # rabbitmq flow
#   FLOW=sse scripts/poll.sh                          # sse flow
#   SEASONS="5:10 15:10 3:20 10:15" scripts/poll.sh   # custom "rps:seconds" seasons
#   CYCLES=3 scripts/poll.sh                          # stop after 3 full cycles (0 = forever)
#   GATEWAY=http://localhost:8080 scripts/poll.sh
#
# Run all three flows at once (separate processes):
#   FLOW=http scripts/poll.sh &  FLOW=rabbitmq scripts/poll.sh &  FLOW=sse scripts/poll.sh &
#
set -u

GATEWAY="${GATEWAY:-http://localhost:8080}"
FLOW="${FLOW:-http}"                            # http | rabbitmq | sse
CYCLES="${CYCLES:-0}"                           # 0 = loop forever

# Seasons as space-separated "rps:seconds". Edit here or override via $SEASONS.
SEASONS="${SEASONS:-5:30 2:30 7:30 3:20 10:15 1:25}"

case "$FLOW" in
  http|rabbitmq) URL="$GATEWAY/api/${FLOW}-call"; METHOD=post ;;
  sse)           URL="$GATEWAY/api/sse-call";     METHOD=stream ;;
  *) echo "unknown FLOW='$FLOW' (use: http | rabbitmq | sse)" >&2; exit 2 ;;
esac

total=0; ok=0; fail=0
start_ts=$(date +%s)

summary() {
  [ -n "${_done:-}" ] && exit 0     # run once (trap + explicit call)
  _done=1
  local elapsed=$(( $(date +%s) - start_ts ))
  [ "$elapsed" -lt 1 ] && elapsed=1
  local avg
  avg=$(awk "BEGIN{printf \"%.2f\", $total/$elapsed}")
  printf '\n──────── stopped ────────\n'
  printf 'flow: %s   fired: %d   ok: %d   fail: %d   elapsed: %ds   avg: %s rps\n' \
    "$FLOW" "$total" "$ok" "$fail" "$elapsed" "$avg"
  exit 0
}
trap summary INT TERM

fire() {
  total=$((total + 1))
  if [ "$METHOD" = stream ]; then
    # long-lived SSE stream: fire-and-forget, bounded so it can't hang forever
    curl -s -o /dev/null --max-time 15 "$URL" >/dev/null 2>&1 &
    ok=$((ok + 1))   # launched (delivery not individually tracked for streams)
    return
  fi
  local value=$(( (RANDOM % 100) + 1 ))
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
    -X POST "$URL" -H 'Content-Type: application/json' -d "{\"value\":$value}" 2>/dev/null)
  if [ "$code" = "200" ]; then ok=$((ok + 1)); else fail=$((fail + 1)); fi
}

run_season() {
  local rps="$1" dur="$2"
  local interval; interval=$(awk "BEGIN{printf \"%.4f\", 1/$rps}")
  local end_ts=$(( $(date +%s) + dur ))
  local season_start=$total
  printf '[%s] season: %s rps for %ss  (interval %ss)\n' "$(date +%H:%M:%S)" "$rps" "$dur" "$interval"
  while [ "$(date +%s)" -lt "$end_ts" ]; do
    fire
    sleep "$interval"
  done
  printf '           -> fired %d this season   (running total: %d, ok=%d, fail=%d)\n' \
    "$((total - season_start))" "$total" "$ok" "$fail"
}

printf 'polling %s  (flow=%s)\n' "$URL" "$FLOW"
printf 'seasons: %s   cycles: %s   (Ctrl+C to stop)\n\n' "$SEASONS" "${CYCLES:-forever}"

cycle=0
while :; do
  for season in $SEASONS; do
    IFS=':' read -r rps sec <<< "$season"
    run_season "$rps" "$sec"
  done
  cycle=$((cycle + 1))
  if [ "$CYCLES" -ne 0 ] && [ "$cycle" -ge "$CYCLES" ]; then break; fi
done
summary
