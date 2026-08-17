#!/usr/bin/env bash
#
# Seasonal traffic generator for the griddog flows.
#
# Fires POST requests at a target rate that changes each "season" (e.g. 5 rps for 30s,
# then 2 rps for 30s, then 7 rps ...), cycling through the seasons until you Ctrl+C.
# Dependency-free: just curl. Sequential fire + fractional sleep — accurate enough for
# the low rates here (request latency is a few ms vs. 100s of ms between requests).
#
# Usage:
#   scripts/poll-rabbitmq.sh                       # default: RabbitMQ flow, default seasons
#   FLOW=mqtt scripts/poll-rabbitmq.sh             # hit the MQTT flow instead
#   SEASONS="10:60 3:60 15:30" scripts/poll-rabbitmq.sh   # custom "rps:seconds" seasons
#   CYCLES=3 scripts/poll-rabbitmq.sh              # stop after 3 full cycles (0 = forever)
#   GATEWAY=http://localhost:8080 scripts/poll-rabbitmq.sh
#
set -u

GATEWAY="${GATEWAY:-http://localhost:8080}"
FLOW="${FLOW:-rabbitmq}"                       # rabbitmq | mqtt | http
URL="$GATEWAY/api/${FLOW}-call"

# Seasons as space-separated "rps:seconds". Edit here or override via $SEASONS.
SEASONS="${SEASONS:-5:30 2:30 7:30 3:20 10:15 1:25}"
CYCLES="${CYCLES:-0}"                           # 0 = loop forever

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
  printf 'requests: %d   ok: %d   fail: %d   elapsed: %ds   avg: %s rps\n' \
    "$total" "$ok" "$fail" "$elapsed" "$avg"
  exit 0
}
trap summary INT TERM

fire() {
  local value=$(( (RANDOM % 100) + 1 ))
  local code
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
    -X POST "$URL" -H 'Content-Type: application/json' -d "{\"value\":$value}" 2>/dev/null)
  total=$((total + 1))
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
  printf '           -> sent %d this season   (running total: %d, ok=%d, fail=%d)\n' \
    "$((total - season_start))" "$total" "$ok" "$fail"
}

printf 'polling %s\n' "$URL"
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
