#!/usr/bin/env bash
# End-to-end check of the local stack: MQTT -> Redpanda Connect -> Redpanda
# -> xstate-kafka -> output topic.
#
#   ./smoke.sh          build and start the stack, then check it
#   ./smoke.sh --no-up  check a stack that is already running
#
# Each run uses fresh device ids, so it can be repeated on the same stack.
set -euo pipefail
cd "$(dirname "$0")"

up=1
case "${1:-}" in
  --no-up) up=0 ;;
  "") ;;
  *) echo "usage: $0 [--no-up]" >&2; exit 2 ;;
esac

run=$(date +%s)
d1="d1-$run"
d2="d2-$run"
work=$(mktemp -d)
trap 'kill "${consumer:-0}" 2>/dev/null || true; rm -rf "$work"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
step() { echo "--- $*"; }
dc() { docker compose "$@"; }

# Waits up to $1 seconds for the command after it to succeed.
wait_for() {
  local timeout=$1; shift
  local deadline=$((SECONDS + timeout))
  until "$@" > /dev/null 2>&1; do
    ((SECONDS < deadline)) || return 1
    sleep 1
  done
}

if ((up)); then
  step "starting the stack (the first build compiles librdkafka and takes a few minutes)"
  dc up -d --build
fi

step "waiting for the bridge and the service"
wait_for 120 curl -fsS http://localhost:4195/ready || fail "Redpanda Connect is not ready (docker compose logs connect)"
wait_for 120 curl -fsS http://localhost:9464/metrics || fail "xstate-kafka metrics are not up (docker compose logs xstate)"
duplicates_before=$(curl -fsS http://localhost:9464/metrics | awk '/^xstate_duplicates_total /{print $2}')

step "reading the output topic from its end"
# Committed records only: the service writes in transactions.
dc exec -T redpanda timeout 120 rpk topic consume sensor-out -o end --read-committed -f '%k %v\n' > "$work/out" &
consumer=$!
sleep 3

publish() { dc exec -T mosquitto mosquitto_pub -h localhost -q 1 -t "devices/$1/events" -m "$2"; }

step "publishing (with a redelivered and a stale event)"
publish "$d1" '{"type":"reading.high","value":91,"seq":1}'
publish "$d1" '{"type":"reading.normal","value":20,"seq":2}'
publish "$d1" '{"type":"reading.high","value":91,"seq":1}' # redelivered: skipped
publish "$d2" '{"type":"reading.high","value":95,"seq":5,"boot":1}'
publish "$d2" '{"type":"reading.normal","value":21,"seq":1,"boot":2}' # rebooted: taken
publish "$d2" '{"type":"reading.high","value":99,"seq":9,"boot":1}' # from before the reboot: skipped

expected="$d1 alarm.raised
$d1 alarm.cleared
$d2 alarm.raised
$d2 alarm.cleared"

# The emitted event types of this run, per key in order (device.offline
# follows 30 s after the last reading; it is not part of the check).
ours() {
  grep -E "^($d1|$d2) " "$work/out" \
    | sed -E 's/^([^ ]+) .*"type":"([^"]+)".*/\1 \2/' \
    | grep -v ' device\.offline$' \
    | sort -s -k1,1 || true
}

step "waiting for the emitted events"
deadline=$((SECONDS + 60))
while [[ "$(ours | wc -l)" -lt 4 ]] && ((SECONDS < deadline)); do sleep 1; done
got=$(ours)
if [[ "$got" != "$expected" ]]; then
  echo "expected:" >&2; echo "$expected" >&2
  echo "got:" >&2; echo "$got" >&2
  echo "raw output:" >&2; cat "$work/out" >&2
  fail "unexpected output events"
fi
echo "$got"

step "checking the dead-letter topic"
# rpk keeps consuming until stopped: give it a few seconds to read everything.
dc exec -T redpanda timeout 10 rpk topic consume sensor-dead -o start -f '%k %h\n' > "$work/dead" || true
if grep -E "^($d1|$d2) " "$work/dead"; then fail "dead letters for this run"; fi

step "checking xstate_duplicates_total"
# Counters move once the transaction's commit call returns.
deadline=$((SECONDS + 30))
while :; do
  duplicates=$(curl -fsS http://localhost:9464/metrics | awk '/^xstate_duplicates_total /{print $2}')
  (( duplicates - ${duplicates_before:-0} >= 2 )) && break
  ((SECONDS < deadline)) || fail "xstate_duplicates_total went from ${duplicates_before:-0} to $duplicates, expected +2"
  sleep 1
done
echo "xstate_duplicates_total $duplicates"

echo "OK"
