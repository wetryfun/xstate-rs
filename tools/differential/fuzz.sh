#!/usr/bin/env bash
# Exploratory differential fuzzing: generates fresh random machines (fuzz.mjs)
# and actor systems (fuzz-system.mjs) with XState and replays them in Rust.
#
#   tools/differential/fuzz.sh [first-seed] [seeds] [machines-per-seed] [systems-per-seed]
#
# A failing seed's corpus is kept in target/fuzz/; replay one case with
#   XSTATE_FUZZ_CORPUS=$PWD/target/fuzz/machines-N.json XSTATE_FUZZ_CASE=seedN-I \
#     cargo test -p xstate-rt --test differential fuzz
#   XSTATE_SYSTEM_FUZZ_CORPUS=$PWD/target/fuzz/systems-N.json XSTATE_FUZZ_CASE=systemN-I \
#     cargo test -p xstate-rt --test system_differential fuzz
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
FIRST="${1:-1000}"
SEEDS="${2:-10}"
MACHINES="${3:-300}"
SYSTEMS="${4:-60}"
mkdir -p "$ROOT/target/fuzz"
test_cmd=(cargo test --quiet -p xstate-rt --manifest-path "$ROOT/Cargo.toml")
"${test_cmd[@]}" --test differential --test system_differential --no-run 2>/dev/null
failed=0
for ((seed = FIRST; seed < FIRST + SEEDS; seed++)); do
  machines="$ROOT/target/fuzz/machines-$seed.json"
  systems="$ROOT/target/fuzz/systems-$seed.json"
  node "$HERE/fuzz.mjs" --seed "$seed" --count "$MACHINES" --out "$machines" >/dev/null
  node "$HERE/fuzz-system.mjs" --seed "$seed" --count "$SYSTEMS" --out "$systems" >/dev/null
  status=ok
  if XSTATE_FUZZ_CORPUS="$machines" "${test_cmd[@]}" --test differential fuzz >/dev/null 2>&1; then
    rm "$machines"
  else
    status="FAILED machines (kept $machines)"
  fi
  if XSTATE_SYSTEM_FUZZ_CORPUS="$systems" "${test_cmd[@]}" --test system_differential fuzz >/dev/null 2>&1; then
    rm "$systems"
  else
    status="$status; FAILED systems (kept $systems)"
  fi
  echo "seed $seed: $status"
  [ "$status" = ok ] || failed=1
done
exit $failed
