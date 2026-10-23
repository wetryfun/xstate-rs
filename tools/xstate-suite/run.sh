#!/usr/bin/env bash
# Runs XState's own core test suite with the xstate-rs core (via wasm) doing
# the statechart algorithm.
#
#   tools/xstate-suite/run.sh            # all of packages/core/test
#   tools/xstate-suite/run.sh history    # files matching a filter
#   XSTATE_RS=0 tools/xstate-suite/run.sh  # the unpatched JS algorithm, for comparison
#
# The checkout lives in tools/xstate-suite/.xstate (git-ignored).
set -euo pipefail

TAG="xstate@6.0.0-alpha.70"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
CHECKOUT="$HERE/.xstate"

if [ ! -d "$CHECKOUT/.git" ]; then
  git clone --quiet --depth 1 --branch "$TAG" https://github.com/statelyai/xstate.git "$CHECKOUT"
fi

cd "$CHECKOUT"
git checkout --quiet -- packages/core/src
git apply "$HERE/xstate-core.patch"
cp "$HERE/rustBridge.ts" packages/core/src/rustBridge.ts
if [ ! -d node_modules ]; then
  pnpm install --frozen-lockfile --silent
fi

cargo build --quiet --release -p xstate-wasm --target wasm32-unknown-unknown --manifest-path "$ROOT/Cargo.toml"
WASM="$ROOT/target/wasm32-unknown-unknown/release/xstate_wasm.wasm"

if [ "${XSTATE_RS:-1}" = "0" ]; then
  unset XSTATE_RS_WASM
else
  export XSTATE_RS_WASM="$WASM"
fi

exec pnpm vitest run --project xstate "$@"
