#!/bin/sh
# Builds the addon and copies it to target/release/xstate_node.node, where
# Node can `require()` it. Extra arguments go to cargo.
set -eu
root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
cargo build -p xstate-node --release "$@"
target=${CARGO_TARGET_DIR:-$root/target}/release
case "$(uname -s)" in
  Darwin) lib=libxstate_node.dylib ;;
  MINGW* | MSYS* | CYGWIN*) lib=xstate_node.dll ;;
  *) lib=libxstate_node.so ;;
esac
cp "$target/$lib" "$target/xstate_node.node"
echo "$target/xstate_node.node"
