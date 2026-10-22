#!/usr/bin/env bash
# Start/stop a local cluster of node processes.
#
#   scripts/cluster.sh start 5     # nodes 1..5 on 127.0.0.1:7001..7005
#   scripts/cluster.sh stop
#   scripts/cluster.sh clean       # stop and delete run/
#
# Extra node flags: NODE_FLAGS="--no-fsync" scripts/cluster.sh start 3
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=run

case "${1:-}" in
start)
  n=${2:-3}
  cargo build --release --bin node -q
  mkdir -p "$RUN"
  peers=""
  for i in $(seq 1 "$n"); do peers+="${peers:+,}$i=127.0.0.1:$((7000 + i))"; done
  for i in $(seq 1 "$n"); do
    ./target/release/node --id "$i" --peers "$peers" --dir "$RUN/$i" ${NODE_FLAGS:-} \
      >"$RUN/$i.log" 2>&1 &
    echo $! >"$RUN/$i.pid"
  done
  addrs=""
  for i in $(seq 1 "$n"); do addrs+="${addrs:+,}127.0.0.1:$((7000 + i))"; done
  echo "started $n nodes; client addresses: $addrs"
  ;;
stop)
  for f in "$RUN"/*.pid; do
    [ -e "$f" ] || continue
    kill "$(cat "$f")" 2>/dev/null || true
    rm -f "$f"
  done
  ;;
clean)
  "$0" stop
  rm -rf "$RUN"
  ;;
*)
  echo "usage: $0 start [n] | stop | clean" >&2
  exit 1
  ;;
esac
