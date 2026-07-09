#!/usr/bin/env bash
# demo.sh - end-to-end demonstration of the HKVC cluster.
#
# It builds the cluster launcher and the hkvcctl CLI, starts a 3-node
# single-group cluster, drives it through a series of operations (set/get/
# create/ls/stat/rm), shows the /metrics endpoint, then shuts down.
#
# Requires Go on PATH. Run from the repo root:  ./demo.sh
set -euo pipefail

BASE=${BASE:-17700}
N=${N:-3}
ADDRS="localhost:$BASE,localhost:$((BASE+3)),localhost:$((BASE+6))"
BIN_DIR="$(mktemp -d)"
trap 'rm -rf "$BIN_DIR"' EXIT

echo "==> building hkvc-cluster and hkvcctl"
( cd hkvc && go build -o "$BIN_DIR/hkvc-cluster" ./cmd/hkvc-cluster )
( cd hkvcctl && go build -o "$BIN_DIR/hkvcctl" . )

echo "==> starting a $N-node cluster on base port $BASE"
"$BIN_DIR/hkvc-cluster" -n "$N" -base "$BASE" >"$BIN_DIR/cluster.log" 2>&1 &
CLUSTER_PID=$!
trap 'kill $CLUSTER_PID 2>/dev/null || true; rm -rf "$BIN_DIR"' EXIT
sleep 5

ctl() { "$BIN_DIR/hkvcctl" -addrs "$ADDRS" "$@"; }

echo "==> set / get"
ctl set / hello world
echo -n "get / hello -> "; ctl get / hello

echo "==> hierarchical namespace"
ctl create / config
ctl set /config timeout 30s
ctl set /config retries 5
echo "ls / :"; ctl ls / | sed 's/^/    /'
echo "ls /config :"; ctl ls /config | sed 's/^/    /'

echo "==> overwrite (get is leader-served and linearizable)"
ctl set / hello WORLD >/dev/null
echo -n "get / hello -> "; ctl get / hello   # authoritative: reflects the overwrite

echo "==> metadata (stat/get_metadata is a relaxed read served by any replica,"
echo "    so its version may briefly lag the leader):"
ctl stat / hello | sed 's/^/    /'

echo "==> delete"
ctl rm / hello
echo "ls / after delete:"; ctl ls / | sed 's/^/    /'

echo "==> a slice of the leader's /metrics"
ctl metrics | grep -E 'hkvc_(requests_total|commits_total|raft_is_leader|raft_commit_index)' | head -12

echo "==> done; shutting down"
