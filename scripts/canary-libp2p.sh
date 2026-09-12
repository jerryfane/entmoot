#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d)"
founder_pid=""
member_pid=""
tail_pid=""
cleanup() {
  status=$?
  for pid in "$tail_pid" "$member_pid" "$founder_pid"; do
    [[ -n "$pid" ]] && kill -TERM "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if (( status != 0 )); then
    for log in "$work"/*.log; do
      [[ -f "$log" ]] && { echo "== $log ==" >&2; cat "$log" >&2; }
    done
  fi
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT

if [[ -n "${ENTMOOTD:-}" ]]; then
  bin="$ENTMOOTD"
else
  bin="$work/entmootd"
  (cd "$repo_root/src" && go build -o "$bin" ./cmd/entmootd)
fi
python="${PYTHON:-python3}"
port_a="${ENTMOOT_CANARY_PORT_A:-$($python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')}"
port_b="${ENTMOOT_CANARY_PORT_B:-$($python -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')}"
data_a="$work/founder"
data_b="$work/member"
mkdir -p "$data_a" "$data_b"
base_a=("$bin" -data "$data_a" -identity "$data_a/identity.json" -listen-port "$port_a")
base_b=("$bin" -data "$data_b" -identity "$data_b/identity.json" -listen-port "$port_b")

json_field() {
  "$python" -c 'import json,sys; value=json.loads(sys.argv[1]);
for key in sys.argv[2].split("."): value=value[key]
print(value)' "$1" "$2"
}
wait_for() {
  local file="$1" pattern="$2" deadline=$((SECONDS + 30))
  until grep -Fq "$pattern" "$file" 2>/dev/null; do
    (( SECONDS < deadline )) || { cat "$file" >&2; return 1; }
    sleep 0.2
  done
}
wait_running() {
  local -n command_ref=$1
  local deadline=$((SECONDS + 30))
  until "${command_ref[@]}" info 2>/dev/null | grep -Fq '"running":true'; do
    (( SECONDS < deadline )) || return 1
    sleep 0.2
  done
}

group_json="$("${base_a[@]}" -allow-new-identity group create -name libp2p-canary -policy none -json)"
group_id="$(json_field "$group_json" group_id)"
founder_peer="$(json_field "$group_json" founder.peer_id)"
member_json="$("${base_b[@]}" -allow-new-identity info)"
member_pubkey="$(json_field "$member_json" entmoot_pubkey)"

"${base_a[@]}" serve >"$work/founder.log" 2>&1 &
founder_pid=$!
wait_running base_a
invite_json="$("${base_a[@]}" invite create -group "$group_id" -target-pubkey "$member_pubkey" -bootstrap "/ip4/127.0.0.1/tcp/$port_a/p2p/$founder_peer")"
printf '%s\n' "$invite_json" >"$work/invite.json"

"${base_b[@]}" join --serve "$work/invite.json" >"$work/member.log" 2>&1 &
member_pid=$!
wait_running base_b
kill -TERM "$member_pid" "$founder_pid"
wait "$member_pid" 2>/dev/null || true
wait "$founder_pid" 2>/dev/null || true
member_pid=""
founder_pid=""
sleep 1
"${base_a[@]}" serve >"$work/founder-restart.log" 2>&1 &
founder_pid=$!
wait_running base_a
"${base_b[@]}" serve >"$work/member-restart-initial.log" 2>&1 &
member_pid=$!
wait_running base_b
deadline=$((SECONDS + 30))
until "${base_b[@]}" query -group "$group_id" -topic canary/backfill -limit 10 2>/dev/null | grep -Fq libp2p-backfill; do
  "${base_a[@]}" publish -group "$group_id" -topic canary/backfill -content libp2p-backfill >/dev/null
  (( SECONDS < deadline )) || exit 1
  sleep 2
done
"${base_b[@]}" tail -group "$group_id" -topic 'canary/#' -n -1 >"$work/tail.log" 2>&1 &
tail_pid=$!
wait_for "$work/tail.log" libp2p-backfill
sleep 1
deadline=$((SECONDS + 30))
until grep -Fq libp2p-live "$work/tail.log" 2>/dev/null; do
  "${base_a[@]}" publish -group "$group_id" -topic canary/live -content libp2p-live >/dev/null
  (( SECONDS < deadline )) || { cat "$work/tail.log" >&2; exit 1; }
  sleep 2
done
"${base_b[@]}" query -group "$group_id" -topic canary/live -limit 10 | grep -Fq libp2p-live

kill -TERM "$tail_pid" "$member_pid"
wait "$tail_pid" 2>/dev/null || true
wait "$member_pid" 2>/dev/null || true
tail_pid=""
member_pid=""
"${base_a[@]}" publish -group "$group_id" -topic canary/catchup -content libp2p-offline >/dev/null
"${base_b[@]}" serve >"$work/member-restart.log" 2>&1 &
member_pid=$!
wait_running base_b
deadline=$((SECONDS + 35))
until "${base_b[@]}" query -group "$group_id" -topic canary/catchup -limit 10 2>/dev/null | grep -Fq libp2p-offline; do
  (( SECONDS < deadline )) || { cat "$work/member-restart.log" >&2; exit 1; }
  sleep 0.5
done

printf 'libp2p canary passed: fresh identity, create, targeted invite/enroll, publish/query/subscribe, restart/catchup\n'
