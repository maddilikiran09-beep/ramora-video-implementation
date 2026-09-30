#!/usr/bin/env bash
set -euo pipefail

PORT="${RAMORA_TEST_PORT:-6389}"
./ramora "$PORT" >/tmp/ramora-test.log 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; rm -f /tmp/ramora-test.log snapshot.db' EXIT

sleep 0.2

python3 - "$PORT" <<'PY'
import socket, sys, time
port = int(sys.argv[1])

def command(s):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as c:
        c.sendall((s + "\r\n").encode())
        return c.recv(4096).decode()

assert "+PONG" in command("PING")
assert "+OK" in command("SET smoke yes")
assert "\nyes" in command("GET smoke")
assert ":1" in command("EXISTS smoke")
assert ":1" in command("INCR counter")
assert ":0" in command("DECR counter")
assert "+OK" in command("SET temp value PX 100")
time.sleep(0.15)
assert ":0" in command("EXISTS temp")
print("smoke test passed")
PY
