# Ramora — Video Reconstruction

A clean-room C implementation reconstructed from the **uploaded Ramora demo video**.

> **Important:** The video demonstrates the behavior and architecture of Ramora, but it does not expose the complete original source tree. This repository therefore implements the demonstrated features rather than claiming to be the original author's source code.

## Demonstrated features implemented

- Single-threaded Linux TCP server using `epoll`
- In-memory key/value store
- `SET`, `GET`, `DEL`
- `EXISTS`, `INCR`, `DECR`
- TTL with `EX` (seconds) and `PX` (milliseconds)
- Expiration min-heap
- `KEYS pattern` with simple glob-style matching
- `INFO` / `STATS`
- Binary snapshot persistence with `SAVE` / `LOAD`
- Multiple simultaneous clients
- Line-oriented text protocol

## Build

```bash
make
```

Requires Linux and a C compiler with POSIX networking support.

## Run

```./ramora
```

Default address: `127.0.0.1:6379`

Custom port:

```./ramora 6380
```

Connect with netcat:

```nc 127.0.0.1 6379
```

## Example session

```text
PING
+PONG

SET user:1 A
+OK

SET user:2 B
+OK

GET user:1
$1
A

EXISTS user:1
:1

SET temp 42 EX 2
+OK

EXISTS temp
:1

KEYS user:*
*2
user:1
user:2

INCR counter
:1

SAVE snapshot.db
+OK

INFO
# keys=4 hits=1 misses=0 clients=1 uptime=...
```

## Persistence

The `SAVE` command writes a portable snapshot containing non-expired string values and their remaining TTL. `LOAD` replaces the current dataset with the snapshot.

## Architecture

```
client
  |
TCP socket
  |
epoll_wait()
  |
read command
  |
parse
  |
dispatch
  +--> hash table
  +--> TTL min-heap
  +--> statistics
  +--> persistence
```

## Scope

This is intended as a practical reconstruction of the functionality shown in the uploaded video. It is not presented as the original Ramora repository or as a byte-for-byte reproduction of code that was not visible in the video.
