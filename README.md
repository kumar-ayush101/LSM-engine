# LSM-engine

[![CI](https://github.com/kumar-ayush101/LSM-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/kumar-ayush101/LSM-engine/actions/workflows/ci.yml)

A log-structured merge-tree (LSM) key-value storage engine written from scratch in Go.
No storage libraries are used (no RocksDB, Pebble, Badger, or LevelDB bindings), only the Go standard library.

LSM trees power write-heavy databases such as RocksDB, Cassandra, and ScyllaDB. They turn random writes
into sequential ones: writes are buffered in a sorted in-memory table, then flushed to immutable sorted
files on disk, which are merged in the background.

## Status

| Stage | Component | State |
|---|---|---|
| W1 | Skip-list memtable, sequence numbers, tombstones, Put/Get/Delete | Done |
| W2 | Write-ahead log (CRC32C records, group commit, replay, torn-write handling, crash test) | Done |
| -- | HTTP server (bearer auth, limits, graceful shutdown) + Docker image | Done |
| W3 | SSTable format (data blocks, sparse index, footer) + background flush | Planned |
| W4 | Bloom filters, MANIFEST, merged read path | Planned |
| W5 | Size-tiered compaction (k-way merge) | Planned |
| W6 | Benchmark harness + results | Planned |

Data is durable: every write goes to the WAL before it is acknowledged, and the WAL is replayed
on startup. Until SSTables exist (W3), the whole dataset must fit in the memtable, which is capped
by `MaxMemtableBytes` (default 256 MiB); writes beyond the cap fail with `ErrMemtableFull`
(HTTP 507) rather than exhausting memory. The WAL is never truncated yet, so restart time grows
with total write volume. Both limits go away with flush to SSTables in W3.

## Architecture

```
            Put / Delete                          Get
                 |                                 |
                 v                                 v
   +---------------------------+     1. memtable (newest)
   |  WAL (append + fsync)     |     2. immutable memtable
   +---------------------------+     3. SSTables, newest first
                 |                      (bloom filter skips files
                 v                       that cannot hold the key)
   +---------------------------+
   |  memtable (skip list)     |  -- full --> immutable memtable
   +---------------------------+                    |
                                           background flush
                                                    v
   +-----------------------------------------------------------+
   |  SSTables on disk (sorted, immutable)  <-- compaction     |
   |  MANIFEST records which SSTables are live                 |
   +-----------------------------------------------------------+
```

Components marked Planned in the table above are not implemented yet; the diagram shows the target design.

## Design (implemented so far)

### Internal keys and sequence numbers (`internal/base`)

Every write is assigned a monotonically increasing 56-bit sequence number. The engine stores
an internal key `(user key, seq, kind)` where kind is `SET` or `DELETE`, packed into an 8-byte
trailer `seq<<8 | kind`.

Internal keys sort by user key ascending, then sequence number descending. Newest-first means a
lookup seeks to `(key, snapshot)` and the first entry it lands on is the newest version visible at
that snapshot, so there is no need to scan all versions.

Sequence numbers exist from day one because later stages depend on them: WAL replay, choosing the
newest version during compaction, deciding when a tombstone is safe to drop, and snapshot reads.

### Skip-list memtable (`memtable`)

The memtable is a skip list: a sorted linked list with extra randomly-built "express lanes".
Each node is promoted to the next level with probability p = 1/4, up to a max height of 12.

- Expected O(log n) search and insert, with ~1.33 pointers per node at p = 1/4 (vs 2 at p = 1/2).
- Why a skip list rather than a balanced tree: inserts never rebalance or move existing nodes, the
  code is short and easy to verify, and ordered iteration (needed for flushing to SSTables) is a
  walk along the bottom level. LevelDB and RocksDB use skip lists for the same reasons.
- Nodes are never removed or modified after insertion. Deletes are recorded as tombstones, and
  overwrites as new versions with higher sequence numbers.

### Concurrency

The memtable uses a `sync.RWMutex` rather than a lock-free skip list. Readers run in parallel;
writers take the exclusive lock. Writes are serialized by the DB anyway (to assign sequence numbers
in order), so a lock-free design would add a lot of complexity for little gain at this stage.

Because nodes are immutable once linked, a reader can release the lock after finding a node and
still read its key and value safely. Iterators take the read lock only while following a pointer,
so a long scan never blocks writers.

The DB publishes a `visibleSeq` (atomic) only after a write is fully inserted. Readers use it as
their snapshot, so they never observe a half-applied write.

### Tombstones

`Delete` writes a tombstone instead of removing anything. Once data lives in several places
(memtable, immutable memtable, SSTables), an older value for the key may exist on disk, and the
tombstone must shadow it. Lookups therefore return one of three results:

- `Found`: newest visible version is a value.
- `Deleted`: newest visible version is a tombstone. Stop searching and return not found.
- `NotFound`: this memtable knows nothing about the key. Continue to older data.

Compaction (W5) will drop a tombstone only when no older version of the key can exist below it.

### Write-ahead log (`wal`)

Every write is appended to `wal.log` before it touches the memtable. On `Open`, the log is
replayed to rebuild the memtable, and sequence numbering resumes after the last replayed record.

Record framing:

```
+-----------+-----------+-------------------+
| crc (4 B) | len (4 B) | payload (len B)   |
+-----------+-----------+-------------------+
payload = seq (8 B) | kind (1 B) | key len (uvarint) | key | value
```

- The checksum is CRC-32C (Castagnoli, hardware-accelerated on modern CPUs) over the length and the
  payload, so a corrupted length field is detected too. An all-zero header never validates.
- Torn writes: a crash can leave the last record half-written. Recovery keeps every valid record
  and truncates the tail when the damage is confined to it: a record cut off by end of file, a
  final record with a bad checksum, or a zero-filled tail (some filesystems leave one after a crash).
- Real corruption is not hidden: a bad record followed by more non-zero data cannot come from a
  crash during append, so `Open` fails with `wal.ErrCorrupt` rather than silently dropping
  acknowledged writes behind it.

#### When to fsync: sync policies

`Append` hands every record to the OS immediately (no user-space buffer), so a process crash,
including `kill -9`, loses nothing under any policy. The policies differ only for power loss or
an OS crash, where data must have been fsynced:

| Policy | Ack after | Power-loss window | Cost |
|---|---|---|---|
| `always` | its own fsync, under the write lock | none | one fsync per write, serialized |
| `group` (default) | an fsync covering it | none | one fsync shared by all concurrent writers |
| `periodic` | write to the OS | up to `SyncInterval` (100 ms) | fsync off the write path |

Group commit is the interesting one. The writer appends under the lock, releases it, then calls
`SyncTo(offset)`. Syncs are serialized; a caller whose offset was already covered by someone
else's fsync returns immediately. Under concurrency one fsync acknowledges many writes. In the
unit test with 8 writers and a simulated 2 ms fsync, 200 sync requests needed 38 fsyncs.

Readers only see a write after it is as durable as the policy promises: `visibleSeq` is published
after the sync, so a reader can never observe a value that a power loss could still erase.

#### fsync failures are fatal (sticky errors)

If fsync fails, Linux may already have dropped the dirty pages and cleared the error, so a retry
can "succeed" while the data is gone. This is the 2018 PostgreSQL "fsyncgate" bug. The WAL writer
therefore makes the first write or fsync error permanent: every later write returns
`wal.ErrFailed` (HTTP 503), and the process must restart so recovery reads back what actually
reached the disk.

#### Directory lock

`Open` takes an OS-level exclusive lock on `LOCK` (`flock` on Unix, `LockFileEx` on Windows). Two
processes appending to one WAL would interleave records. The OS releases the lock when a process
dies, so a crash never leaves a stale lock behind.

#### Crash test

`TestCrashRecovery` re-runs the test binary as a child process that writes from 4 goroutines and
prints `ACK` only after `Put` returns. The parent kills it (SIGKILL / TerminateProcess) at a random
moment, reopens the directory, and checks that every acknowledged put and delete survived. It runs
4 kill/restart rounds per sync policy. To check that the test can actually catch lost writes, it was
run against a build that skipped every 50th record during replay, and it failed as expected.

## HTTP server (`server`, `cmd/lsmserver`)

| Method | Path | Result |
|---|---|---|
| `PUT` | `/v1/kv/{key}` | body is the value; `204` |
| `GET` | `/v1/kv/{key}` | `200` with raw value, or `404` |
| `DELETE` | `/v1/kv/{key}` | `204` (also when the key is missing) |
| `GET` | `/v1/stats` | JSON: entries, memtable bytes, WAL bytes, last seq, sync policy |
| `GET` | `/healthz` | `200 ok`, no auth (for load balancers) |

Keys are the URL path after `/v1/kv/` (percent-decoded, may contain `/`); values are raw bytes.

- Auth: every `/v1` route requires `Authorization: Bearer <LSM_AUTH_TOKEN>`. Tokens are compared in
  constant time (SHA-256 of both sides, then `subtle.ConstantTimeCompare`). The token is read only
  from the environment, never from a flag, so it does not show up in `ps` output. The server
  refuses to start without a token of at least 16 characters.
- Limits: key length (1 KiB), value size (`LSM_MAX_VALUE_KB`, default 1 MiB, enforced for both
  `Content-Length` and chunked bodies), header size, and read/write/idle timeouts (including
  `ReadHeaderTimeout` against slowloris).
- Errors: `401` bad token, `404` missing, `413` too large, `507` memtable full, `503` shutting down
  or WAL failed.
- Logs: one JSON line per request with the route pattern (`/v1/kv/{key...}`), not the URL, so
  keys never end up in logs.
- Shutdown: on SIGTERM/SIGINT it stops accepting connections, drains in-flight requests (15 s),
  then closes the DB, which fsyncs the WAL.

The server speaks plain HTTP. Put it behind TLS (a reverse proxy or the platform's load balancer)
before exposing it to the internet; the bearer token is sent with every request.

### Configuration

| Env var | Flag | Default |
|---|---|---|
| `LSM_AUTH_TOKEN` | (env only) | required, 16+ chars |
| `LSM_ADDR` | `-addr` | `:8080` |
| `LSM_DATA_DIR` | `-data` | `./data` |
| `LSM_SYNC` | `-sync` | `group` (`always`, `periodic`) |
| `LSM_MAX_MEMTABLE_MB` | `-max-memtable-mb` | `256` |
| `LSM_MAX_VALUE_KB` | `-max-value-kb` | `1024` |

## Running and deploying

### Locally

```powershell
$env:LSM_AUTH_TOKEN = "change-me-to-a-long-random-string"
go run ./cmd/lsmserver
```

```bash
TOKEN=change-me-to-a-long-random-string
curl -X PUT -H "Authorization: Bearer $TOKEN" --data 'alice' http://localhost:8080/v1/kv/user:1
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/kv/user:1      # alice
curl -X DELETE -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/kv/user:1
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v1/stats
```

### Docker

The image is a static binary on `distroless/static` (no shell, runs as non-root), with data in
the `/data` volume.

```bash
docker build -t lsmserver .
docker volume create lsmdata
docker run -d --name lsm -p 8080:8080 \
  -e LSM_AUTH_TOKEN="$(openssl rand -hex 32)" \
  -v lsmdata:/data lsmserver
```

CI builds this image on every push, writes a key, stops the container with SIGTERM, starts a new
container on the same volume, and checks that the key is still there.

### Cloud

Any host that runs a container and offers a persistent disk works, for example a VM (AWS EC2,
GCP Compute Engine, a DigitalOcean droplet) with Docker, or Fly.io / Render / Railway with an
attached volume. Requirements:

- A persistent volume mounted at `/data`. Without it, data is lost whenever the container is
  replaced.
- Exactly one instance per volume. This is a single-node engine; the directory lock rejects a
  second process. Replication is Phase 2 (Raft).
- `LSM_AUTH_TOKEN` set as a platform secret, not baked into the image.
- TLS in front (most platforms terminate HTTPS for you).
- A health check on `GET /healthz`.

## Roadmap details

- W3 SSTables: data blocks + sparse index + bloom filter + footer; memtable rotation to an
  immutable memtable and background flush; WAL rotation, with old logs deleted once their
  memtable is flushed.
- W4 Bloom filters sized by `m = -n ln p / (ln 2)^2` bits and `k = (m/n) ln 2` hash functions;
  MANIFEST file tracking live SSTables for correct recovery after compaction; merged read path.
- W5 Size-tiered compaction via k-way merge with a min-heap, keeping the newest version per key.
- W6 Benchmarks: write/read throughput, p99 latency, write/read/space amplification.
  Results will be added here only from real measurements.
- Stretch: leveled compaction, range iterators, snapshots API, LRU block cache, block
  compression, Prometheus metrics.
- Phase 2: Raft replication (own implementation) to turn this into a distributed KV store.

## Layout

```
db/             public API: Open, Put, Get, Delete, Close, Stats; WAL replay, sync policies
memtable/       skip-list memtable + iterator
internal/base/  internal keys (user key + seq number + kind) and ordering
wal/            write-ahead log: framing, group commit, recovery
server/         HTTP API: auth, limits, error mapping
cmd/lsmserver/  server binary: config, timeouts, graceful shutdown
sstable/        on-disk sorted tables            (W3)
bloom/          bloom filters                    (W4)
manifest/       live-SSTable tracking            (W4)
compaction/     background merging               (W5)
bench/          benchmark harness                (W6)
```

## Usage

```go
import "github.com/kumar-ayush101/LSM-engine/db"

d, err := db.Open("./data", nil)
if err != nil {
	log.Fatal(err)
}
defer d.Close()

d.Put([]byte("k"), []byte("v"))
v, err := d.Get([]byte("k")) // "v"
d.Delete([]byte("k"))
_, err = d.Get([]byte("k")) // db.ErrNotFound
```

## Testing

```
go test -race ./...
```

Requires Go 1.25+. The race detector needs cgo (a C compiler such as gcc) on Windows.

The test suite covers:

- Internal key ordering, encode/decode round trips, and corrupt-key rejection.
- Skip-list structural invariants (every level sorted, towers contiguous, length matches) and the
  statistical distribution of random node heights.
- Memtable behaviour: newest version wins, snapshot reads, tombstones vs not-found, empty values,
  buffer copying, duplicate sequence numbers.
- A randomized test comparing the memtable against a simple reference model over 20,000 operations.
- Concurrent readers, writers, and iterators, run under the race detector.
- WAL: a torn tail at every possible byte offset, damaged final records, zero-filled tails,
  mid-log corruption (must fail, not truncate), sticky write/fsync errors with an injected
  failing file, and group-commit coalescing.
- DB: persistence across reopen for each sync policy, reopen without `Close`, directory locking,
  memtable cap, randomized writes with repeated reopens checked against a map.
- Crash test: real process kills mid-write (see above).
- Server: CRUD, binary values and escaped keys, auth failures, size limits (including chunked
  bodies), error codes, keys absent from logs, and a full start/stop/restart of the binary.

CI (GitHub Actions) runs `gofmt`, `go vet`, and `go test -race` on every push and pull request,
then builds the Docker image and smoke-tests it (auth, write, container restart, read back).
