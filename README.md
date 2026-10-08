# LSM-engine

[![CI](https://github.com/kumar-ayush101/LSM-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/kumar-ayush101/LSM-engine/actions/workflows/ci.yml)

**Live demo: [lsm-engine.onrender.com](https://lsm-engine.onrender.com)**. Leave the token empty, pick a
quick-start example, press Put, then Get. Stats on the page update live as you write.

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
| -- | HTTP server (bearer auth, limits, graceful shutdown), Docker image, public demo sandbox, live deployment | Done |
| W3 | SSTable format (data blocks, sparse index, bloom filter, footer), immutable memtable, background flush, WAL rotation | Done |
| W4 | Bloom filters, MANIFEST, merged read path, range scans | Done |
| W5 | Size-tiered compaction (k-way min-heap merge, tombstone-safe drop rules) | Done |
| W6 | Benchmark harness + measured results | Done |

Data is durable: every write goes to the WAL before it is acknowledged. A full memtable is flushed
in the background to an immutable SSTable and committed in the MANIFEST, after which its WAL file is
deleted, so memory use and restart time stay bounded regardless of how much has been written.

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

Every box in the diagram is implemented.

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

Every write is appended to the current WAL file (`NNNNNN.log`) before it touches the memtable. On
`Open`, unflushed logs are replayed to rebuild the memtable, and sequence numbering resumes after
the last replayed record.

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

### WAL rotation, immutable memtable and flush (`db`)

When the active memtable reaches the flush threshold (`MemtableSize`, default 4 MiB):

1. A new numbered WAL file (`000007.log`) is opened; the old one is closed, which fsyncs it.
2. The memtable becomes immutable and a fresh one takes writes immediately.
3. A background goroutine writes the immutable memtable to an SSTable, fsyncs it, renames it from
   `.tmp` into place, and appends a MANIFEST edit ("table 8 added, WALs below 7 are flushed").
4. Only after that edit is fsynced is the old WAL deleted.

Every crash point is safe: before step 3's MANIFEST edit, recovery replays the old WAL; after it,
the WAL is garbage and is deleted on startup. At most one memtable is being flushed; if a writer
fills the next one first, it waits (a write stall, counted in stats) instead of letting memory grow.

### SSTables (`sstable`)

```
[data block 0][data block 1]...[filter block][index block][footer 48 B]
```

- Data blocks (~4 KiB) hold entries in internal-key order: `klen | vlen | internal key | value`.
- The sparse index has one entry per block: the block's last key plus its offset and length. A
  lookup binary-searches the index (held in memory) and reads exactly one block.
- Every block carries a CRC-32C, verified on each read; a bad block is an error, never silently
  wrong data.
- Tables are written to `NNNNNN.sst.tmp`, fsynced, then renamed, so a table under its final name
  is always complete.
- Readers are reference-counted. A table removed by compaction stays readable until the last Get
  or scan using it finishes, then it is deleted.

### Bloom filters (`bloom`)

Each table has a bloom filter over its user keys, sized with the optimal formulas:

```
m = -n ln p / (ln 2)^2   bits        (9.6 bits/key at p = 1%)
k = (m / n) ln 2         hash fns    (7 at p = 1%)
```

The k positions come from one 64-bit hash by double hashing, `h1 + i*h2 mod m` (Kirsch and
Mitzenmacher), so a lookup hashes the key once. Measured in the unit test: 1.015% false positives
for a 1% target and 5.08% for 5%. A filter says "definitely absent" or "maybe"; a damaged filter
answers "maybe", which costs a block read, never correctness.

### MANIFEST (`manifest`)

The MANIFEST is a log of version edits (table added, table deleted, WAL number, next file number,
last sequence), framed with the same CRC-checked records as the WAL. A table is live if and only
if the MANIFEST says so. This is what makes recovery correct after a crash mid-compaction: both
the inputs and the output exist on disk, and only the MANIFEST knows which set is real. Anything
else is deleted on startup. On every open the log is rewritten as one snapshot edit (temp file +
atomic rename), so it never grows without bound.

### Read path

A Get takes a consistent view (memtable, immutable memtable, list of tables, snapshot sequence
number) under a read lock, then searches newest to oldest:

1. active memtable, 2. immutable memtable, 3. SSTables newest-first.

Tables whose key range cannot contain the key, or whose bloom filter rules it out, are skipped.
The first version found wins; a tombstone stops the search with "not found".

`Scan(start, end, limit)` merges all of these sources with the same min-heap merging iterator
compaction uses, keeps the newest version of each key at the snapshot, and skips tombstones.

### Size-tiered compaction (`compaction`)

Tables are kept newest-first and always cover disjoint sequence-number ranges. The picker looks
for the longest run of adjacent tables of similar size (each within 2x of the run's average), and
merges it when it has at least 4 tables. If there are more than 12 tables and no such run, it
merges the 4 adjacent tables with the smallest combined size. Merging only adjacent tables keeps
the sequence ranges disjoint, so "newest-first" stays well defined.

The merge is a k-way merge with a min-heap (`container/heap`), O(log k) per entry. Drop rules, per
user key (versions arrive newest-first):

- Versions newer than the oldest live snapshot are kept.
- The newest version at or below that snapshot is kept; anything older is dropped.
- That version is dropped too if it is a tombstone **and** the merge includes the oldest table in
  the database. Otherwise an older table below the merge could still hold a value for the key, and
  dropping the tombstone would resurrect it. `TestPartialCompactionKeepsTombstones` checks this.

**Size-tiered vs leveled.** Size-tiered rewrites each byte about log_T(N) times (low write
amplification) at the cost of more tables per read (mitigated by bloom filters) and temporary
space for a merge. Leveled compaction (LevelDB, RocksDB default) bounds reads to one table per
level and space overhead to ~10%, but rewrites each byte ~10 times per level. Size-tiered suits a
write-heavy engine; leveled is on the stretch list.

## Benchmarks

`go run ./cmd/lsmbench` runs the suite below and prints this table. All numbers were measured on
one machine and depend heavily on the disk's fsync latency: Intel Core i3-1215U (8 threads),
Samsung MZAL4512HBLU SSD, Windows (amd64), Go 1.25.5. Run it on your hardware for your own numbers.

Workload: 100,000 distinct keys, 100-byte values, uniformly random overwrites (500,000 writes for
`periodic`/`group`, fewer for `always` because each write waits for its own fsync), then 200,000
random reads, half of them for keys that were never written. 4 MiB memtables.

| Sync | Writers | Writes/s | Write p50 | Write p99 | Reads/s | Read p50 | Read p99 | Write amp | Tables probed / Get | Space amp | Bloom skips |
|---|---|---|---|---|---|---|---|---|---|---|---|
| periodic | 8 | 50,865 | 25.7 µs | 2.08 ms | 127,295 | 9.5 µs | 746 µs | 3.13x | 1.72 | 1.11x | 243,556 |
| group | 8 | 1,932 | 3.49 ms | 12.55 ms | 119,727 | 12.1 µs | 788 µs | 3.13x | 1.73 | 1.11x | 244,906 |
| always | 8 | 751 | 10.11 ms | 33.12 ms | 291,459 | 1.7 µs | 763 µs | 2.26x | 0.50 | 1.11x | 81,022 |
| always | 1 | 801 | 1.16 ms | 2.37 ms | 554,842 | 1.0 µs | 11.0 µs | 2.26x | 0.50 | 1.11x | 93,956 |

What the numbers show:

- **Group commit**: with 8 concurrent writers, sharing fsyncs gives 2.6x the throughput of one
  fsync per write (1,932 vs 751 writes/s) with the same durability. `periodic` is ~26x faster
  again because nothing waits for the disk, at the cost of a 100 ms power-loss window.
- **Write amplification** (WAL + flush + compaction bytes / user bytes) is ~3.1x with compaction
  running: each byte is written once to the WAL, once at flush, and about once more by merges.
- **Bloom filters**: of the reads for absent keys, about 244,000 table reads were skipped without
  touching disk; Get probed 1.7 tables on average with 5 tables live.
- **Space amplification** after a final compaction is 1.11x: the overhead is block framing, the
  index and the bloom filter (overwritten versions and tombstones are gone).
- The `always` runs wrote fewer keys, so the dataset fit in one table and reads were faster; they
  are there for the fsync comparison, not for reads.

## HTTP server (`server`, `cmd/lsmserver`)

| Method | Path | Result |
|---|---|---|
| `PUT` | `/v1/kv/{key}` | body is the value; `204` |
| `GET` | `/v1/kv/{key}` | `200` with raw value, or `404` |
| `DELETE` | `/v1/kv/{key}` | `204` (also when the key is missing) |
| `GET` | `/v1/stats` | JSON engine stats: memtable, tables, flushes, compactions, amplification, bloom skips |
| `GET` | `/v1/scan?prefix=&start=&end=&limit=` | Ordered range scan as JSON, paged with `next` (max 1000 per page) |
| `POST` | `/v1/admin/flush` | Flush the memtable to an SSTable |
| `POST` | `/v1/admin/compact` | Flush, then merge every SSTable into one |
| `GET` | `/healthz` | `200 ok`, no auth (for load balancers) |
| `GET` | `/stats.json` | Public aggregate counters (entries, last seq, WAL bytes, memtable bytes, uptime, demo budget). No auth; never keys or values. |
| `GET` | `/` | Public landing page (see below). |

#### Landing page (`server/web`)

A single self-contained page embedded in the binary with `go:embed`, so there are no extra files
to deploy and no third-party scripts, fonts or images. It has:

- Live engine stats refreshed every 10 s from `/stats.json` (and after each request), with a
  memtable usage meter.
- A "Try it" playground: quick-start examples, Put / Get / Delete / Stats buttons, a terminal-style
  response panel with status, latency and an explanation of what the engine did, and a short
  request history. With no token it uses the demo sandbox; an optional owner-token field unlocks
  the full API.
- How a write flows, engineering highlights, the API reference, and the roadmap.
- Strict CSP (`default-src 'none'`, same-origin scripts and styles only, no framing), responses
  rendered as text (never `innerHTML`), light/dark themes, keyboard focus styles, ARIA labels, and
  reduced-motion support.

#### Public demo sandbox (`LSM_DEMO=on`)

Optional, off by default. Lets visitors try the engine without the token:

| Method | Path | Result |
|---|---|---|
| `PUT` / `GET` / `DELETE` | `/v1/demo/kv/{key}` | same as the owner API, no auth |
| `GET` | `/v1/demo/stats` | aggregate counters plus demo budget used |
| `GET` | `/v1/demo/scan` | range scan confined to the sandbox (max 50 per page) |

Every demo key is stored as `demo/<key>`, so visitors can never read or change other keys; the
`demo/` prefix is reserved. Abuse is bounded in layers:

- Hard memory budget for all demo writes (8 MiB; tombstones count too). When it is used up,
  demo writes get `507` until restart; the owner API is unaffected.
- Global rate limit (600 requests/min) and a per-client token bucket (30/min, burst 10), with
  `429` and `Retry-After`. The limiter table is bounded and fails closed under an address flood.
- Small keys (64 B) and values (1 KiB).
- Values are served with `nosniff` and a `sandbox` CSP, so stored HTML can never run as a page.

Behind a reverse proxy (Render, Fly), set `LSM_TRUST_PROXY=on` so the per-client limit uses the
rightmost `X-Forwarded-For` entry (the address the proxy saw, which the client cannot forge).

Keys are the URL path after `/v1/kv/` (percent-decoded, may contain `/`); values are raw bytes.

- Auth: every `/v1` route requires `Authorization: Bearer <LSM_AUTH_TOKEN>`. Tokens are compared in
  constant time (SHA-256 of both sides, then `subtle.ConstantTimeCompare`). The token is read only
  from the environment, never from a flag, so it does not show up in `ps` output. The server
  refuses to start without a token of at least 16 characters.
- Limits: key length (1 KiB), value size (`LSM_MAX_VALUE_KB`, default 1 MiB, enforced for both
  `Content-Length` and chunked bodies), header size, and read/write/idle timeouts (including
  `ReadHeaderTimeout` against slowloris).
- Errors: `401` bad token, `404` missing, `413` too large, `507` demo budget used up, `503`
  shutting down, WAL failed, or a background flush/compaction failed.
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
| `LSM_MEMTABLE_MB` | `-memtable-mb` | `4` (flush threshold) |
| `LSM_MAX_MEMTABLE_MB` | `-max-memtable-mb` | `256` (cap on memtable memory) |
| `LSM_MAX_VALUE_KB` | `-max-value-kb` | `1024` |
| `LSM_DEMO` | `-demo` | `off` (`on` enables the public sandbox) |
| `LSM_TRUST_PROXY` | `-trust-proxy` | `off` (`on` behind a reverse proxy) |

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

#### Render (the live demo)

The live demo runs as a Render web service built from the `Dockerfile` (Language: Docker, health
check path `/healthz`). Environment variables:

| Key | Value | Why |
|---|---|---|
| `LSM_AUTH_TOKEN` | generated by Render | owner token; never commit it |
| `PORT` | `8080` | port Render routes to |
| `LSM_MAX_MEMTABLE_MB` | `128` | stay well under the 512 MB instance |
| `LSM_DEMO` | `on` | enable the public sandbox |
| `LSM_TRUST_PROXY` | `on` | per-client rate limits behind Render's proxy |

It uses the free instance type, which has no persistent disk, so the demo data resets on every
restart or deploy, and the first request after an idle period is slow while the instance wakes.
For durable data, use a paid instance with a disk mounted at `/data` and set `RUNTIME_USER=root`
(build arg) so the process can write to the root-owned mount.

#### Fly.io

`fly.toml` is included: Mumbai region, one machine, a 1 GB volume at `/data`, a `/healthz` check,
and a 20 s kill timeout so graceful shutdown can finish. Set the token with
`fly secrets set LSM_AUTH_TOKEN=...` and deploy with `fly deploy --ha=false`.

## Roadmap details

- Stretch: leveled compaction (flag-switchable), a public snapshots API, LRU block cache, block
  compression, Prometheus metrics.
- Phase 2: Raft replication (own implementation) to turn this into a distributed KV store.

## Layout

```
db/             public API (Open, Put, Get, Delete, Scan, Flush, Compact, Stats); recovery,
                WAL rotation, background flush and compaction
memtable/       skip-list memtable + iterator
internal/base/  internal keys (user key + seq number + kind) and ordering
wal/            write-ahead log: framing, group commit, recovery
server/         HTTP API: auth, limits, error mapping, demo sandbox, rate limiter
server/web/     embedded landing page (HTML, CSS, JS)
cmd/lsmserver/  server binary: config, timeouts, graceful shutdown
sstable/        SSTable writer and reader: blocks, sparse index, filter, footer
bloom/          bloom filters (optimal m/k, double hashing)
manifest/       version-edit log of live SSTables
compaction/     k-way merging iterator, drop rules, size-tiered picker
bench/          benchmark harness
cmd/lsmbench/   benchmark binary
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
- Crash test: real process kills mid-write, mid-flush and mid-compaction (tiny memtables keep
  the child flushing and compacting constantly), 4 rounds per sync policy, zero acknowledged writes lost.
- SSTables: round trips across many blocks, Seek/Get at random snapshots against a model, bloom
  filters skipping ~99% of absent-key lookups, out-of-order and empty tables rejected, checksum
  and footer corruption detected, reference counting deferring deletion, concurrent readers.
- Bloom filters: sizing formulas, no false negatives, measured false-positive rate vs target.
- MANIFEST: edit round trips, replay and snapshot rewrite, torn final edit, unknown deletes.
- Compaction: k-way merge order and seeks, error propagation, drop rules at and above the
  bottom, snapshot horizon, the size-tiered picker.
- LSM DB: flush, tombstones shadowing older tables, full and partial compaction (tombstones kept
  when older tables exist), scans against a model, a long random workload with constant flushing
  and compaction plus reopens, concurrent reads and scans during compaction, garbage cleanup on
  recovery, and upgrading a W2 single-WAL directory.
- Server: CRUD, binary values and escaped keys, auth failures, size limits (including chunked
  bodies), error codes, keys absent from logs, and a full start/stop/restart of the binary.
- Demo sandbox: isolation from owner keys (including path tricks), size limits, the total byte
  budget (exact under concurrent writers), per-client and global rate limits with `Retry-After`,
  proxy-aware client IPs, and the bounded limiter table failing closed.
- Landing page and `/stats.json`: render without auth, never contain keys, values or the token,
  and carry the expected security headers.

CI (GitHub Actions) runs `gofmt`, `go vet`, and `go test -race` on every push and pull request,
then builds the Docker image and smoke-tests it (auth, write, container restart, read back).
