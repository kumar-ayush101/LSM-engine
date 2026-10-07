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
| W2 | Write-ahead log (CRC32 records, fsync policies, replay, torn-write handling) | Planned |
| W3 | SSTable format (data blocks, sparse index, footer) + background flush | Planned |
| W4 | Bloom filters, MANIFEST, merged read path | Planned |
| W5 | Size-tiered compaction (k-way merge) | Planned |
| W6 | Benchmark harness + results | Planned |

Data is currently held in memory only. Durability arrives with the WAL in W2.

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

## Roadmap details

- W2 WAL: length-prefixed records with CRC32, configurable fsync policy (every write, group
  commit, periodic), replay on startup, truncation of a torn final record, WAL deletion after flush.
  Crash test: kill the process mid-write, restart, verify no acknowledged write is lost.
- W3 SSTables: data blocks + sparse index + bloom filter + footer; memtable rotation to an
  immutable memtable and background flush.
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
db/             public API: Open, Put, Get, Delete, Close
memtable/       skip-list memtable + iterator
internal/base/  internal keys (user key + seq number + kind) and ordering
wal/            write-ahead log                  (W2)
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

CI (GitHub Actions) runs `gofmt`, `go vet`, and `go test -race` on every push and pull request.
