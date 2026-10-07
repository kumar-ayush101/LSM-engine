# LSM-engine

An LSM-tree key-value storage engine written from scratch in Go (no RocksDB/Pebble/Badger).

## Status

| Stage | Component | State |
|---|---|---|
| W1 | Skip-list memtable, sequence numbers, tombstones, Put/Get/Delete | Done |
| W2 | Write-ahead log (CRC32 records, fsync policies, replay) | Planned |
| W3 | SSTable format + background flush | Planned |
| W4 | Bloom filters, MANIFEST, merged read path | Planned |
| W5 | Size-tiered compaction | Planned |
| W6 | Benchmarks + results | Planned |

Data is currently held in memory only; durability arrives with the WAL.

## Layout

```
db/             public API: Open, Put, Get, Delete, Close
memtable/       skip-list memtable + iterator
internal/base/  internal keys (user key + seq number + kind) and ordering
wal/ sstable/ bloom/ compaction/ manifest/ bench/   upcoming stages
```

## Usage

```go
d, err := db.Open("./data", nil)
if err != nil { log.Fatal(err) }
defer d.Close()

d.Put([]byte("k"), []byte("v"))
v, err := d.Get([]byte("k"))   // "v"
d.Delete([]byte("k"))
_, err = d.Get([]byte("k"))    // db.ErrNotFound
```

## Testing

```
go test -race ./...
```

CI runs gofmt, `go vet` and `go test -race` on every push.
