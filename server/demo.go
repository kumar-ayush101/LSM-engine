package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// DemoPrefix is prepended to every key written through the public demo
// API. Demo visitors can therefore never read or modify keys outside it.
// Keys starting with this prefix are reserved for the demo.
const DemoPrefix = "demo/"

// DemoConfig enables an unauthenticated sandbox at /v1/demo/... so visitors
// can try the engine without the admin token.
//
// Protection layers, from hardest to softest:
//   - MaxTotalBytes: a hard cap on the bytes all demo writes (including
//     deletes, which are tombstones) may add. Once reached, demo writes get
//     507 until the process restarts. The counter is monotonic, so it is an
//     upper bound on what visitors added to the WAL and SSTables (compaction
//     may later reclaim some of it).
//   - GlobalPerMinute: total demo request rate across all clients.
//   - PerIPPerMinute: per-client fairness. Depends on TrustProxy being set
//     correctly for the deployment (see clientIP).
//
// The byte counter is per process: after a restart with a persistent disk,
// replayed demo data is not counted again. The memtable cap (Options.
// MaxMemtableBytes) still bounds total memory in that case.
type DemoConfig struct {
	Enabled         bool
	MaxKeyBytes     int   // default 64
	MaxValueBytes   int64 // default 1 KiB
	MaxTotalBytes   int64 // default 8 MiB
	PerIPPerMinute  int   // default 30 (burst 10)
	GlobalPerMinute int   // default 600 (burst 100)
	// TrustProxy derives the client IP from the rightmost X-Forwarded-For
	// entry. Enable only behind a reverse proxy that sets that header.
	TrustProxy bool
}

const demoEntryOverhead = 64 // matches the memtable's per-node estimate

type demo struct {
	cfg    DemoConfig
	used   atomic.Int64
	perIP  *rateLimiter
	global *rateLimiter
}

func newDemo(cfg DemoConfig) *demo {
	if cfg.MaxKeyBytes <= 0 {
		cfg.MaxKeyBytes = 64
	}
	if cfg.MaxValueBytes <= 0 {
		cfg.MaxValueBytes = 1 << 10
	}
	if cfg.MaxTotalBytes <= 0 {
		cfg.MaxTotalBytes = 8 << 20
	}
	if cfg.PerIPPerMinute <= 0 {
		cfg.PerIPPerMinute = 30
	}
	if cfg.GlobalPerMinute <= 0 {
		cfg.GlobalPerMinute = 600
	}
	return &demo{
		cfg:    cfg,
		perIP:  newRateLimiter(cfg.PerIPPerMinute, max(1, cfg.PerIPPerMinute/3), 10000),
		global: newRateLimiter(cfg.GlobalPerMinute, max(1, cfg.GlobalPerMinute/6), 1),
	}
}

// reserve atomically claims n bytes of the demo budget.
func (d *demo) reserve(n int64) bool {
	for {
		cur := d.used.Load()
		if cur+n > d.cfg.MaxTotalBytes {
			return false
		}
		if d.used.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (d *demo) release(n int64) { d.used.Add(-n) }

// limit applies the global and per-client rate limits.
func (h *handler) demoLimit(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, wait := h.demo.global.allow("")
		if ok {
			ok, wait = h.demo.perIP.allow(clientIP(r, h.demo.cfg.TrustProxy))
		}
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
			httpError(w, http.StatusTooManyRequests, "demo rate limit reached; try again shortly")
			return
		}
		next(w, r)
	})
}

func (h *handler) demoKey(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	k := r.PathValue("key")
	switch {
	case k == "":
		httpError(w, http.StatusBadRequest, "empty key")
		return nil, false
	case len(k) > h.demo.cfg.MaxKeyBytes:
		httpError(w, http.StatusRequestURITooLong, fmt.Sprintf("demo keys are limited to %d bytes", h.demo.cfg.MaxKeyBytes))
		return nil, false
	}
	return []byte(DemoPrefix + k), true
}

func (h *handler) demoGet(w http.ResponseWriter, r *http.Request) {
	k, ok := h.demoKey(w, r)
	if !ok {
		return
	}
	v, err := h.db.Get(k)
	if err != nil {
		h.dbError(w, err)
		return
	}
	writeValue(w, v)
}

func (h *handler) demoPut(w http.ResponseWriter, r *http.Request) {
	k, ok := h.demoKey(w, r)
	if !ok {
		return
	}
	limit := h.demo.cfg.MaxValueBytes
	tooBig := fmt.Sprintf("demo values are limited to %d bytes", limit)
	if r.ContentLength > limit {
		httpError(w, http.StatusRequestEntityTooLarge, tooBig)
		return
	}
	v, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			httpError(w, http.StatusRequestEntityTooLarge, tooBig)
			return
		}
		httpError(w, http.StatusBadRequest, "reading body: "+err.Error())
		return
	}
	cost := int64(len(k) + len(v) + demoEntryOverhead)
	if !h.demo.reserve(cost) {
		httpError(w, http.StatusInsufficientStorage, "demo sandbox is full; it resets when the server restarts")
		return
	}
	if err := h.db.Put(k, v); err != nil {
		h.demo.release(cost)
		h.dbError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) demoDelete(w http.ResponseWriter, r *http.Request) {
	k, ok := h.demoKey(w, r)
	if !ok {
		return
	}
	// A delete writes a tombstone, which uses memory too.
	cost := int64(len(k) + demoEntryOverhead)
	if !h.demo.reserve(cost) {
		httpError(w, http.StatusInsufficientStorage, "demo sandbox is full; it resets when the server restarts")
		return
	}
	if err := h.db.Delete(k); err != nil {
		h.demo.release(cost)
		h.dbError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// DemoStats is returned by GET /v1/demo/stats. Aggregate numbers only.
type DemoStats struct {
	Entries        int    `json:"entries"`
	LastSeq        uint64 `json:"last_seq"`
	WALBytes       int64  `json:"wal_bytes"`
	MemtableBytes  int64  `json:"memtable_bytes"`
	SyncPolicy     string `json:"sync_policy"`
	DemoBytesUsed  int64  `json:"demo_bytes_used"`
	DemoBytesLimit int64  `json:"demo_bytes_limit"`
	Uptime         string `json:"uptime"`
}

func (h *handler) demoStats(w http.ResponseWriter, r *http.Request) {
	st := h.db.Stats()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(DemoStats{
		Entries:        st.Entries,
		LastSeq:        st.LastSeq,
		WALBytes:       st.WALBytes,
		MemtableBytes:  st.MemtableBytes,
		SyncPolicy:     st.SyncPolicy,
		DemoBytesUsed:  h.demo.used.Load(),
		DemoBytesLimit: h.demo.cfg.MaxTotalBytes,
		Uptime:         time.Since(h.started).Round(time.Second).String(),
	})
}
