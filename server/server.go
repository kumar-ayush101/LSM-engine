// Package server exposes a db.DB over HTTP.
//
// API (all /v1 routes require "Authorization: Bearer <token>"):
//
//	PUT    /v1/kv/{key}   body = value         -> 204
//	GET    /v1/kv/{key}                        -> 200 body = value, or 404
//	DELETE /v1/kv/{key}                        -> 204 (also for missing keys)
//	GET    /v1/stats                           -> 200 JSON
//	GET    /v1/scan?start=&end=&prefix=&limit= -> 200 JSON range scan
//	POST   /v1/admin/flush                     -> flush memtable to an SSTable
//	POST   /v1/admin/compact                   -> merge all SSTables into one
//	GET    /healthz                            -> 200 "ok" (no auth, for load balancers)
//	GET    /                                   -> public landing page (no auth;
//	                                              aggregate stats only, never keys)
//
// Optional public demo sandbox (Config.Demo.Enabled), no auth, rate-limited,
// every key silently prefixed with DemoPrefix:
//
//	GET|PUT|DELETE /v1/demo/kv/{key}
//	GET            /v1/demo/stats
//	GET            /v1/demo/scan   (within demo/ only, max 50 pairs)
//
// Keys are the rest of the path after /v1/kv/, percent-decoded, so they may
// contain slashes. Values are raw bytes (application/octet-stream).
package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/kumar-ayush101/LSM-engine/db"
	"github.com/kumar-ayush101/LSM-engine/wal"
)

// MinTokenLen is the shortest accepted auth token.
const MinTokenLen = 16

// Config configures the HTTP handler.
type Config struct {
	// Token is the shared bearer token. Required, at least MinTokenLen bytes.
	Token string
	// MaxKeyBytes bounds key length. Default 1 KiB.
	MaxKeyBytes int
	// MaxValueBytes bounds request bodies. Default 1 MiB.
	MaxValueBytes int64
	// Logger receives one line per request. Default slog.Default().
	Logger *slog.Logger
	// Demo configures the optional public sandbox. Disabled by default.
	Demo DemoConfig
	// Cluster, when set, routes writes through Raft replication: the leader
	// accepts PUT/DELETE and replicates them; a follower returns 421 with the
	// leader's address so the client can retry there. Reads are always served
	// locally. When nil the server runs single-node (writes go straight to
	// the db).
	Cluster Replicator
}

// Replicator is the subset of a cluster node the server needs to replicate
// writes and report cluster status. The cluster package's *cluster.Node
// satisfies it.
type Replicator interface {
	// Put and Delete replicate a write; they return ErrNotLeader (wrapped)
	// when this node is not the leader.
	Put(key, value []byte) error
	Delete(key []byte) error
	// IsLeader reports whether this node is the current leader.
	IsLeader() bool
	// Leader returns the current leader's node ID and client-facing address
	// hint (may be empty if unknown).
	Leader() (id, addr string)
	// Status returns the current cluster status for monitoring.
	Status() ClusterStatus
}

// ClusterStatus is the replicated-cluster state surfaced on /v1/cluster and
// the landing page. It carries no keys or values.
type ClusterStatus struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	LeaderID    string `json:"leader_id"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
	LastLog     uint64 `json:"last_log_index"`
	Peers       int    `json:"peers"`
}

// ErrNotLeader lets the server detect a non-leader write without importing
// the cluster package. A Replicator should return an error that wraps this
// (via errors.Is) when a write hits a follower.
var ErrNotLeader = errors.New("server: not leader")

const (
	defaultMaxKeyBytes   = 1 << 10
	defaultMaxValueBytes = 1 << 20
)

type handler struct {
	db        *db.DB
	cluster   Replicator // nil in single-node mode
	tokenHash [32]byte
	maxKey    int
	maxValue  int64
	log       *slog.Logger
	started   time.Time
	demo      *demo // nil when the demo sandbox is disabled
}

// New returns the HTTP handler for d.
func New(d *db.DB, cfg Config) (http.Handler, error) {
	if len(cfg.Token) < MinTokenLen {
		return nil, fmt.Errorf("server: auth token must be at least %d bytes", MinTokenLen)
	}
	if cfg.MaxKeyBytes <= 0 {
		cfg.MaxKeyBytes = defaultMaxKeyBytes
	}
	if cfg.MaxValueBytes <= 0 {
		cfg.MaxValueBytes = defaultMaxValueBytes
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	h := &handler{
		db: d,
		// Compare hashes, not raw tokens: equal-length inputs make the
		// constant-time comparison independent of the token's length too.
		tokenHash: sha256.Sum256([]byte(cfg.Token)),
		maxKey:    cfg.MaxKeyBytes,
		maxValue:  cfg.MaxValueBytes,
		log:       cfg.Logger,
		started:   time.Now(),
		cluster:   cfg.Cluster,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", h.index)
	mux.Handle("GET /static/", h.static())
	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("GET /stats.json", h.publicStats)
	mux.Handle("GET /v1/kv/{key...}", h.auth(h.get))
	mux.Handle("PUT /v1/kv/{key...}", h.auth(h.put))
	mux.Handle("DELETE /v1/kv/{key...}", h.auth(h.del))
	mux.Handle("GET /v1/stats", h.auth(h.stats))
	mux.Handle("GET /v1/scan", h.auth(h.ownerScan))
	mux.Handle("POST /v1/admin/flush", h.auth(h.flush))
	mux.Handle("POST /v1/admin/compact", h.auth(h.compact))
	if cfg.Cluster != nil {
		mux.Handle("GET /v1/cluster", h.auth(h.clusterStatus))
	}
	if cfg.Demo.Enabled {
		h.demo = newDemo(cfg.Demo)
		mux.Handle("GET /v1/demo/kv/{key...}", h.demoLimit(h.demoGet))
		mux.Handle("PUT /v1/demo/kv/{key...}", h.demoLimit(h.demoPut))
		mux.Handle("DELETE /v1/demo/kv/{key...}", h.demoLimit(h.demoDelete))
		mux.Handle("GET /v1/demo/stats", h.demoLimit(h.demoStats))
		mux.Handle("GET /v1/demo/scan", h.demoLimit(h.demoScan))
	}
	return h.logRequests(mux), nil
}

func (h *handler) auth(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		sum := sha256.Sum256([]byte(got))
		if !ok || subtle.ConstantTimeCompare(sum[:], h.tokenHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="lsm"`)
			httpError(w, http.StatusUnauthorized, "missing or invalid bearer token")
			return
		}
		next(w, r)
	})
}

// key extracts and validates the key, writing an error response if invalid.
func (h *handler) key(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	k := r.PathValue("key")
	switch {
	case k == "":
		httpError(w, http.StatusBadRequest, "empty key")
		return nil, false
	case len(k) > h.maxKey:
		httpError(w, http.StatusRequestURITooLong, fmt.Sprintf("key longer than %d bytes", h.maxKey))
		return nil, false
	}
	return []byte(k), true
}

func (h *handler) get(w http.ResponseWriter, r *http.Request) {
	k, ok := h.key(w, r)
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

// writeValue sends a stored value. Values are arbitrary bytes from clients
// (including anonymous demo users), so the response is locked down: no MIME
// sniffing, and a sandbox CSP so a value containing HTML can never run as a
// page on this origin even if a browser navigates to it directly.
func writeValue(w http.ResponseWriter, v []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(len(v)))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	w.WriteHeader(http.StatusOK)
	w.Write(v)
}

func (h *handler) put(w http.ResponseWriter, r *http.Request) {
	k, ok := h.key(w, r)
	if !ok {
		return
	}
	if r.ContentLength > h.maxValue {
		httpError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("value larger than %d bytes", h.maxValue))
		return
	}
	v, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxValue))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			httpError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("value larger than %d bytes", h.maxValue))
			return
		}
		httpError(w, http.StatusBadRequest, "reading body: "+err.Error())
		return
	}
	if err := h.writeKV(w, func() error { return h.doPut(k, v) }); err != nil {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// doPut performs a put through the cluster (replicated) or directly on the db
// (single-node).
func (h *handler) doPut(k, v []byte) error {
	if h.cluster != nil {
		return h.cluster.Put(k, v)
	}
	return h.db.Put(k, v)
}

func (h *handler) doDelete(k []byte) error {
	if h.cluster != nil {
		return h.cluster.Delete(k)
	}
	return h.db.Delete(k)
}

// writeKV runs a replicated/local write and maps a not-leader result to a 421
// redirect carrying the leader's address. It returns a non-nil error if it
// already wrote a response (so the caller should stop).
func (h *handler) writeKV(w http.ResponseWriter, write func() error) error {
	// In cluster mode, redirect followers before attempting the write so the
	// client fails fast with the leader's address.
	if h.cluster != nil && !h.cluster.IsLeader() {
		h.redirectToLeader(w)
		return errHandled
	}
	if err := write(); err != nil {
		if errors.Is(err, ErrNotLeader) {
			h.redirectToLeader(w)
			return errHandled
		}
		h.dbError(w, err)
		return errHandled
	}
	return nil
}

// errHandled signals that a response has already been written.
var errHandled = errors.New("server: response already written")

// redirectToLeader writes a 421 Misdirected Request with the leader's address
// so a client or proxy can retry the write on the leader.
func (h *handler) redirectToLeader(w http.ResponseWriter) {
	id, addr := "", ""
	if h.cluster != nil {
		id, addr = h.cluster.Leader()
	}
	if addr != "" {
		w.Header().Set("Location", addr)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMisdirectedRequest) // 421
	json.NewEncoder(w).Encode(map[string]string{
		"error":     "not leader; retry on the leader",
		"leader_id": id,
		"leader":    addr,
	})
}

func (h *handler) del(w http.ResponseWriter, r *http.Request) {
	k, ok := h.key(w, r)
	if !ok {
		return
	}
	if err := h.writeKV(w, func() error { return h.doDelete(k) }); err != nil {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// clusterStatus reports the raft cluster state (role, term, leader, commit
// index). Registered only when the server runs in cluster mode.
func (h *handler) clusterStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.cluster.Status())
}

func (h *handler) stats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.db.Stats())
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write([]byte("ok\n"))
}

// dbError maps engine errors to HTTP statuses. Unexpected errors are logged
// in full but reported to the client generically.
func (h *handler) dbError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, db.ErrNotFound):
		httpError(w, http.StatusNotFound, "not found")
	case errors.Is(err, db.ErrTooLarge):
		httpError(w, http.StatusRequestEntityTooLarge, "key or value too large")
	case errors.Is(err, db.ErrMemtableFull):
		httpError(w, http.StatusInsufficientStorage, "storage full")
	case errors.Is(err, db.ErrClosed):
		httpError(w, http.StatusServiceUnavailable, "shutting down")
	case errors.Is(err, wal.ErrFailed):
		// Sticky: every later write fails too until the process restarts
		// and recovery re-reads what actually reached the disk.
		h.log.Error("write-ahead log failed; restart required", "err", err)
		httpError(w, http.StatusServiceUnavailable, "storage failure; server needs restart")
	default:
		h.log.Error("unexpected db error", "err", err)
		httpError(w, http.StatusInternalServerError, "internal error")
	}
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.bytes += n
	return n, err
}

// logRequests logs method, route and status. It logs r.Pattern rather than
// the URL so keys (which may be sensitive) never reach the logs.
func (h *handler) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		h.log.Info("request",
			"method", r.Method,
			"route", route,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_ms", float64(time.Since(start).Microseconds())/1000,
			"remote", r.RemoteAddr,
		)
	})
}
