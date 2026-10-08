// Package server exposes a db.DB over HTTP.
//
// API (all /v1 routes require "Authorization: Bearer <token>"):
//
//	PUT    /v1/kv/{key}   body = value         -> 204
//	GET    /v1/kv/{key}                        -> 200 body = value, or 404
//	DELETE /v1/kv/{key}                        -> 204 (also for missing keys)
//	GET    /v1/stats                           -> 200 JSON
//	GET    /healthz                            -> 200 "ok" (no auth, for load balancers)
//	GET    /                                   -> public landing page (no auth;
//	                                              aggregate stats only, never keys)
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
}

const (
	defaultMaxKeyBytes   = 1 << 10
	defaultMaxValueBytes = 1 << 20
)

type handler struct {
	db        *db.DB
	tokenHash [32]byte
	maxKey    int
	maxValue  int64
	log       *slog.Logger
	started   time.Time
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
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", h.index)
	mux.Handle("GET /static/", h.static())
	mux.HandleFunc("GET /healthz", h.health)
	mux.Handle("GET /v1/kv/{key...}", h.auth(h.get))
	mux.Handle("PUT /v1/kv/{key...}", h.auth(h.put))
	mux.Handle("DELETE /v1/kv/{key...}", h.auth(h.del))
	mux.Handle("GET /v1/stats", h.auth(h.stats))
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
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(len(v)))
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
	if err := h.db.Put(k, v); err != nil {
		h.dbError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) del(w http.ResponseWriter, r *http.Request) {
	k, ok := h.key(w, r)
	if !ok {
		return
	}
	if err := h.db.Delete(k); err != nil {
		h.dbError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
