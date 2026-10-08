// Command lsmserver serves an LSM-engine database over HTTP.
//
// Configuration (environment variables; flags override where available):
//
//	LSM_AUTH_TOKEN   required. Bearer token clients must send (min 16 bytes).
//	                 Read from the environment only, never a flag, so it does
//	                 not appear in process listings.
//	LSM_ADDR         listen address            (default ":8080",  -addr)
//	LSM_DATA_DIR     database directory        (default "./data", -data)
//	LSM_SYNC         group | always | periodic (default "group",  -sync)
//	LSM_MEMTABLE_MB      flush threshold, MiB  (default 4,        -memtable-mb)
//	LSM_MAX_MEMTABLE_MB  memtable memory cap   (default 256,      -max-memtable-mb)
//	LSM_MAX_VALUE_KB     max value size in KiB (default 1024,     -max-value-kb)
//	LSM_DEMO         on | off: public, rate-limited demo sandbox at
//	                 /v1/demo/... with no token (default off, -demo)
//	LSM_TRUST_PROXY  on | off: take the client IP for rate limiting from
//	                 X-Forwarded-For; enable only behind a reverse proxy
//	                 such as Render or Fly (default off, -trust-proxy)
//
// On SIGINT or SIGTERM it stops accepting connections, lets in-flight
// requests finish (up to 15s), then closes the DB, which syncs the WAL.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kumar-ayush101/LSM-engine/db"
	"github.com/kumar-ayush101/LSM-engine/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(ctx, os.Args[1:], os.Getenv, logger, nil); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

type config struct {
	addr          string
	dataDir       string
	sync          db.SyncPolicy
	token         string
	maxMemtableMB int64
	memtableMB    int64
	maxValueKB    int64
	demo          bool
	trustProxy    bool
}

func parseSwitch(name, s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "0", "off", "false", "no":
		return false, nil
	case "1", "on", "true", "yes":
		return true, nil
	}
	return false, fmt.Errorf("%s must be on or off, got %q", name, s)
}

func parseConfig(args []string, getenv func(string) string) (config, error) {
	envOr := func(key, def string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return def
	}
	fs := flag.NewFlagSet("lsmserver", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	addr := fs.String("addr", envOr("LSM_ADDR", ":8080"), "listen address")
	dataDir := fs.String("data", envOr("LSM_DATA_DIR", "./data"), "database directory")
	syncStr := fs.String("sync", envOr("LSM_SYNC", "group"), "fsync policy: group, always, periodic")
	memMB := fs.String("max-memtable-mb", envOr("LSM_MAX_MEMTABLE_MB", "256"), "cap on memtable memory in MiB")
	flushMB := fs.String("memtable-mb", envOr("LSM_MEMTABLE_MB", "4"), "memtable flush threshold in MiB")
	valKB := fs.String("max-value-kb", envOr("LSM_MAX_VALUE_KB", "1024"), "max value size in KiB")
	demoStr := fs.String("demo", envOr("LSM_DEMO", "off"), "enable the public demo sandbox: on or off")
	proxyStr := fs.String("trust-proxy", envOr("LSM_TRUST_PROXY", "off"), "take client IP from X-Forwarded-For: on or off")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if fs.NArg() > 0 {
		return config{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}

	c := config{addr: *addr, dataDir: *dataDir, token: getenv("LSM_AUTH_TOKEN")}
	var err error
	if c.sync, err = db.ParseSyncPolicy(*syncStr); err != nil {
		return config{}, err
	}
	if c.maxMemtableMB, err = positive("max-memtable-mb", *memMB); err != nil {
		return config{}, err
	}
	if c.memtableMB, err = positive("memtable-mb", *flushMB); err != nil {
		return config{}, err
	}
	if c.maxValueKB, err = positive("max-value-kb", *valKB); err != nil {
		return config{}, err
	}
	if c.demo, err = parseSwitch("demo", *demoStr); err != nil {
		return config{}, err
	}
	if c.trustProxy, err = parseSwitch("trust-proxy", *proxyStr); err != nil {
		return config{}, err
	}
	if len(c.token) < server.MinTokenLen {
		return config{}, fmt.Errorf("LSM_AUTH_TOKEN must be set to at least %d characters (e.g. `openssl rand -hex 32`)", server.MinTokenLen)
	}
	return c, nil
}

func positive(name, s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", name, s)
	}
	return n, nil
}

// run starts the server and blocks until ctx is cancelled or the server
// fails. If ready is non-nil, the bound address is sent on it once the
// listener is open (used by tests with -addr 127.0.0.1:0).
func run(ctx context.Context, args []string, getenv func(string) string, logger *slog.Logger, ready chan<- string) error {
	cfg, err := parseConfig(args, getenv)
	if err != nil {
		return err
	}

	d, err := db.Open(cfg.dataDir, &db.Options{
		Sync:             cfg.sync,
		MaxMemtableBytes: cfg.maxMemtableMB << 20,
		MemtableSize:     cfg.memtableMB << 20,
	})
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	st := d.Stats()
	logger.Info("database opened", "dir", cfg.dataDir, "sync", st.SyncPolicy,
		"entries", st.Entries, "tables", st.Tables, "table_bytes", st.TableBytes, "last_seq", st.LastSeq)

	h, err := server.New(d, server.Config{
		Token:         cfg.token,
		MaxValueBytes: cfg.maxValueKB << 10,
		Demo:          server.DemoConfig{Enabled: cfg.demo, TrustProxy: cfg.trustProxy},
		Logger:        logger,
	})
	if err != nil {
		d.Close()
		return err
	}

	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		d.Close()
		return err
	}
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second, // slowloris protection
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	logger.Info("listening", "addr", ln.Addr().String())
	if ready != nil {
		ready <- ln.Addr().String()
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err = <-serveErr:
		// Serve failed on its own (not via Shutdown).
	case <-ctx.Done():
		logger.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err = srv.Shutdown(shutdownCtx)
		cancel()
		<-serveErr
	}
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}
	// Close after the HTTP server so no handler is still writing.
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		logger.Info("stopped cleanly")
	}
	return err
}
