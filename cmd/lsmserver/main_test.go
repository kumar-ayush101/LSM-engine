package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kumar-ayush101/LSM-engine/db"
)

const tok = "0123456789abcdef0123"

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestParseConfig(t *testing.T) {
	c, err := parseConfig(nil, env(map[string]string{"LSM_AUTH_TOKEN": tok}))
	if err != nil {
		t.Fatal(err)
	}
	if c.addr != ":8080" || c.dataDir != "./data" || c.sync != db.SyncGroup || c.maxMemtableMB != 256 || c.maxValueKB != 1024 || c.demo || c.trustProxy {
		t.Fatalf("defaults: %+v", c)
	}
	c, err = parseConfig(nil, env(map[string]string{"LSM_AUTH_TOKEN": tok, "LSM_DEMO": "on", "LSM_TRUST_PROXY": "true"}))
	if err != nil || !c.demo || !c.trustProxy {
		t.Fatalf("demo switches: %+v, %v", c, err)
	}

	c, err = parseConfig([]string{"-addr", ":9000", "-sync", "always"}, env(map[string]string{
		"LSM_AUTH_TOKEN": tok, "LSM_ADDR": ":1", "LSM_DATA_DIR": "/var/lib/lsm", "LSM_SYNC": "periodic",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.addr != ":9000" || c.dataDir != "/var/lib/lsm" || c.sync != db.SyncAlways {
		t.Fatalf("flags should override env: %+v", c)
	}

	bad := []struct {
		args []string
		env  map[string]string
	}{
		{nil, map[string]string{}},                          // no token
		{nil, map[string]string{"LSM_AUTH_TOKEN": "short"}}, // short token
		{[]string{"-sync", "sometimes"}, map[string]string{"LSM_AUTH_TOKEN": tok}},
		{[]string{"-max-memtable-mb", "0"}, map[string]string{"LSM_AUTH_TOKEN": tok}},
		{[]string{"-max-value-kb", "abc"}, map[string]string{"LSM_AUTH_TOKEN": tok}},
		{[]string{"stray"}, map[string]string{"LSM_AUTH_TOKEN": tok}},
		{[]string{"-token", tok}, map[string]string{}}, // no token flag exists
		{nil, map[string]string{"LSM_AUTH_TOKEN": tok, "LSM_DEMO": "maybe"}},
		{nil, map[string]string{"LSM_AUTH_TOKEN": tok, "LSM_TRUST_PROXY": "2"}},
	}
	for i, b := range bad {
		if _, err := parseConfig(b.args, env(b.env)); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
}

// TestRunServesAndPersistsAcrossRestart starts the real server, writes
// through HTTP, shuts it down gracefully, restarts it on the same data
// directory, and reads the value back.
func TestRunServesAndPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	e := env(map[string]string{"LSM_AUTH_TOKEN": tok, "LSM_DATA_DIR": dir})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	start := func() (string, context.CancelFunc, chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		ready := make(chan string, 1)
		done := make(chan error, 1)
		go func() { done <- run(ctx, []string{"-addr", "127.0.0.1:0"}, e, logger, ready) }()
		select {
		case addr := <-ready:
			return "http://" + addr, cancel, done
		case err := <-done:
			t.Fatalf("server exited: %v", err)
		case <-time.After(10 * time.Second):
			t.Fatal("server did not start")
		}
		return "", nil, nil
	}
	call := func(method, url, body string) (int, string) {
		req, _ := http.NewRequest(method, url, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	stop := func(cancel context.CancelFunc, done chan error) {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("shutdown: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("server did not shut down")
		}
	}

	base, cancel, done := start()
	if code, _ := call("PUT", base+"/v1/kv/greeting", "hello"); code != 204 {
		t.Fatalf("PUT: %d", code)
	}
	stop(cancel, done)

	base, cancel, done = start()
	if code, body := call("GET", base+"/v1/kv/greeting", ""); code != 200 || body != "hello" {
		t.Fatalf("after restart: %d %q", code, body)
	}
	stop(cancel, done)
}
