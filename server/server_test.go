package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/db"
)

const testToken = "test-token-0123456789"

type fixture struct {
	t   *testing.T
	srv *httptest.Server
	db  *db.DB
	log *bytes.Buffer
}

func newFixture(t *testing.T, dbOpts *db.Options, cfg Config) *fixture {
	t.Helper()
	d, err := db.Open(t.TempDir(), dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	logBuf := &bytes.Buffer{}
	var mu sync.Mutex
	cfg.Token = testToken
	cfg.Logger = slog.New(slog.NewTextHandler(lockedWriter{&mu, logBuf}, nil))
	h, err := New(d, cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		d.Close()
	})
	return &fixture{t: t, srv: srv, db: d, log: logBuf}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (f *fixture) do(method, path, token string, body []byte) (int, []byte, http.Header) {
	f.t.Helper()
	req, err := http.NewRequest(method, f.srv.URL+path, bytes.NewReader(body))
	if err != nil {
		f.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		f.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b, resp.Header
}

func TestNewRejectsShortToken(t *testing.T) {
	d, err := db.Open(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, tok := range []string{"", "short"} {
		if _, err := New(d, Config{Token: tok}); err == nil {
			t.Fatalf("token %q should be rejected", tok)
		}
	}
}

func TestCRUD(t *testing.T) {
	f := newFixture(t, nil, Config{})

	if code, _, _ := f.do("GET", "/v1/kv/user:1", testToken, nil); code != 404 {
		t.Fatalf("GET missing: %d", code)
	}
	if code, _, _ := f.do("PUT", "/v1/kv/user:1", testToken, []byte("alice")); code != 204 {
		t.Fatalf("PUT: %d", code)
	}
	code, body, hdr := f.do("GET", "/v1/kv/user:1", testToken, nil)
	if code != 200 || string(body) != "alice" || hdr.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("GET: %d %q %v", code, body, hdr)
	}
	f.do("PUT", "/v1/kv/user:1", testToken, []byte("alice-v2"))
	if _, body, _ := f.do("GET", "/v1/kv/user:1", testToken, nil); string(body) != "alice-v2" {
		t.Fatalf("overwrite: %q", body)
	}
	if code, _, _ := f.do("DELETE", "/v1/kv/user:1", testToken, nil); code != 204 {
		t.Fatalf("DELETE: %d", code)
	}
	if code, _, _ := f.do("GET", "/v1/kv/user:1", testToken, nil); code != 404 {
		t.Fatalf("GET after DELETE: %d", code)
	}
	if code, _, _ := f.do("DELETE", "/v1/kv/never", testToken, nil); code != 204 {
		t.Fatalf("DELETE missing: %d", code)
	}
}

func TestBinaryValuesAndEscapedKeys(t *testing.T) {
	f := newFixture(t, nil, Config{})
	val := []byte{0, 1, 2, 0xff, '\n', 0}
	// Slashes and percent-encoded bytes in keys.
	if code, _, _ := f.do("PUT", "/v1/kv/a/b%20c%2Fd", testToken, val); code != 204 {
		t.Fatalf("PUT: %d", code)
	}
	if v, err := f.db.Get([]byte("a/b c/d")); err != nil || !bytes.Equal(v, val) {
		t.Fatalf("stored under unexpected key: %q, %v", v, err)
	}
	if _, body, _ := f.do("GET", "/v1/kv/a/b%20c%2Fd", testToken, nil); !bytes.Equal(body, val) {
		t.Fatalf("GET: %q", body)
	}
	// Empty value is valid and distinct from a missing key.
	f.do("PUT", "/v1/kv/empty", testToken, nil)
	if code, body, _ := f.do("GET", "/v1/kv/empty", testToken, nil); code != 200 || len(body) != 0 {
		t.Fatalf("empty value: %d %q", code, body)
	}
}

func TestAuth(t *testing.T) {
	f := newFixture(t, nil, Config{})
	for _, path := range []string{"/v1/kv/k", "/v1/stats"} {
		for _, tok := range []string{"", "wrong-token-0123456789", testToken + "x", testToken[:len(testToken)-1]} {
			code, _, hdr := f.do("GET", path, tok, nil)
			if code != 401 || !strings.HasPrefix(hdr.Get("WWW-Authenticate"), "Bearer") {
				t.Fatalf("%s with token %q: %d, %v", path, tok, code, hdr)
			}
		}
	}
	// Wrong scheme.
	req, _ := http.NewRequest("GET", f.srv.URL+"/v1/kv/k", nil)
	req.Header.Set("Authorization", "Basic "+testToken)
	resp, _ := f.srv.Client().Do(req)
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("Basic scheme: %d", resp.StatusCode)
	}
	// Unauthenticated writes must not reach the DB.
	f.do("PUT", "/v1/kv/k", "", []byte("v"))
	if _, err := f.db.Get([]byte("k")); err == nil {
		t.Fatal("unauthenticated PUT was applied")
	}
	// Health check is open.
	if code, body, _ := f.do("GET", "/healthz", "", nil); code != 200 || string(body) != "ok\n" {
		t.Fatalf("healthz: %d %q", code, body)
	}
}

func TestLimits(t *testing.T) {
	f := newFixture(t, nil, Config{MaxKeyBytes: 8, MaxValueBytes: 16})
	if code, _, _ := f.do("PUT", "/v1/kv/123456789", testToken, []byte("v")); code != http.StatusRequestURITooLong {
		t.Fatalf("long key: %d", code)
	}
	if code, _, _ := f.do("PUT", "/v1/kv/k", testToken, make([]byte, 17)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large value: %d", code)
	}
	if code, _, _ := f.do("PUT", "/v1/kv/k", testToken, make([]byte, 16)); code != 204 {
		t.Fatalf("value at limit: %d", code)
	}
	if code, _, _ := f.do("GET", "/v1/kv/", testToken, nil); code != 400 {
		t.Fatalf("empty key: %d", code)
	}
}

func TestChunkedBodyOverLimit(t *testing.T) {
	// No Content-Length: the MaxBytesReader path must catch it.
	f := newFixture(t, nil, Config{MaxValueBytes: 16})
	req, _ := http.NewRequest("PUT", f.srv.URL+"/v1/kv/k", io.MultiReader(strings.NewReader(strings.Repeat("x", 100))))
	req.ContentLength = -1
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked oversize: %d", resp.StatusCode)
	}
	if _, err := f.db.Get([]byte("k")); err == nil {
		t.Fatal("oversize value was stored")
	}
}

// TestWritesBeyondMemtableSize: a small memtable is flushed to SSTables
// instead of rejecting writes, so every write succeeds and stays readable.
func TestWritesBeyondMemtableSize(t *testing.T) {
	f := newFixture(t, &db.Options{MaxMemtableBytes: 1024}, Config{})
	for i := 0; i < 100; i++ {
		if code, _, _ := f.do("PUT", "/v1/kv/k"+strings.Repeat("x", i), testToken, make([]byte, 100)); code != 204 {
			t.Fatalf("put %d: %d", i, code)
		}
	}
	if code, body, _ := f.do("GET", "/v1/kv/k", testToken, nil); code != 200 || len(body) != 100 {
		t.Fatalf("read back: %d, %d bytes", code, len(body))
	}
	if f.db.Stats().Flushes == 0 {
		t.Fatal("expected memtable flushes")
	}
}

func TestClosedDBIs503(t *testing.T) {
	f := newFixture(t, nil, Config{})
	f.db.Close()
	if code, _, _ := f.do("GET", "/v1/kv/k", testToken, nil); code != 503 {
		t.Fatalf("GET on closed DB: %d", code)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	f := newFixture(t, nil, Config{})
	if code, _, _ := f.do("POST", "/v1/kv/k", testToken, nil); code != 405 {
		t.Fatalf("POST: %d", code)
	}
}

func TestStats(t *testing.T) {
	f := newFixture(t, &db.Options{Sync: db.SyncAlways}, Config{})
	f.do("PUT", "/v1/kv/a", testToken, []byte("1"))
	f.do("DELETE", "/v1/kv/a", testToken, nil)
	code, body, _ := f.do("GET", "/v1/stats", testToken, nil)
	var st db.Stats
	if code != 200 || json.Unmarshal(body, &st) != nil {
		t.Fatalf("stats: %d %s", code, body)
	}
	if st.Entries != 2 || st.LastSeq != 2 || st.SyncPolicy != "always" || st.WALBytes == 0 {
		t.Fatalf("unexpected stats %+v", st)
	}
}

func TestLogsDoNotContainKeys(t *testing.T) {
	f := newFixture(t, nil, Config{})
	f.do("PUT", "/v1/kv/secret-customer-email", testToken, []byte("v"))
	f.do("GET", "/v1/kv/secret-customer-email", "", nil)
	logs := f.log.String()
	if strings.Contains(logs, "secret-customer-email") || strings.Contains(logs, testToken) {
		t.Fatalf("logs leak key or token:\n%s", logs)
	}
	if !strings.Contains(logs, "PUT /v1/kv/{key...}") {
		t.Fatalf("expected route pattern in logs:\n%s", logs)
	}
}

func TestConcurrentClients(t *testing.T) {
	f := newFixture(t, nil, Config{})
	var wg sync.WaitGroup
	for c := 0; c < 8; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				k := "/v1/kv/c" + string(rune('a'+c)) + "-" + strings.Repeat("i", i+1)
				req, _ := http.NewRequest("PUT", f.srv.URL+k, strings.NewReader(k))
				req.Header.Set("Authorization", "Bearer "+testToken)
				resp, err := f.srv.Client().Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != 204 {
					t.Errorf("PUT %s: %d", k, resp.StatusCode)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	if st := f.db.Stats(); st.Entries != 200 {
		t.Fatalf("entries = %d, want 200", st.Entries)
	}
}
