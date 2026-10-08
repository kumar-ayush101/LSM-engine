package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func demoFixture(t *testing.T, demo DemoConfig) *fixture {
	t.Helper()
	demo.Enabled = true
	return newFixture(t, nil, Config{Demo: demo})
}

func TestDemoDisabledByDefault(t *testing.T) {
	f := newFixture(t, nil, Config{})
	for _, p := range []string{"/v1/demo/kv/k", "/v1/demo/stats"} {
		if code, _, _ := f.do("GET", p, "", nil); code != 404 {
			t.Fatalf("GET %s with demo off: %d, want 404", p, code)
		}
	}
	if code, _, _ := f.do("PUT", "/v1/demo/kv/k", "", []byte("v")); code != 404 {
		t.Fatalf("PUT with demo off: %d", code)
	}
	_, body, _ := f.do("GET", "/", "", nil)
	if strings.Contains(string(body), "public demo sandbox") {
		t.Fatal("landing page advertises a disabled sandbox")
	}
}

func TestDemoCRUDWithoutToken(t *testing.T) {
	f := demoFixture(t, DemoConfig{})
	if code, _, _ := f.do("GET", "/v1/demo/kv/greeting", "", nil); code != 404 {
		t.Fatalf("GET missing: %d", code)
	}
	if code, _, _ := f.do("PUT", "/v1/demo/kv/greeting", "", []byte("hello")); code != 204 {
		t.Fatalf("PUT: %d", code)
	}
	code, body, hdr := f.do("GET", "/v1/demo/kv/greeting", "", nil)
	if code != 200 || string(body) != "hello" {
		t.Fatalf("GET: %d %q", code, body)
	}
	if hdr.Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(hdr.Get("Content-Security-Policy"), "sandbox") {
		t.Fatalf("value response not locked down: %v", hdr)
	}
	if code, _, _ := f.do("DELETE", "/v1/demo/kv/greeting", "", nil); code != 204 {
		t.Fatalf("DELETE: %d", code)
	}
	if code, _, _ := f.do("GET", "/v1/demo/kv/greeting", "", nil); code != 404 {
		t.Fatalf("GET after DELETE: %d", code)
	}
	_, page, _ := f.do("GET", "/", "", nil)
	if !strings.Contains(string(page), "public demo sandbox") || !strings.Contains(string(page), `data-demo="on"`) {
		t.Fatal("landing page does not offer the sandbox")
	}
}

// TestDemoIsIsolated: demo users can only reach keys under DemoPrefix, and
// the owner API still requires the token.
func TestDemoIsIsolated(t *testing.T) {
	f := demoFixture(t, DemoConfig{})
	f.do("PUT", "/v1/kv/secret", testToken, []byte("owner-data"))

	// Demo GET of the same name reads demo/secret, not secret.
	if code, _, _ := f.do("GET", "/v1/demo/kv/secret", "", nil); code != 404 {
		t.Fatalf("demo read reached owner key: %d", code)
	}
	// Path tricks do not escape the prefix.
	for _, p := range []string{"/v1/demo/kv/../kv/secret", "/v1/demo/kv/%2e%2e/secret", "/v1/demo/kv//secret"} {
		code, body, _ := f.do("GET", p, "", nil)
		if code == 200 && string(body) == "owner-data" {
			t.Fatalf("%s escaped the demo prefix", p)
		}
	}
	// Demo writes land under the prefix and do not overwrite owner data.
	f.do("PUT", "/v1/demo/kv/secret", "", []byte("visitor"))
	if v, err := f.db.Get([]byte("secret")); err != nil || string(v) != "owner-data" {
		t.Fatalf("owner key changed: %q, %v", v, err)
	}
	if v, err := f.db.Get([]byte(DemoPrefix + "secret")); err != nil || string(v) != "visitor" {
		t.Fatalf("demo key not stored under prefix: %q, %v", v, err)
	}
	f.do("DELETE", "/v1/demo/kv/secret", "", nil)
	if _, err := f.db.Get([]byte("secret")); err != nil {
		t.Fatal("demo delete removed the owner key")
	}
	// Owner routes are unaffected by the sandbox.
	if code, _, _ := f.do("GET", "/v1/kv/secret", "", nil); code != 401 {
		t.Fatalf("owner route without token: %d", code)
	}
	if code, _, _ := f.do("GET", "/v1/stats", "", nil); code != 401 {
		t.Fatalf("owner stats without token: %d", code)
	}
}

func TestDemoLimits(t *testing.T) {
	f := demoFixture(t, DemoConfig{MaxKeyBytes: 8, MaxValueBytes: 16})
	if code, _, _ := f.do("PUT", "/v1/demo/kv/123456789", "", []byte("v")); code != http.StatusRequestURITooLong {
		t.Fatalf("long key: %d", code)
	}
	if code, _, _ := f.do("PUT", "/v1/demo/kv/k", "", make([]byte, 17)); code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large value: %d", code)
	}
	// Chunked body over the limit.
	req, _ := http.NewRequest("PUT", f.srv.URL+"/v1/demo/kv/k", strings.NewReader(strings.Repeat("x", 100)))
	req.ContentLength = -1
	resp, err := f.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked oversize: %d", resp.StatusCode)
	}
	if code, _, _ := f.do("PUT", "/v1/demo/kv/k", "", make([]byte, 16)); code != 204 {
		t.Fatalf("at limit: %d", code)
	}
}

func TestDemoTotalBytesCap(t *testing.T) {
	// Each write costs len("demo/kN") + 100 + 64 bytes; a 1000-byte budget
	// fits a handful, then the sandbox must report full.
	f := demoFixture(t, DemoConfig{MaxTotalBytes: 1000, MaxValueBytes: 100, PerIPPerMinute: 10000, GlobalPerMinute: 10000})
	ok, full := 0, 0
	for i := 0; i < 20; i++ {
		code, _, _ := f.do("PUT", "/v1/demo/kv/k"+string(rune('a'+i)), "", make([]byte, 100))
		switch code {
		case 204:
			ok++
		case http.StatusInsufficientStorage:
			full++
		default:
			t.Fatalf("unexpected %d", code)
		}
	}
	if ok == 0 || full == 0 || ok > 6 {
		t.Fatalf("ok=%d full=%d", ok, full)
	}
	// Deletes also consume budget (tombstones): the small remainder fits a
	// couple of tombstones, then deletes are refused too.
	refused := false
	for i := 0; i < 5 && !refused; i++ {
		code, _, _ := f.do("DELETE", "/v1/demo/kv/zz"+string(rune('a'+i)), "", nil)
		refused = code == http.StatusInsufficientStorage
	}
	if !refused {
		t.Fatal("deletes were never refused once the sandbox was full")
	}
	// Reads still work, and the owner API is not affected by the demo cap.
	if code, _, _ := f.do("GET", "/v1/demo/kv/ka", "", nil); code != 200 {
		t.Fatalf("read when full: %d", code)
	}
	if code, _, _ := f.do("PUT", "/v1/kv/owner", testToken, make([]byte, 500)); code != 204 {
		t.Fatalf("owner write when demo full: %d", code)
	}
}

func TestDemoCapIsExactUnderConcurrency(t *testing.T) {
	f := demoFixture(t, DemoConfig{MaxTotalBytes: 5000, MaxValueBytes: 100, PerIPPerMinute: 100000, GlobalPerMinute: 100000})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				req, _ := http.NewRequest("PUT", f.srv.URL+"/v1/demo/kv/g"+string(rune('a'+g))+string(rune('a'+i)), strings.NewReader(strings.Repeat("x", 100)))
				resp, err := f.srv.Client().Do(req)
				if err == nil {
					resp.Body.Close()
				}
			}
		}(g)
	}
	wg.Wait()
	_, body, _ := f.do("GET", "/v1/demo/stats", "", nil)
	var st DemoStats
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	// Never over budget, and filled to within one entry of it.
	if used := st.DemoBytesUsed; used > 5000 || used < 5000-(8+100+64) {
		t.Fatalf("demo bytes used = %d, budget 5000", used)
	}
	if st.Entries*(8+100+64) > 5000 {
		t.Fatalf("%d entries stored exceed the budget", st.Entries)
	}
}

func TestDemoRateLimit(t *testing.T) {
	f := demoFixture(t, DemoConfig{PerIPPerMinute: 6, GlobalPerMinute: 1000}) // burst 2
	codes := map[int]int{}
	var retry string
	for i := 0; i < 5; i++ {
		code, _, hdr := f.do("GET", "/v1/demo/kv/k", "", nil)
		codes[code]++
		if code == http.StatusTooManyRequests {
			retry = hdr.Get("Retry-After")
		}
	}
	if codes[404] != 2 || codes[http.StatusTooManyRequests] != 3 {
		t.Fatalf("codes = %v, want 2x404 then 429s", codes)
	}
	if retry == "" || retry == "0" {
		t.Fatalf("Retry-After = %q", retry)
	}
	// The landing page and owner API are not rate limited by the sandbox.
	if code, _, _ := f.do("GET", "/", "", nil); code != 200 {
		t.Fatalf("landing page: %d", code)
	}
	if code, _, _ := f.do("GET", "/v1/stats", testToken, nil); code != 200 {
		t.Fatalf("owner stats: %d", code)
	}
}

func TestDemoStats(t *testing.T) {
	f := demoFixture(t, DemoConfig{})
	f.do("PUT", "/v1/demo/kv/a", "", []byte("1"))
	code, body, _ := f.do("GET", "/v1/demo/stats", "", nil)
	s := string(body)
	if code != 200 || !strings.Contains(s, `"entries":1`) || !strings.Contains(s, `"last_seq":1`) || !strings.Contains(s, `"demo_bytes_used":`) {
		t.Fatalf("demo stats: %d %s", code, s)
	}
	if strings.Contains(s, "demo/a") {
		t.Fatal("stats leak keys")
	}
}

func TestRateLimiter(t *testing.T) {
	now := time.Unix(0, 0)
	l := newRateLimiter(60, 2, 2) // 1 token/s, burst 2, at most 2 clients
	l.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		if ok, _ := l.allow("a"); !ok {
			t.Fatalf("burst request %d rejected", i)
		}
	}
	ok, wait := l.allow("a")
	if ok || wait < time.Second {
		t.Fatalf("over burst: ok=%v wait=%v", ok, wait)
	}
	now = now.Add(time.Second)
	if ok, _ := l.allow("a"); !ok {
		t.Fatal("token not refilled after 1s")
	}
	// Separate client has its own bucket.
	if ok, _ := l.allow("b"); !ok {
		t.Fatal("client b rejected")
	}
	// Table full (a, b active): a third client is rejected (fail closed)...
	if ok, _ := l.allow("c"); ok {
		t.Fatal("client c admitted beyond maxEntries")
	}
	// ...until idle buckets are pruned.
	now = now.Add(time.Minute)
	if ok, _ := l.allow("c"); !ok {
		t.Fatal("client c rejected after idle buckets expired")
	}
}

func TestClientIP(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "10.0.0.5:1234"
	r.Header.Set("X-Forwarded-For", "6.6.6.6, 203.0.113.9")
	if got := clientIP(r, false); got != "10.0.0.5" {
		t.Fatalf("untrusted: %q", got)
	}
	// Rightmost entry, not the client-controlled leftmost one.
	if got := clientIP(r, true); got != "203.0.113.9" {
		t.Fatalf("trusted: %q", got)
	}
	r.Header.Del("X-Forwarded-For")
	if got := clientIP(r, true); got != "10.0.0.5" {
		t.Fatalf("trusted without header: %q", got)
	}
}
