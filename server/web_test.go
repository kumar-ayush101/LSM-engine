package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestLandingPage(t *testing.T) {
	f := newFixture(t, nil, Config{})
	f.do("PUT", "/v1/kv/private-key-name", testToken, []byte("private-value"))

	code, body, hdr := f.do("GET", "/", "", nil) // no token needed
	page := string(body)
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/html") {
		t.Fatalf("GET /: %d %v", code, hdr)
	}
	for _, want := range []string{"<title>LSM-engine", RepoURL, "Entries in memtable", "/static/app.js", `lang="en"`} {
		if !strings.Contains(page, want) {
			t.Errorf("landing page missing %q", want)
		}
	}
	// Live stats are rendered, but never keys, values or the token.
	if !strings.Contains(page, `data-stat="entries">1</dd>`) {
		t.Errorf("expected entry count 1 on page")
	}
	for _, secret := range []string{"private-key-name", "private-value", testToken} {
		if strings.Contains(page, secret) {
			t.Fatalf("landing page leaks %q", secret)
		}
	}
	csp := hdr.Get("Content-Security-Policy")
	if !strings.Contains(csp, "default-src 'none'") || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Fatalf("missing or weak CSP: %q", csp)
	}
	if hdr.Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing nosniff")
	}
}

func TestLandingPageHEAD(t *testing.T) {
	// Render probes with HEAD /; it must not be a 404.
	f := newFixture(t, nil, Config{})
	if code, _, _ := f.do("HEAD", "/", "", nil); code != 200 {
		t.Fatalf("HEAD /: %d", code)
	}
}

func TestStaticAssets(t *testing.T) {
	f := newFixture(t, nil, Config{})
	for path, ctype := range map[string]string{
		"/static/app.js":    "javascript",
		"/static/style.css": "text/css",
	} {
		code, body, hdr := f.do("GET", path, "", nil)
		if code != 200 || len(body) == 0 || !strings.Contains(hdr.Get("Content-Type"), ctype) {
			t.Errorf("%s: %d, %d bytes, %q", path, code, len(body), hdr.Get("Content-Type"))
		}
	}
	if code, _, _ := f.do("GET", "/static/missing.js", "", nil); code != http.StatusNotFound {
		t.Errorf("missing asset: %d", code)
	}
}

func TestUnknownPathsStill404(t *testing.T) {
	f := newFixture(t, nil, Config{})
	for _, p := range []string{"/nope", "/v1", "/index.html", "/static/../server.go"} {
		if code, _, _ := f.do("GET", p, "", nil); code != 404 {
			t.Errorf("GET %s: %d, want 404", p, code)
		}
	}
	// The landing page must not make the API reachable without auth.
	if code, _, _ := f.do("GET", "/v1/stats", "", nil); code != 401 {
		t.Errorf("/v1/stats without token: %d", code)
	}
}

func TestPublicStatsJSON(t *testing.T) {
	f := newFixture(t, nil, Config{})
	f.do("PUT", "/v1/kv/private-key-name", testToken, []byte("private-value"))
	code, body, hdr := f.do("GET", "/stats.json", "", nil) // no token
	if code != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "application/json") {
		t.Fatalf("GET /stats.json: %d %v", code, hdr)
	}
	var ps PublicStats
	if err := json.Unmarshal(body, &ps); err != nil {
		t.Fatal(err)
	}
	if ps.Entries != 1 || ps.LastSeq != 1 || ps.WALBytes == 0 || ps.MemtableLimit == 0 || ps.DemoEnabled {
		t.Fatalf("unexpected %+v", ps)
	}
	for _, secret := range []string{"private-key-name", "private-value", testToken} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("/stats.json leaks %q", secret)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	for n, want := range map[int64]string{0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 128 << 20: "128.0 MiB", 3 << 30: "3.0 GiB"} {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}
