package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kumar-ayush101/LSM-engine/db"
)

func decodeScan(t *testing.T, body []byte) ScanResponse {
	t.Helper()
	var r ScanResponse
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("bad scan JSON %q: %v", body, err)
	}
	return r
}

func TestOwnerScanAndPaging(t *testing.T) {
	f := newFixture(t, &db.Options{MemtableSize: 2 << 10}, Config{})
	for i := 0; i < 30; i++ {
		f.do("PUT", fmt.Sprintf("/v1/kv/user:%02d", i), testToken, []byte(fmt.Sprint(i)))
	}
	f.do("PUT", "/v1/kv/bin", testToken, []byte{0xff, 0x00})
	f.do("DELETE", "/v1/kv/user:05", testToken, nil)

	if code, _, _ := f.do("GET", "/v1/scan", "", nil); code != 401 {
		t.Fatalf("scan without token: %d", code)
	}
	code, body, _ := f.do("GET", "/v1/scan?prefix=user:&limit=10", testToken, nil)
	r := decodeScan(t, body)
	if code != 200 || r.Count != 10 || r.Items[0].Key != "user:00" || r.Next != "user:11" {
		t.Fatalf("page 1: %d %+v", code, r)
	}
	for _, it := range r.Items {
		if it.Key == "user:05" {
			t.Fatal("deleted key returned by scan")
		}
	}
	_, body, _ = f.do("GET", "/v1/scan?prefix=user:&limit=100&start=user:"+r.Next[5:], testToken, nil)
	r2 := decodeScan(t, body)
	if r2.Count != 19 || r2.Next != "" {
		t.Fatalf("page 2: %+v", r2)
	}
	_, body, _ = f.do("GET", "/v1/scan?start=bin&end=bio", testToken, nil)
	r3 := decodeScan(t, body)
	if r3.Count != 1 || string(r3.Items[0].ValueB64) != "\xff\x00" || r3.Items[0].Value != "" {
		t.Fatalf("binary value: %+v", r3)
	}
	if code, _, _ := f.do("GET", "/v1/scan?limit=abc", testToken, nil); code != 400 {
		t.Fatalf("bad limit: %d", code)
	}
}

func TestDemoScanIsConfined(t *testing.T) {
	f := demoFixture(t, DemoConfig{})
	f.do("PUT", "/v1/kv/secret", testToken, []byte("owner"))
	f.do("PUT", "/v1/kv/zzz", testToken, []byte("owner"))
	for _, k := range []string{"a", "b", "c"} {
		f.do("PUT", "/v1/demo/kv/"+k, "", []byte(k))
	}
	code, body, _ := f.do("GET", "/v1/demo/scan?limit=1000&end=zzzz", "", nil)
	r := decodeScan(t, body)
	if code != 200 || r.Count != 3 {
		t.Fatalf("demo scan: %d %+v", code, r)
	}
	if strings.Contains(string(body), "owner") || strings.Contains(string(body), "demo/") {
		t.Fatalf("demo scan leaked or exposed prefix: %s", body)
	}
}

func TestAdminFlushAndCompact(t *testing.T) {
	f := newFixture(t, nil, Config{})
	for i := 0; i < 3; i++ {
		f.do("PUT", "/v1/kv/k", testToken, []byte(fmt.Sprint(i)))
		if code, _, _ := f.do("POST", "/v1/admin/flush", testToken, nil); code != 200 {
			t.Fatalf("flush: %d", code)
		}
	}
	if f.db.Stats().Tables < 1 {
		t.Fatal("flush created no tables")
	}
	if code, _, _ := f.do("POST", "/v1/admin/compact", "", nil); code != 401 {
		t.Fatalf("compact without token: %d", code)
	}
	code, body, _ := f.do("POST", "/v1/admin/compact", testToken, nil)
	var st db.Stats
	json.Unmarshal(body, &st)
	if code != 200 || st.Tables != 1 || st.TableEntries != 1 {
		t.Fatalf("compact: %d %+v", code, st)
	}
	if _, body, _ := f.do("GET", "/v1/kv/k", testToken, nil); string(body) != "2" {
		t.Fatalf("value after compaction: %q", body)
	}
	_, body, _ = f.do("GET", "/stats.json", "", nil)
	var ps PublicStats
	json.Unmarshal(body, &ps)
	if ps.Tables != 1 || ps.Compactions < 1 || ps.Flushes < 3 {
		t.Fatalf("public stats: %+v", ps)
	}
}
