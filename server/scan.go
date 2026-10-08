package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MaxScanLimit caps the number of pairs one scan request returns.
const MaxScanLimit = 1000

// ScanItem is one pair in a scan response. Value is returned as a string
// when it is valid UTF-8, otherwise base64 in ValueB64, so binary values
// survive JSON.
type ScanItem struct {
	Key      string `json:"key"`
	Value    string `json:"value,omitempty"`
	ValueB64 []byte `json:"value_b64,omitempty"`
}

// ScanResponse is returned by the scan endpoints.
type ScanResponse struct {
	Items []ScanItem `json:"items"`
	Count int        `json:"count"`
	// Next is the key to pass as start to fetch the following page; empty
	// when the range is exhausted.
	Next string `json:"next,omitempty"`
}

func scanLimit(r *http.Request, def, maxLimit int) (int, bool) {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return def, true
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, false
	}
	return min(n, maxLimit), true
}

// scan runs a range scan over [prefix+start, prefix+end) and strips prefix
// from the returned keys. It fetches one extra pair to compute Next.
func (h *handler) scan(w http.ResponseWriter, r *http.Request, prefix string, defLimit, maxLimit int) {
	q := r.URL.Query()
	limit, ok := scanLimit(r, defLimit, maxLimit)
	if !ok {
		httpError(w, http.StatusBadRequest, "limit must be a positive integer")
		return
	}
	start := []byte(prefix + q.Get("start"))
	var end []byte
	switch {
	case q.Get("end") != "":
		end = []byte(prefix + q.Get("end"))
	case prefix != "":
		end = prefixEnd([]byte(prefix))
	}
	if p := q.Get("prefix"); p != "" {
		if q.Get("start") == "" {
			start = []byte(prefix + p)
		}
		if pe := prefixEnd([]byte(prefix + p)); end == nil || string(pe) < string(end) {
			end = pe
		}
	}
	kvs, err := h.db.Scan(start, end, limit+1)
	if err != nil {
		h.dbError(w, err)
		return
	}
	resp := ScanResponse{Items: []ScanItem{}}
	if len(kvs) > limit {
		resp.Next = strings.TrimPrefix(string(kvs[limit].Key), prefix)
		kvs = kvs[:limit]
	}
	for _, kv := range kvs {
		it := ScanItem{Key: strings.TrimPrefix(string(kv.Key), prefix)}
		if utf8.Valid(kv.Value) {
			it.Value = string(kv.Value)
		} else {
			it.ValueB64 = kv.Value
		}
		resp.Items = append(resp.Items, it)
	}
	resp.Count = len(resp.Items)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	json.NewEncoder(w).Encode(resp)
}

// prefixEnd returns the smallest key greater than every key with prefix p,
// or nil if there is none (p is all 0xff).
func prefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// ownerScan: GET /v1/scan?start=&end=&prefix=&limit= over all keys.
func (h *handler) ownerScan(w http.ResponseWriter, r *http.Request) {
	h.scan(w, r, "", 100, MaxScanLimit)
}

// demoScan: GET /v1/demo/scan, restricted to the demo/ namespace.
func (h *handler) demoScan(w http.ResponseWriter, r *http.Request) {
	h.scan(w, r, DemoPrefix, 20, 50)
}

// flush: POST /v1/admin/flush writes the memtable to an SSTable.
func (h *handler) flush(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Flush(); err != nil {
		h.dbError(w, err)
		return
	}
	h.stats(w, r)
}

// compact: POST /v1/admin/compact merges all SSTables into one.
func (h *handler) compact(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Flush(); err != nil {
		h.dbError(w, err)
		return
	}
	if err := h.db.Compact(); err != nil {
		h.dbError(w, err)
		return
	}
	h.stats(w, r)
}
