package server

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"time"
)

//go:embed web
var webFS embed.FS

var indexTmpl = template.Must(template.New("index.html").
	Funcs(template.FuncMap{"bytes": humanBytes}).
	ParseFS(webFS, "web/index.html"))

// staticFS serves web/static at /static/.
var staticFS = func() fs.FS {
	sub, err := fs.Sub(webFS, "web/static")
	if err != nil {
		panic(err)
	}
	return sub
}()

// RepoURL is linked from the landing page.
const RepoURL = "https://github.com/kumar-ayush101/LSM-engine"

// humanBytes formats n as B, KiB, MiB or GiB.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 2; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMG"[exp])
}

type indexData struct {
	RepoURL       string
	Entries       int
	MemtableBytes int64
	MaxMemtable   int64
	MemPercent    int
	WALBytes      int64
	LastSeq       uint64
	SyncPolicy    string
	Uptime        string

	DemoEnabled   bool
	DemoPrefix    string
	DemoMaxKey    int
	DemoMaxValue  int64
	DemoPerMinute int
}

// index renders the public landing page. It shows aggregate counters only
// (no keys or values), so it needs no authentication.
func (h *handler) index(w http.ResponseWriter, r *http.Request) {
	st := h.db.Stats()
	data := indexData{
		RepoURL:       RepoURL,
		Entries:       st.Entries,
		MemtableBytes: st.MemtableBytes,
		MaxMemtable:   st.MaxMemtable,
		WALBytes:      st.WALBytes,
		LastSeq:       st.LastSeq,
		SyncPolicy:    st.SyncPolicy,
		Uptime:        time.Since(h.started).Round(time.Second).String(),
	}
	if st.MaxMemtable > 0 {
		data.MemPercent = int(st.MemtableBytes * 100 / st.MaxMemtable)
	}
	if h.demo != nil {
		data.DemoEnabled = true
		data.DemoPrefix = DemoPrefix
		data.DemoMaxKey = h.demo.cfg.MaxKeyBytes
		data.DemoMaxValue = h.demo.cfg.MaxValueBytes
		data.DemoPerMinute = h.demo.cfg.PerIPPerMinute
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	setPageSecurityHeaders(w)
	if err := indexTmpl.Execute(w, data); err != nil {
		h.log.Error("rendering index", "err", err)
	}
}

// PublicStats is served at GET /stats.json without auth. It carries the
// same aggregate counters the landing page already shows (never keys or
// values), so the page can refresh them live. It is not rate limited: like
// the landing page it only reads a few counters.
type PublicStats struct {
	Entries        int    `json:"entries"`
	LastSeq        uint64 `json:"last_seq"`
	WALBytes       int64  `json:"wal_bytes"`
	MemtableBytes  int64  `json:"memtable_bytes"`
	MemtableLimit  int64  `json:"memtable_limit"`
	SyncPolicy     string `json:"sync_policy"`
	UptimeSeconds  int64  `json:"uptime_seconds"`
	DemoEnabled    bool   `json:"demo_enabled"`
	DemoBytesUsed  int64  `json:"demo_bytes_used,omitempty"`
	DemoBytesLimit int64  `json:"demo_bytes_limit,omitempty"`
}

func (h *handler) publicStats(w http.ResponseWriter, r *http.Request) {
	st := h.db.Stats()
	ps := PublicStats{
		Entries:       st.Entries,
		LastSeq:       st.LastSeq,
		WALBytes:      st.WALBytes,
		MemtableBytes: st.MemtableBytes,
		MemtableLimit: st.MaxMemtable,
		SyncPolicy:    st.SyncPolicy,
		UptimeSeconds: int64(time.Since(h.started).Seconds()),
	}
	if h.demo != nil {
		ps.DemoEnabled = true
		ps.DemoBytesUsed = h.demo.used.Load()
		ps.DemoBytesLimit = h.demo.cfg.MaxTotalBytes
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	json.NewEncoder(w).Encode(ps)
}

func (h *handler) static() http.Handler {
	fsrv := http.StripPrefix("/static/", http.FileServerFS(staticFS))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=300")
		setPageSecurityHeaders(w)
		fsrv.ServeHTTP(w, r)
	})
}

// setPageSecurityHeaders locks the page down to same-origin resources: no
// inline scripts, no third-party content, no framing.
func setPageSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Security-Policy",
		"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
}
