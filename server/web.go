package server

import (
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	"time"
)

//go:embed web
var webFS embed.FS

var indexTmpl = template.Must(template.ParseFS(webFS, "web/index.html"))

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

type indexData struct {
	RepoURL       string
	Entries       int
	MemtableBytes int64
	MaxMemtable   int64
	WALBytes      int64
	LastSeq       uint64
	SyncPolicy    string
	Uptime        string
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	setPageSecurityHeaders(w)
	if err := indexTmpl.Execute(w, data); err != nil {
		h.log.Error("rendering index", "err", err)
	}
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
