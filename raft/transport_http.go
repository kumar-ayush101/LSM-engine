package raft

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTP transport for Raft peer RPCs.
//
// This is a SEPARATE listener from the client-facing KV API. Peers talk to
// each other over it, authenticated with a shared cluster secret (a bearer
// token distinct from the client token). Keeping the two planes apart means
// a client can never inject a RequestVote, and the cluster secret never has
// to be shared with KV clients.
//
// Wire format: JSON over POST. Raft RPCs are small and infrequent (heartbeats
// a few times a second, entries only on writes), so JSON's overhead is
// irrelevant and its debuggability is worth a lot.

const (
	// RPCVotePath and RPCAppendPath are the internal RPC endpoints.
	RPCVotePath   = "/raft/vote"
	RPCAppendPath = "/raft/append"

	rpcHeader = "X-Raft-Cluster"
)

// HTTPTransport sends Raft RPCs to peers over HTTP and (via Handler) serves
// incoming ones. Peer addresses in Config.Peers are base URLs like
// "http://10.0.0.2:9001".
type HTTPTransport struct {
	secretHash [32]byte
	client     *http.Client
}

// NewHTTPTransport builds a transport. secret is the shared cluster secret;
// timeout bounds each RPC (0 means a sensible default).
func NewHTTPTransport(secret string, timeout time.Duration) *HTTPTransport {
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	return &HTTPTransport{
		secretHash: sha256.Sum256([]byte(secret)),
		client:     &http.Client{Timeout: timeout},
	}
}

func (h *HTTPTransport) post(peerAddr, path string, req, resp any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequest(http.MethodPost, peerAddr+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(rpcHeader, encodeSecret(h.secretHash))
	r, err := h.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		io.Copy(io.Discard, r.Body)
		return fmt.Errorf("raft rpc %s: status %d", path, r.StatusCode)
	}
	return json.NewDecoder(r.Body).Decode(resp)
}

func (h *HTTPTransport) SendRequestVote(peerAddr string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	err := h.post(peerAddr, RPCVotePath, args, &reply)
	return reply, err
}

func (h *HTTPTransport) SendAppendEntries(peerAddr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	err := h.post(peerAddr, RPCAppendPath, args, &reply)
	return reply, err
}

// Handler returns an http.Handler serving the two RPC endpoints for node n,
// authenticated with the same cluster secret. Mount it on the internal peer
// listener (NOT the public client mux).
func (h *HTTPTransport) Handler(n *Node) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+RPCVotePath, h.authed(func(w http.ResponseWriter, r *http.Request) {
		var args RequestVoteArgs
		if !decode(w, r, &args) {
			return
		}
		writeJSON(w, n.HandleRequestVote(args))
	}))
	mux.HandleFunc("POST "+RPCAppendPath, h.authed(func(w http.ResponseWriter, r *http.Request) {
		var args AppendEntriesArgs
		if !decode(w, r, &args) {
			return
		}
		writeJSON(w, n.HandleAppendEntries(args))
	}))
	return mux
}

func (h *HTTPTransport) authed(next http.HandlerFunc) http.HandlerFunc {
	want := encodeSecret(h.secretHash)
	return func(w http.ResponseWriter, r *http.Request) {
		got := r.Header.Get(rpcHeader)
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next(w, r)
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(io.LimitReader(r.Body, MaxCommandSize+1<<20)).Decode(v); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// The secret is sent hashed so it never travels in cleartext in the header,
// and compared in constant time. (TLS should still protect the peer plane in
// production; this just avoids logging the raw secret.)
func encodeSecret(h [32]byte) string {
	const hex = "0123456789abcdef"
	b := make([]byte, 64)
	for i, c := range h {
		b[i*2] = hex[c>>4]
		b[i*2+1] = hex[c&0xf]
	}
	return string(b)
}
