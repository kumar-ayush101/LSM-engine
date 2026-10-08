package server

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// rateLimiter is a token-bucket limiter keyed by string (a client IP, or ""
// for a single global bucket). Each key refills at rate tokens/second up to
// burst tokens.
//
// The number of tracked keys is bounded: idle buckets (already refilled to
// full, so forgetting them changes nothing) are pruned when the table is
// full, and if it is still full the request is rejected. Failing closed
// keeps memory bounded even under a flood of distinct addresses.
type rateLimiter struct {
	mu         sync.Mutex
	rate       float64 // tokens per second
	burst      float64
	maxEntries int
	buckets    map[string]*bucket
	now        func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(perMinute, burst, maxEntries int) *rateLimiter {
	return &rateLimiter{
		rate:       float64(perMinute) / 60,
		burst:      float64(burst),
		maxEntries: maxEntries,
		buckets:    make(map[string]*bucket),
		now:        time.Now,
	}
}

// allow consumes one token for key. If it returns false, retryAfter is how
// long until a token is available.
func (l *rateLimiter) allow(key string) (ok bool, retryAfter time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()

	b := l.buckets[key]
	if b == nil {
		if len(l.buckets) >= l.maxEntries {
			l.prune(now)
			if len(l.buckets) >= l.maxEntries {
				return false, time.Second
			}
		}
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(l.burst, b.tokens+elapsed*l.rate)
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
	return false, max(wait, time.Second)
}

// prune drops buckets that have been idle long enough to be full again.
func (l *rateLimiter) prune(now time.Time) {
	full := time.Duration(l.burst / l.rate * float64(time.Second))
	for k, b := range l.buckets {
		if now.Sub(b.last) >= full {
			delete(l.buckets, k)
		}
	}
}

// clientIP returns the address used for per-client rate limiting.
//
// With trustProxy, it takes the rightmost X-Forwarded-For entry: the address
// our own reverse proxy saw. Entries further left are supplied by the client
// and trivially spoofed. Without trustProxy (no proxy in front), it uses the
// TCP peer address.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if xff := r.Header.Values("X-Forwarded-For"); len(xff) > 0 {
			parts := strings.Split(xff[len(xff)-1], ",")
			if ip := strings.TrimSpace(parts[len(parts)-1]); ip != "" {
				return ip
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
