package hkvc

// metrics.go exposes per-participant operational metrics in the Prometheus text
// exposition format at GET /metrics, plus a small structured-logging helper.
// Counters are plain atomics so recording them adds negligible overhead and
// never contends on p.mu.

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// metrics holds atomic counters for a single participant. Per-endpoint request
// and error counts live in maps keyed by endpoint name, guarded by their own
// tiny mutex (write-rarely, read-on-scrape).
type metrics struct {
	requests    atomic.Int64 // total client HTTP requests received
	errors      atomic.Int64 // total client requests answered with an error status (>=400)
	commits     atomic.Int64 // raft commands committed via this participant (as leader)
	snapshots   atomic.Int64 // snapshots taken by this participant
	installRecv atomic.Int64 // snapshots installed from a leader (as follower)

	mu       sync.Mutex
	byOp     map[string]int64 // endpoint -> request count
	byOpErr  map[string]int64 // endpoint -> error count
	nanosSum map[string]int64 // endpoint -> summed handler latency (ns)
}

func newMetrics() *metrics {
	return &metrics{
		byOp:     make(map[string]int64),
		byOpErr:  make(map[string]int64),
		nanosSum: make(map[string]int64),
	}
}

// recordRequest tallies one handled request: its endpoint, whether it errored
// (HTTP status >= 400), and how long the handler took in nanoseconds.
func (m *metrics) recordRequest(endpoint string, status int, elapsedNanos int64) {
	if m == nil {
		return
	}
	m.requests.Add(1)
	errored := status >= 400
	if errored {
		m.errors.Add(1)
	}
	m.mu.Lock()
	m.byOp[endpoint]++
	m.nanosSum[endpoint] += elapsedNanos
	if errored {
		m.byOpErr[endpoint]++
	}
	m.mu.Unlock()
}

func (m *metrics) recordCommit() {
	if m != nil {
		m.commits.Add(1)
	}
}
func (m *metrics) recordSnapshot() {
	if m != nil {
		m.snapshots.Add(1)
	}
}
func (m *metrics) recordInstallRecv() {
	if m != nil {
		m.installRecv.Add(1)
	}
}

// statusRecorder wraps http.ResponseWriter to remember the status code written,
// so the instrumentation middleware can classify a request as success/error.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// instrument wraps an endpoint handler so each call is timed and its outcome
// recorded to the participant's metrics. The endpoint label is fixed per route.
func (p *HKVCParticipant) instrument(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(rec, r)
		p.metrics.recordRequest(endpoint, rec.status, time.Since(start).Nanoseconds())
	}
}

// handleMetrics serves the metrics in Prometheus text format. It also folds in
// live raft state (term, commit index, leadership) sampled at scrape time.
func (p *HKVCParticipant) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m := p.metrics
	var b strings.Builder

	writeCounter := func(name, help string, val int64) {
		fmt.Fprintf(&b, "# HELP hkvc_%s %s\n# TYPE hkvc_%s counter\n", name, help, name)
		fmt.Fprintf(&b, "hkvc_%s{uid=\"%d\"} %d\n", name, p.uid, val)
	}
	writeCounter("requests_total", "Total client requests received.", m.requests.Load())
	writeCounter("request_errors_total", "Total client requests answered with an error status.", m.errors.Load())
	writeCounter("commits_total", "Raft commands committed via this participant.", m.commits.Load())
	writeCounter("snapshots_total", "Snapshots taken by this participant.", m.snapshots.Load())
	writeCounter("snapshot_installs_total", "Snapshots installed from a leader.", m.installRecv.Load())

	// Per-endpoint request/error counts and average latency.
	m.mu.Lock()
	endpoints := make([]string, 0, len(m.byOp))
	for ep := range m.byOp {
		endpoints = append(endpoints, ep)
	}
	sort.Strings(endpoints)
	fmt.Fprintf(&b, "# HELP hkvc_endpoint_requests_total Requests per endpoint.\n# TYPE hkvc_endpoint_requests_total counter\n")
	for _, ep := range endpoints {
		fmt.Fprintf(&b, "hkvc_endpoint_requests_total{uid=\"%d\",endpoint=\"%s\"} %d\n", p.uid, ep, m.byOp[ep])
	}
	fmt.Fprintf(&b, "# HELP hkvc_endpoint_latency_avg_ms Average handler latency per endpoint in milliseconds.\n# TYPE hkvc_endpoint_latency_avg_ms gauge\n")
	for _, ep := range endpoints {
		avgMs := 0.0
		if m.byOp[ep] > 0 {
			avgMs = float64(m.nanosSum[ep]) / float64(m.byOp[ep]) / 1e6
		}
		fmt.Fprintf(&b, "hkvc_endpoint_latency_avg_ms{uid=\"%d\",endpoint=\"%s\"} %.3f\n", p.uid, ep, avgMs)
	}
	m.mu.Unlock()

	// Live raft state per group, sampled now.
	p.mu.Lock()
	gids := make([]int, 0, len(p.raftPeers))
	for gid := range p.raftPeers {
		gids = append(gids, gid)
	}
	p.mu.Unlock()
	sort.Ints(gids)
	fmt.Fprintf(&b, "# HELP hkvc_raft_term Current raft term per group.\n# TYPE hkvc_raft_term gauge\n")
	fmt.Fprintf(&b, "# HELP hkvc_raft_commit_index Current raft commit index per group.\n# TYPE hkvc_raft_commit_index gauge\n")
	fmt.Fprintf(&b, "# HELP hkvc_raft_is_leader Whether this participant leads the group (1/0).\n# TYPE hkvc_raft_is_leader gauge\n")
	for _, gid := range gids {
		p.mu.Lock()
		rp := p.raftPeers[gid]
		p.mu.Unlock()
		sr, _ := rp.GetStatus()
		leader := 0
		if sr.IsLeader {
			leader = 1
		}
		fmt.Fprintf(&b, "hkvc_raft_term{uid=\"%d\",group=\"%d\"} %d\n", p.uid, gid, sr.Term)
		fmt.Fprintf(&b, "hkvc_raft_commit_index{uid=\"%d\",group=\"%d\"} %d\n", p.uid, gid, sr.CommitIndex)
		fmt.Fprintf(&b, "hkvc_raft_is_leader{uid=\"%d\",group=\"%d\"} %d\n", p.uid, gid, leader)
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(b.String()))
}

// newParticipantLogger builds a structured logger tagged with the participant's
// uid, writing text to stderr. Level defaults to Info; set HKVC_LOG_LEVEL=debug
// for verbose output.
func newParticipantLogger(uid int) *slog.Logger {
	level := slog.LevelInfo
	if strings.EqualFold(os.Getenv("HKVC_LOG_LEVEL"), "debug") {
		level = slog.LevelDebug
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("component", "hkvc", "uid", uid)
}
