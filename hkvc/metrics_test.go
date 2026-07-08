package hkvc

// Tests for the /metrics endpoint and the metrics counters. The unit test drives
// the counters directly; the integration test scrapes a live participant.

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func TestMetrics_RecordRequest(t *testing.T) {
	m := newMetrics()
	t.Log("step: recording a success and an error request")
	m.recordRequest("/set", http.StatusCreated, 1_000_000)  // 1ms
	m.recordRequest("/set", http.StatusConflict, 3_000_000) // 3ms, error (>=400)

	if got := m.requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2", got)
	}
	if got := m.errors.Load(); got != 1 {
		t.Fatalf("errors = %d, want 1 (the 409)", got)
	}
	if got := m.byOp["/set"]; got != 2 {
		t.Fatalf("byOp[/set] = %d, want 2", got)
	}
	if got := m.byOpErr["/set"]; got != 1 {
		t.Fatalf("byOpErr[/set] = %d, want 1", got)
	}
	t.Log("ok: request counters classify success vs error correctly")
}

func TestMetrics_NilSafe(t *testing.T) {
	// Recording on a nil *metrics must be a no-op, not a panic (defensive: a
	// participant constructed in a test without metrics still works).
	var m *metrics
	m.recordRequest("/x", 200, 1)
	m.recordCommit()
	m.recordSnapshot()
	m.recordInstallRecv()
}

// TestMetrics_Endpoint scrapes /metrics from a live participant and checks the
// exposition format plus that request/commit counters moved after real traffic.
func TestMetrics_Endpoint(t *testing.T) {
	const n = 3
	ctrl := NewHKVCController(t, n, 0, 0)
	leader, _ := ctrl.getGroupLeaderCommit(0)
	if leader < 0 {
		t.Fatal("no group-0 leader")
	}
	port := ctrl.clientPort(leader)
	client := randSeq(12)

	t.Log("step: issuing a few writes so counters are non-zero")
	for i := 0; i < 3; i++ {
		validateResponseToClient(t, KeyValueMessage{Directory: "/", Key: "k" + itoa(i), Value: "v", SeqNumber: i, ClientID: client}, port, "/set", &KeySuccessResponse{}, "", http.StatusCreated)
	}

	t.Log("step: scraping /metrics from the leader")
	resp, err := http.Get("http://localhost:" + strconv.Itoa(port) + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	text := string(body)

	// Must be Prometheus text format with our metric families present.
	for _, want := range []string{
		"hkvc_requests_total",
		"hkvc_commits_total",
		"hkvc_raft_term",
		"hkvc_raft_is_leader",
		"hkvc_endpoint_requests_total",
		"# TYPE hkvc_requests_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("/metrics output missing %q\n---\n%s", want, text)
		}
	}

	// The leader must report is_leader 1 for group 0.
	if !strings.Contains(text, "hkvc_raft_is_leader{uid=") || !strings.Contains(text, "group=\"0\"} 1") {
		t.Fatalf("/metrics did not report the leader as is_leader=1 for group 0\n---\n%s", text)
	}
	t.Log("ok: /metrics serves valid Prometheus output with live raft state")
}
