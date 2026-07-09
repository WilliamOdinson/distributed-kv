package main

// Unit tests for hkvcctl's request routing: it must skip participants that
// answer HKVCNonRaftLeaderError, surface real application errors, and decode a
// success response from whichever address is the leader. These use httptest
// servers, so they need no real cluster and run instantly.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newClientFor builds a client pointed at the given test-server URLs (their
// host:port), in order.
func newClientFor(t *testing.T, servers ...*httptest.Server) *client {
	t.Helper()
	addrs := make([]string, len(servers))
	for i, s := range servers {
		addrs[i] = strings.TrimPrefix(s.URL, "http://")
	}
	return &client{
		addrs:    addrs,
		clientID: "test",
		http:     &http.Client{Timeout: 5 * time.Second},
	}
}

// notLeaderServer answers every request with HKVCNonRaftLeaderError.
func notLeaderServer(t *testing.T) *httptest.Server {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(errorResponse{ErrorType: "HKVCNonRaftLeaderError", ErrorInfo: "not leader"})
	}))
	t.Cleanup(s.Close)
	return s
}

func TestDo_SkipsNonLeaderThenSucceeds(t *testing.T) {
	follower := notLeaderServer(t)

	var gotBody keyValueMessage
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(keyValueMessage{Directory: "/", Key: "k", Value: "v"})
	}))
	t.Cleanup(leader.Close)

	c := newClientFor(t, follower, leader) // follower first, leader second

	var out keyValueMessage
	code, err := c.do("/get", keyRequest{Directory: "/", Key: "k", ClientID: "test"}, &out)
	if err != nil {
		t.Fatalf("do returned error: %v", err)
	}
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if out.Value != "v" {
		t.Fatalf("decoded value = %q, want v", out.Value)
	}
	if gotBody.Key != "k" {
		t.Fatalf("leader received key %q, want k", gotBody.Key)
	}
}

func TestDo_ReturnsApplicationError(t *testing.T) {
	// A conflict is a real application error, not a leadership problem: do must
	// surface it immediately rather than trying other addresses.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(errorResponse{ErrorType: "HKVCConflictExistingKeyError", ErrorInfo: "path conflicts with existing key"})
	}))
	t.Cleanup(srv.Close)

	c := newClientFor(t, srv)
	_, err := c.do("/set", keyValueMessage{Directory: "/a/k", Key: "x", ClientID: "test"}, nil)
	if err == nil {
		t.Fatal("expected an application error, got nil")
	}
	if !strings.Contains(err.Error(), "HKVCConflictExistingKeyError") {
		t.Fatalf("error = %v, want it to mention the conflict type", err)
	}
}

func TestDo_NoLeaderFound(t *testing.T) {
	// Every address answers not-leader: do must fail with a clear message.
	c := newClientFor(t, notLeaderServer(t), notLeaderServer(t))
	_, err := c.do("/get", keyRequest{Directory: "/", Key: "k", ClientID: "test"}, nil)
	if err == nil {
		t.Fatal("expected a no-leader error, got nil")
	}
	if !strings.Contains(err.Error(), "no leader found") {
		t.Fatalf("error = %v, want 'no leader found'", err)
	}
}

func TestSplitAddrs(t *testing.T) {
	got := splitAddrs(" a:1, b:2 ,,c:3 ")
	want := []string{"a:1", "b:2", "c:3"}
	if len(got) != len(want) {
		t.Fatalf("splitAddrs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("splitAddrs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
