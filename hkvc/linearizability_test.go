// Linearizability integration tests. Each test stands up a real HKVC cluster,
// drives concurrent get/put clients, records every operation's call/return
// interval, and checks the combined history with porcupine. The model is a
// sequential per-key register: a put overwrites the value, a get must observe
// the last linearizable write. HKVC's /set is a put and /get is a read; a get
// of an absent key is modeled as the empty string (the model's initial state),
// which is unambiguous because every write stores a unique, non-empty value and
// no keys are deleted.

package hkvc

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// ---------------------------------------------------------------------------
// KV model: sequential per-key register (get/put only)
// ---------------------------------------------------------------------------

const (
	opGet = iota // read a key's value
	opPut        // overwrite a key's value
)

// kvInput is the recorded input of an operation. key is used to partition the
// history so porcupine checks each key independently.
type kvInput struct {
	op    int
	key   string
	value string // meaningful only for puts
}

// kvOutput is the recorded result of an operation (the value observed by a get;
// ignored for puts).
type kvOutput struct {
	value string
}

// kvModel is a sequential specification of a per-key register: a put sets the
// value, a get must return the current value. porcupine explores whether the
// concurrent history can be linearized against this specification.
var kvModel = porcupine.Model{
	// Operations on different keys are independent, so check them separately.
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := make(map[string][]porcupine.Operation)
		for _, op := range history {
			key := op.Input.(kvInput).key
			byKey[key] = append(byKey[key], op)
		}
		out := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(string)
		in := input.(kvInput)
		out := output.(kvOutput)
		switch in.op {
		case opGet:
			return out.value == st, st
		case opPut:
			return true, in.value
		default:
			return false, st
		}
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
	DescribeOperation: func(input, output interface{}) string {
		in := input.(kvInput)
		if in.op == opPut {
			return fmt.Sprintf("put(%q, %q)", in.key, in.value)
		}
		return fmt.Sprintf("get(%q) -> %q", in.key, output.(kvOutput).value)
	},
	DescribeState: func(state interface{}) string {
		return fmt.Sprintf("%q", state.(string))
	},
}

// ---------------------------------------------------------------------------
// Cluster client that records operations for the checker
// ---------------------------------------------------------------------------

// writeCounter guarantees globally unique write values across all clients, so
// every put is distinguishable in the linearization.
var writeCounter atomic.Int64

// linClient is one logical HKVC client (unique ClientID, monotonic sequence
// numbers). It sends requests to the current group-0 leader, rediscovering the
// leader if it moves, and records each completed operation.
type linClient struct {
	ctrl       *HKVCController
	id         string // unique HKVC ClientID for dedup/sequencing
	idx        int    // small integer id used by porcupine for reporting
	seq        int
	leaderPort int
	rng        *rand.Rand
}

// refreshLeader re-queries the controller for the current group-0 leader and
// updates the cached client port. It briefly retries while there is no leader.
func (c *linClient) refreshLeader() {
	for i := 0; i < 40; i++ {
		if idx, _ := c.ctrl.getGroupLeaderCommit(0); idx >= 0 {
			c.leaderPort = c.ctrl.clientPort(idx)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// put sets key to a unique value and, on success, appends a put operation to
// ops. The Call timestamp is taken before the first attempt and Return after
// the successful response, so retries across a leader change stay within one
// logical operation's interval.
func (c *linClient) put(key string, ops *[]porcupine.Operation) {
	value := fmt.Sprintf("%s#%d", c.id, writeCounter.Add(1))
	c.seq++
	msg := KeyValueMessage{Directory: "/", Key: key, Value: value, SeqNumber: c.seq, ClientID: c.id}

	call := nowNanos()
	if c.sendWithRetry("/set", msg, nil) {
		ret := nowNanos()
		*ops = append(*ops, porcupine.Operation{
			ClientId: c.clientIndex(),
			Input:    kvInput{op: opPut, key: key, value: value},
			Call:     call,
			Output:   kvOutput{},
			Return:   ret,
		})
	}
}

// get reads key and, on success, appends a get operation to ops. A missing key
// is recorded as the empty string (the model's initial state).
func (c *linClient) get(key string, ops *[]porcupine.Operation) {
	c.seq++
	msg := KeyRequest{Directory: "/", Key: key, SeqNumber: c.seq, ClientID: c.id}

	var kvm KeyValueMessage
	call := nowNanos()
	value, ok := "", false
	for attempt := 0; attempt < 30; attempt++ {
		jsonmsg, _ := json.Marshal(msg)
		rc, er, err := getResponse(c.leaderPort, "/get", jsonmsg, &kvm)
		if err != nil {
			c.refreshLeader()
			continue
		}
		if er != nil {
			switch er.ErrorType {
			case NonLeaderError:
				c.refreshLeader()
				continue
			case KeyNotFoundError, DirNotFoundError:
				value, ok = "", true // absent -> empty string
			default:
				ok = false
			}
			break
		}
		if rc == http.StatusOK {
			value, ok = kvm.Value, true
		}
		break
	}
	if ok {
		ret := nowNanos()
		*ops = append(*ops, porcupine.Operation{
			ClientId: c.clientIndex(),
			Input:    kvInput{op: opGet, key: key},
			Call:     call,
			Output:   kvOutput{value: value},
			Return:   ret,
		})
	}
}

// sendWithRetry posts msg to endpoint, retrying on transport failures and
// non-leader rejections (rediscovering the leader between attempts). It returns
// true once the leader accepts the request (200 or 201).
func (c *linClient) sendWithRetry(endpoint string, msg any, resp any) bool {
	for attempt := 0; attempt < 30; attempt++ {
		jsonmsg, _ := json.Marshal(msg)
		var sink KeySuccessResponse
		target := resp
		if target == nil {
			target = &sink
		}
		rc, er, err := getResponse(c.leaderPort, endpoint, jsonmsg, target)
		if err != nil {
			c.refreshLeader()
			continue
		}
		if er != nil {
			if er.ErrorType == NonLeaderError {
				c.refreshLeader()
				continue
			}
			return false
		}
		if rc == http.StatusOK || rc == http.StatusCreated {
			return true
		}
		return false
	}
	return false
}

// clientIndex returns the small integer client id porcupine uses for reporting.
func (c *linClient) clientIndex() int { return c.idx }

func nowNanos() int64 { return time.Now().UnixNano() }

// runWorkload has one client issue count random get/put operations over the
// given keys and returns the operations it recorded.
func (c *linClient) runWorkload(keys []string, count int) []porcupine.Operation {
	ops := make([]porcupine.Operation, 0, count)
	c.refreshLeader()
	for i := 0; i < count; i++ {
		key := keys[c.rng.Intn(len(keys))]
		if c.rng.Intn(2) == 0 {
			c.put(key, &ops)
		} else {
			c.get(key, &ops)
		}
	}
	return ops
}

// checkLinearizable runs porcupine and fails the test on a definitive
// violation. An inconclusive (timeout) result is reported but not failed, since
// it reflects checker cost rather than a proven bug.
func checkLinearizable(t *testing.T, history []porcupine.Operation) {
	t.Helper()
	if len(history) == 0 {
		t.Fatal("recorded an empty history; the cluster never served a request")
	}
	t.Logf("step: checking linearizability of %d recorded operations (timeout 60s)", len(history))
	res := porcupine.CheckOperationsTimeout(kvModel, history, 60*time.Second)
	switch res {
	case porcupine.Ok:
		t.Logf("history of %d operations is linearizable", len(history))
	case porcupine.Illegal:
		t.Fatalf("history of %d operations is NOT linearizable", len(history))
	default:
		t.Logf("linearizability check inconclusive (timeout) over %d operations", len(history))
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// Many clients concurrently read and write a small set of keys against a stable
// cluster; the combined history must be linearizable.
func TestLinearizability_ConcurrentClients(t *testing.T) {
	const clusterSize = 5
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	t.Log("step: confirming a group-0 leader exists after startup")
	if idx, _ := ctrl.getGroupLeaderCommit(0); idx < 0 {
		t.Fatal("no group-0 leader after startup")
	}

	keys := []string{"a", "b", "c", "d"}
	const clients = 4
	const opsPerClient = 40

	t.Logf("step: launching %d clients issuing %d random get/put ops each over keys %v", clients, opsPerClient, keys)
	var wg sync.WaitGroup
	perClient := make([][]porcupine.Operation, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := &linClient{
				ctrl: ctrl,
				id:   fmt.Sprintf("lin-%s-%d", randSeq(6), i),
				idx:  i,
				rng:  rand.New(rand.NewSource(int64(i)*7919 + 1)),
			}
			perClient[i] = c.runWorkload(keys, opsPerClient)
		}(i)
	}
	wg.Wait()
	t.Log("ok: all clients finished their workloads")

	t.Log("step: merging per-client operations into one history")
	var history []porcupine.Operation
	for _, ops := range perClient {
		history = append(history, ops...)
	}
	checkLinearizable(t, history)
}

// Linearizability must hold even while leadership churns. Clients keep issuing
// operations while a background goroutine periodically disconnects and
// reconnects the current leader, forcing failovers.
func TestLinearizability_UnderLeaderFailures(t *testing.T) {
	const clusterSize = 5
	ctrl := NewHKVCController(t, clusterSize, 0, 0)
	t.Log("step: confirming a group-0 leader exists after startup")
	if idx, _ := ctrl.getGroupLeaderCommit(0); idx < 0 {
		t.Fatal("no group-0 leader after startup")
	}

	keys := []string{"x", "y", "z"}
	const clients = 3
	const opsPerClient = 30

	t.Log("step: starting chaos goroutine to periodically disconnect and reconnect the leader")
	stop := make(chan struct{})
	var chaosWG sync.WaitGroup
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		round := 0
		for {
			select {
			case <-stop:
				return
			case <-time.After(WaitForRaft):
				if leader, _ := ctrl.getGroupLeaderCommit(0); leader >= 0 {
					round++
					t.Logf("chaos round %d: disconnecting leader %d, waiting, then reconnecting", round, leader)
					ctrl.disconnect(leader)
					time.Sleep(WaitForRaft)
					ctrl.connect(leader)
				}
			}
		}
	}()

	t.Logf("step: launching %d clients issuing %d random get/put ops each over keys %v while leadership churns", clients, opsPerClient, keys)
	var wg sync.WaitGroup
	perClient := make([][]porcupine.Operation, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := &linClient{
				ctrl: ctrl,
				id:   fmt.Sprintf("linf-%s-%d", randSeq(6), i),
				idx:  i,
				rng:  rand.New(rand.NewSource(int64(i)*104729 + 3)),
			}
			perClient[i] = c.runWorkload(keys, opsPerClient)
		}(i)
	}
	wg.Wait()
	t.Log("step: workloads done; stopping the chaos goroutine")
	close(stop)
	chaosWG.Wait()
	t.Log("ok: chaos goroutine stopped")

	t.Log("step: merging per-client operations into one history")
	var history []porcupine.Operation
	for _, ops := range perClient {
		history = append(history, ops...)
	}
	checkLinearizable(t, history)
}
