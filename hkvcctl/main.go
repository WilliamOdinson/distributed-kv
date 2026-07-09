// Command hkvcctl is a command-line client for an HKVC cluster. It speaks the
// participant HTTP API directly (std-lib only) and transparently finds the
// group's leader by trying every address until one accepts the request.
//
// Usage:
//
//	hkvcctl -addrs host1:port1,host2:port2,... <command> [args]
//
// Commands:
//
//	ls   <dir>                 list the children of a directory
//	get  <dir> <key>           print the value of a key
//	set  <dir> <key> <value>   create or overwrite a key
//	create <dir> <key>         create a subdirectory
//	rm   <dir> <key>           delete a key or subdirectory
//	stat <dir> <key>           print metadata for a key or directory (key "." = the dir)
//	metrics                    print each address's /metrics
//
// Each invocation uses a fresh random client id, so sequence numbers always
// start at 0 and a human never has to track them.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// ---- wire types (mirror hkvc's JSON contract) --------------------------------

type directoryRequest struct {
	Directory string `json:"directory"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

type keyRequest struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

type keyValueMessage struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	SeqNumber int    `json:"seq_number"`
	ClientID  string `json:"client_id"`
}

type listResponse struct {
	Directory string   `json:"directory"`
	List      []string `json:"list"`
}

type keySuccessResponse struct {
	Directory string `json:"directory"`
	Key       string `json:"key"`
	Success   bool   `json:"success"`
}

type metadataResponse struct {
	Directory   string   `json:"directory"`
	Key         string   `json:"key"`
	IsDirectory bool     `json:"is_directory"`
	Size        int      `json:"size"`
	Version     uint64   `json:"version"`
	PAddrList   []string `json:"p_addr_list"`
	LeaderIdx   int      `json:"leader_index"`
}

type errorResponse struct {
	ErrorType string `json:"error_type"`
	ErrorInfo string `json:"error_info"`
}

// errNotLeader signals that the addressed participant is not the leader, so the
// caller should try the next address.
var errNotLeader = errors.New("not leader")

// client drives a set of participant addresses with a stable per-run identity.
type client struct {
	addrs    []string
	clientID string
	seq      int
	http     *http.Client
}

// do sends msg to endpoint, trying each address until one answers as leader.
// On success it decodes into out (if non-nil) and returns the HTTP status.
func (c *client) do(endpoint string, msg any, out any) (int, error) {
	c.seq++
	body, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	var lastErr error
	for _, addr := range c.addrs {
		url := "http://" + addr + endpoint
		resp, err := c.http.Post(url, "application/json", bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue // participant down; try the next
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if strings.Contains(string(raw), "error_type") {
			var e errorResponse
			_ = json.Unmarshal(raw, &e)
			if e.ErrorType == "HKVCNonRaftLeaderError" {
				lastErr = errNotLeader
				continue // wrong participant; try the next
			}
			// A real application error (bad request, not found, conflict, ...).
			return resp.StatusCode, fmt.Errorf("%s: %s", e.ErrorType, e.ErrorInfo)
		}
		if out != nil {
			if err := json.Unmarshal(raw, out); err != nil {
				return resp.StatusCode, fmt.Errorf("decoding response: %w", err)
			}
		}
		return resp.StatusCode, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no addresses responded")
	}
	return 0, fmt.Errorf("no leader found among %v: %w", c.addrs, lastErr)
}

func main() {
	addrs := flag.String("addrs", "localhost:15440", "comma-separated participant client addresses")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}

	c := &client{
		addrs:    splitAddrs(*addrs),
		clientID: "hkvcctl-" + randID(),
		http:     &http.Client{Timeout: 10 * time.Second},
	}

	if err := run(c, args); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(c *client, args []string) error {
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "ls":
		if len(rest) != 1 {
			return errors.New("usage: ls <dir>")
		}
		var lr listResponse
		if _, err := c.do("/list", directoryRequest{Directory: rest[0], SeqNumber: c.seq, ClientID: c.clientID}, &lr); err != nil {
			return err
		}
		for _, name := range lr.List {
			fmt.Println(name)
		}
		return nil

	case "get":
		if len(rest) != 2 {
			return errors.New("usage: get <dir> <key>")
		}
		var kv keyValueMessage
		if _, err := c.do("/get", keyRequest{Directory: rest[0], Key: rest[1], SeqNumber: c.seq, ClientID: c.clientID}, &kv); err != nil {
			return err
		}
		fmt.Println(kv.Value)
		return nil

	case "set":
		if len(rest) != 3 {
			return errors.New("usage: set <dir> <key> <value>")
		}
		var ksr keySuccessResponse
		code, err := c.do("/set", keyValueMessage{Directory: rest[0], Key: rest[1], Value: rest[2], SeqNumber: c.seq, ClientID: c.clientID}, &ksr)
		if err != nil {
			return err
		}
		fmt.Printf("ok (%s, success=%v)\n", statusWord(code), ksr.Success)
		return nil

	case "create":
		if len(rest) != 2 {
			return errors.New("usage: create <dir> <key>")
		}
		var ksr keySuccessResponse
		code, err := c.do("/create", keyRequest{Directory: rest[0], Key: rest[1], SeqNumber: c.seq, ClientID: c.clientID}, &ksr)
		if err != nil {
			return err
		}
		fmt.Printf("ok (%s, success=%v)\n", statusWord(code), ksr.Success)
		return nil

	case "rm":
		if len(rest) != 2 {
			return errors.New("usage: rm <dir> <key>")
		}
		var ksr keySuccessResponse
		if _, err := c.do("/delete", keyRequest{Directory: rest[0], Key: rest[1], SeqNumber: c.seq, ClientID: c.clientID}, &ksr); err != nil {
			return err
		}
		fmt.Printf("deleted (success=%v)\n", ksr.Success)
		return nil

	case "stat":
		if len(rest) != 2 {
			return errors.New("usage: stat <dir> <key>   (key \".\" means the directory itself)")
		}
		var mr metadataResponse
		if _, err := c.do("/get_metadata", keyRequest{Directory: rest[0], Key: rest[1], SeqNumber: c.seq, ClientID: c.clientID}, &mr); err != nil {
			return err
		}
		fmt.Printf("directory:   %s\nkey:         %s\nis_directory: %v\nsize:        %d\nversion:     %d\nleader_index: %d\nmembers:     %s\n",
			mr.Directory, mr.Key, mr.IsDirectory, mr.Size, mr.Version, mr.LeaderIdx, strings.Join(mr.PAddrList, ", "))
		return nil

	case "metrics":
		for _, addr := range c.addrs {
			resp, err := c.http.Get("http://" + addr + "/metrics")
			if err != nil {
				fmt.Printf("# %s: unreachable (%v)\n", addr, err)
				continue
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Printf("# ==== %s ====\n%s\n", addr, string(raw))
		}
		return nil

	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `hkvcctl - command-line client for an HKVC cluster

usage:
  hkvcctl -addrs a1:p1,a2:p2,... <command> [args]

commands:
  ls   <dir>                 list children of a directory
  get  <dir> <key>           print a key's value
  set  <dir> <key> <value>   create or overwrite a key
  create <dir> <key>         create a subdirectory
  rm   <dir> <key>           delete a key or subdirectory
  stat <dir> <key>           metadata (key "." = the directory itself)
  metrics                    dump each address's /metrics
`)
}

func splitAddrs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func statusWord(code int) string {
	switch code {
	case http.StatusCreated:
		return "created"
	case http.StatusOK:
		return "ok"
	default:
		return strconv.Itoa(code)
	}
}

// randID returns a short random hex-ish id for this run's client identity.
func randID() string {
	const letters = "0123456789abcdef"
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, 8)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
