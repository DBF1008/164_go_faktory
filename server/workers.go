package server

import (
	"io"
	"sort"
	"sync"
	"time"

	"github.com/contribsys/faktory/util"
)

// This represents a single client process.  It may have many network
// connections open to Faktory.
//
// A client can be a producer AND/OR consumer of jobs.  Typically a process will
// either only produce jobs (like a webapp pushing jobs) or produce/consume jobs
// (like a faktory worker process where a job can create other jobs while
// executing).
//
// Each Faktory worker process should send a BEAT command every 15 seconds.
// Only consumers should send a BEAT.  If Faktory does not receive a BEAT from a
// worker process within 60 seconds, it expires and is removed from the Busy
// page.
//
// From Faktory's POV, the worker can BEAT again and resume normal operations,
// e.g. due to a network partition.  If a process dies, it will be removed
// after 1 minute and its jobs recovered after the job reservation timeout has
// passed (typically 30 minutes).
//
// A worker process has a simple three-state lifecycle:
//
//	running -> quiet -> terminate
//
// - Running means the worker is alive and processing jobs.
// - Quiet means the worker should stop FETCHing new jobs but continue working on existing jobs.
// It should not exit, even if no jobs are processing.
// - Terminate means the worker should exit within N seconds, where N is recommended to be
// 30 seconds.  In practice, faktory_worker_ruby waits up to 25 seconds and any
// threads that are still busy are forcefully killed and their associated jobs reported
// as FAILed so they will be retried shortly.
//
// A worker process should never stop sending BEAT.  Even after "quiet" or
// "terminate", the BEAT should continue, only stopping due to process exit().
// Workers should never move backward in state - you cannot "unquiet" a worker,
// it must be restarted.
//
// Workers will typically also respond to standard Unix signals.
// faktory_worker_ruby uses TSTP ("Threads SToP") as the quiet signal and TERM as the terminate signal.
//
// A ClientData is shared between the command server (which mutates it on every
// BEAT) and the Web UI (which reads it to render the Busy page and mutates it to
// send quiet/terminate signals). Its mutable fields - state, lastHeartbeat,
// RssKb and connections - are therefore guarded by mu. The remaining fields are
// set once when the worker registers and are immutable afterward, so they can be
// read without locking. Callers must never expose a *ClientData to other
// goroutines for direct field access; use the accessor methods (or a
// WorkerSnapshot) instead.
type ClientData struct {
	mu sync.RWMutex

	StartedAt time.Time

	// this only applies to clients that are workers and
	// are sending BEAT
	lastHeartbeat time.Time
	connections   map[io.Closer]bool
	Hostname      string   `json:"hostname"`
	Wid           string   `json:"wid"`
	PasswordHash  string   `json:"pwdhash"`
	Username      string   `json:"username"`
	Labels        []string `json:"labels"`
	Pid           int      `json:"pid"`
	RssKb         int64    `json:"rss_kb"`
	state         WorkerState
	Version       uint8 `json:"v"`
}

type WorkerState int

const (
	Running WorkerState = iota
	Quiet
	Terminate
)

func stateString(state WorkerState) string {
	switch state {
	case Quiet:
		return "quiet"
	case Terminate:
		return "terminate"
	default:
		return ""
	}
}

func stateFromString(state string) WorkerState {
	switch state {
	case "quiet":
		return Quiet
	case "terminate":
		return Terminate
	default:
		return Running
	}
}

func clientDataFromHello(data string) (*ClientData, error) {
	var client ClientData
	err := util.JsonUnmarshal([]byte(data), &client)
	if err != nil {
		return nil, err
	}

	return &client, nil
}

func (worker *ClientData) ConnectionCount() int {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	return len(worker.connections)
}

func (worker *ClientData) IsQuiet() bool {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	return worker.state != Running
}

// State returns the worker's current lifecycle state, taking the lock so it is
// safe to call concurrently with BEAT and signal handling.
func (worker *ClientData) State() WorkerState {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	return worker.state
}

/*
 * Send "quiet" or "terminate" to the given client
 * worker process.  Other signals are undefined.
 */
func (worker *ClientData) Signal(newstate WorkerState) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	worker.signalLocked(newstate)
}

// signalLocked applies the running -> quiet -> terminate state machine. The
// caller must hold worker.mu.
func (worker *ClientData) signalLocked(newstate WorkerState) {
	if worker.state == newstate {
		return
	}

	if worker.state == Running {
		worker.state = newstate
		return
	}

	// only allow running -> quiet -> terminate
	// can't go from quiet -> running, terminate -> quiet, etc.
	if worker.state == Quiet && newstate == Terminate {
		worker.state = newstate
		return
	}

	if worker.state == Terminate {
		return
	}
}

func (worker *ClientData) IsConsumer() bool {
	// Wid is immutable after registration, no lock required.
	return worker.Wid != ""
}

func (worker *ClientData) addConnection(c io.Closer) {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.connections == nil {
		worker.connections = map[io.Closer]bool{}
	}
	worker.connections[c] = true
}

// removeConnection drops c and returns the number of connections still open.
func (worker *ClientData) removeConnection(c io.Closer) int {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	delete(worker.connections, c)
	return len(worker.connections)
}

// closeConnections closes every open connection and returns how many were
// closed. Used when a heartbeat is reaped.
func (worker *ClientData) closeConnections() int {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	conns := 0
	for conn := range worker.connections {
		_ = conn.Close()
		conns += 1
	}
	worker.connections = map[io.Closer]bool{}
	return conns
}

// expired reports whether the worker's last heartbeat is older than deadline.
func (worker *ClientData) expired(deadline time.Time) bool {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	return worker.lastHeartbeat.Before(deadline)
}

// beat applies a single BEAT atomically: it refreshes RssKb and lastHeartbeat
// and, when currentState is non-empty, advances the lifecycle state. It returns
// the resulting state so the caller does not need a second lock acquisition.
func (worker *ClientData) beat(rssKb int64, when time.Time, currentState string) WorkerState {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	worker.RssKb = rssKb
	worker.lastHeartbeat = when
	if currentState != "" {
		worker.signalLocked(stateFromString(currentState))
	}
	return worker.state
}

// snapshot returns an immutable, point-in-time copy of the worker's state. It
// takes the read lock; callers iterating the registry must hold workers.mu so
// the entry cannot be removed while it is read.
func (worker *ClientData) snapshot() *WorkerSnapshot {
	worker.mu.RLock()
	defer worker.mu.RUnlock()
	labels := append([]string(nil), worker.Labels...)
	return &WorkerSnapshot{
		StartedAt:   worker.StartedAt,
		Hostname:    worker.Hostname,
		Wid:         worker.Wid,
		Labels:      labels,
		Pid:         worker.Pid,
		RssKb:       worker.RssKb,
		connections: len(worker.connections),
		quiet:       worker.state != Running,
	}
}

// WorkerSnapshot is an immutable, point-in-time view of a worker's heartbeat
// state. It is what the Web UI renders, so the UI never touches the live,
// mutable ClientData and cannot tear reads against a concurrent BEAT. The field
// and method names mirror ClientData so templates can use either.
type WorkerSnapshot struct {
	StartedAt   time.Time
	Hostname    string
	Wid         string
	Labels      []string
	Pid         int
	RssKb       int64
	connections int
	quiet       bool
}

func (s *WorkerSnapshot) ConnectionCount() int { return s.connections }
func (s *WorkerSnapshot) IsQuiet() bool        { return s.quiet }

type workers struct {
	heartbeats map[string]*ClientData
	mu         sync.RWMutex
}

func newWorkers() *workers {
	return &workers{
		heartbeats: make(map[string]*ClientData, 12),
	}
}

func (w *workers) Count() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.heartbeats)
}

// setupHeartbeat registers a new worker (or finds the existing one) and records
// the connection. The boolean reports whether the worker was already known.
func (w *workers) setupHeartbeat(client *ClientData, cls io.Closer) (*ClientData, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry, existed := w.heartbeats[client.Wid]
	if !existed {
		client.StartedAt = time.Now()
		client.lastHeartbeat = time.Now()
		w.heartbeats[client.Wid] = client
		entry = client
	}
	entry.addConnection(cls)
	return entry, existed
}

func (w *workers) heartbeat(client *ClientBeat) (*ClientData, bool) {
	w.mu.RLock()
	entry, ok := w.heartbeats[client.Wid]
	w.mu.RUnlock()

	if !ok {
		return nil, ok
	}

	// util.Debugf("BEAT for %s", client.Wid)
	entry.beat(client.RssKb, time.Now(), client.CurrentState)
	return entry, ok
}

func (w *workers) RemoveConnection(c *Connection) {
	w.mu.Lock()
	defer w.mu.Unlock()
	cd, ok := w.heartbeats[c.client.Wid]
	if ok {
		if cd.removeConnection(c) == 0 {
			// util.Debugf("All worker connections closed, reaping %s", c.client.Wid)
			delete(w.heartbeats, c.client.Wid)
		}
	}
}

func (w *workers) reapHeartbeats(t time.Time) int {
	toDelete := []string{}

	w.mu.Lock()
	defer w.mu.Unlock()

	for k, worker := range w.heartbeats {
		if worker.expired(t) {
			// util.Debugf("Reaping %s", worker.Wid)
			toDelete = append(toDelete, k)
		}
	}

	count := len(toDelete)
	conns := 0
	if count > 0 {
		for idx := range toDelete {
			cd := w.heartbeats[toDelete[idx]]
			conns += cd.closeConnections()
			delete(w.heartbeats, toDelete[idx])
		}

		util.Debugf("Reaped %d worker heartbeats", count)
		if conns > 0 {
			util.Warnf("Reaped %d lingering connections, this is a sign your workers are having problems", conns)
			util.Warn("All worker processes should send a heartbeat every 15 seconds")
		}
	}
	return count
}

// BusyWorkers returns an immutable, Wid-sorted snapshot of every registered
// worker. This is the single read path for the Web UI's Busy page.
func (w *workers) BusyWorkers() []*WorkerSnapshot {
	w.mu.RLock()
	defer w.mu.RUnlock()

	snaps := make([]*WorkerSnapshot, 0, len(w.heartbeats))
	for _, cd := range w.heartbeats {
		snaps = append(snaps, cd.snapshot())
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].Wid < snaps[j].Wid
	})
	return snaps
}

// Signal sends sig to the worker identified by wid, or to every worker when wid
// is "all". It returns the number of workers signalled. This is the single
// dispatch path shared by the Busy page's per-worker and batch ("all") buttons;
// the legal running -> quiet -> terminate transitions are enforced by
// ClientData.Signal.
func (w *workers) Signal(wid string, sig WorkerState) int {
	w.mu.RLock()
	defer w.mu.RUnlock()

	count := 0
	for _, cd := range w.heartbeats {
		if wid == "all" || cd.Wid == wid {
			cd.Signal(sig)
			count += 1
		}
	}
	return count
}

// add inserts a worker into the registry, initializing the fields a live worker
// would have. It is used by the connection layer's tests to seed a known worker
// without driving a full handshake.
func (w *workers) add(cd *ClientData) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cd.connections == nil {
		cd.connections = map[io.Closer]bool{}
	}
	if cd.lastHeartbeat.IsZero() {
		cd.lastHeartbeat = time.Now()
	}
	w.heartbeats[cd.Wid] = cd
}
