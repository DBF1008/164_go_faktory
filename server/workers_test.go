package server

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClientData(t *testing.T) {
	t.Parallel()

	cw, err := clientDataFromHello("")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientDataFromHello("{")
	assert.Error(t, err)
	assert.Nil(t, cw)

	cw, err = clientDataFromHello("{}")
	assert.NoError(t, err)
	assert.NotNil(t, cw)
	assert.False(t, cw.IsConsumer())

	ahoy := `{"hostname":"MikeBookPro.local","wid":"78629a0f5f3f164f","pid":40275,"labels":["blue","seven"],"salt":"123456","pwdhash":"958d51602bbfbd18b2a084ba848a827c29952bfef170c936419b0922994c0589"}`
	cw, err = clientDataFromHello(ahoy)
	assert.NoError(t, err)
	assert.NotNil(t, cw)
	assert.True(t, cw.IsConsumer())

	assert.Equal(t, Running, cw.state)
	assert.False(t, cw.IsQuiet())

	cw.Signal(Quiet)
	assert.Equal(t, Quiet, cw.state)
	assert.True(t, cw.IsQuiet())

	cw.Signal(Terminate)
	assert.Equal(t, Terminate, cw.state)
	assert.True(t, cw.IsQuiet())

	// can't go back to quiet
	cw.Signal(Quiet)
	assert.Equal(t, Terminate, cw.state)
	assert.True(t, cw.IsQuiet())
}

func TestWorkers(t *testing.T) {
	t.Parallel()

	workers := newWorkers()
	assert.Equal(t, 0, workers.Count())

	beat := &ClientBeat{
		Wid: "78629a0f5f3f164f",
	}
	entry, ok := workers.heartbeat(beat)
	assert.Equal(t, 0, workers.Count())
	assert.Nil(t, entry)
	assert.False(t, ok)

	client := &ClientData{
		Hostname:    "MikeBookPro.local",
		Wid:         "78629a0f5f3f164f",
		connections: map[io.Closer]bool{},
	}
	entry, ok = workers.setupHeartbeat(client, &cls{})
	assert.NotNil(t, entry)
	assert.False(t, ok)

	entry, ok = workers.heartbeat(beat)
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)

	before := time.Now()
	entry, ok = workers.heartbeat(beat)
	after := time.Now()
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, entry)
	assert.True(t, ok)
	assert.LessOrEqual(t, before, entry.lastHeartbeat)
	assert.LessOrEqual(t, entry.lastHeartbeat, after)

	assert.Equal(t, Running, entry.state)
	beat.CurrentState = "quiet"
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Quiet, entry.state)
	assert.True(t, entry.IsQuiet())

	beat.CurrentState = ""
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Quiet, entry.state)

	beat.CurrentState = "terminate"
	entry, _ = workers.heartbeat(beat)
	assert.Equal(t, Terminate, entry.state)

	count := workers.reapHeartbeats(client.lastHeartbeat)
	assert.Equal(t, 1, workers.Count())
	assert.Equal(t, 0, count)

	count = workers.reapHeartbeats(time.Now())
	assert.Equal(t, 0, workers.Count())
	assert.Equal(t, 1, count)
}

type cls struct{}

func (c cls) Close() error {
	return nil
}

func TestWorkersSignalBroadcast(t *testing.T) {
	t.Parallel()

	workers := newWorkers()

	wid := "78629a0f5f3f164f"
	client := &ClientData{Wid: wid, Hostname: "host.local"}
	_, existed := workers.setupHeartbeat(client, &cls{})
	assert.False(t, existed)
	assert.Equal(t, Running, client.State())

	// Signalling an unknown worker matches nothing.
	assert.Equal(t, 0, workers.Signal("does-not-exist", Quiet))
	assert.Equal(t, Running, client.State())

	// "all" reaches the single registered worker (single-worker full broadcast).
	assert.Equal(t, 1, workers.Signal("all", Quiet))
	assert.Equal(t, Quiet, client.State())
	assert.True(t, client.IsQuiet())

	// A specific wid matches that worker too.
	assert.Equal(t, 1, workers.Signal(wid, Terminate))
	assert.Equal(t, Terminate, client.State())

	// The running -> quiet -> terminate machine still forbids moving backward,
	// even through the batch "all" path.
	assert.Equal(t, 1, workers.Signal("all", Quiet))
	assert.Equal(t, Terminate, client.State())
}

func TestWorkersReapEviction(t *testing.T) {
	t.Parallel()

	workers := newWorkers()

	conn := &recordingCloser{}
	client := &ClientData{Wid: "evictme", Hostname: "host.local"}
	_, existed := workers.setupHeartbeat(client, conn)
	assert.False(t, existed)
	assert.Equal(t, 1, workers.Count())
	assert.Equal(t, 1, client.ConnectionCount())

	// A deadline older than the heartbeat must not reap the worker.
	assert.Equal(t, 0, workers.reapHeartbeats(time.Now().Add(-time.Hour)))
	assert.Equal(t, 1, workers.Count())
	assert.False(t, conn.isClosed())

	// A deadline newer than the heartbeat reaps the worker and closes its
	// lingering connection.
	assert.Equal(t, 1, workers.reapHeartbeats(time.Now().Add(time.Minute)))
	assert.Equal(t, 0, workers.Count())
	assert.True(t, conn.isClosed())
}

// TestWorkersConcurrentAccess exercises the Busy view, batch signal dispatch,
// BEAT, and heartbeat eviction against the registry simultaneously. Before the
// registry was made lock-safe this raced (and could panic with "concurrent map
// iteration and map write"); it is meant to be run with `go test -race`.
func TestWorkersConcurrentAccess(t *testing.T) {
	t.Parallel()

	workers := newWorkers()

	const seeded = 4
	for i := 0; i < seeded; i++ {
		_, _ = workers.setupHeartbeat(&ClientData{Wid: fmt.Sprintf("seed-%d", i)}, &cls{})
	}

	const iterations = 500
	stale := time.Now().Add(-time.Hour)

	var wg sync.WaitGroup
	wg.Add(4)

	// Render the Busy page view.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			for _, snap := range workers.BusyWorkers() {
				_ = snap.Wid
				_ = snap.IsQuiet()
				_ = snap.ConnectionCount()
				_ = snap.RssKb
			}
		}
	}()

	// Batch-signal every worker.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			workers.Signal("all", Quiet)
		}
	}()

	// BEAT the seeded workers.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			workers.heartbeat(&ClientBeat{Wid: fmt.Sprintf("seed-%d", i%seeded), RssKb: int64(i)})
		}
	}()

	// Register transient workers and evict them.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			cd := &ClientData{Wid: fmt.Sprintf("tmp-%d", i)}
			cd.lastHeartbeat = stale
			workers.add(cd)
			// Only the stale transient workers are old enough to be reaped.
			workers.reapHeartbeats(time.Now().Add(-time.Minute))
		}
	}()

	wg.Wait()

	// The seeded workers are never stale enough to be evicted, so they survive.
	assert.GreaterOrEqual(t, workers.Count(), seeded)
}

type recordingCloser struct {
	mu     sync.Mutex
	closed bool
}

func (r *recordingCloser) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	return nil
}

func (r *recordingCloser) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}
