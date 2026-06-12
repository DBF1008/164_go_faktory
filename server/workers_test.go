package server

import (
	"fmt"
	"io"
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
	res := workers.heartbeat(beat)
	assert.Equal(t, 0, workers.Count())
	assert.Nil(t, res.cd)
	assert.False(t, res.ok)

	client := &ClientData{
		Hostname:    "MikeBookPro.local",
		Wid:         "78629a0f5f3f164f",
		connections: map[io.Closer]bool{},
	}
	entry, ok := workers.setupHeartbeat(client, &cls{})
	assert.NotNil(t, entry)
	assert.False(t, ok)

	res = workers.heartbeat(beat)
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, res.cd)
	assert.True(t, res.ok)

	before := time.Now()
	res = workers.heartbeat(beat)
	after := time.Now()
	assert.Equal(t, 1, workers.Count())
	assert.NotNil(t, res.cd)
	assert.True(t, res.ok)
	assert.LessOrEqual(t, before, res.cd.lastHeartbeat)
	assert.LessOrEqual(t, res.cd.lastHeartbeat, after)

	assert.Equal(t, Running, res.state)
	beat.CurrentState = "quiet"
	res = workers.heartbeat(beat)
	assert.Equal(t, Quiet, res.state)
	assert.True(t, res.cd.IsQuiet())

	beat.CurrentState = ""
	res = workers.heartbeat(beat)
	assert.Equal(t, Quiet, res.state)

	beat.CurrentState = "terminate"
	res = workers.heartbeat(beat)
	assert.Equal(t, Terminate, res.state)

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

func TestSignalOne(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	for _, wid := range []string{"w1", "w2", "w3"} {
		w.SetupWorker(&ClientData{Wid: wid})
	}

	assert.True(t, w.SignalOne("w2", Quiet))
	st, _ := w.State("w2")
	assert.Equal(t, Quiet, st)

	// other workers untouched
	st, _ = w.State("w1")
	assert.Equal(t, Running, st)
	st, _ = w.State("w3")
	assert.Equal(t, Running, st)

	// non-existent worker
	assert.False(t, w.SignalOne("w99", Quiet))
}

func TestSignalAll(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	for _, wid := range []string{"w1", "w2", "w3"} {
		w.SetupWorker(&ClientData{Wid: wid})
	}

	changed := w.SignalAll(Quiet)
	assert.Equal(t, 3, changed)

	for _, wid := range []string{"w1", "w2", "w3"} {
		st, _ := w.State(wid)
		assert.Equal(t, Quiet, st)
	}
}

func TestSignalAllPartial(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	w.SetupWorker(&ClientData{Wid: "w1"})
	w.SetupWorker(&ClientData{Wid: "w2"})
	w.SetupWorker(&ClientData{Wid: "w3"})

	// pre-signal w1 to Quiet
	w.SignalOne("w1", Quiet)

	// Terminate all: w1 Quiet→Terminate, w2 Running→Terminate, w3 Running→Terminate
	changed := w.SignalAll(Terminate)
	assert.Equal(t, 3, changed)

	for _, wid := range []string{"w1", "w2", "w3"} {
		st, _ := w.State(wid)
		assert.Equal(t, Terminate, st)
	}

	// signaling again should be a no-op
	changed = w.SignalAll(Terminate)
	assert.Equal(t, 0, changed)
}

func TestConcurrentHeartbeatAndSignal(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	for i := range 10 {
		wid := fmt.Sprintf("worker-%d", i)
		w.SetupWorker(&ClientData{Wid: wid})
	}

	done := make(chan struct{})

	// goroutine A: continuous heartbeats
	go func() {
		defer func() { done <- struct{}{} }()
		for i := range 500 {
			wid := fmt.Sprintf("worker-%d", i%10)
			w.heartbeat(&ClientBeat{Wid: wid, RssKb: int64(i)})
		}
	}()

	// goroutine B: continuous signal broadcasts
	go func() {
		defer func() { done <- struct{}{} }()
		for i := range 500 {
			if i%2 == 0 {
				w.SignalAll(Quiet)
			} else {
				w.SignalAll(Running)
			}
		}
	}()

	<-done
	<-done
}

func TestReapDuringSnapshot(t *testing.T) {
	t.Parallel()

	w := newWorkers()
	for i := range 10 {
		wid := fmt.Sprintf("worker-%d", i)
		w.SetupWorker(&ClientData{Wid: wid})
	}

	done := make(chan struct{})

	// goroutine A: continuous snapshots
	go func() {
		defer func() { done <- struct{}{} }()
		for range 500 {
			snaps := w.Snapshot()
			// verify snapshot is usable
			for _, s := range snaps {
				_ = s.Wid
				_ = s.IsQuiet()
				_ = s.ConnectionCount()
			}
		}
	}()

	// goroutine B: continuous reap + re-register cycle
	go func() {
		defer func() { done <- struct{}{} }()
		for i := range 500 {
			wid := fmt.Sprintf("worker-%d", i%10)
			// heartbeat to keep alive or let it expire
			w.heartbeat(&ClientBeat{Wid: wid})
			// occasionally reap with a very old time to trigger deletion
			if i%50 == 0 {
				w.reapHeartbeats(time.Now().Add(time.Hour))
				// re-register after reap
				for j := range 10 {
					w.SetupWorker(&ClientData{Wid: fmt.Sprintf("worker-%d", j)})
				}
			}
		}
	}()

	<-done
	<-done
}
