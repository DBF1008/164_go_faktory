package server

import (
	"encoding/json"
	"fmt"
	"regexp"
	"testing"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestCommands(t *testing.T) {
	// util.LogInfo = true
	// util.LogDebug = true
	runServer("localhost:4478", func(s *Server) {
		t.Run("queue pause", func(t *testing.T) {
			c := dummyConnection()
			assert.NotNil(t, c.Context)

			queue(c, s, "QUEUE PAUSE *")
			txt := output(c)
			assert.Equal(t, "+OK\r\n", txt)

			queue(c, s, "QUEUE UNPAUSE *")
			txt = output(c)
			assert.Equal(t, "-ERR no such QUEUE subcommand: UNPAUSE\r\n", txt)

			queue(c, s, "QUEUE RESUME *")
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)

			queue(c, s, "QUEUE REMOVE foo")
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)

			queue(c, s, "QUEUE REMOVE *")
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)

			queue(c, s, "QUEUE PAUSE default")
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)

			queue(c, s, "QUEUE RESUME default")
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)
		})

		t.Run("queue latency", func(t *testing.T) {
			c := dummyConnection()
			assert.NotNil(t, c.Context)

			queue(c, s, "QUEUE LATENCY *")
			txt := output(c)
			assert.Equal(t, "-ERR QUEUE LATENCY does not support wildcards\r\n", txt)

			queue(c, s, "QUEUE LATENCY default")
			txt = output(c)
			assert.Equal(t, "$13\r\n{\"default\":0}\r\n", txt)

			ctx := c.Context
			job := client.NewJob("jobtype", 1, 2, "mike")
			assert.NoError(t, s.Manager().Push(ctx, job))

			queue(c, s, "queue latency default foo")
			txt = output(c)
			assert.Regexp(t, regexp.MustCompile("\"default\":0.\\d{4}"), txt)
			assert.Regexp(t, regexp.MustCompile("\"foo\":0"), txt)
		})

		t.Run("PUSHB", func(t *testing.T) {
			jobs := []*client.Job{}
			for range 10 {
				job := client.NewJob("Mike", 1, 2, "foo")
				jobs = append(jobs, job)
			}

			c := dummyConnection()
			flush(c, s, "flush")
			txt := output(c)
			assert.Equal(t, "+OK\r\n", txt)

			data, err := json.Marshal(jobs)
			assert.NoError(t, err)
			cmd := fmt.Sprintf("pushb %s", data)
			pushBulk(c, s, cmd)
			txt = output(c)
			// no errors, all 10 pushed
			assert.Equal(t, "$2\r\n{}\r\n", txt)
			x, _ := s.CurrentState()
			data, _ = json.Marshal(x)
			util.Infof("State: %s", string(data))
			qsize := x.Data.Queues["default"]
			assert.EqualValues(t, 10, qsize)

			job1 := jobs[0]
			job1.Type = ""
			data, err = json.Marshal(jobs)
			assert.NoError(t, err)
			cmd = fmt.Sprintf("pushb %s", data)
			pushBulk(c, s, cmd)
			txt = output(c)
			assert.Equal(t, fmt.Sprintf("$57\r\n{%q:\"jobs must have a jobtype parameter\"}\r\n", job1.Jid), txt)
		})
	})
}

// TestQueueWildcards covers the QUEUE *  wildcard batch operations end to end,
// confirming they touch every queue (no drift) and that paused state stays
// consistent across the storage views afterwards.
func TestQueueWildcards(t *testing.T) {
	runServer("localhost:7421", func(s *Server) {
		t.Run("wildcard remove removes every queue", func(t *testing.T) {
			c := dummyConnection()
			ctx := c.Context
			assert.NoError(t, s.Store().Flush(ctx))

			for _, n := range []string{"q1", "q2", "q3", "q4", "q5"} {
				j := client.NewJob("SomeJob", 1)
				j.Queue = n
				assert.NoError(t, s.Manager().Push(ctx, j))
			}
			// pause a couple to also prove their paused state is cleared
			assert.NoError(t, s.Manager().PauseQueue(ctx, "q2"))
			assert.NoError(t, s.Manager().PauseQueue(ctx, "q4"))

			count := 0
			s.Store().EachQueue(ctx, func(storage.Queue) { count++ })
			assert.Equal(t, 5, count)

			queue(c, s, "QUEUE REMOVE *")
			assert.Equal(t, "+OK\r\n", output(c))

			// every queue must be gone and no paused flag may linger. The
			// unified remove path clears Redis and the in-memory mirror
			// together, and EachQueue iterates a snapshot so a wildcard REMOVE
			// that mutates the queue set mid-iteration stays consistent.
			count = 0
			s.Store().EachQueue(ctx, func(storage.Queue) { count++ })
			assert.Equal(t, 0, count)

			pq, err := s.Store().PausedQueues(ctx)
			assert.NoError(t, err)
			assert.Equal(t, []string{}, pq)
		})

		t.Run("wildcard pause then resume every queue", func(t *testing.T) {
			c := dummyConnection()
			ctx := c.Context
			assert.NoError(t, s.Store().Flush(ctx))

			names := []string{"w1", "w2", "w3"}
			for _, n := range names {
				j := client.NewJob("SomeJob", 1)
				j.Queue = n
				assert.NoError(t, s.Manager().Push(ctx, j))
			}

			queue(c, s, "QUEUE PAUSE *")
			assert.Equal(t, "+OK\r\n", output(c))

			pq, err := s.Store().PausedQueues(ctx)
			assert.NoError(t, err)
			assert.Equal(t, names, pq) // PausedQueues is sorted; names already sorted
			s.Store().EachQueue(ctx, func(q storage.Queue) {
				assert.True(t, q.IsPaused(ctx), "queue %s should be paused", q.Name())
			})

			queue(c, s, "QUEUE RESUME *")
			assert.Equal(t, "+OK\r\n", output(c))

			pq, err = s.Store().PausedQueues(ctx)
			assert.NoError(t, err)
			assert.Equal(t, []string{}, pq)
			s.Store().EachQueue(ctx, func(q storage.Queue) {
				assert.False(t, q.IsPaused(ctx), "queue %s should be active", q.Name())
			})
		})
	})
}
