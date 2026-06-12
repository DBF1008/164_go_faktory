package server

import (
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
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

		t.Run("RENEW", func(t *testing.T) {
			c := dummyConnection()

			// Flush first
			flush(c, s, "flush")
			_ = output(c)

			// RENEW on non-existent job
			renew(c, s, `RENEW {"jid":"doesnotexist"}`)
			txt := output(c)
			assert.Contains(t, txt, "-ERR")
			assert.Contains(t, txt, "no such job")

			// Push + Fetch + RENEW success
			job := client.NewJob("RenewableType", 1, 2)
			job.ReserveFor = 120
			assert.NoError(t, s.Manager().Push(c.Context, job))

			fetched, err := s.Manager().Fetch(c.Context, "testwid", "default")
			assert.NoError(t, err)
			assert.NotNil(t, fetched)

			renew(c, s, fmt.Sprintf(`RENEW {"jid":%q,"reserve_for":3600}`, job.Jid))
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)

			// RENEW with default reserve_for
			renew(c, s, fmt.Sprintf(`RENEW {"jid":%q}`, job.Jid))
			txt = output(c)
			assert.Equal(t, "+OK\r\n", txt)

			// RENEW with invalid JSON
			renew(c, s, `RENEW {bad json}`)
			txt = output(c)
			assert.Contains(t, txt, "-ERR")

			// RENEW with missing jid
			renew(c, s, `RENEW {"reserve_for":3600}`)
			txt = output(c)
			assert.Contains(t, txt, "-ERR")
			assert.Contains(t, txt, "missing jid")

			// ACK then RENEW — should fail
			_, err = s.Manager().Acknowledge(c.Context, job.Jid)
			assert.NoError(t, err)

			renew(c, s, fmt.Sprintf(`RENEW {"jid":%q}`, job.Jid))
			txt = output(c)
			assert.Contains(t, txt, "-ERR")
			assert.Contains(t, txt, "no such job")
		})

		t.Run("RENEW prevents reaping", func(t *testing.T) {
			c := dummyConnection()

			flush(c, s, "flush")
			_ = output(c)

			// Push a job with very short reserve_for (60s = minimum)
			job := client.NewJob("LongRunningJob", 1, 2)
			job.ReserveFor = 60
			assert.NoError(t, s.Manager().Push(c.Context, job))

			// Fetch it to put it in the working set
			fetched, err := s.Manager().Fetch(c.Context, "testwid", "default")
			assert.NoError(t, err)
			assert.NotNil(t, fetched)

			// RENEW it for 1 hour
			renew(c, s, fmt.Sprintf(`RENEW {"jid":%q,"reserve_for":3600}`, job.Jid))
			txt := output(c)
			assert.Equal(t, "+OK\r\n", txt)

			// Reap at 70 seconds (past original 60s expiry) — should NOT reap
			ctx := c.Context
			count, err := s.Manager().ReapExpiredJobs(ctx, time.Now().Add(70*time.Second))
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)

			// Reap at 3610 seconds (past renewed expiry) — should reap
			count, err = s.Manager().ReapExpiredJobs(ctx, time.Now().Add(3610*time.Second))
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
		})
	})
}
