package webui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/contribsys/faktory/server"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

// TestBulkActionsConsistency exercises the shared actOn() mutation chain for
// every key=all bulk action across the retries, scheduled and dead sets.
//
// It guards against a regression where "kill all" re-enqueued every job onto
// its queue (via EnqueueAll) instead of moving it to the Dead set — the exact
// opposite of killing a single job, which silently re-ran whole batches of
// failed jobs in production. Each bulk action must match its single-key
// counterpart.
func TestBulkActionsConsistency(t *testing.T) {
	bootRuntime(t, "bulkactions", func(ui *WebUI, s *server.Server, t *testing.T) {
		bg := context.Background()
		str := s.Store()

		// reset clears all three sorted sets and the default queue so each
		// subtest starts from a known, isolated state, and returns the
		// (now empty) default queue that fakeJob() jobs belong to.
		reset := func(t *testing.T) storage.Queue {
			assert.NoError(t, str.Retries().Clear(bg))
			assert.NoError(t, str.Scheduled().Clear(bg))
			assert.NoError(t, str.Dead().Clear(bg))
			q, err := str.GetQueue(bg, "default")
			assert.NoError(t, err)
			_, err = q.Clear(bg)
			assert.NoError(t, err)
			return q
		}

		seed := func(t *testing.T, set storage.SortedSet, n int) {
			for i := 0; i < n; i++ {
				jid, data := fakeJob()
				assert.NoError(t, set.AddElement(bg, util.Nows(), jid, data))
			}
			assert.EqualValues(t, n, set.Size(bg))
		}

		// postAll submits the "all" group form (key=all) for the given action
		// to a handler and asserts the success redirect.
		postAll := func(t *testing.T, h func(http.ResponseWriter, *http.Request), path, action string) {
			payload := url.Values{"key": {"all"}, "action": {action}}
			req, err := ui.NewRequest("POST", "http://localhost:7420"+path, strings.NewReader(payload.Encode()))
			assert.NoError(t, err)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			w := httptest.NewRecorder()
			h(w, req)
			assert.Equal(t, 302, w.Code, w.Body.String())
			assert.Equal(t, "", w.Body.String())
		}

		// --- retries ---------------------------------------------------------

		t.Run("RetriesKillAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Retries(), 3)

			postAll(t, retriesHandler, "/retries", "kill")

			assert.EqualValues(t, 0, str.Retries().Size(bg), "kill all must empty the retry set")
			assert.EqualValues(t, 3, str.Dead().Size(bg), "kill all must move every job to the dead set")
			assert.EqualValues(t, 0, q.Size(bg), "kill all must NOT re-enqueue jobs onto their queue")
		})

		t.Run("RetriesKillAllAcrossPages", func(t *testing.T) {
			// Each() pages 50 entries at a time; expanding "all" must not skip
			// any entry when the set spans more than one page.
			q := reset(t)
			seed(t, str.Retries(), 60)

			postAll(t, retriesHandler, "/retries", "kill")

			assert.EqualValues(t, 0, str.Retries().Size(bg))
			assert.EqualValues(t, 60, str.Dead().Size(bg), "kill all must move every page of jobs to the dead set")
			assert.EqualValues(t, 0, q.Size(bg))
		})

		t.Run("RetriesRetryAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Retries(), 3)

			postAll(t, retriesHandler, "/retries", "retry")

			assert.EqualValues(t, 0, str.Retries().Size(bg), "retry all must empty the retry set")
			assert.EqualValues(t, 3, q.Size(bg), "retry all must re-enqueue every job")
			assert.EqualValues(t, 0, str.Dead().Size(bg), "retry all must not touch the dead set")
		})

		t.Run("RetriesDeleteAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Retries(), 3)

			postAll(t, retriesHandler, "/retries", "delete")

			assert.EqualValues(t, 0, str.Retries().Size(bg), "delete all must empty the retry set")
			assert.EqualValues(t, 0, q.Size(bg), "delete all must not re-enqueue jobs")
			assert.EqualValues(t, 0, str.Dead().Size(bg), "delete all must not move jobs to the dead set")
		})

		// --- scheduled -------------------------------------------------------

		t.Run("ScheduledKillAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Scheduled(), 3)

			postAll(t, scheduledHandler, "/scheduled", "kill")

			assert.EqualValues(t, 0, str.Scheduled().Size(bg), "kill all must empty the scheduled set")
			assert.EqualValues(t, 3, str.Dead().Size(bg), "kill all must move every scheduled job to the dead set")
			assert.EqualValues(t, 0, q.Size(bg), "kill all must NOT re-enqueue scheduled jobs")
		})

		t.Run("ScheduledAddToQueueAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Scheduled(), 3)

			postAll(t, scheduledHandler, "/scheduled", "add_to_queue")

			assert.EqualValues(t, 0, str.Scheduled().Size(bg), "add to queue all must empty the scheduled set")
			assert.EqualValues(t, 3, q.Size(bg), "add to queue all must enqueue every job")
			assert.EqualValues(t, 0, str.Dead().Size(bg))
		})

		t.Run("ScheduledDeleteAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Scheduled(), 3)

			postAll(t, scheduledHandler, "/scheduled", "delete")

			assert.EqualValues(t, 0, str.Scheduled().Size(bg), "delete all must empty the scheduled set")
			assert.EqualValues(t, 0, q.Size(bg), "delete all must not enqueue jobs")
			assert.EqualValues(t, 0, str.Dead().Size(bg))
		})

		// --- dead ------------------------------------------------------------

		t.Run("DeadRetryAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Dead(), 3)

			postAll(t, morgueHandler, "/morgue", "retry")

			assert.EqualValues(t, 0, str.Dead().Size(bg), "retry all must empty the dead set")
			assert.EqualValues(t, 3, q.Size(bg), "retry all must re-enqueue every dead job")
		})

		t.Run("DeadDeleteAll", func(t *testing.T) {
			q := reset(t)
			seed(t, str.Dead(), 3)

			postAll(t, morgueHandler, "/morgue", "delete")

			assert.EqualValues(t, 0, str.Dead().Size(bg), "delete all must empty the dead set")
			assert.EqualValues(t, 0, q.Size(bg), "delete all must not enqueue dead jobs")
		})
	})
}
