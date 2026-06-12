package manager

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestDeadRetention(t *testing.T) {
	withRedis(t, "morgue", func(t *testing.T, store storage.Store) {
		bg := context.Background()

		// Default configuration: a fresh manager keeps the documented 180-day
		// TTL and the 10,000-job cap, and the automatic failure path applies
		// that TTL when a job enters the morgue.
		t.Run("Defaults", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			assert.EqualValues(t, DeadTTL, m.deadTTL)
			assert.EqualValues(t, DefaultDeadMaxSize, m.deadMaxSize)

			job := client.NewJob("DeadJob", 1, 2, 3)
			assert.NoError(t, m.sendToMorgue(bg, job))
			assert.EqualValues(t, 1, store.Dead().Size(bg))

			entries := deadEntries(bg, t, store)
			assert.Len(t, entries, 1)
			assertApproxExpiry(t, entryExpiry(t, entries[0]), DeadTTL)
		})

		// Custom retention duration: both the automatic failure path
		// (sendToMorgue) and the manual move path (MoveToDead) must use the
		// configured TTL so background and manual behaviour stay consistent.
		t.Run("CustomTTL", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)
			m.SetDeadRetention(48*time.Hour, DefaultDeadMaxSize)

			// automatic path
			job := client.NewJob("DeadJob", 1, 2, 3)
			assert.NoError(t, m.sendToMorgue(bg, job))
			entries := deadEntries(bg, t, store)
			assert.Len(t, entries, 1)
			assertApproxExpiry(t, entryExpiry(t, entries[0]), 48*time.Hour)

			// manual move path: stage a job in Retries, then move it to dead
			assert.NoError(t, store.Flush(bg))
			staged := client.NewJob("StagedJob", 9)
			addJob(bg, t, store.Retries(), util.Thens(time.Now().Add(time.Hour)), staged)

			var src storage.SortedEntry
			assert.NoError(t, store.Retries().Each(bg, func(_ int, e storage.SortedEntry) error {
				src = e
				return nil
			}))
			assert.NotNil(t, src)

			assert.NoError(t, m.MoveToDead(bg, store.Retries(), src))
			assert.EqualValues(t, 0, store.Retries().Size(bg))
			assert.EqualValues(t, 1, store.Dead().Size(bg))

			entries = deadEntries(bg, t, store)
			assert.Len(t, entries, 1)
			assertApproxExpiry(t, entryExpiry(t, entries[0]), 48*time.Hour)
		})

		// Over-limit trimming: when more jobs enter the morgue than the
		// configured maximum, the oldest are trimmed and the most recent are
		// kept.
		t.Run("MaxSizeTrim", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)
			m.SetDeadRetention(DeadTTL, 3)

			for i := 0; i < 5; i++ {
				job := client.NewJob(fmt.Sprintf("Dead-%d", i), i)
				assert.NoError(t, m.sendToMorgue(bg, job))
				// strictly increasing expiry scores make the trim order deterministic
				time.Sleep(2 * time.Millisecond)
			}

			assert.EqualValues(t, 3, store.Dead().Size(bg))
			assert.ElementsMatch(t,
				[]string{"Dead-2", "Dead-3", "Dead-4"},
				deadJobTypes(bg, t, store),
				"the three most recently dead jobs should survive, oldest trimmed")
		})

		// Background cleanup consistency: Purge enforces the same maximum size
		// even for jobs that are nowhere near their TTL expiry.
		t.Run("PurgeEnforcesMaxSize", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)
			m.SetDeadRetention(DeadTTL, 3)

			// five dead jobs, none past their TTL (expiry is in the future)
			for i := 0; i < 5; i++ {
				job := client.NewJob(fmt.Sprintf("Dead-%d", i), i)
				expiry := util.Thens(time.Now().Add(time.Duration(i+1) * time.Hour))
				addJob(bg, t, store.Dead(), expiry, job)
			}
			assert.EqualValues(t, 5, store.Dead().Size(bg))

			count, err := m.Purge(bg, time.Now())
			assert.NoError(t, err)
			assert.EqualValues(t, 2, count) // nothing TTL-expired, two trimmed for size
			assert.EqualValues(t, 3, store.Dead().Size(bg))
			assert.ElementsMatch(t,
				[]string{"Dead-2", "Dead-3", "Dead-4"},
				deadJobTypes(bg, t, store))
		})
	})
}

// deadEntries collects every entry currently in the dead set.
func deadEntries(ctx context.Context, t *testing.T, store storage.Store) []storage.SortedEntry {
	var entries []storage.SortedEntry
	err := store.Dead().Each(ctx, func(_ int, e storage.SortedEntry) error {
		entries = append(entries, e)
		return nil
	})
	assert.NoError(t, err)
	return entries
}

// deadJobTypes returns the jobtype of every entry in the dead set.
func deadJobTypes(ctx context.Context, t *testing.T, store storage.Store) []string {
	var types []string
	for _, e := range deadEntries(ctx, t, store) {
		j, err := e.Job()
		assert.NoError(t, err)
		types = append(types, j.Type)
	}
	return types
}

// entryExpiry decodes the expiry timestamp encoded in a sorted entry's key
// ("timestamp|jid"), which is the score used when the job entered the set.
func entryExpiry(t *testing.T, e storage.SortedEntry) time.Time {
	k, err := e.Key()
	assert.NoError(t, err)
	parts := strings.SplitN(string(k), "|", 2)
	assert.Len(t, parts, 2)
	tm, err := util.ParseTime(parts[0])
	assert.NoError(t, err)
	return tm
}

// assertApproxExpiry checks that got is approximately now+ttl. A generous
// tolerance absorbs the few milliseconds between computing the expiry and the
// assertion.
func assertApproxExpiry(t *testing.T, got time.Time, ttl time.Duration) {
	want := time.Now().Add(ttl)
	diff := got.Sub(want)
	if diff < 0 {
		diff = -diff
	}
	assert.Less(t, diff, 10*time.Second,
		"expiry %v should be within tolerance of now+%v (%v)", got, ttl, want)
}
