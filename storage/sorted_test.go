package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestBasicSortedOps(t *testing.T) {
	withRedis(t, "sorted", func(t *testing.T, store Store) {
		bg := context.Background()

		t.Run("large set", func(t *testing.T) {
			sset := store.Retries()
			err := sset.Clear(bg)
			assert.NoError(t, err)

			for i := range 550 {
				job := client.NewJob("OtherType", 1, 2, 3)
				if i%100 == 0 {
					job = client.NewJob("SpecialType", 1, 2, 3)
				}
				job.At = util.Nows()
				err = sset.Add(bg, job)
				assert.NoError(t, err)
			}
			assert.EqualValues(t, 550, sset.Size(bg))

			count := 0
			err = sset.Each(bg, func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				assert.NotNil(t, j)
				count += 1
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 550, count)

			spcount := 0
			err = sset.Find(bg, "*SpecialType*", func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				assert.NotNil(t, j)
				assert.Equal(t, "SpecialType", j.Type)
				spcount += 1
				return nil
			})
			assert.NoError(t, err)
			assert.EqualValues(t, 6, spcount)
			err = sset.Clear(bg)
			assert.NoError(t, err)
		})

		t.Run("junk data", func(t *testing.T) {
			sset := store.Retries()
			assert.EqualValues(t, 0, sset.Size(bg))

			tim := util.Nows()
			jid, data := fakeJob()
			err := sset.AddElement(bg, tim, jid, data)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size(bg))

			key := fmt.Sprintf("%s|%s", tim, jid)
			entry, err := sset.Get(bg, []byte(key))
			assert.NoError(t, err)
			assert.NotNil(t, entry)
			job, err := entry.Job()
			assert.NoError(t, err)
			assert.Equal(t, jid, job.Jid)

			// add a second job with exact same time to handle edge case of
			// sorted set entries with same score.
			newjid, payload := fakeJob()
			err = sset.AddElement(bg, tim, newjid, payload)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, sset.Size(bg))

			newkey := fmt.Sprintf("%s|%s", tim, newjid)
			entry, err = sset.Get(bg, []byte(newkey))
			assert.NoError(t, err)
			assert.Equal(t, payload, entry.Value())

			ok, err := sset.Remove(bg, []byte(newkey))
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size(bg))
			assert.True(t, ok)

			ok, err = sset.RemoveElement(bg, tim, jid)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, sset.Size(bg))
			assert.True(t, ok)

			err = sset.AddElement(bg, tim, newjid, payload)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size(bg))

			assert.Equal(t, sset.Name(), "retries")
			assert.NoError(t, sset.Clear(bg))
			assert.EqualValues(t, 0, sset.Size(bg))
		})

		t.Run("good data", func(t *testing.T) {
			sset := store.Scheduled()
			job := client.NewJob("SomeType", 1, 2, 3)

			assert.EqualValues(t, 0, sset.Size(bg))
			err := sset.Add(bg, job)
			assert.Error(t, err)

			job.At = util.Nows()
			err = sset.Add(bg, job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size(bg))

			job = client.NewJob("OtherType", 1, 2, 3)
			job.At = util.Nows()
			err = sset.Add(bg, job)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, sset.Size(bg))

			expectedTypes := []string{"SomeType", "OtherType"}
			actualTypes := []string{}

			err = sset.Each(bg, func(idx int, entry SortedEntry) error {
				j, err := entry.Job()
				assert.NoError(t, err)
				actualTypes = append(actualTypes, j.Type)
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, expectedTypes, actualTypes)

			var jkey []byte
			err = sset.Each(bg, func(idx int, entry SortedEntry) error {
				k, err := entry.Key()
				assert.NoError(t, err)
				jkey = k
				return nil
			})
			assert.NoError(t, err)

			q, err := store.GetQueue(bg, "default")
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 2, sset.Size(bg))

			err = store.EnqueueFrom(bg, sset, jkey)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, q.Size(bg))
			assert.EqualValues(t, 1, sset.Size(bg))

			err = store.EnqueueAll(bg, sset)
			assert.NoError(t, err)
			assert.EqualValues(t, 2, q.Size(bg))
			assert.EqualValues(t, 0, sset.Size(bg))

			job = client.NewJob("CronType", 1, 2, 3)
			job.At = util.Nows()
			err = sset.Add(bg, job)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, sset.Size(bg))

			err = sset.Each(bg, func(idx int, entry SortedEntry) error {
				k, err := entry.Key()
				assert.NoError(t, err)
				jkey = k
				return nil
			})
			assert.NoError(t, err)

			entry, err := sset.Get(bg, jkey)
			assert.NoError(t, err)

			expiry := time.Now().Add(180 * 24 * time.Hour)

			assert.EqualValues(t, 1, sset.Size(bg))
			assert.EqualValues(t, 0, store.Dead().Size(bg))
			err = sset.MoveTo(bg, store.Dead(), entry, expiry)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, sset.Size(bg))
			assert.EqualValues(t, 1, store.Dead().Size(bg))

		})
	})
}

// Regression: RemoveBefore must NOT remove an entry when the callback
// returns an error, so the next scan can retry it.
func TestRemoveBeforePreservesEntryOnCallbackError(t *testing.T) {
	withRedis(t, "removebefore", func(t *testing.T, store Store) {
		bg := context.Background()
		sset := store.Retries()
		assert.NoError(t, sset.Clear(bg))

		tim := util.Nows()
		jid, data := fakeJob()
		err := sset.AddElement(bg, tim, jid, data)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, sset.Size(bg))

		future := util.Thens(time.Now().Add(time.Hour))

		// Callback returns error → entry must stay in the sorted set
		count, err := sset.RemoveBefore(bg, future, 10, func(d []byte) error {
			return fmt.Errorf("simulated processing failure")
		})
		assert.NoError(t, err)
		assert.EqualValues(t, 0, count)
		assert.EqualValues(t, 1, sset.Size(bg))

		// Callback succeeds → entry should be removed
		count, err = sset.RemoveBefore(bg, future, 10, func(d []byte) error {
			return nil
		})
		assert.NoError(t, err)
		assert.EqualValues(t, 1, count)
		assert.EqualValues(t, 0, sset.Size(bg))
	})
}

// Regression: when a batch contains both failing and succeeding callbacks,
// only the successful entries are removed.
func TestRemoveBeforeMixedCallbacks(t *testing.T) {
	withRedis(t, "removebeforemixed", func(t *testing.T, store Store) {
		bg := context.Background()
		sset := store.Retries()
		assert.NoError(t, sset.Clear(bg))

		// Add 3 jobs
		for range 3 {
			jid, data := fakeJob()
			err := sset.AddElement(bg, util.Nows(), jid, data)
			assert.NoError(t, err)
		}
		assert.EqualValues(t, 3, sset.Size(bg))

		future := util.Thens(time.Now().Add(time.Hour))
		callCount := 0
		count, err := sset.RemoveBefore(bg, future, 10, func(d []byte) error {
			callCount++
			if callCount == 2 {
				return fmt.Errorf("fail on second entry")
			}
			return nil
		})
		assert.NoError(t, err)
		assert.EqualValues(t, 2, count)      // 1st and 3rd succeeded
		assert.EqualValues(t, 1, sset.Size(bg)) // 2nd stayed
	})
}
