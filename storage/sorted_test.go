package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

		t.Run("RemoveBefore restores jobs when processing fails", func(t *testing.T) {
			sset := store.Scheduled()
			assert.NoError(t, sset.Clear(bg))

			tim := util.Nows()
			goodJid, goodData := fakeJob()
			badJid, badData := fakeJob()
			good2Jid, good2Data := fakeJob()
			assert.NoError(t, sset.AddElement(bg, tim, goodJid, goodData))
			assert.NoError(t, sset.AddElement(bg, tim, badJid, badData))
			assert.NoError(t, sset.AddElement(bg, tim, good2Jid, good2Data))
			assert.EqualValues(t, 3, sset.Size(bg))

			// Simulate a downstream failure while processing one specific job.
			processed := 0
			count, err := sset.RemoveBefore(bg, util.Nows(), 100, func(data []byte) error {
				if strings.Contains(string(data), badJid) {
					return errors.New("boom: downstream processing failed")
				}
				processed++
				return nil
			})
			// A single failing job must not abort the whole batch, and the
			// error is not propagated to the caller.
			assert.NoError(t, err)
			// Only the two healthy jobs are processed and removed.
			assert.EqualValues(t, 2, count)
			assert.Equal(t, 2, processed)

			// The failed job must still be in the set so a later scan retries
			// it, rather than being silently dropped.
			assert.EqualValues(t, 1, sset.Size(bg))
			remaining := []string{}
			err = sset.Each(bg, func(idx int, e SortedEntry) error {
				j, err := e.Job()
				assert.NoError(t, err)
				remaining = append(remaining, j.Jid)
				return nil
			})
			assert.NoError(t, err)
			assert.Equal(t, []string{badJid}, remaining)

			assert.NoError(t, sset.Clear(bg))
		})
	})
}
