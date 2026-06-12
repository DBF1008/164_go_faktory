package manager

import (
	"context"
	"testing"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
	"github.com/stretchr/testify/assert"
)

func TestLoadWorkingSet(t *testing.T) {
	withRedis(t, "working", func(t *testing.T, store storage.Store) {
		bg := context.Background()
		t.Run("LoadWorkingSet", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())

			m2 := newManager(store)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m2.WorkingCount())
		})

		t.Run("ManagerReserve", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			job.ReserveFor = 600
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err := m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
		})

		t.Run("ReserveWithInvalidTimeout", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			timeouts := []int{0, 20, 50, 59, 86401, 100000}
			for _, timeout := range timeouts {
				job := client.NewJob("InvalidJob", 1, 2, 3)
				job.ReserveFor = timeout
				assert.EqualValues(t, 0, store.Working().Size(bg))

				// doesn't return an error but resets to default timeout
				lease := &simpleLease{job: job}
				err := m.reserve(bg, "workerId", lease)

				assert.NoError(t, err)
				assert.EqualValues(t, 1, store.Working().Size(bg))
				err = store.Working().Clear(bg)
				assert.NoError(t, err)
			}
		})

		t.Run("ManagerAcknowledge", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job, err := m.Acknowledge(bg, "")
			assert.NoError(t, err)
			assert.Nil(t, job)

			job = client.NewJob("AckJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))

			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())
			assert.EqualValues(t, 0, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.False(t, lease.released)

			assert.EqualValues(t, 1, m.BusyCount("workerId"))
			assert.EqualValues(t, 0, m.BusyCount("fakeId"))

			aJob, err := m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.Equal(t, job.Jid, aJob.Jid)
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
			assert.EqualValues(t, 0, m.BusyCount("workerId"))
			assert.True(t, lease.released)

			aJob, err = m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			assert.Nil(t, aJob)
			assert.EqualValues(t, 1, store.TotalProcessed(bg))
			assert.EqualValues(t, 0, store.TotalFailures(bg))
		})

		t.Run("ManagerReapExpiredJobs", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			job := client.NewJob("WorkingJob", 1, 2, 3)
			q, err := store.GetQueue(bg, job.Queue)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 0, store.Working().Size(bg))
			assert.EqualValues(t, 0, m.WorkingCount())

			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)

			assert.NoError(t, err)
			assert.EqualValues(t, 0, q.Size(bg))
			assert.EqualValues(t, 1, store.Working().Size(bg))
			assert.EqualValues(t, 1, m.WorkingCount())

			exp := time.Now().Add(time.Duration(10) * time.Second)
			count, err := m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			found, err := m.ExtendReservation(bg, "nosuch", time.Now().Add(50*time.Hour))
			assert.NoError(t, err)
			assert.False(t, found)

			util.LogInfo = true
			util.LogDebug = true
			util.Infof("Extending %s", job.Jid)
			found, err = m.ExtendReservation(bg, job.Jid, time.Now().Add(50*time.Hour))
			assert.NoError(t, err)
			assert.True(t, found)

			exp = time.Now().Add(time.Duration(DefaultTimeout+10) * time.Second)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			exp = time.Now().Add(51 * time.Hour)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
		})

		t.Run("ManagerRenewReservation", func(t *testing.T) {
			assert.NoError(t, store.Flush(bg))
			m := newManager(store)

			// Renew on non-existent JID returns false
			found, err := m.ExtendReservation(bg, "nonexistent", time.Now().Add(1*time.Hour))
			assert.NoError(t, err)
			assert.False(t, found)

			// Reserve a job with short timeout
			job := client.NewJob("RenewableJob", 1, 2, 3)
			job.ReserveFor = 120
			lease := &simpleLease{job: job}
			err = m.reserve(bg, "workerId", lease)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, m.WorkingCount())

			// Renew the reservation with a longer time
			newUntil := time.Now().Add(1 * time.Hour)
			found, err = m.ExtendReservation(bg, job.Jid, newUntil)
			assert.NoError(t, err)
			assert.True(t, found)
			assert.True(t, m.workingMap[job.Jid].extension.Equal(newUntil))

			// Renew with shorter time than current extension — no-op
			shortUntil := time.Now().Add(30 * time.Second)
			found, err = m.ExtendReservation(bg, job.Jid, shortUntil)
			assert.NoError(t, err)
			assert.True(t, found)
			// extension should still be the longer one
			assert.True(t, m.workingMap[job.Jid].extension.Equal(newUntil))

			// ACK the job, then try to renew — should return false
			_, err = m.Acknowledge(bg, job.Jid)
			assert.NoError(t, err)
			found, err = m.ExtendReservation(bg, job.Jid, time.Now().Add(1*time.Hour))
			assert.NoError(t, err)
			assert.False(t, found)

			// Verify that a renewed reservation survives past the original expiry
			assert.NoError(t, store.Flush(bg))
			m = newManager(store)

			job2 := client.NewJob("RenewSurvivalJob", 1, 2, 3)
			job2.ReserveFor = 120 // 2 minutes
			lease2 := &simpleLease{job: job2}
			err = m.reserve(bg, "workerId", lease2)
			assert.NoError(t, err)

			// Extend reservation to 1 hour from now
			extendUntil := time.Now().Add(1 * time.Hour)
			found, err = m.ExtendReservation(bg, job2.Jid, extendUntil)
			assert.NoError(t, err)
			assert.True(t, found)

			// Reap at original expiry + 10s — should NOT reap (extended)
			exp := time.Now().Add(130 * time.Second)
			count, err := m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 0, count)
			assert.EqualValues(t, 0, store.Retries().Size(bg))

			// Reap at extended expiry + 10s — should reap
			exp = time.Now().Add(1*time.Hour + 10*time.Second)
			count, err = m.ReapExpiredJobs(bg, exp)
			assert.NoError(t, err)
			assert.EqualValues(t, 1, count)
			assert.EqualValues(t, 1, store.Retries().Size(bg))
		})
	})
}
