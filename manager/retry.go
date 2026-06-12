package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

type FailPayload struct {
	Jid          string   `json:"jid"`
	ErrorMessage string   `json:"message"`
	ErrorType    string   `json:"errtype"`
	Backtrace    []string `json:"backtrace"`
}

func (m *manager) Fail(ctx context.Context, failure *FailPayload) error {
	if failure == nil {
		return fmt.Errorf("missing failure info")
	}

	jid := failure.Jid
	if jid == "" {
		return fmt.Errorf("missing JID")
	}

	cleanse(failure)

	return m.processFailure(ctx, jid, failure)
}

func cleanse(failure *FailPayload) {
	failure.ErrorType = strings.TrimSpace(failure.ErrorType)
	failure.ErrorMessage = strings.TrimSpace(failure.ErrorMessage)

	if failure.ErrorType != "" {
		if len(failure.ErrorType) > 100 {
			failure.ErrorType = failure.ErrorType[0:100]
		}
	} else {
		failure.ErrorType = "unknown"
	}

	if failure.ErrorMessage != "" {
		if len(failure.ErrorMessage) > 1000 {
			failure.ErrorMessage = failure.ErrorMessage[0:1000]
		}
	} else {
		failure.ErrorMessage = "unknown"
	}

	if failure.Backtrace == nil {
		failure.Backtrace = []string{}
	}
	if len(failure.Backtrace) > 50 {
		failure.Backtrace = failure.Backtrace[0:50]
	}
}

func (m *manager) clearReservation(jid string) *Reservation {
	m.workingMutex.Lock()
	res, ok := m.workingMap[jid]
	if !ok {
		m.workingMutex.Unlock()
		return nil
	}

	delete(m.workingMap, jid)
	m.workingMutex.Unlock()
	return res
}

func (m *manager) processFailure(ctx context.Context, jid string, failure *FailPayload) error {
	res := m.clearReservation(jid)
	if res == nil {
		return fmt.Errorf("Job not found %s", jid)
	}

	// Lease is in-memory only
	// A reservation can have a nil Lease if we restarted
	if res.lease != nil {
		if err := res.lease.Release(); err != nil {
			// Don't let a lease release error prevent job
			// retry processing — log and continue.
			util.Warnf("Error releasing lease for %s: %v", jid, err)
		}
	}

	// when expiring overdue jobs in the working set, we remove in
	// bulk so this job is no longer in the working set already.
	if failure != JobReservationExpired {
		ok, err := m.store.Working().RemoveElement(ctx, res.Expiry, jid)
		if err != nil {
			m.restoreReservation(jid, res)
			return err
		}
		if !ok {
			return nil
		}
	}

	_ = m.store.Failure(ctx)

	job := res.Job

	if job.Failure != nil {
		job.Failure.RetryCount++
		if job.Failure.RetryRemaining > 0 {
			job.Failure.RetryRemaining--
		}
		job.Failure.ErrorMessage = failure.ErrorMessage
		job.Failure.ErrorType = failure.ErrorType
		job.Failure.Backtrace = failure.Backtrace
	} else {
		if job.Retry == nil {
			job.Retry = &client.RetryPolicyDefault
		}
		job.Failure = &client.Failure{
			RetryCount:     0,
			RetryRemaining: *job.Retry,
			FailedAt:       util.Nows(),
			ErrorMessage:   failure.ErrorMessage,
			ErrorType:      failure.ErrorType,
			Backtrace:      failure.Backtrace,
		}
	}

	ctxh := context.WithValue(ctx, MiddlewareHelperKey, Ctx{job, m, res})
	err := callMiddleware(ctxh, m.failChain, func() error {
		if job.Retry == nil || *job.Retry == 0 {
			// no retry, no death, completely ephemeral, goodbye
			return nil
		}
		if job.Failure.RetryCount < *job.Retry {
			return retryLater(ctx, m.store, job)
		}
		return sendToMorgue(ctx, m.store, job)
	})
	if err != nil {
		// Processing failed — restore the reservation so the
		// next ReapExpiredJobs scan (or Fail retry) can pick
		// it up again.  RemoveBefore keeps the sorted-set
		// entry intact when the callback returns an error,
		// so together they prevent job loss.
		m.restoreReservation(jid, res)
		return err
	}
	return nil
}

// restoreReservation puts a reservation back into workingMap if it
// has not already been replaced by a new reservation for the same JID.
func (m *manager) restoreReservation(jid string, res *Reservation) {
	m.workingMutex.Lock()
	if _, exists := m.workingMap[jid]; !exists {
		m.workingMap[jid] = res
	}
	m.workingMutex.Unlock()
}

func retryLater(ctx context.Context, store storage.Store, job *client.Job) error {
	when := util.Thens(nextRetry(job))
	job.Failure.NextAt = when
	bytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("cannot marshal job payload: %w", err)
	}

	return store.Retries().AddElement(ctx, when, job.Jid, bytes)
}

func sendToMorgue(ctx context.Context, store storage.Store, job *client.Job) error {
	bytes, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("cannot marshal job payload: %w", err)
	}

	expiry := util.Thens(time.Now().Add(DeadTTL))
	return store.Dead().AddElement(ctx, expiry, job.Jid, bytes)
}

func nextRetry(job *client.Job) time.Time {
	count := job.Failure.RetryCount
	secs := (count * count * count * count) + 15 + (rand.Intn(30) * (count + 1)) //nolint:gosec
	return time.Now().Add(time.Duration(secs) * time.Second)
}
