package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/contribsys/faktory/client"
	"github.com/contribsys/faktory/storage"
	"github.com/contribsys/faktory/util"
)

func (m *manager) Purge(ctx context.Context, when time.Time) (int64, error) {
	// First drop everything past its retention TTL.
	dead, err := m.store.Dead().RemoveBefore(ctx, util.Thens(when), 100, func([]byte) error {
		return nil
	})
	if err != nil {
		return 0, err
	}

	// Then enforce the maximum dead set size so a flood of failures can't grow
	// it without bound. This keeps background cleanup consistent with the
	// trimming done when jobs enter the morgue (sendToMorgue / MoveToDead).
	trimmed, err := m.trimDeadSet(ctx)
	if err != nil {
		return dead, err
	}
	return dead + trimmed, nil
}

func (m *manager) EnqueueScheduledJobs(ctx context.Context, when time.Time) (int64, error) {
	return m.schedule(ctx, when, m.store.Scheduled())
}

func (m *manager) RetryJobs(ctx context.Context, when time.Time) (int64, error) {
	return m.schedule(ctx, when, m.store.Retries())
}

func (m *manager) schedule(ctx context.Context, when time.Time, set storage.SortedSet) (int64, error) {
	total := int64(0)
	for {
		count, err := set.RemoveBefore(ctx, util.Thens(when), 100, func(data []byte) error {
			var job client.Job
			if err := util.JsonUnmarshal(data, &job); err != nil {
				return fmt.Errorf("cannot unmarshal job payload: %w", err)
			}

			if err := m.enqueue(ctx, &job); err != nil {
				return fmt.Errorf("cannot push job to %q queue: %w", job.Queue, err)
			}
			return nil
		})
		total += count
		if err != nil {
			return total, err
		}
		if count != 100 {
			break
		}
	}
	return total, nil
}
