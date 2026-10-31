package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// countingStore counts claims and implements nothing else a poll needs.
type countingStore struct {
	Store
	claims atomic.Int32
}

func (s *countingStore) Claim(context.Context, *ClaimRequest) ([]*Claim, error) {
	s.claims.Add(1)
	return nil, nil
}

// Finding 16: once the context is cancelled, poll starts no claim, even when a freed slot or the
// timer is ready at the same time (select picks among ready cases at random).
func TestPollStartsNoClaimAfterTheCancel(t *testing.T) {
	store := &countingStore{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 64 {
		runner := newQueueRunner(runnerDependencies{store: store, observer: guardedObserver{observer: silentObserver{}}}, "mail", QueueOptions{Concurrency: 1, LeaseDuration: time.Second, PollInterval: time.Second}, nil)
		runner.released <- struct{}{}

		runner.poll(ctx)
	}

	require.Zero(t, store.claims.Load())
}

// Round 2, finding 3: the heartbeat runs three times per lease (J7).
func TestTheHeartbeatRunsThreeTimesPerLease(t *testing.T) {
	runner := newQueueRunner(runnerDependencies{}, "mail", QueueOptions{Concurrency: 1, LeaseDuration: 300 * time.Millisecond}, nil)

	require.Equal(t, 100*time.Millisecond, runner.heartbeatInterval())
}
