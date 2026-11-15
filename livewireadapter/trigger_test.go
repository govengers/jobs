package livewireadapter_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	livewirepostgres "github.com/govengers/livewire/postgresadapter"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

// change is one notification of livewire's trigger function.
type change struct {
	Table string `json:"table"`
	Op    string `json:"op"`
	ID    string `json:"id"`
}

// TestTheTriggerNotifiesWhatAViewShowsButNoHeartbeat pins LivewireTriggerSQL (J56): the
// changes of a job's life notify, a lease extension does not. Other tests of the binary share
// the database and its channel, so the notifications are taken by the job's id until a
// sentinel of this test arrives, which commits after every write below.
func TestTheTriggerNotifiesWhatAViewShowsButNoHeartbeat(t *testing.T) {
	fixture := newOperatorFixture(t)
	ctx := context.Background()
	for _, migration := range []string{livewirepostgres.TriggerFunctionSQL, postgresadapter.LivewireTriggerSQL} {
		_, err := fixture.pool.Exec(ctx, migration)
		require.NoError(t, err)
	}
	listener, err := fixture.pool.Acquire(ctx)
	require.NoError(t, err)
	defer listener.Release()
	_, err = listener.Exec(ctx, "LISTEN "+livewirepostgres.Channel)
	require.NoError(t, err)

	id := fixture.enqueue(mailKind.New(mailPayload{}, nil))
	claims, err := fixture.store.Claim(ctx, &jobs.ClaimRequest{Queue: "mail", Limit: 1, LeaseDuration: time.Minute, Worker: "test"})
	require.NoError(t, err)
	lease := claims[0].Lease
	held, err := fixture.store.ExtendLeases(ctx, []jobs.Lease{lease}, time.Minute)
	require.NoError(t, err)
	require.Len(t, held, 1)
	written, err := fixture.store.WriteProgress(ctx, lease, jobs.Progress{Fraction: 0.5, Message: "half"})
	require.NoError(t, err)
	require.True(t, written)
	_, err = fixture.store.Finish(ctx, &jobs.Finish{Lease: lease, Status: jobs.StatusCompleted})
	require.NoError(t, err)
	sentinel := uuid.NewString()
	_, err = fixture.pool.Exec(ctx, "SELECT pg_notify($1, $2)", livewirepostgres.Channel, sentinel)
	require.NoError(t, err)

	var operations []string
	waiting, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		notification, err := listener.Conn().WaitForNotification(waiting)
		require.NoError(t, err)
		if notification.Payload == sentinel {
			break
		}
		var received change
		if json.Unmarshal([]byte(notification.Payload), &received) == nil && received.ID == id.String() {
			require.Equal(t, "govengers_jobs", received.Table)
			operations = append(operations, received.Op)
		}
	}
	require.Equal(t, []string{"insert", "update", "update", "update"}, operations, "enqueue, claim, progress and finish; the heartbeat is silent")
}
