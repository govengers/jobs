package livewireadapter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/govengers/livewire"
	livewirepostgres "github.com/govengers/livewire/postgresadapter"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

// TestTheTriggerNotifiesWhatAViewShowsButNoHeartbeat pins LivewireTriggerSQL (J56, J61): the
// changes of a job's life notify in the shape of livewire's trigger function, without that
// function installed, a lease extension does not. The notifications are received through
// livewire's own Listener, whose strict decode drops a payload of another shape and reports
// it to OnError, so a drift between the two modules fails here. Other tests of the binary
// share the database and its channel, so the changes are taken by the job's id until a
// sentinel change of this test arrives, which commits after every write below.
func TestTheTriggerNotifiesWhatAViewShowsButNoHeartbeat(t *testing.T) {
	fixture := newOperatorFixture(t)
	ctx := context.Background()
	_, err := fixture.pool.Exec(ctx, postgresadapter.LivewireTriggerSQL)
	require.NoError(t, err)
	var mutex sync.Mutex
	var failures []error
	listener, err := livewirepostgres.NewListener(&livewirepostgres.ListenerOptions{
		DSN: fixture.pool.Config().ConnString(),
		OnError: func(err error) {
			mutex.Lock()
			defer mutex.Unlock()
			failures = append(failures, err)
		},
	})
	require.NoError(t, err)
	listening, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	changes := make(chan livewire.Change, 64)
	resynced := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		done <- listener.Run(listening, func(change livewire.Change) { changes <- change }, func() {
			select {
			case resynced <- struct{}{}:
			default:
			}
		})
	}()
	select {
	case <-resynced:
	case <-listening.Done():
		t.Fatal("the listener did not LISTEN")
	}

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
	deleted, err := fixture.store.Sweep(ctx, &jobs.SweepRequest{Limit: 1})
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	_, err = fixture.pool.Exec(ctx, "TRUNCATE govengers_jobs")
	require.NoError(t, err)
	sentinel := uuid.NewString()
	_, err = fixture.pool.Exec(ctx, "SELECT pg_notify($1, json_build_object('table', 'sentinel', 'op', 'insert', 'id', $2::text, 'partition', '')::text)",
		livewirepostgres.Channel, sentinel)
	require.NoError(t, err)

	var operations []string
	for received := false; !received; {
		select {
		case change := <-changes:
			switch {
			case change.Table == "sentinel" && change.ID == sentinel:
				received = true
			case change.ID == id.String() || change.ID == "":
				require.Equal(t, "govengers_jobs", change.Table)
				require.Empty(t, change.Partition)
				operations = append(operations, string(change.Operation)+" "+change.ID)
			}
		case <-listening.Done():
			t.Fatal("the sentinel change did not arrive")
		}
	}
	stop()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Empty(t, failures, "livewire's Listener dropped or failed nothing")
	require.Equal(t, []string{"insert " + id.String(), "update " + id.String(), "update " + id.String(), "update " + id.String(), "delete " + id.String(), "delete "},
		operations, "enqueue, claim, progress, finish, sweep and TRUNCATE; the heartbeat is silent")
}
