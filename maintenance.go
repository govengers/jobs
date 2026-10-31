package jobs

import (
	"context"
	"time"
)

// maintain rescues expired leases, sweeps old jobs and enqueues due schedules until ctx is
// cancelled. Every worker does it; the store's row locks keep the workers from doing the same
// work twice, so no leader is needed. Each task runs once at start, then on its interval.
func (w *Worker) maintain(ctx context.Context) error {
	rescue := time.NewTicker(w.timing.rescueInterval)
	defer rescue.Stop()
	sweep := time.NewTicker(w.timing.sweepInterval)
	defer sweep.Stop()
	schedules := time.NewTicker(w.timing.scheduleInterval)
	defer schedules.Stop()
	w.rescue(ctx)
	w.sweep(ctx)
	w.enqueueDueSchedules(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-rescue.C:
			w.rescue(ctx)
		case <-sweep.C:
			w.sweep(ctx)
		case <-schedules.C:
			w.enqueueDueSchedules(ctx)
		}
	}
}

// rescue runs batches until one comes back short, so a backlog clears within one interval.
func (w *Worker) rescue(ctx context.Context) {
	w.repeatBatches(ctx, OperationRescue, rescueBatch, func(operationContext context.Context) (int, error) {
		return w.store.Rescue(operationContext, rescueBatch)
	})
}

func (w *Worker) sweep(ctx context.Context) {
	request := &SweepRequest{RetainCompleted: w.timing.retainCompleted, RetainDead: w.timing.retainDead, Limit: sweepBatch}
	w.repeatBatches(ctx, OperationSweep, sweepBatch, func(operationContext context.Context) (int, error) {
		return w.store.Sweep(operationContext, request)
	})
}

func (w *Worker) repeatBatches(ctx context.Context, name OperationName, batch int, operation func(context.Context) (int, error)) {
	for ctx.Err() == nil {
		count, err := w.observe(ctx, name, operation)
		if err != nil || count < batch {
			return
		}
	}
}

func (w *Worker) enqueueDueSchedules(ctx context.Context) {
	if len(w.schedules) == 0 {
		return
	}
	// A scheduled job belongs to no request: it is enqueued on a context without the values of
	// Run's context, so the propagators inject no identity or trace main put there. observe
	// reports the error to the Observer; the next interval tries again.
	_, _ = w.observe(context.Background(), OperationSchedules, func(operationContext context.Context) (int, error) {
		return w.store.EnqueueDueSchedules(operationContext, w.schedules)
	})
}

// observe runs one store operation under the operation timeout and reports it.
func (w *Worker) observe(ctx context.Context, name OperationName, operation func(context.Context) (int, error)) (int, error) {
	operationContext, cancel := detached(ctx)
	defer cancel()
	started := time.Now()
	count, err := operation(operationContext)
	w.observer.operationFinished(&Operation{Name: name, Count: count, Duration: time.Since(started), Err: err})
	return count, err
}
