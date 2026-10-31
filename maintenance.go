package jobs

import (
	"context"
	"time"
)

// maintain rescues expired leases and sweeps old jobs until ctx is cancelled. Every worker
// does it; the store's row locks keep the workers from doing the same work twice, so no leader
// is needed. Each task runs once at start, then on its interval. Schedules have a loop of their
// own (enqueueSchedules), so a schedule that waits never delays a rescue.
func (w *Worker) maintain(ctx context.Context) error {
	rescue := time.NewTicker(w.timing.rescueInterval)
	defer rescue.Stop()
	sweep := time.NewTicker(w.timing.sweepInterval)
	defer sweep.Stop()
	w.rescue(ctx)
	w.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-rescue.C:
			w.rescue(ctx)
		case <-sweep.C:
			w.sweep(ctx)
		}
	}
}

// enqueueSchedules checks the schedules once at start, then every ScheduleInterval, until ctx
// is cancelled.
func (w *Worker) enqueueSchedules(ctx context.Context) error {
	ticker := time.NewTicker(w.timing.scheduleInterval)
	defer ticker.Stop()
	for {
		for _, schedule := range w.schedules {
			if ctx.Err() != nil {
				return nil
			}
			w.enqueueSchedule(schedule)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
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

// enqueueSchedule checks one schedule under its own operation timeout. A scheduled job belongs
// to no request: it is enqueued on a context without the values of Run's context, so the
// propagators inject no identity or trace main put there. The store turns a panic of the
// schedule's Next into this check's error; a panic of the store itself ends Run (J35). observe
// reports the error to the Observer; the next interval tries again.
func (w *Worker) enqueueSchedule(schedule Schedule) {
	_, _ = w.observe(context.Background(), OperationSchedules, func(operationContext context.Context) (int, error) {
		wasDue, err := w.store.EnqueueDueSchedule(operationContext, schedule)
		if wasDue {
			return 1, err
		}
		return 0, err
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
