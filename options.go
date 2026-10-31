package jobs

import (
	"errors"
	"fmt"
	"time"

	"github.com/govengers/jobs/internal/recovery"
)

// minimumLeaseDuration keeps the heartbeat interval (Lease/3) meaningful.
const minimumLeaseDuration = 30 * time.Millisecond

func queuesWithDefaults(queues map[string]QueueOptions) (map[string]QueueOptions, error) {
	withDefaults := make(map[string]QueueOptions, len(queues))
	for name, options := range queues {
		if name == "" {
			return nil, errors.New("jobs: a queue needs a name")
		}
		if options.Concurrency < 0 || options.LeaseDuration < 0 || options.PollInterval < 0 {
			return nil, fmt.Errorf("jobs: queue %s: negative concurrency, lease or poll interval", name)
		}
		options.Concurrency = orDefault(options.Concurrency, DefaultConcurrency)
		options.LeaseDuration = orDefault(options.LeaseDuration, DefaultLeaseDuration)
		options.PollInterval = orDefault(options.PollInterval, DefaultPollInterval)
		if options.LeaseDuration < minimumLeaseDuration {
			return nil, fmt.Errorf("jobs: queue %s: lease %s is shorter than %s", name, options.LeaseDuration, minimumLeaseDuration)
		}
		withDefaults[name] = options
	}
	return withDefaults, nil
}

// timing holds the intervals, timeouts and retentions of a worker after defaults.
type timing struct {
	drainTimeout     time.Duration
	rescueInterval   time.Duration
	sweepInterval    time.Duration
	scheduleInterval time.Duration
	retainCompleted  time.Duration
	retainDead       time.Duration
}

func timingWithDefaults(options *WorkerOptions) (timing, error) {
	durations := timing{
		drainTimeout:     orDefault(options.DrainTimeout, DefaultDrainTimeout),
		rescueInterval:   orDefault(options.RescueInterval, DefaultRescueInterval),
		sweepInterval:    orDefault(options.SweepInterval, DefaultSweepInterval),
		scheduleInterval: orDefault(options.ScheduleInterval, DefaultScheduleInterval),
		retainCompleted:  orDefault(options.RetainCompleted, DefaultRetainCompleted),
		retainDead:       orDefault(options.RetainDead, DefaultRetainDead),
	}
	for _, duration := range []time.Duration{
		durations.drainTimeout, durations.rescueInterval, durations.sweepInterval,
		durations.scheduleInterval, durations.retainCompleted, durations.retainDead,
	} {
		if duration < 0 {
			return timing{}, errors.New("jobs: negative durations in WorkerOptions")
		}
	}
	return durations, nil
}

// snapshotSchedules validates the schedules and returns the worker's own copies, their jobs'
// payloads encoded once.
func snapshotSchedules(schedules []Schedule) ([]Schedule, error) {
	if err := validateSchedules(schedules); err != nil {
		return nil, err
	}
	snapshots := make([]Schedule, 0, len(schedules))
	for _, schedule := range schedules {
		job, err := schedule.Job.snapshot()
		if err != nil {
			return nil, fmt.Errorf("jobs: schedule %s: %w", schedule.Name, err)
		}
		schedule.Job = job
		snapshots = append(snapshots, schedule)
	}
	return snapshots, nil
}

func validateSchedules(schedules []Schedule) error {
	names := make(map[string]bool, len(schedules))
	now := time.Now()
	for _, schedule := range schedules {
		if schedule.Name == "" || schedule.Next == nil || schedule.Job == nil {
			return errors.New("jobs: a schedule needs a name, a Next function and a job")
		}
		if names[schedule.Name] {
			return fmt.Errorf("jobs: schedule %s is declared twice", schedule.Name)
		}
		names[schedule.Name] = true
		if err := checkNext(schedule, now); err != nil {
			return err
		}
	}
	return nil
}

// checkNext calls the schedule's Next once and refuses a time that is not after now, or a panic.
// The payload is not encoded here: snapshotSchedules encodes it, once.
func checkNext(schedule Schedule, now time.Time) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("jobs: %w", recovery.Error("schedule "+schedule.Name+": Next", recovered))
		}
	}()
	if !schedule.Next(now).After(now) {
		return fmt.Errorf("jobs: schedule %s: Next must return a time after now", schedule.Name)
	}
	return nil
}

// validateCollaborators rejects nil middleware and propagators, which would panic on the
// first attempt instead of at startup (DES-001).
func validateCollaborators(options *WorkerOptions) error {
	for index, middleware := range options.Middleware {
		if middleware == nil {
			return fmt.Errorf("jobs: WorkerOptions.Middleware[%d] is nil", index)
		}
	}
	for index, propagator := range options.Propagators {
		if propagator == nil {
			return fmt.Errorf("jobs: WorkerOptions.Propagators[%d] is nil", index)
		}
	}
	return nil
}

// orDefault returns fallback for a zero value.
func orDefault[T comparable](value, fallback T) T {
	var zero T
	if value == zero {
		return fallback
	}
	return value
}
