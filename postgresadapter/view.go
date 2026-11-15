package postgresadapter

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/govengers/jobs"
)

// State is a job's status as a user interface names it (J55).
type State string

// The states of View.Status.
const (
	StateQueued    State = "queued"    // pending: waiting for its run time or a free worker, a retry included
	StateRunning   State = "running"   // a worker runs an attempt
	StateSucceeded State = "succeeded" // completed
	StateFailed    State = "failed"    // dead: a permanent error, or every attempt failed
	StateCancelled State = "cancelled" // cancelled before or while it ran
)

// StateOf maps a stored status to its State.
func StateOf(status jobs.Status) State {
	switch status {
	case jobs.StatusPending:
		return StateQueued
	case jobs.StatusRunning:
		return StateRunning
	case jobs.StatusCompleted:
		return StateSucceeded
	case jobs.StatusDead:
		return StateFailed
	default:
		return StateCancelled
	}
}

// View is the stable view of a job for user interfaces and API clients (J55): what a person or
// an agent following a job needs, without the payload and the headers, which may carry
// references and the identity snapshot of whoever enqueued it. Its JSON names are the field
// names of livewireadapter's resource. Fields are only added in minor versions.
type View struct {
	ID              uuid.UUID `json:"id"`
	Queue           string    `json:"queue"`
	Kind            string    `json:"kind"`
	Status          State     `json:"status"`
	Attempt         int       `json:"attempt"` // attempts started; 0 while the first has not
	MaximumAttempts int       `json:"maximumAttempts"`
	// Progress is in percent, 0 to 100: nil while the running or last attempt reported none,
	// 100 once the job succeeded.
	Progress        *float64        `json:"progress"`
	ProgressMessage string          `json:"progressMessage"`
	Result          json.RawMessage `json:"result"`     // what the handler set with jobs.SetResult; null unless succeeded with one
	Error           string          `json:"error"`      // the last failed attempt's error; a later success keeps it
	ErrorClass      jobs.ErrorClass `json:"errorClass"` // empty without an error
	CancelRequested bool            `json:"cancelRequested"`
	CreatedAt       time.Time       `json:"createdAt"`
	RunAt           time.Time       `json:"runAt"`     // when a queued job may run next
	StartedAt       *time.Time      `json:"startedAt"` // when the last attempt started
	FinishedAt      *time.Time      `json:"finishedAt"`
	UpdatedAt       time.Time       `json:"updatedAt"`
}

// View returns the record's view.
func (r *Record) View() View {
	view := View{
		ID: r.ID, Queue: r.Queue, Kind: r.Kind, Status: StateOf(r.Status),
		Attempt: r.Attempt, MaximumAttempts: r.MaximumAttempts,
		Result: r.Result, Error: r.LastError, ErrorClass: r.ErrorClass, CancelRequested: r.CancelRequested,
		CreatedAt: r.CreatedAt, RunAt: r.RunAt, StartedAt: r.StartedAt, FinishedAt: r.FinishedAt, UpdatedAt: r.UpdatedAt,
	}
	if r.Progress != nil {
		view.Progress = new(r.Progress.Fraction * 100)
		view.ProgressMessage = r.Progress.Message
	}
	if r.Status == jobs.StatusCompleted {
		view.Progress = new(float64(100))
	}
	return view
}
