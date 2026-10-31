package jobs

import (
	"encoding/json"

	"github.com/google/uuid"
)

// Status is the state of a stored job.
type Status string

// The states of a job. A job starts pending, is running while a worker holds its lease and
// ends completed, dead or cancelled; a retry, a snooze, a release or a rescued lease returns
// it to pending.
const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusDead      Status = "dead"
	StatusCancelled Status = "cancelled"
)

// Lease identifies one attempt of a job: the token changes with every claim, so a write that
// matches it proves the writer still holds the job.
type Lease struct {
	JobID uuid.UUID
	Token uuid.UUID
}

// Claim is a job a worker claimed for one attempt, as middleware and the store see it.
type Claim struct {
	Lease           Lease
	Queue           string
	Kind            string
	Payload         json.RawMessage
	Headers         map[string]string // written by the Propagators at enqueue time
	Attempt         int               // 1 for the first attempt
	MaximumAttempts int
}

// Job is the typed view of a claimed job that a handler registered with Handle receives.
type Job[P any] struct {
	ID              uuid.UUID
	Queue           string
	Kind            string
	Attempt         int // 1 for the first attempt
	MaximumAttempts int
	Payload         P
}
