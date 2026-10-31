package jobs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultMaximumAttempts is the number of attempts of a kind whose options leave MaximumAttempts at zero.
const DefaultMaximumAttempts = 10

// ErrInvalidKind is returned by Handle and by the store's Enqueue for a kind without a name or a queue.
var ErrInvalidKind = errors.New("jobs: invalid kind")

// Kind is the typed contract of one kind of job, shared by the code that enqueues it and the
// handler that runs it. Declare it once, usually as a package-level variable:
//
//	var RenderKind = jobs.NewKind[RenderPayload]("generation.render", &jobs.KindOptions{Queue: "generation"})
//
// The payload P is encoded as JSON. A Kind is immutable and safe for concurrent use.
type Kind[P any] struct {
	name            string
	queue           string
	maximumAttempts int
	backoff         Backoff
}

// KindOptions configures a Kind.
type KindOptions struct {
	Queue           string  // required; the queue the kind's jobs are enqueued on
	MaximumAttempts int     // attempts before the job is dead; zero means DefaultMaximumAttempts
	Backoff         Backoff // delay before a retry; nil means DefaultBackoff
}

// NewKind declares a kind. It does not validate, so it can initialize a package-level
// variable: Handle and Enqueue return ErrInvalidKind for an empty name, an empty queue or a
// negative MaximumAttempts. A nil options value declares a kind without a queue.
func NewKind[P any](name string, options *KindOptions) *Kind[P] {
	kind := &Kind[P]{name: name, maximumAttempts: DefaultMaximumAttempts, backoff: DefaultBackoff}
	if options == nil {
		return kind
	}
	kind.queue = options.Queue
	if options.MaximumAttempts != 0 {
		kind.maximumAttempts = options.MaximumAttempts
	}
	if options.Backoff != nil {
		kind.backoff = options.Backoff
	}
	return kind
}

// Name is the kind's name as stored with every job.
func (k *Kind[P]) Name() string { return k.name }

// Queue is the queue the kind's jobs are enqueued on.
func (k *Kind[P]) Queue() string { return k.queue }

// New prepares a job of this kind for the store's Enqueue. A nil options value enqueues it to
// run now without a unique key.
func (k *Kind[P]) New(payload P, options *EnqueueOptions) *NewJob {
	job := &NewJob{kind: k.name, queue: k.queue, maximumAttempts: k.maximumAttempts, payload: payload}
	if options != nil {
		job.runAt = options.RunAt
		job.uniqueKey = options.UniqueKey
	}
	return job
}

func (k *Kind[P]) validate() error {
	return validateKind(k.name, k.queue, k.maximumAttempts)
}

// validateKind is the rule of J20 for a kind and for every job built from one, including a
// zero NewJob.
func validateKind(name, queue string, maximumAttempts int) error {
	if name == "" || queue == "" || maximumAttempts < 1 {
		return fmt.Errorf("%w: name %q, queue %q, maximum attempts %d", ErrInvalidKind, name, queue, maximumAttempts)
	}
	return nil
}

// EnqueueOptions configures one enqueued job.
type EnqueueOptions struct {
	RunAt time.Time // the job is not claimed before RunAt; zero means now (database time)
	// UniqueKey coalesces enqueues: while a job of the same queue and key has not started, a
	// second enqueue returns that job's ID instead of inserting. Claiming a job releases its key
	// (J19), so an enqueue during a run starts another run.
	UniqueKey string
}

// NewJob is a job prepared by Kind.New, ready for the store's Enqueue.
type NewJob struct {
	kind            string
	queue           string
	maximumAttempts int
	payload         any
	runAt           time.Time
	uniqueKey       string
	encoded         json.RawMessage // set by snapshot: the payload as JSON, fixed
}

// Kind is the name of the job's kind.
func (j *NewJob) Kind() string { return j.kind }

// Queue is the queue the job is enqueued on.
func (j *NewJob) Queue() string { return j.queue }

// MaximumAttempts is the number of attempts before the job is dead.
func (j *NewJob) MaximumAttempts() int { return j.maximumAttempts }

// RunAt is the earliest time the job may be claimed; zero means now.
func (j *NewJob) RunAt() time.Time { return j.runAt }

// UniqueKey is the job's unique key; empty means none.
func (j *NewJob) UniqueKey() string { return j.uniqueKey }

// EncodePayload returns the payload as JSON. It fails with ErrInvalidKind when the job's kind
// is invalid (also for a zero NewJob), and with the encoding error when the payload cannot be
// encoded.
func (j *NewJob) EncodePayload() (json.RawMessage, error) {
	if err := validateKind(j.kind, j.queue, j.maximumAttempts); err != nil {
		return nil, err
	}
	if j.encoded != nil {
		return bytes.Clone(j.encoded), nil
	}
	payload, err := json.Marshal(j.payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload of %s: %w", j.kind, err)
	}
	return payload, nil
}

// snapshot returns a copy of the job with its payload encoded now, so a job a worker keeps (a
// schedule's) does not follow later changes of the caller's payload value.
func (j *NewJob) snapshot() (*NewJob, error) {
	encoded, err := j.EncodePayload()
	if err != nil {
		return nil, err
	}
	copied := *j
	copied.payload, copied.encoded = nil, encoded
	return &copied, nil
}
