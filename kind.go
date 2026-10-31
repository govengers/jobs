package jobs

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// DefaultMaxAttempts is the number of attempts of a kind whose options leave MaxAttempts at zero.
const DefaultMaxAttempts = 10

// ErrInvalidKind is returned by Handle and by the store's Enqueue for a kind without a name or a queue.
var ErrInvalidKind = errors.New("jobs: invalid kind")

// Kind is the typed contract of one kind of job, shared by the code that enqueues it and the
// handler that runs it. Declare it once, usually as a package-level variable:
//
//	var RenderKind = jobs.NewKind[RenderPayload]("generation.render", &jobs.KindOptions{Queue: "generation"})
//
// The payload P is encoded as JSON. A Kind is immutable and safe for concurrent use.
type Kind[P any] struct {
	name        string
	queue       string
	maxAttempts int
	backoff     Backoff
}

// KindOptions configures a Kind.
type KindOptions struct {
	Queue       string  // required; the queue the kind's jobs are enqueued on
	MaxAttempts int     // attempts before the job is dead; zero means DefaultMaxAttempts
	Backoff     Backoff // delay before a retry; nil means DefaultBackoff
}

// NewKind declares a kind. It does not validate, so it can initialize a package-level
// variable: Handle and Enqueue return ErrInvalidKind for an empty name, an empty queue or a
// negative MaxAttempts. A nil options value declares a kind without a queue.
func NewKind[P any](name string, options *KindOptions) *Kind[P] {
	kind := &Kind[P]{name: name, maxAttempts: DefaultMaxAttempts, backoff: DefaultBackoff}
	if options == nil {
		return kind
	}
	kind.queue = options.Queue
	if options.MaxAttempts != 0 {
		kind.maxAttempts = options.MaxAttempts
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
	job := &NewJob{kind: k.name, queue: k.queue, maxAttempts: k.maxAttempts, payload: payload, err: k.validate()}
	if options != nil {
		job.runAt = options.RunAt
		job.uniqueKey = options.UniqueKey
	}
	return job
}

func (k *Kind[P]) validate() error {
	if k.name == "" || k.queue == "" || k.maxAttempts < 1 {
		return fmt.Errorf("%w: name %q, queue %q, max attempts %d", ErrInvalidKind, k.name, k.queue, k.maxAttempts)
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
	kind        string
	queue       string
	maxAttempts int
	payload     any
	runAt       time.Time
	uniqueKey   string
	err         error
}

// Kind is the name of the job's kind.
func (j *NewJob) Kind() string { return j.kind }

// Queue is the queue the job is enqueued on.
func (j *NewJob) Queue() string { return j.queue }

// MaxAttempts is the number of attempts before the job is dead.
func (j *NewJob) MaxAttempts() int { return j.maxAttempts }

// RunAt is the earliest time the job may be claimed; zero means now.
func (j *NewJob) RunAt() time.Time { return j.runAt }

// UniqueKey is the job's unique key; empty means none.
func (j *NewJob) UniqueKey() string { return j.uniqueKey }

// EncodePayload returns the payload as JSON. It fails with ErrInvalidKind when the job's kind
// is invalid, and with the encoding error when the payload cannot be encoded.
func (j *NewJob) EncodePayload() (json.RawMessage, error) {
	if j.err != nil {
		return nil, j.err
	}
	payload, err := json.Marshal(j.payload)
	if err != nil {
		return nil, fmt.Errorf("encode payload of %s: %w", j.kind, err)
	}
	return payload, nil
}
