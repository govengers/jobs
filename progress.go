package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
	"unicode/utf8"
)

// MaximumProgressMessageLength is the number of characters of a progress message the store
// keeps; ReportProgress cuts a longer message.
const MaximumProgressMessageLength = 200

// MaximumResultBytes is the size of the largest result SetResult accepts, as encoded JSON.
const MaximumResultBytes = 64 << 10

// Errors of SetResult.
var (
	// ErrNoAttempt is returned by SetResult outside a running attempt: on a context no worker
	// gave to a handler, or after the attempt's handler returned.
	ErrNoAttempt = errors.New("jobs: not inside a job attempt")
	// ErrResultTooLarge is returned by SetResult for a result above MaximumResultBytes.
	ErrResultTooLarge = errors.New("jobs: result too large")
)

// Progress is how far a running attempt got, as a handler reports it with ReportProgress.
type Progress struct {
	Fraction float64 // from 0 (nothing done) to 1 (all done); a percentage is Fraction * 100
	Message  string  // a short line for people, such as "page 4 of 10"
}

// ReportProgress records how far the attempt ctx belongs to got. It never blocks on the
// database: the worker writes the latest report in the background, at most once per queue's
// ProgressInterval and fenced by the attempt's lease, so a handler may report as often as it
// likes; the report left unwritten when the handler returns is written with the attempt's
// outcome. A message longer than MaximumProgressMessageLength characters is cut. Each claim
// of the job clears the progress of the attempt before. Outside an attempt, and after its
// handler returned, it does nothing, so code shared with other callers may report freely. It
// returns an error only for a fraction outside 0 to 1, or NaN.
func ReportProgress(ctx context.Context, progress Progress) error {
	if math.IsNaN(progress.Fraction) || progress.Fraction < 0 || progress.Fraction > 1 {
		return fmt.Errorf("jobs: progress fraction %v is not between 0 and 1", progress.Fraction)
	}
	output, ok := ctx.Value(attemptOutputKey{}).(*attemptOutput)
	if !ok {
		return nil
	}
	progress.Message = truncate(progress.Message, MaximumProgressMessageLength)
	output.report(progress)
	return nil
}

// SetResult attaches value, encoded as JSON, to the attempt ctx belongs to; the store keeps it
// once the job completed, and the admin reads return it. A later call replaces an earlier
// one; a result of a failed attempt is dropped. It returns ErrNoAttempt outside an attempt,
// ErrResultTooLarge above MaximumResultBytes and the encoding error for a value json.Marshal
// refuses. A value that encodes as JSON null removes the result.
func SetResult(ctx context.Context, value any) error {
	output, ok := ctx.Value(attemptOutputKey{}).(*attemptOutput)
	if !ok {
		return ErrNoAttempt
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("jobs: encode result: %w", err)
	}
	if len(encoded) > MaximumResultBytes {
		return fmt.Errorf("%w: %d bytes, at most %d", ErrResultTooLarge, len(encoded), MaximumResultBytes)
	}
	if string(encoded) == "null" {
		encoded = nil
	}
	return output.setResult(encoded)
}

func truncate(text string, characters int) string {
	if utf8.RuneCountInString(text) <= characters {
		return text
	}
	return string([]rune(text)[:characters])
}

// attemptOutputKey keys an attempt's output in its context.
type attemptOutputKey struct{}

// progressWriter writes one progress report fenced by the attempt's lease.
type progressWriter func(progress Progress)

// attemptOutput collects what a handler reports during its attempt besides its error: the
// progress, written in the background at most once per interval, and the result, written with
// the outcome. Trailing-edge throttling: a report inside the interval is kept and written when
// the interval ends, so the last report always reaches the store.
type attemptOutput struct {
	write    progressWriter
	interval time.Duration

	mutex       sync.Mutex
	closed      bool        // the handler returned; later reports are dropped
	pending     *Progress   // the latest report not handed to a write yet
	scheduled   *time.Timer // the timer of the next background write, nil when none is due
	lastWritten time.Time
	// flushes counts the scheduled timers until their flush returned or they were stopped, so
	// close can wait until no goroutine of the attempt's reporting runs any more.
	flushes sync.WaitGroup
	result  json.RawMessage
}

func withAttemptOutput(ctx context.Context, write progressWriter, interval time.Duration) (context.Context, *attemptOutput) {
	output := &attemptOutput{write: write, interval: interval}
	return context.WithValue(ctx, attemptOutputKey{}, output), output
}

func (o *attemptOutput) report(progress Progress) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.closed {
		return
	}
	o.pending = &progress
	o.scheduleLocked()
}

// scheduleLocked starts the timer of the next write unless one is due or running; a write that
// ends schedules the next one itself.
func (o *attemptOutput) scheduleLocked() {
	if o.scheduled != nil || o.pending == nil {
		return
	}
	delay := max(0, o.interval-time.Since(o.lastWritten))
	o.flushes.Add(1)
	o.scheduled = time.AfterFunc(delay, o.flush)
}

// flush writes the pending report in the timer's goroutine; only one write runs at a time,
// because the next timer starts when this one ends.
func (o *attemptOutput) flush() {
	defer o.flushes.Done()
	o.mutex.Lock()
	if o.closed {
		o.mutex.Unlock()
		return // close took the pending report for the finish write
	}
	progress := o.pending
	o.pending = nil
	o.mutex.Unlock()

	o.write(*progress)

	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.lastWritten = time.Now()
	o.scheduled = nil
	if !o.closed {
		o.scheduleLocked()
	}
}

// close ends the attempt's reporting: it stops the next background write, waits for a write
// in flight, and returns the report no write took, for the finish write, with the result.
// Once it returned, no goroutine of the reporting runs.
func (o *attemptOutput) close() (*Progress, json.RawMessage) {
	o.mutex.Lock()
	o.closed = true
	if o.scheduled != nil && o.scheduled.Stop() {
		o.scheduled = nil
		o.flushes.Done() // the stopped timer never runs its flush
	}
	o.mutex.Unlock()
	o.flushes.Wait()
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.pending, o.result
}

func (o *attemptOutput) setResult(encoded json.RawMessage) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	if o.closed {
		return ErrNoAttempt
	}
	o.result = encoded
	return nil
}
