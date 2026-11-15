package jobs

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// recordingWriter is a progressWriter that keeps what it wrote; block, when set, holds every
// write until it is closed.
type recordingWriter struct {
	mutex   sync.Mutex
	written []Progress
	block   chan struct{}
}

func (w *recordingWriter) write(progress Progress) {
	if w.block != nil {
		<-w.block
	}
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.written = append(w.written, progress)
}

func (w *recordingWriter) messages() []string {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	messages := make([]string, 0, len(w.written))
	for _, progress := range w.written {
		messages = append(messages, progress.Message)
	}
	return messages
}

func report(ctx context.Context, t *testing.T, message string) {
	t.Helper()
	require.NoError(t, ReportProgress(ctx, Progress{Fraction: 0.5, Message: message}))
}

// J54: the first report is written at once, later ones at most once per interval, and the
// latest one of an interval wins.
func TestProgressIsWrittenAtMostOncePerInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &recordingWriter{}
		ctx, output := withAttemptOutput(context.Background(), writer.write, time.Second)

		report(ctx, t, "1")
		synctest.Wait()
		require.Equal(t, []string{"1"}, writer.messages(), "the first report goes out at once")

		report(ctx, t, "2")
		report(ctx, t, "3")
		synctest.Wait()
		require.Equal(t, []string{"1"}, writer.messages(), "inside the interval nothing more is written")

		time.Sleep(time.Second) // the bubble's clock: the interval ends without waiting
		synctest.Wait()
		require.Equal(t, []string{"1", "3"}, writer.messages(), "the latest report of the interval")

		time.Sleep(time.Minute)
		synctest.Wait()
		require.Equal(t, []string{"1", "3"}, writer.messages(), "nothing new, nothing written")

		progress, _ := output.close()
		require.Nil(t, progress)
	})
}

// J54: the report no write took goes to the finish write; reports after the handler returned
// are dropped and no timer outlives the attempt.
func TestCloseHandsOverTheUnwrittenReportAndStopsWriting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &recordingWriter{}
		ctx, output := withAttemptOutput(context.Background(), writer.write, time.Hour)
		report(ctx, t, "first")
		synctest.Wait()
		report(ctx, t, "last")

		progress, _ := output.close()
		require.Equal(t, &Progress{Fraction: 0.5, Message: "last"}, progress)

		report(ctx, t, "too late")
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		require.Equal(t, []string{"first"}, writer.messages())
		require.ErrorIs(t, SetResult(ctx, "late"), ErrNoAttempt)
	})
}

// J54: close waits for a write in flight, so no progress write runs after the finish write.
func TestCloseWaitsForTheWriteInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &recordingWriter{block: make(chan struct{})}
		ctx, output := withAttemptOutput(context.Background(), writer.write, time.Second)
		report(ctx, t, "slow")
		synctest.Wait() // the write started and blocks
		closed := make(chan *Progress)
		go func() {
			progress, _ := output.close()
			closed <- progress
		}()
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("close returned while a write was in flight")
		default:
		}

		close(writer.block)

		require.Nil(t, <-closed)
		require.Equal(t, []string{"slow"}, writer.messages())
	})
}

func TestReportProgressValidatesAndCutsTheMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		writer := &recordingWriter{}
		ctx, output := withAttemptOutput(context.Background(), writer.write, time.Second)
		for _, fraction := range []float64{-0.1, 1.1, nan()} {
			require.Error(t, ReportProgress(ctx, Progress{Fraction: fraction}))
		}
		long := make([]rune, MaximumProgressMessageLength+5)
		for index := range long {
			long[index] = 'ä'
		}

		require.NoError(t, ReportProgress(ctx, Progress{Fraction: 1, Message: string(long)}))
		synctest.Wait()
		output.close()

		require.Len(t, writer.written, 1)
		require.Equal(t, string(long[:MaximumProgressMessageLength]), writer.written[0].Message)
	})
}

// The store keeps text as valid UTF-8 without NUL, so a message from foreign data is written.
func TestStorableTextDropsNULAndReplacesInvalidBytes(t *testing.T) {
	require.Equal(t, "ab.csv", storableText("a\x00b.csv"))
	require.Equal(t, "M\uFFFDller", storableText("M\xfcller"))
	require.Equal(t, "Müller", storableText("Müller"))
}

func nan() float64 {
	zero := 0.0
	return zero / zero
}

func TestOutsideAnAttemptProgressIsIgnoredAndAResultRefused(t *testing.T) {
	require.NoError(t, ReportProgress(context.Background(), Progress{Fraction: 0.5}))
	require.ErrorIs(t, SetResult(context.Background(), 1), ErrNoAttempt)
}

func TestSetResultEncodesCapsAndReplaces(t *testing.T) {
	ctx, output := withAttemptOutput(context.Background(), func(Progress) {}, time.Second)

	require.ErrorIs(t, SetResult(ctx, make([]byte, MaximumResultBytes)), ErrResultTooLarge) // base64 grows it
	require.Error(t, SetResult(ctx, func() {}))
	require.NoError(t, SetResult(ctx, map[string]int{"pages": 1}))
	require.NoError(t, SetResult(ctx, map[string]int{"pages": 2}))
	_, result := output.close()
	require.JSONEq(t, `{"pages": 2}`, string(result))

	ctx, output = withAttemptOutput(context.Background(), func(Progress) {}, time.Second)
	require.ErrorIs(t, SetResult(ctx, "a\x00b"), ErrResultNotStorable, "jsonb refuses the escape \\u0000")
	require.ErrorIs(t, SetResult(ctx, json.RawMessage("\"\xff\"")), ErrResultNotStorable)
	require.NoError(t, SetResult(ctx, `a\u0000b`), "an escaped backslash before u0000 is text")
	require.NoError(t, SetResult(ctx, map[string]int{"pages": 1}))
	require.NoError(t, SetResult(ctx, nil))
	_, result = output.close()
	require.Nil(t, result, "a JSON null removes the result")
}
