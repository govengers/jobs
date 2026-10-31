package postgrestest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Round 2, finding 20: the orphan cleanup does not signal a process that only reuses the
// recorded postmaster's process ID.
func TestOrphanCleanupSparesAProcessThatReusedThePostmastersID(t *testing.T) {
	finished := exec.CommandContext(context.Background(), "true")
	require.NoError(t, finished.Run())
	unrelated := exec.CommandContext(context.Background(), "sleep", "30")
	require.NoError(t, unrelated.Start())
	exited := make(chan struct{})
	go func() {
		_ = unrelated.Wait() // its exit status is irrelevant; the channel says whether it ended
		close(exited)
	}()
	t.Cleanup(func() {
		_ = unrelated.Process.Kill() // the helper of this test
		<-exited
	})
	directory, err := os.MkdirTemp("", instancePrefix)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(directory, ownerFile), []byte(strconv.Itoa(finished.Process.Pid)), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(directory, "runtime", "data"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "runtime", "data", "postmaster.pid"), []byte(strconv.Itoa(unrelated.Process.Pid)+"\n"), 0o600))

	stopOrphans()

	require.NoDirExists(t, directory, "the orphan's directory is removed")
	select {
	case <-exited:
		t.Fatal("an unrelated process with the postmaster's old ID was signalled")
	case <-time.After(500 * time.Millisecond): // a signalled sleep ends at once
	}
}
