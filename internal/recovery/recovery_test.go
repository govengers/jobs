package recovery_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs/internal/recovery"
)

func TestErrorNamesTheSubjectAndCarriesTheStack(t *testing.T) {
	err := recovery.Error("job 7", "template missing")

	require.ErrorContains(t, err, "job 7 panicked: template missing")
	require.ErrorContains(t, err, "recovery_test.go")
}
