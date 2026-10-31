// Package recovery formats a recovered panic as an error with its stack: the one format the
// worker and the wide event middleware share.
package recovery

import (
	"fmt"
	"runtime/debug"
)

// Error returns the panic of subject (a goroutine, a job) as an error that carries the stack.
func Error(subject string, recovered any) error {
	return fmt.Errorf("%s panicked: %v\n%s", subject, recovered, debug.Stack())
}
