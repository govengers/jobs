//go:build e2e

// Package contract holds the kinds the end-to-end suite and its worker binary share, so the
// two cannot drift apart.
package contract

import "github.com/govengers/jobs"

// Queue is the one queue of the suite.
const Queue = "e2e"

// TaskPayload is the payload of TaskKind.
type TaskPayload struct {
	Block bool `json:"block"` // block the first run until the attempt's context ends
}

// The suite's kinds: TaskKind records every run in e2e_runs and, with Block, blocks its first
// run; TickKind is enqueued by the worker's schedule and records its runs.
var (
	TaskKind = jobs.NewKind[TaskPayload]("e2e.task", &jobs.KindOptions{Queue: Queue, MaximumAttempts: 5})
	TickKind = jobs.NewKind[struct{}]("e2e.tick", &jobs.KindOptions{Queue: Queue})
)
