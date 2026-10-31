package wideeventadapter_test

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	wideevent "github.com/govengers/wide-event-logger"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/wideeventadapter"
)

func ExampleNewMiddleware() {
	// In a service this is the emitter main builds for the HTTP middleware too.
	emitter := wideevent.EmitterFunc(func(we *wideevent.Event) {
		fields := we.FieldsMap()
		fmt.Println(fields["name"], fields["outcome"], fields["mail"].(map[string]any)["template"])
	})
	middleware, err := wideeventadapter.NewMiddleware(&wideeventadapter.Options{Emitter: emitter})
	if err != nil {
		fmt.Println(err)
		return
	}
	// The worker applies it to every attempt: jobs.WorkerOptions{Middleware: []jobs.Middleware{middleware}}.
	handler := middleware(func(ctx context.Context, _ *jobs.Claim) error {
		wideevent.FromContext(ctx).String("mail.template", "welcome")
		return nil
	})
	claim := &jobs.Claim{Lease: jobs.Lease{JobID: uuid.New()}, Queue: "mail", Kind: "mail.send", Attempt: 1, MaxAttempts: 10}

	if err := handler(context.Background(), claim); err != nil {
		fmt.Println(err)
	}
	// Output: job.mail success welcome
}
