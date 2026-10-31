package postgresadapter_test

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/postgresadapter"
)

type invoicePayload struct {
	InvoiceID string `json:"invoiceId"`
}

var invoiceKind = jobs.NewKind[invoicePayload]("billing.render_invoice", &jobs.KindOptions{Queue: "billing"})

// A repository enqueues on the transaction of its change, so the job commits or rolls back with
// it. The example only compiles: it needs a database with MigrationV1 applied.
func ExampleStore_Enqueue() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, "postgres://localhost/billing")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pool.Close()
	jobStore, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	if err != nil {
		fmt.Println(err)
		return
	}
	transaction, err := pool.Begin(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = transaction.Rollback(ctx) }() // a no-op after Commit
	// ... insert the invoice on transaction, then:
	ids, err := jobStore.Enqueue(ctx, transaction, invoiceKind.New(invoicePayload{InvoiceID: "INV-1"}, nil))
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := transaction.Commit(ctx); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(len(ids))
}
