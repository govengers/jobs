package mailer_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs/examples/signup/internal/client/mailer"
	"github.com/govengers/jobs/examples/signup/internal/model"
)

func TestSendWritesOneJSONLinePerMail(t *testing.T) {
	var output bytes.Buffer
	client, err := mailer.NewClient(&mailer.ClientOptions{Writer: &output})
	require.NoError(t, err)

	require.NoError(t, client.Send(context.Background(), &model.Mail{IdempotencyKey: "welcome-mail:1", To: "ada@example.com"}))

	require.JSONEq(t, `{"IdempotencyKey":"welcome-mail:1","To":"ada@example.com","Subject":"","Body":""}`, output.String())
	_, err = mailer.NewClient(&mailer.ClientOptions{})
	require.Error(t, err)
}

func TestSendStopsOnACancelledContext(t *testing.T) {
	client, err := mailer.NewClient(&mailer.ClientOptions{Writer: &bytes.Buffer{}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, client.Send(ctx, &model.Mail{}), context.Canceled)
}
