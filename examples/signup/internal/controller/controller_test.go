package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs"
	"github.com/govengers/jobs/examples/signup/internal/controller"
	"github.com/govengers/jobs/examples/signup/internal/database"
	"github.com/govengers/jobs/examples/signup/internal/model"
	"github.com/govengers/jobs/examples/signup/internal/repository"
	"github.com/govengers/jobs/examples/signup/internal/service"
	"github.com/govengers/jobs/internal/postgrestest"
	"github.com/govengers/jobs/jobstest"
	"github.com/govengers/jobs/postgresadapter"
)

func TestMain(m *testing.M) { postgrestest.Main(m) }

// fakeMailSender stands in for the mail client through the interface the service declares
// (TST-012); it fails the first sends when told to.
type fakeMailSender struct {
	mutex    sync.Mutex
	sent     []*model.Mail
	failures int
}

func (f *fakeMailSender) Send(_ context.Context, mail *model.Mail) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.failures > 0 {
		f.failures--
		return errors.New("provider unavailable")
	}
	f.sent = append(f.sent, mail)
	return nil
}

func (f *fakeMailSender) mails() []*model.Mail {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return append([]*model.Mail(nil), f.sent...)
}

type testApplication struct {
	pool     *pgxpool.Pool
	jobStore *postgresadapter.Store
	server   *httptest.Server
	worker   *jobs.Worker
	mail     *fakeMailSender
}

// newApplication wires the example like its main does, on a fresh database and a fake mail sender.
func newApplication(t *testing.T) *testApplication {
	t.Helper()
	pool := postgrestest.New(t)
	_, err := pool.Exec(t.Context(), database.MigrationUsers)
	require.NoError(t, err)
	jobStore, err := postgresadapter.New(&postgresadapter.Options{Pool: pool})
	require.NoError(t, err)
	userRepository, err := repository.NewUserRepository(&repository.UserRepositoryOptions{Pool: pool, JobStore: jobStore})
	require.NoError(t, err)
	mail := &fakeMailSender{}
	userService, err := service.NewUserService(&service.UserServiceOptions{UserRepository: userRepository, MailSender: mail})
	require.NoError(t, err)
	worker, err := jobs.NewWorker(&jobs.WorkerOptions{Store: jobStore, Queues: map[string]jobs.QueueOptions{"mail": {}}})
	require.NoError(t, err)
	mailJobController, err := controller.NewMailJobController(&controller.MailJobControllerOptions{UserService: userService})
	require.NoError(t, err)
	require.NoError(t, mailJobController.Register(worker))
	userController, err := controller.NewUserController(&controller.UserControllerOptions{UserService: userService})
	require.NoError(t, err)
	router := http.NewServeMux()
	userController.Register(router)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return &testApplication{pool: pool, jobStore: jobStore, server: server, worker: worker, mail: mail}
}

func (application *testApplication) signUp(t *testing.T, body string) int {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, application.server.URL+"/api/users", strings.NewReader(body))
	require.NoError(t, err)
	response, err := application.server.Client().Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	return response.StatusCode
}

func (application *testApplication) jobs(t *testing.T) []*postgresadapter.Record {
	t.Helper()
	records, err := application.jobStore.List(t.Context(), &postgresadapter.ListFilter{Queue: "mail"})
	require.NoError(t, err)
	return records
}

func TestSignUpSendsOneWelcomeMail(t *testing.T) {
	application := newApplication(t)

	require.Equal(t, http.StatusCreated, application.signUp(t, `{"email":" Ada@Example.com ","name":"Ada"}`))
	require.Len(t, application.jobs(t), 1, "the welcome mail is enqueued with the user")
	require.Empty(t, application.mail.mails(), "nothing is sent inside the request")

	require.Equal(t, 1, jobstest.Drain(t, application.worker))

	mails := application.mail.mails()
	require.Len(t, mails, 1)
	require.Equal(t, "ada@example.com", mails[0].To)
	require.True(t, strings.HasPrefix(mails[0].IdempotencyKey, "welcome-mail:"))
	require.Equal(t, jobs.StatusCompleted, application.jobs(t)[0].Status)
	require.Equal(t, 0, jobstest.Drain(t, application.worker))
}

func TestRejectedSignUpEnqueuesNothing(t *testing.T) {
	application := newApplication(t)
	require.Equal(t, http.StatusCreated, application.signUp(t, `{"email":"ada@example.com","name":"Ada"}`))

	require.Equal(t, http.StatusConflict, application.signUp(t, `{"email":"ADA@example.com","name":"Ada again"}`))
	require.Equal(t, http.StatusBadRequest, application.signUp(t, `{"email":"not an address","name":"Bob"}`))
	require.Equal(t, http.StatusBadRequest, application.signUp(t, `{"email":"bob@example.com","name":""}`))
	require.Equal(t, http.StatusBadRequest, application.signUp(t, `{"email":"bob@example.com","name":"Bob","admin":true}`))

	require.Len(t, application.jobs(t), 1, "a rolled-back sign-up leaves no job behind")
}

func TestAFailedSendIsRetriedLater(t *testing.T) {
	application := newApplication(t)
	application.mail.failures = 1
	require.Equal(t, http.StatusCreated, application.signUp(t, `{"email":"ada@example.com","name":"Ada"}`))

	require.Equal(t, 1, jobstest.Drain(t, application.worker), "the retry waits for its backoff")

	record := application.jobs(t)[0]
	require.Equal(t, jobs.StatusPending, record.Status)
	require.Equal(t, 1, record.Attempt)
	require.Contains(t, record.LastError, "provider unavailable")
	require.Empty(t, application.mail.mails())
}

func TestWelcomeMailOfAMissingUserIsDead(t *testing.T) {
	application := newApplication(t)
	missing := uuid.New()
	_, err := application.jobStore.Enqueue(t.Context(), application.pool, model.WelcomeMailKind.New(model.WelcomeMailPayload{UserID: missing}, nil))
	require.NoError(t, err)

	jobstest.Drain(t, application.worker)

	record := application.jobs(t)[0]
	require.Equal(t, jobs.StatusDead, record.Status)
	require.Equal(t, 1, record.Attempt)
	require.Contains(t, record.LastError, "get user "+missing.String(), "the repository names the operation and the user (ERR-004)")
}

func TestADuplicateDeliverySendsNoSecondMail(t *testing.T) {
	application := newApplication(t)
	require.Equal(t, http.StatusCreated, application.signUp(t, `{"email":"ada@example.com","name":"Ada"}`))
	jobstest.Drain(t, application.worker)
	var delivered model.WelcomeMailPayload
	require.NoError(t, json.Unmarshal(application.jobs(t)[0].Payload, &delivered))

	_, err := application.jobStore.Enqueue(t.Context(), application.pool, model.WelcomeMailKind.New(delivered, nil))
	require.NoError(t, err)
	jobstest.Drain(t, application.worker)

	require.Len(t, application.mail.mails(), 1, "at-least-once delivery, one mail (JOB-003)")
}
