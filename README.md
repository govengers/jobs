# jobs

Durable PostgreSQL work queue for govengers services. Jobs are enqueued inside the caller's transaction, claimed under a lease by workers on any replica, retried with backoff and fenced, so a worker that lost its lease cannot overwrite a newer attempt.

**v1 is in development.** The design is in [docs/design.md](docs/design.md).

```sh
go get github.com/govengers/jobs
```

## License

Apache 2.0, see [LICENSE](LICENSE).
