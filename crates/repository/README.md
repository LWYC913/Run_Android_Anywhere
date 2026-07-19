# Run Anywhere Repository

PostgreSQL persistence for Run Android Anywhere. The repository uses the root
`migrations/` directory and SQLx 0.8.6.

## Local PostgreSQL

Start PostgreSQL 16 with a development database:

```sh
docker run --name run-anywhere-postgres --rm -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=run_anywhere_dev -p 5432:5432 -d postgres:16-alpine
```

Copy the root `.env.example` to `.env`. The example connects as the local
PostgreSQL superuser so SQLx integration tests can create and remove their
isolated test databases.

Install the pinned SQLx CLI:

```sh
cargo install sqlx-cli --version 0.8.6 --no-default-features --features rustls,postgres --locked
```

## Database commands

Run all pending root migrations:

```sh
just db-migrate
```

Drop, recreate, and migrate the development database:

```sh
just db-reset
```

`db-reset` refuses to run unless `DATABASE_URL` uses PostgreSQL, points to a
loopback host, and names the database exactly `run_anywhere_dev`.

Run the repository's real-PostgreSQL integration tests:

```sh
just repository-test
```

Stop the disposable local database when finished:

```sh
docker stop run-anywhere-postgres
```
