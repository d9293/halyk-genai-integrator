# SQL scripts

Plain SQL to create the tables by hand (or hand to a DBA). One script does it all:

| Script | What it does |
|---|---|
| `01_init.sql` | Tables `integration_queue`, `integration_documents`, `integration_attempts`, `integration_audit` in schema `public`; their constraints, indexes and documentation comments. |

Every object the app owns (tables, indexes, constraints) starts with
`integration_`, so it stays apart from other applications' objects in `public`.

Local infra (database `postgres`, user `admin`, as in `dev.env`): `make db-init`, or

```sh
docker exec -i infra-postgres psql -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/01_init.sql
```

To start over (deletes all tasks): `make db-reset`, then `make db-init`.

In test/prod the app should use its own user, not a superuser, with
`SELECT, INSERT, UPDATE, DELETE` on the four `integration_*` tables only. Since
PostgreSQL 15 ordinary users cannot create objects in `public` by default, so a
DBA runs the script (or grants `CREATE` on `public` for that one run).

The script is **generated** from `schema/postgresql/migration/` by
`make sql-scripts`. Change the migration, then regenerate; never edit it by
hand. It is not rerunnable: it fails if the tables exist.
Column meanings: [docs/database.md](../docs/database.md).
