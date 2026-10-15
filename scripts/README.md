# SQL scripts

Plain SQL to create the schema and tables by hand (or hand to a DBA). Run in order:

| # | Script | What it does |
|---|---|---|
| 1 | `02_create_tables.sql` | Schema `integration`; tables `queue`, `documents`, `audit`. |
| 2 | `03_create_attempts.sql` | Table `attempts`; FKs from `queue` and `audit` to it. |
| 3 | `04_documents_claim_index.sql` | Index for claiming documents to copy. |
| 4 | `05_queue_ready_index.sql` | Index for picking the next task to send to AI. |

Local infra (database `postgres`, user `admin`, as in `dev.env`): `make db-init`, or

```sh
docker exec -i infra-postgres psql -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/02_create_tables.sql
docker exec -i infra-postgres psql -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/03_create_attempts.sql
docker exec -i infra-postgres psql -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/04_documents_claim_index.sql
docker exec -i infra-postgres psql -v ON_ERROR_STOP=1 -U admin -d postgres < scripts/05_queue_ready_index.sql
```

To start over (deletes all tasks):

```sh
docker exec infra-postgres psql -U admin -d postgres -c 'DROP SCHEMA integration CASCADE'
```

In test/prod the app should use its own user with access to schema
`integration` only, not a superuser.

The scripts are **generated** from `schema/postgresql/migration/` by
`make sql-scripts`. Change the migration, then regenerate; never edit them by
hand. They are not rerunnable: they fail if the tables exist.
Column meanings: [docs/database.md](../docs/database.md).
