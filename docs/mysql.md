# MySQL storage

For Kubernetes manifests, credentials, and the GitHub Actions image pipeline, see the [k3s deployment guide](../deploy/k3s/README.md).

Manager Server supports MySQL 8.4 LTS as an alternative to the default SQLite database. It stores events, settings, credentials, prices, quota history, inspection results, archive metadata, and derived analytics in MySQL. Use one Manager Server per database and CPA queue; MySQL support does not enable multiple collectors.

## Configuration

Create an empty database and a dedicated account. For example, run these statements as a database administrator, replacing the password:

```sql
CREATE DATABASE cpamp CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
CREATE USER 'cpamp'@'%' IDENTIFIED BY 'replace-with-a-strong-password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, REFERENCES,
      CREATE VIEW, SHOW VIEW ON cpamp.* TO 'cpamp'@'%';
```

Restrict the account's host and network access to your Manager Server. Configure the application through the environment:

```sh
export USAGE_DB_DRIVER=mysql
export USAGE_DB_DSN='cpamp:replace-with-a-strong-password@tcp(mysql.example:3306)/cpamp?tls=true'
export USAGE_DATA_DIR=/var/lib/cpa-manager-plus
./cpa-manager-plus
```

The DSN uses the [Go MySQL driver's format](https://github.com/go-sql-driver/mysql#dsn-data-source-name). `tls=true` verifies the server certificate using the operating system's trusted certificates. The bundled Compose example uses an internal Docker network; it does not publish the database port.

| Environment variable | `config.json` field | Purpose |
| --- | --- | --- |
| `USAGE_DB_DRIVER` | `dbDriver` | `sqlite` (default) or `mysql`. Unknown values fail startup. |
| `USAGE_DB_DSN` | `dbDsn` | MySQL credentials, address, database, and optional driver parameters. |
| `USAGE_DB_DSN_FILE` | `dbDsnFile` | Read the DSN from a secret file. Relative JSON paths resolve beside `config.json`. |
| `USAGE_DATA_DIR` | `dataDir` | Local storage for encryption keys, import sessions, and archives. |
| `USAGE_DB_PATH` | `dbPath` | SQLite only. It does not select or locate the MySQL database. |

An environment DSN overrides a secret file. An explicitly configured secret file overrides a JSON DSN and must be readable and non-empty. A MySQL database is never inferred from a SQLite file path.

Example JSON configuration using a secret file:

```json
{
  "dbDriver": "mysql",
  "dbDsnFile": "./secrets/mysql-dsn",
  "dataDir": "./data"
}
```

Store the file with permissions that allow only the service account to read it. Neither status responses nor generated default configuration include the DSN. Connection pools use up to 10 connections, with five idle connections and a three-minute connection lifetime. Session settings enforce UTF-8, UTC, strict data validation, and repeatable-read snapshots.

For a fresh container installation, set `MYSQL_ROOT_PASSWORD` and `MYSQL_PASSWORD`, then run:

```sh
docker compose -f docker-compose.mysql.yml up -d --build
```

Do not start this fresh-install command over an existing SQLite installation before migrating its data.

## Move an existing SQLite installation

1. Stop Manager Server and back up `usage.sqlite`, its WAL/SHM files if present, `data.key`, and `usage-archives/`.
2. Create an empty MySQL database. Set the MySQL configuration above, and keep the existing data directory and encryption key.
3. Run the migration with the new binary:

   ```sh
   ./cpa-manager-plus migrate-to-mysql --sqlite-path /path/to/usage.sqlite
   ```

4. Start Manager Server with `USAGE_DB_DRIVER=mysql`. Check login, request history, prices, and archive status before retiring the SQLite backup.

The command locks both databases, creates and upgrades a private SQLite snapshot, copies current tables, checks row counts, and preserves auto-increment high-water marks. It does not modify the source database or copy local archive files. The operating-system temporary directory needs enough space for a SQLite snapshot.

All copied rows commit in one MySQL transaction. Schema creation is separate because [MySQL DDL commits implicitly](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html). If copying fails, rows roll back; retry with a new empty destination. The command refuses any destination that already contains tables. Keep the original `data.key`; a new key cannot decrypt the migrated settings.

## Guided installation and upgrades

The guided installer supports both Docker and native MySQL installations. Select `mysql` when prompted, or set `CPAMP_DB_DRIVER=mysql` and `CPAMP_MYSQL_DSN_FILE=/path/to/mysql-dsn` before running `bin/install-cpamp.sh`. `CPAMP_MYSQL_DSN` is also supported. Provision MySQL separately; the installer connects to that server and saves the DSN in `secrets/mysql-dsn` with mode `0600`.

Native installations reference that secret from `config.json`; Docker installations mount it as a Compose secret. Upgrades preserve existing MySQL configuration. Native upgrades and Docker legacy-connection imports use the native snapshot commands below for rollback. Make a separate retained backup before upgrading; successful installer rollback snapshots are removed. Changing an existing SQLite installation to MySQL requires the explicit migration above, not just selecting a different driver.

## Operations and recovery

All offline commands use the configured MySQL database when `--db-path` is omitted. An explicit `--db-path` selects SQLite. Stop Manager Server before running them; they require exclusive database ownership.

```sh
./cpa-manager-plus manager-data-snapshot create --snapshot-dir /backups/cpamp-before-upgrade
./cpa-manager-plus manager-data-snapshot restore --snapshot-dir /backups/cpamp-before-upgrade
./cpa-manager-plus manager-data-snapshot delete --snapshot-dir /backups/cpamp-before-upgrade
./cpa-manager-plus cleanup-derived
./cpa-manager-plus compact-usage
```

For Compose, stop the service and prefix a command with `docker compose run --rm --no-deps cpa-manager-plus`. Use a snapshot directory on a persistent mounted volume, outside `usage-archives/`, and restart the service after maintenance.

Snapshots contain all database rows, auto-increment high-water marks, the local `data.key`, and the complete archive directory. They use checksums and a compressed, lossless row stream. The current binary accepts the explicitly supported v1 and current MySQL snapshot layouts, restores into an initialized or empty MySQL database, validates foreign keys, and replaces rows in one transaction. Restoring a v1 snapshot keeps the destination's current physical schema version and initializes new hourly acceleration states as pending; it does not downgrade the schema. Unknown future layouts are rejected. Auto-increment counters never move backwards. Keep the exact binary/image version with a backup. If you supply `CPA_MANAGER_DATA_KEY` directly, retain that external secret separately; it is not a local file in the snapshot.

Both v1 and current v2 restores need temporary disk space for the decoded row stream. The binary validates snapshot integrity and stream structure into a private, bounded-memory spool before upgrading the destination schema or replacing rows. This also recovers legacy v1 sequence high-water marks. The spool is removed on success or failure. Allow space for the decoded data, not only the compressed backup. Database constraints and destination failures remain transactional restore checks; a completed schema upgrade is not rolled back by a later row-level failure.

Local-file changes and the database commit use a durable recovery journal. If a restore is interrupted, startup refuses to proceed until you run the following command with the same database and data-key paths:

```sh
./cpa-manager-plus manager-data-snapshot recover
```

Recovery checks whether the database transaction committed, then completes or rolls back the local-file replacement. Keep the pending journal and its staged/rollback files until recovery succeeds. Do not point recovery at another database.

- `reset-admin-key` and `store-cpa-connection` work against MySQL, including encrypted credentials.
- `cleanup-derived` removes eligible derived data in bounded batches. `compact-usage` runs MySQL `OPTIMIZE TABLE`; it can rebuild tables, take time, and require additional disk space. The storage panel reports estimated MySQL table/index sizes and provides the appropriate backup and optimization commands.
- Database-native backups remain an option. Retain `data.key` plus `usage-archives/` from the same stopped-service backup. Raw JSONL exports alone do not include settings or deleted archive segments.
- Startup creates and versions the MySQL schema. A named database lock prevents simultaneous managers; losing the lock connection stops the running server.
- Tables use InnoDB. Full-value SHA-256 keys enforce long text identities; prefix indexes support lookups without truncating stored values. Foreign keys and cascade deletes remain enforced. Prepared queries preserve bound parameters.
- Substring search retains its behavior using the retained event projection. MySQL does not use SQLite's FTS5 trigram index, so broad substring searches can cost more on large histories. Measure your workload before setting retention or capacity targets.

MySQL 5.7 and MariaDB are not supported by this implementation.

## Analytics performance

See the [MySQL analytics implementation and validation guide](performance/mysql-analytics.md) for progressive analytics, native optimization switches, upgrade precautions, exact-metric guarantees, and a disposable-database acceptance matrix. Native query plans and latency must be validated on MySQL; SQLite timings are not MySQL performance evidence.

## Development validation

Run the normal suite for SQLite:

```sh
cd apps/manager-server
go test ./...
```

From the repository root, run the full Go suite with shared fixtures using a disposable MySQL 8.4 server:

```sh
export CPA_TEST_MYSQL_DSN='root:test-password@tcp(127.0.0.1:3306)/mysql'
bash bin/ci/test-mysql.sh
bash bin/ci/test-mysql-recovery.sh
```

Use an administrator on the disposable test server: fixtures need `CREATE DATABASE` and `DROP DATABASE`, and the warning-truncation fault test changes the restricted `max_error_count` session variable. Application accounts do not need that privilege. Fixtures create random `cpamp_test_*` databases, clean those databases up, and do not modify the database named by the supplied DSN. Without the variable, MySQL-specific tests skip and shared tests use SQLite. Tests of SQLite file formats, query plans, and old SQLite migrations intentionally retain SQLite fixtures. CI runs both modes explicitly.

The recovery script requires Docker and creates its own disposable MySQL container. It kills MySQL during an actual Manager process, checks shutdown on lost ownership, restarts both, verifies committed data, and exercises native snapshot restoration. The integration suite also covers concurrent ingestion with duplicate events, migration, API/worker/service workflows, encrypted settings, and backup rollback. These checks do not replace a staged migration of your production data or workload-specific capacity testing.

The initial native schema was generated from SQLite and is now a frozen v1 upgrade/restore baseline. `go generate ./internal/repository/mysql` from `apps/manager-server` checks that baseline rather than silently replacing it. Review schema changes and add a numbered MySQL migration for new layouts; regenerating version 1 does not upgrade an existing MySQL database. Fresh databases also run the numbered migrations. Version 2 adds the independent hourly dimension tables and their initial pending states, not a synchronous history backfill.
