# MySQL analytics implementation and validation

## Scope and validation status

The MySQL analytics work preserves exact metrics and the existing SQLite API behavior. It targets foreground request cost, shared-pool starvation, network-sensitive write loops, rolling-window aggregation, and historical-data correctness. It is not a claim that a compatibility test or a SQLite benchmark establishes MySQL performance.

**Live MySQL validation is deferred for this development session.** Native query plans, populated upgrades, native bulk-write behavior, and measured MySQL latency must be accepted on a disposable MySQL 8.4 instance before enabling the native optimizations. Never use a production DSN for test fixtures, load tests, or migration experiments.

## Architecture

### Progressive analytics

An analytics-section capability lets the page fetch core totals and a core timeline without exact percentile work. Exact summary/bucket P95 and complete anomaly analysis load independently; dimension statistics, selectors, previews, and dependent trends have separate request state. An optional failure must not erase a successful core response or be presented as a zero metric.

Legacy callers retain the existing include semantics when the new fields are absent. Older servers that do not advertise the capability continue to receive legacy combined requests. Requests share frozen client bounds and filter scope, but separate requests are not a single database snapshot. Coverage and generation checks remain important during ingestion, price edits, retention, and refreshes.

Exact P95 still requires examining its sample population. With native analytics enabled, combining requested results and returning only winners avoids duplicate application work and raw-sample transfer; it does not prove a single physical scan or eliminate the global/bucket sorts. With native analytics disabled on MySQL, a separate reference route counts independent positive metric populations and streams ordered samples within one read-only snapshot, retaining only per-bucket counters/results—not the full sample population in Go. This fallback still transfers samples and may do multiple passes, so it is not a constant-cost or native-performance claim. SQLite retains its portable ranked implementation. Never compute an overall P95 by averaging hourly P95 values. Archived TTFT samples that were not retained cannot be reconstructed by a new schema.

### Request profiles and errors

For an authenticated `POST /v0/management/monitoring/analytics`, use the same `from_ms`, `to_ms`, `now_ms`, `time_zone`, and filters in each section request. The following `include` object asks for core data without exact percentiles:

```json
{
  "summary": true,
  "summary_profile": "compact",
  "model_stats": true,
  "timeline": true,
  "timeline_profile": "core",
  "granularity": "hour"
}
```

Fetch exact details separately with `"include": {"granularity": "hour", "percentiles": {"summary": true, "timeline": true}}`. The response adds `percentiles.summary` and `percentiles.timeline`; unavailable P95 values are `null`, and a requested empty timeline is `[]`. Adding `anomaly_points: true` also requires bucket percentiles and the timeline used for anomaly analysis. Omitting the new profiles retains legacy semantics—it does not implicitly opt into the cheap core profile.

Timeline and anomaly points include `bucket_end_ms` and `bucket_range_exact`. When exact is true, the bucket is the half-open range `[bucket_ms, bucket_end_ms)`. Exceptional historical timezone transitions can produce disconnected membership, or a physical start different from the canonical bucket ID. Those points set exact to false and provide `bucket_intervals: [{from_ms, to_ms}, ...]` instead. Intersect those intervals with the frozen request range; do not silently treat the bounding envelope as the exact event filter. The preview/events request API still accepts a single range, so an unsupported union must be shown explicitly as unavailable rather than returning extra or missing events. Older servers omit the new fields.

Responses carry a server-generated `X-Request-ID`. Terminal analytics errors also include `request_id` and a stable code:

| HTTP status | Code                            | Meaning                                                                       |
| ----------- | ------------------------------- | ----------------------------------------------------------------------------- |
| 503         | `analytics_overloaded`          | Bounded admission rejected the request; `Retry-After: 1` is provided.         |
| 504         | `analytics_deadline_exceeded`   | The analytics budget or an earlier caller deadline expired.                   |
| 408         | `analytics_canceled`            | The request was canceled.                                                     |
| 503         | `analytics_coverage_incomplete` | Required historical coverage is unavailable; do not treat this as zero usage. |
| 500         | `analytics_query_failed`        | A terminal query error occurred; inspect correlated server diagnostics.       |

`GET /v0/management/monitoring/analytics/diagnostics` uses the same panel authentication as analytics. It reports active/queued requests, configured limits, background/percentile occupancy, shared pool counters, configured native switches, and MySQL writer-admission counters when applicable. It does not run a database query or establish that every rollup is ready. Pool and writer counters are shared across requests, not exact attribution to one request.

### Writer admission and strict batching

The application keeps its serialized writer contract but acquires write admission before checking out a shared connection. Waiting writers must not occupy reader capacity. Read-only snapshots remain independent. Transaction ownership includes cancellation-driven rollback; permits must not outlive commit and deadlock a subsequent logging write.

Native bulk paths preserve the existing identity ledger and full-batch atomicity. They do not infer row IDs from an auto-increment range, resurrect archived identities, truncate historical hashes, or treat `INSERT IGNORE` as permission to discard invalid data. Archive deletion still validates coverage and retains the event projection required by historical reads.

### Hourly dimensions and native grouping

MySQL hourly account/API-key/selector side structures supplement, rather than replace, the existing daily aggregates. Rolling ranges compose disjoint complete buckets, exact partial edges, and uncovered event-ID tails. Independent coverage states prevent a newly installed cache from inheriting a populated daily cache's watermark. Late events must update old buckets correctly.

Grouped native readers return sufficient aggregate statistics rather than every event. Go remains authoritative for timezone bucket mapping, including fractional offsets and daylight-saving changes; MySQL timezone-table installation is not a prerequisite.

Search retains arbitrary substring behavior. A plain MySQL FULLTEXT word index is not an equivalent substitute for SQLite's trigram search. Time-scoped candidate filtering helps, but substring performance still needs representative measurement.

## Configuration and safe rollout

The native switches are independent and default off. Progressive API support is independently switchable and defaults on:

| Environment variable                  | `config.json` field           | Default | Purpose                                                  |
| ------------------------------------- | ----------------------------- | ------- | -------------------------------------------------------- |
| `USAGE_MYSQL_ANALYTICS_ENABLED`       | `mysqlAnalyticsEnabled`       | `false` | Native grouped/exact-percentile read paths.              |
| `USAGE_MYSQL_BULK_ENABLED`            | `mysqlBulkEnabled`            | `false` | Native bounded ingestion and archival writes.            |
| `USAGE_MYSQL_HOURLY_ENABLED`          | `mysqlHourlyEnabled`          | `false` | Hourly dimension/selector acceleration and its catch-up. |
| `USAGE_ANALYTICS_PROGRESSIVE_ENABLED` | `analyticsProgressiveEnabled` | `true`  | Advertise progressive analytics support.                 |

The analytics execution controls are also configurable:

| Environment variable                   | `config.json` field           | Default | Scope                                                                         |
| -------------------------------------- | ----------------------------- | ------- | ----------------------------------------------------------------------------- |
| `USAGE_ANALYTICS_TIMEOUT_MS`           | `analyticsTimeoutMs`          | `20000` | Total analytics request budget, including admission and price-snapshot waits. |
| `USAGE_ANALYTICS_MAX_CONCURRENT`       | `analyticsMaxConcurrent`      | `2`     | Active analytics requests.                                                    |
| `USAGE_ANALYTICS_QUEUE_SIZE`           | `analyticsQueueSize`          | `4`     | Bounded pending analytics requests.                                           |
| `USAGE_ANALYTICS_ADMISSION_TIMEOUT_MS` | `analyticsAdmissionTimeoutMs` | `500`   | Maximum admission wait, bounded by the total request budget.                  |

Environment values override JSON values. Validation accepts a total budget of 1–120000 ms, 1–8 active requests, 0–64 queued requests, and an admission budget of 1–30000 ms that cannot exceed the total budget. A queue size of zero disables waiting rather than restoring the default. Invalid values fail configuration loading. Native options do not enable MySQL paths on SQLite.

The existing browser deadline is 30 seconds; an operator increasing the server budget must also account for browser/proxy deadlines. These controls do not apply a blanket timeout to full exports, imports or restore. Raising them does not fix an inefficient query.

Correctness repairs and pre-checkout writer admission are not optional bypasses. Disabling a fast path must preserve safe historical coverage, not silently fall back to incomplete raw-only history.

1. Retain a stopped-service database and local-file backup, including the encryption key and archive directory.
2. Validate a populated v1 upgrade and supported snapshot restoration on an isolated copy before scheduling deployment.
3. Allow the new binary to install additive schema and resume backfills. Schema version alone does not establish that new caches are ready.
4. Complete the native acceptance checks below, then enable one native switch at a time and compare the same workload.
5. Disable the affected optimization on the new binary if needed. Do not rewrite the schema-version marker to run an older binary. A binary downgrade requires a verified compatible pre-upgrade database/files snapshot and a plan for any writes made since it.

Restore validates manifest integrity and decoded stream structure before destination upgrade, using private temporary spools for both supported snapshot layouts. Provide disk space for decoded rows and staged archive files. Database constraints and destination failures are still checked during the row-restoration transaction; successful schema DDL cannot be rolled back by a later row failure.

MySQL DDL implicitly commits. Restartable guarded migrations are not a transactional rollback guarantee. Even online secondary-index creation can wait for metadata locks and consume substantial I/O. Adding stored generated columns to existing populated tables can require a copy/rebuild; do not assume such changes are instant. Historical backfills belong in bounded resumable workers, not an unbounded startup transaction.

## Correctness acceptance

Test these independently from timing:

- Exact summary and bucket P95: empty populations, null/zero/negative samples, ties, unequal latency/TTFT populations, and legacy versus progressive requests.
- Aggregates: success/failure separated, late timestamps, partial hours/days, independently lagging states, pricing thresholds/service tiers, and provider/account/credential matching.
- Timezones: UTC, Kathmandu, daylight-saving spring/fall, repeated hours, and fractional-hour transitions.
- Retention: cache-accounting correction followed by completed catch-up and archive verification; structural price changes after deletion; numeric-rate-only controls; archived low/high IDs; later ingestion; all raw rows deleted; missing retained rows; A→B→A price structures.
- Writes: duplicate-only and overlapping batches, repeated input identities, archived ledger-only identities, legacy raw rows without a ledger, long/multibyte/case-distinct values, packet/chunk boundaries, trigger failures and rollback of every dependent record.
- Admission: more waiting writers than pool capacity while cheap reads continue; begin/execute/commit/rollback failures; cancellation; `ErrBadConn`; explicit connections; prepared and write-returning statements; immediate post-commit writes.
- Portability: fresh/current and populated v1→current schema parity, interrupted migration restart, SQLite import, supported old/current snapshots, preserved sequence high-water marks, foreign keys, encrypted settings and file-journal recovery.

## Local validation without MySQL

Unset `CPA_TEST_MYSQL_DSN` when checking SQLite/reference and fake-driver behavior. On a shared high-core-count host, cap package/test workers instead of launching one worker per core:

```sh
env -u CPA_TEST_MYSQL_DSN GOMAXPROCS=2 go -C apps/manager-server test -p 1 ./...
npm --workspace apps/web run test -- src --maxWorkers=1
npm run test:repo -- --maxWorkers=1
npm run type-check
npm run lint
npm run build
npm run build:demo
```

Use a writable enabled Go build cache and a local temporary filesystem with sufficient space. Worker-start timeouts or a killed build are failed validation, not application test passes; rerun after addressing resource contention. Native-only tests skip without a test DSN.

## Native commands — run later on disposable infrastructure

Configure `CPA_TEST_MYSQL_DSN` through a secret environment mechanism, not shell history or a shared transcript. The test account needs the permissions documented in [MySQL storage](../mysql.md#development-validation). Fixtures create and remove `cpamp_test_*` databases; that isolation is not permission to point them at a production server.

```sh
# Shared repository/service/worker correctness suite against MySQL 8.4.
bash bin/ci/test-mysql.sh

# Separate Docker-based crash/recovery workflow; uses disposable services.
bash bin/ci/test-mysql-recovery.sh

# Explicit acknowledgement required by the benchmark wrapper.
CPA_MYSQL_BENCHMARK_DISPOSABLE=1 \
CPA_MANAGER_MONITORING_BENCH_EVENTS=100000 \
CPA_MYSQL_BENCHMARK_COUNT=3 \
bash bin/ci/bench-mysql-analytics.sh
```

Increase to one million or representative production cardinality only after estimating fixture size and available resources. Benchmark fixture creation can itself take time and disk space.

`internal/analyticsbench/BenchmarkUsageAnalyticsRollingWindow` explicitly wires `CPA_MYSQL_BENCH_NATIVE_READS`, `CPA_MYSQL_BENCH_BULK_WRITES`, and `CPA_MYSQL_BENCH_HOURLY` (`1` or `true`) into the store. All default off. It uses fixed nonmidnight 24-hour bounds, varied latency/TTFT values, ready initial rollups, and separate core, exact, concurrent core/selector, and concurrent core/selector/ingestion profiles. Ingested timestamps are beyond the frozen request window, so concurrent writes do not change the expected in-window totals. This is in-process service measurement, not browser/proxy measurement; it does not substitute for the larger retained-history/rebuild matrix below.

`internal/analyticsbench/BenchmarkUsageAnalyticsHeatmap` covers fixed nonmidnight seven-/30-day windows in `Asia/Hong_Kong`, both heatmap-only and compact-summary-plus-heatmap requests. It uses the same three explicit flag inputs, synthetic pricing tiers, and up to 1,009 API keys, five providers, and 12 models. Setup/catch-up and one warmup request are excluded from the timed loop; each timed request checks calls/tokens/status totals and, for combined requests, summary/heatmap cost consistency. The normal service deadline remains enforced: a timeout fails the benchmark rather than being hidden by a larger timeout. This fixture is not a production dump, does not simulate collection gaps or retained history, and does not prove production performance. Native reads being enabled does not imply an aggregate-backed heatmap source.

**Remaining heatmap limit:** the native heatmap still prices and groups filtered raw `usage_events`; it does not read the monitoring-hourly tables. `USAGE_MYSQL_HOURLY_ENABLED` therefore does not select a rollup-backed heatmap. Progressive loading now isolates heatmap from summary, and the SQL omits unused timeline metrics/date grouping, but neither is evidence that the original seven-day request meets its deadline. An aggregate-backed reader needs exact pricing/contributor dimensions, timezone and partial-hour handling, late-event tails, and verified retained coverage before it can replace this source safely.

For a targeted reference/native comparison, keep the event count, hardware, and other flags identical and change only `CPA_MYSQL_BENCH_NATIVE_READS` between `false` and `true`:

```sh
CPA_MYSQL_BENCHMARK_DISPOSABLE=1 \
CPA_MYSQL_BENCH_NATIVE_READS=true \
CPA_MYSQL_BENCH_BULK_WRITES=false \
CPA_MYSQL_BENCH_HOURLY=false \
CPA_MANAGER_MONITORING_BENCH_EVENTS=100000 \
bash bin/ci/bench-mysql-analytics.sh -bench '^BenchmarkUsageAnalyticsHeatmap$'
```

The older `internal/service/monitoring` benchmarks retain their own fixture/options behavior. Check each benchmark's flags and coverage state: an environment variable does not magically enable a fast path in a test that constructs a default store directly.

A Go benchmark's average `ns/op` and a few repeated benchmark means are **not** p95/p99 request latency. Use a controlled request-level workload for tail latency and error rate. Report fixture construction separately from measured requests.

## Performance experiment matrix

Keep seed, exact bounds, model/identity cardinality, payload size, engine version/settings, CPU/memory/storage, database network placement, coverage, and concurrency constant between baseline and candidate.

| Dimension   | Cases                                                                                                        |
| ----------- | ------------------------------------------------------------------------------------------------------------ |
| Window      | Nonmidnight rolling 24 hours; completed day; 7/30 days; representative custom range.                         |
| Request     | Core; exact summary; exact bucket metrics; full legacy Overview; selectors; key timelines; preview; heatmap. |
| Data        | Varied latency distributions and keys; small/large payloads; long identities; retained historical events.    |
| Read state  | Ready; uncovered tail; revision rebuild; archive coverage gap that must fail closed.                         |
| Filters     | Unfiltered; selective model/key/provider; substring search; status/cache/latency scope.                      |
| Concurrency | Single request; browser-style section overlap; multiple viewers with ingestion and catch-up.                 |
| Mutation    | Normal ingestion; duplicates; retention; rate-only price edit; structural pricing change.                    |
| Cache       | Cold versus warm database/application state, recorded explicitly.                                            |

Record **time-to-core** separately from **time-to-all-exact**. Also record errors/timeouts, p50/p95/p99 with sufficient samples, allocations/peak memory, rows sent/examined, sort/temp work, statement counts per batch, transaction duration, writer throughput, pool waits, admission waits and cancellation recovery.

No production SLA or measured speedup is supplied by this document. Establish numeric acceptance thresholds on the agreed test hardware before interpreting results.

## Diagnose a slow request

1. Identify the exact failed URL and sanitized include profile. The page also loads metadata; a metadata error does not prove MySQL analytics failed.
2. Record request bounds, timezone, filters, elapsed time and HTTP/error code. Browser deadline, server analytics budget, driver socket timeout, proxy deadline and admission rejection are different outcomes.
3. Replay the same request sequentially on staging. Disable summary percentiles, then disable **both** timeline and anomaly work, then compare compact core and each optional section. Overlapping stages make simple wall-clock subtraction misleading.
4. Correlate request stages with pool/admission wait, active MySQL statements, statement digest deltas, current rollup coverage and resource evidence. Background sections log `<section>_admission` separately from `<section>_read`, while retaining `<section>_prefetch` for their combined elapsed time. Admission includes local/shared query and percentile capacity waits; read includes repository execution, database-pool wait, row transfer and repository-side processing, not just SQL execution. A deadline during admission emits no read stage. Many occupied client connections with few active SQL statements can mean application-side waiting rather than InnoDB blocking.
5. Capture actual translated SQL with safe bound values in a controlled environment. Start with `EXPLAIN FORMAT=TREE` or `FORMAT=JSON`. `EXPLAIN ANALYZE` executes the query; use it only for bounded approved staging measurements.

Useful database evidence includes index definitions and cardinalities, statement timer/row/temp/sort counters, metadata/data-lock waits, transaction age, buffer-pool behavior, storage latency, and Manager-to-database RTT. Counts and sizes may be estimates; cumulative counters require before/after deltas. Session settings in an unrelated CLI connection do not necessarily equal the application's forced session settings. SQL cannot expose Go admission or mutex waits.

Keep DSNs, keys, authorization headers, raw event bodies, and raw filter/search values out of logs. Keep profiling endpoints private. Healthy liveness probes do not establish query capacity or complete rollup coverage.

Do not start remediation by increasing every timeout/pool limit, running `OPTIMIZE TABLE`, deleting retained projections, changing historical generation markers, or assuming existing timestamp indexes are absent.
