# Usage analytics SQL catalog

## 1. Status, scope, and reading guide

**Documented baseline:** `87d975a2db2355c69b9c9b51da2cf7de2a84a9a2`, branch `perf/usage-analytics-mysql-redesign`. **Reconciled:** 2026-11-13.

This is a source-derived catalog of the usage-analytics page, its selectable repository paths, and the maintenance dependencies that make those paths correct. It reconciles the primary inventories and their independent verification, including the subsequently extracted derived-source, SQLite-history, and validation findings. Those findings are available; an earlier completeness check's missing-report warning is superseded.

The redesign is in progress. Unless explicitly labeled **proposal**, statements below describe the baseline, not later implementation. Source line ranges also refer to that baseline. Record subsequent changes in [Implementation deltas](#13-implementation-deltas); do not silently reinterpret baseline costs as post-change measurements.

No production instrumentation, production data, database experiment, benchmark, or test run was required to assemble this catalog. Statement counts are source-derived under stated conditions. Index availability is not an observed execution plan. Logical CTE width is not proof of physical materialization. No latest-commit latency or per-operator runtime is claimed.

Navigation:

- [Notation and cost boundaries](#2-notation-and-cost-boundaries)
- [Page request graph](#3-page-request-graph)
- [Normalized templates and filters](#4-normalized-templates-and-filters)
- [Raw/reference/native family catalog](#5-rawreferencenative-family-catalog)
- [Derived and retained-source catalog](#6-derived-and-retained-source-catalog)
- [Auxiliary and maintenance SQL](#7-auxiliary-and-maintenance-sql)
- [Index catalog](#8-index-catalog)
- [SQLite lineage and portability](#9-sqlite-lineage-and-portability)
- [Design priorities](#10-recommended-design-priorities)
- [Correctness invariants](#11-immutable-correctness-invariants)
- [Offline acceptance and CI](#12-bounded-offline-acceptance-and-ci)

### Source reference convention

Paths below are repository-relative. Short references such as `UE:492–529` identify the linked file and baseline lines. `UM/file.go` means a file under the linked monitoring repository directory. SQL in this document is normalized explanatory SQL: aliases, expression macros, and named binds are not executable replacements for the source builders.

| Reference | Source |
|---|---|
| UI / UI-S / UI-M | [useUsageAnalytics.ts](../../apps/web/src/features/usage-analytics/useUsageAnalytics.ts), [usageAnalyticsSections.ts](../../apps/web/src/features/usage-analytics/usageAnalyticsSections.ts), [usageAnalyticsModel.ts](../../apps/web/src/features/usage-analytics/usageAnalyticsModel.ts) |
| API / LOAD | [usageService.ts](../../apps/web/src/services/api/usageService.ts), [useUsageData.ts](../../apps/web/src/features/monitoring/hooks/useUsageData.ts) |
| S / PLAN / EXEC | [monitoring/service.go](../../apps/manager-server/internal/service/monitoring/service.go), [sections.go](../../apps/manager-server/internal/service/monitoring/sections.go), [execution.go](../../apps/manager-server/internal/service/monitoring/execution.go) |
| STORE / HR / MR | [store.go](../../apps/manager-server/internal/store/store.go), [usagehourly/reader.go](../../apps/manager-server/internal/service/usagehourly/reader.go), [usagemonitoring/reader.go](../../apps/manager-server/internal/service/usagemonitoring/reader.go) |
| UE / AG / GR | [usageevent/analytics.go](../../apps/manager-server/internal/repository/usageevent/analytics.go), [aggregate.go](../../apps/manager-server/internal/repository/usageevent/aggregate.go), [analytics_grouped.go](../../apps/manager-server/internal/repository/usageevent/analytics_grouped.go) |
| LAT / STREAM / CAL | [analytics_latency.go](../../apps/manager-server/internal/repository/usageevent/analytics_latency.go), [analytics_latency_reference.go](../../apps/manager-server/internal/repository/usageevent/analytics_latency_reference.go), [analytics_buckets.go](../../apps/manager-server/internal/repository/usageevent/analytics_buckets.go) |
| UM | [repository/usagemonitoring](../../apps/manager-server/internal/repository/usagemonitoring) — `event_filter.go`, `event_stats_read.go`, `core_stats_read.go`, `stats_read.go`, `metadata_read.go`, `event_page_read.go`, `account_window_read.go`, `hourly.go`, `filter.go`, `stats_upsert.go`, `metadata_upsert.go`, `repository.go`, `types.go` |
| CORE / PRICE / RECOVER | [usageaggregate/repository.go](../../apps/manager-server/internal/repository/usageaggregate/repository.go), [usagepricing/repository.go](../../apps/manager-server/internal/repository/usagepricing/repository.go), [usagepricing/projection_read.go](../../apps/manager-server/internal/repository/usagepricing/projection_read.go) |
| PROJ / RET / ARCH | [usageprojection/projection.go](../../apps/manager-server/internal/repository/usageprojection/projection.go), [retained_source.go](../../apps/manager-server/internal/repository/usageprojection/retained_source.go), [usagearchive/repository.go](../../apps/manager-server/internal/repository/usagearchive/repository.go) |
| BOOK / RULE / COST | [modelprice/repository.go](../../apps/manager-server/internal/repository/modelprice/repository.go), [model_price_rules.go](../../apps/manager-server/internal/model/model_price_rules.go), [pricing/cost.go](../../apps/manager-server/internal/service/pricing/cost.go) |
| MODEL / ID / EVENT | [usageidentity/model.go](../../apps/manager-server/internal/usageidentity/model.go), [usageidentity/identity.go](../../apps/manager-server/internal/usageidentity/identity.go), [usage/event.go](../../apps/manager-server/internal/usage/event.go) |
| DDL / HDDL / DIA / GEN | [mysql/schema.sql](../../apps/manager-server/internal/repository/mysql/schema.sql), [002_monitoring_hourly.sql](../../apps/manager-server/internal/repository/mysql/migrations/002_monitoring_hourly.sql), [mysql/dialect.go](../../apps/manager-server/internal/repository/mysql/dialect.go), [schemagen/main.go](../../apps/manager-server/internal/repository/mysql/internal/schemagen/main.go) |
| CFG / BENCH | [config/analytics.go](../../apps/manager-server/internal/config/analytics.go), [analyticsbench](../../apps/manager-server/internal/analyticsbench) |

## 2. Notation and cost boundaries

The earlier inventories used different meanings of `N`, `K`, and `B`. This document uses the following definitions consistently.

| Symbol | Meaning |
|---|---|
| `Nall` | All live raw `usage_events` rows, regardless of request time. |
| `Nwindow` | Live raw rows in the requested half-open time interval, before other filters. |
| `N` | Live raw rows satisfying the complete request filter; `0 ≤ N ≤ Nwindow ≤ Nall`. |
| `Vwindow`, `V` | Event-projection/live-tail candidates and matches, respectively; retained deleted events can make these larger than the corresponding live-raw population. |
| `T` | Matching uncovered event-ID tail inside the selected span, relative to that source's watermark. A global ID-range access can examine more rows; a timestamp-range access can examine `Nwindow` before rejecting covered IDs. |
| `R` | Matching **covered-history Codex-marker residual** deliberately excluded from account rollups. `R` can be nonzero when `T=0`; it is not an uncovered tail. |
| `E` | Matching event-level contributions in exact partial boundary intervals. Stored interiors, tails, and edges must not overlap in contribution. |
| `H_x`, `D_x` | Stored hourly/daily rows at source grain `x`, in the selected generation/range. Neither means elapsed hours/days. |
| `G_x` | Groups at the explicitly named SQL/application grain, e.g. `G_heatmap`, `G_model`, `G_selector`. Different `G`s are not interchangeable. |
| `M`, `Kc`, `Ks`, `K` | Configured base models, context-tier rows, service-tier rows, and `K=Kc+Ks`. Band classification uses `Kc`, not all `K`. |
| `B` | Requested/populated timeline bucket count, with exact calendar membership distinguished from a contiguous display range. |
| `P` | Requested page/preview/recent-row limit; keyset prefetch is `P+1`. Worker batch limits are written `batch_limit`, never `B`. |
| `C`, `U` | Source coverage ID and transaction-visible high-water ID. Each source owns its own `C`. |
| `A`, `W` | Archive references examined, and logical selected/intermediate row width. Neither is a measured constant. |
| `Q` | Positive latency sample count plus positive TTFT sample count, independently selected; `Q ≤ 2N`. |

Always separate **SQL statements**, **candidate rows examined**, **matching contributions**, **group/rank intermediates**, **database-to-Go transfer**, **Go state**, and **HTTP output**. A `UNION ALL` or scalar subquery does not add a client round trip; one statement can contain several scans, joins, sorts, and materializations.

Minimum useful cases have empty matches or strongly compressible dimensions. Empty scalar aggregates can still return one row and incur all preparation/catalog queries. Worst structural cases include `G≈N`, `H≈N`, stale `T≈N`, large `R`, and history-wide proof/search work. These are bounds/counterexamples, not measured production distributions.

A native heatmap can return `G_heatmap × 23` selected values, versus the compatibility stream's `N × 15`; final output is at most 168 cells. A native exact query can rank up to `4N` samples and return only `B+1` rows. A one-row options result can encode `O(V)` distinct strings. An event preview returning 12 items does not promise 12 candidate visits.

## 3. Page request graph

### 3.1 Independent switches and request admission

`POST /v0/management/monitoring/analytics` follows UI → `useMonitoringAnalytics` → controller → `Service.Analytics` → execution/admission → store/readers → repositories. Valid positive bounds, monitoring availability, and connection configuration are prerequisites; demo mode does not issue real analytics SQL.

| Switch | Baseline default / effect |
|---|---|
| `USAGE_ANALYTICS_PROGRESSIVE_ENABLED` | `true`; advertises `analyticsSectionsVersion=1`. UI capability negotiation chooses split sections versus legacy combined requests. |
| `USAGE_MYSQL_ANALYTICS_ENABLED` | `false` when omitted; on an actual MySQL connector selects native grouping/ranked percentiles where eligible. Independent of progressive capability. |
| `USAGE_MYSQL_HOURLY_ENABLED` | `false` when omitted; enables supplemental monitoring-hourly statistics and metadata processing/reads. Does **not** route heatmap or credential timelines to stored hourly data. |
| `USAGE_DASHBOARD_HOURLY_ROLLUP_ENABLED` | `true`; separately enables the permanent core/pricing hourly reader for representable filters/ranges. |

Sources: `CFG:18–38`, `config/config.go:230`, `service/setup/service.go:87–92`, `STORE:196–218`, `UI:110,321–391`.

The browser admits at most two analytics POSTs; core/preview have higher priority than optional sections/selectors. Server admission defaults to two active analytics requests, four queued, 500 ms admission wait, and a 20-second service deadline. Browser transport allows 30 seconds. Server background prefetch has two shared query slots and one percentile slot; foreground work can use additional connections. These are scheduling limits, not reductions in SQL work or a two-connection page guarantee.

### 3.2 Six tabs, nine hook instances

All cores request **compact summary**, which implicitly needs model/pricing groups for cost. The nine hook instances are **core, dimensions, exact, preview, selectors, date heatmap, top-key timeline, selected-key timeline, selected-credential timeline**. Inapplicable hooks receive no valid bounds or are disabled; nine instances do not mean nine simultaneous requests.

| Tab | Progressive core beyond summary | Dimensions | Exact after successful core | Conditional additional request | Fresh successful progressive POSTs |
|---|---|---|---|---|---|
| Overview | Comparison, core timeline, model stats | Channel share, API-key stats | Summary + bucket percentiles, anomalies | One batched timeline for up to four selectable keys; clicked preview | 4, or 5 with keys; +1 preview |
| Trends | Comparison, core timeline, model stats | API-key stats | Bucket percentiles, anomalies | Same top-key timeline and preview | 4, or 5 with keys; +1 preview |
| Models | Core timeline, model stats | API-key stats | None | Model detail/trend selection is local | 3 |
| API keys | None | API-key stats | None | Selected/default key's ordinary core timeline | 3, or 4 with a selectable key |
| Credentials | None | Raw credential stats | None | Selected/default credential timeline | 3, or 4 with a credential |
| Heatmap | None | Full-window heatmap | None | Selected date-option interval heatmap | 3; +1 selected date |

Every row includes a separate selector POST. Counts assume a new valid, stable, successful generation without retries, cancellations, or metadata-driven reselection. Identical bodies can survive a tab switch. Legacy mode has **one combined request plus selectors**, then the same conditional detail requests; it builds the main timeline once, not once in core and again in exact.

Sources: `UI-M:799–862`, `UI-S:51–106`, `UI:346–786`. Legacy Models and selected-key timelines omit `timeline_profile=core`, so they additionally request implicit exact bucket percentiles. Progressive Overview/Trends exact requests contain `anomaly_points=true`: they are **not** the standalone unpriced percentile shape and rebuild a priced timeline already loaded by core.

### 3.3 Interaction, scope, and auxiliary requests

- Time changes and explicit refresh create new fixed bounds; global search is trimmed/debounced. Model/key/provider/auth-file/outcome/min-latency/cache filters affect main sections. UI-local metrics, model details, heatmap scale/cell highlight, and most table controls do not issue SQL.
- Selectors retain time/search but clear dropdown, outcome, minimum-latency, and cache restrictions. Their generation excludes tab, selected dimensions, and granularity. Both `filter_selectors` and `filter_options` are sent; current-server **selector precedence** prevents legacy full-option/statistics fanout.
- Top-key selection happens **after full API-key statistics**; the top-four cap does not limit that statistics query. One timeline request includes up to four hashes. Selected-key timelines replace the key scope while retaining other filters; selected-credential timelines add the selected credential identity.
- A timeline click requests a preview of at most 12 events only when bucket membership is exact. `total_count=len(items)` is **not** a population count. Main and preview coverage can independently count raw events after archival. Full pagination is reached only after navigation to `/monitoring`.
- Heatmap is eager over the selected range. A cell click is local; choosing a date makes another full interval query, not a weekday/hour-filtered query. Date options are formed from UTC-hour chunks, so fractional-offset options are not guaranteed exact civil-midnight intervals. Changing the displayed metric does not remove pricing.
- Heatmap failure no longer prevents a successful core from rendering. The reverse independence is absent: merged results/heatmap rendering still require core. Separate POSTs have separate data snapshots and coverage; stale-response fencing is not cross-request snapshot consistency.
- `useUsageData({loadUsageEvents:false})` still GETs model prices and aliases. This page consumes aliases, not that frontend price book. Empty server prices plus browser fallback can cause a conditional price-book PUT. Page refresh reloads aliases and auth/channel metadata, not prices.
- Page metadata normally includes price, alias, auth-file, and compatibility-channel GETs. An uncached successful availability probe makes two info GETs plus a manager-config GET, shared across hooks. Shell config may add a cached/proxied GET. Auth/channel payloads come from upstream HTTP; Manager authorization/setup can add settings reads, not usage-event scans.

Sources: `UI:119,393–497,532–594,678–786,919–926`; `LOAD:181–214,253–257`; `UI-M:1264–1292`; `S:983–1055,1235–1255,1586–1623`.

### 3.4 All 26 Include fields accounted for

| Classification | Include fields and dependency rule |
|---|---|
| Current summary/timeline work | `summary`, `summary_comparison`, `timeline`, `model_stats`; summary/model stats share the same model load within one POST. |
| Modifiers, not independent SQL families | `summary_profile` (page always compact), `timeline_profile`, `granularity`. |
| Exact work | `summary_percentiles` (legacy Overview), `percentiles` (progressive), `anomaly_points` (priced timeline + bucket percentiles, then Go calculation). |
| Current dimension/detail work | `channel_share`, `credential_stats`, `credential_timeline`, `api_key_timeline`, `api_key_stats`, `heatmap`, `drilldown_preview`. |
| Current selector request | `filter_options` + `filter_selectors`; the latter wins. |
| Not emitted by this page | `hourly_distribution`, `model_share`, `failure_sources`, `account_stats`, `task_buckets`, `recent_failures`, `events_page`. These remain cataloged for shared endpoint/adjacent consumers. |

Compact summaries omit rolling-30-minute work, active-day enumeration, zero-token-model enumeration, and implicit task buckets in **both** UI modes. Header snapshots, account history/windows, quota snapshots, model-price usage summaries, import/export, and diagnostics polling are not hidden page requests.

## 4. Normalized templates and filters

### 4.1 `F`: time, identity, and search predicates

All ordinary range predicates are `timestamp_ms >= :from AND timestamp_ms < :to`. Dynamic predicates are template variants, not separate families for every parameter combination. JSON-list bindings become `JSON_TABLE(... '$[*]' COLUMNS(value LONGTEXT PATH '$'))` on MySQL, preserving large-list support without one placeholder per value.

| Filter variant | Actual semantic expression / source eligibility |
|---|---|
| Models | Normalized authoritative requested-model identity, not raw `model`; recognized reasoning suffixes only are stripped. |
| Providers | Lowercased `provider OR auth_provider_snapshot`; attribution separately prefers nonempty snapshot provider. |
| Accounts | Lowercased account, label, source, or auth-index OR; not canonical `account_key` equality. |
| Credentials | First nonempty auth-file → auth-index → source-hash → source → `'-'`; not just the file/index pair. |
| Exact lists | Auth files/indices, key/source hashes, request type, header kind/code/plan/trace use complete-value membership, commonly wrapped in `COALESCE`. |
| Projects | Cleaned project expression hides historical Codex markers; cannot substitute bare project equality. |
| Outcome | `failed=0` and/or `failed=1`; contradictory inputs can legitimately match nothing. |
| Latency/cache | `latency_ms>=minimum`; hit/miss use cache-field positivity, read/creation use their own counters. Stored dimensional stats cannot represent these filters. |
| Raw text | Lowercased substring OR across searchable fields; with text, search-key hash is another **OR**, not an AND restriction. Without text it is direct hash equality. |
| Projection text | Packed `search_text`; suitable length ≥3 uses the search relation subquery, short/control variants use direct LIKE; `%`, `_`, or U+001F make the projection path unsupported. |

Sources: `UE:2931–3082`, `PROJ:77–144`, `UM/event_filter.go:101–198`, `UM/filter.go:12–79`.

On MySQL, `usage_monitoring_event_search_v1` is a plain view. The subquery `event_id IN (SELECT rowid FROM ... WHERE search_text LIKE :pattern)` has no explicit inner time bound; history-wide work is possible, not established. SQLite's relation is FTS5 trigram. Existing raw free-text search can match a stored Codex project marker that projected search hides. That is a verified preexisting cross-path discrepancy, not permission to claim raw/projection filter equivalence.

### 4.2 `PRICE0` / `PRICEF`: shared event pricing pipeline

```sql
WITH base AS (
  SELECT e.*, requested_expr AS requested_name, analytics_expr AS analytics_name,
         COALESCE(NULLIF(resolved_model,''), analytics_expr) AS billing_name,
         COALESCE(normalized_total_input_tokens,input_tokens,0) AS input_value
  FROM usage_events e /* WHERE F only in PRICEF */
), resolved AS (
  SELECT b.*, CASE WHEN pb.model IS NOT NULL THEN billing_name
                  WHEN pa.model IS NOT NULL THEN analytics_name
                  WHEN pd.model IS NOT NULL THEN requested_name
                  ELSE billing_name END AS pricing_name
  FROM base b
  LEFT JOIN model_prices pb ON pb.model=b.billing_name
  LEFT JOIN model_prices pa ON pa.model=b.analytics_name
  LEFT JOIN model_prices pd ON pd.model=b.requested_name
), banded AS (
  SELECT r.*, COALESCE((SELECT MAX(t.threshold_tokens)
    FROM model_price_context_tiers t
    WHERE t.model=r.pricing_name AND r.input_value>t.threshold_tokens),-1) AS band
  FROM resolved r
)
SELECT required_columns FROM banded /* WHERE F for PRICE0 */;
```

`PRICEF` explicitly restricts the initial event relation; latest native grouping already does this. Most legacy priced analytics use `PRICE0` with outer filters. Their requested output is window-scoped, but source alone does not bound pricing/materialization to `Nwindow`; without effective pushdown it can extend to `Nall`. Do not claim that the optimizer necessarily scans/materializes all history or all payload columns.

Define pricing grain `J=(analytics_name,billing_name,pricing_name,band,service_tier)`. Define `I=normalized input`, `Ccache=max(max(cached,cache)-max(read,0)-max(create,0),0)`, additive token/status measures `Sums`, and five legacy long-context sums `LC` selected **per event** by `I>272000`. Native final heatmap omits reasoning/latency, not price-band classification. Sources: `AG:13–57`, `UE:19–35`, `RULE:90–157`, `COST:117–142,185–223,276–391`.

### 4.3 `GROUP`, `EVENTSTREAM`, and calendar variants

```sql
-- Raw scalar totals: no pricing joins.
SELECT COUNT(*), success_count, failure_count, SUM(token_measures),
       AVG(NULLIF(latency_ms,0)), zero_token_success_count FROM usage_events WHERE F;
-- Priced dimensional statistics: one group per identity × J.
WITH PRICE0 SELECT identity_dimensions,J,counts,Sums,LC,representative_metadata
FROM banded WHERE F GROUP BY identity_dimensions,J ORDER BY family_order;
-- Native timeline / key timeline / credential edge / heatmap.
WITH PRICEF, calendar_intervals AS (... optional exact relation ...)
SELECT requested_bucket_or_weekday_hour, contributor_dimensions,J,counts,Sums,LC
FROM banded /* optional interval join */ GROUP BY requested_grain
ORDER BY MIN(timestamp_ms), family_identity_order;
-- Compatibility timeline / heatmap: database ordering, event transfer, Go fold.
WITH PRICE0 SELECT timestamp_ms,J,identity_fields,failed,required_token_fields
FROM banded WHERE F ORDER BY timestamp_ms, family_identity_order;
```

Constant-offset compatible ranges use arithmetic with positive modulo; transition ranges use Go-derived exact half-open calendar intervals, at most 4,000. Native raw grouping supports fractional offsets/DST where this plan is available. Unavailable plans fall back **before execution**; query/compile errors do not trigger another scan. UTC-hour stored aggregation has stricter representability than raw event bucketing. Sources: `CAL:24–108`, `GR:17–74`.

Credential timelines separately partition whole representable UTC hours and up to two fringes. `credentialTimelineHourlyWithFilter` runs `GROUP BY` over **raw priced events**, optionally joined to a bucket map; it is not a stored `H` reader and has no watermark tail. Its core retains a wider provenance grain, later merged in Go. First-event provenance and weighted latency differ from max-string dimension summaries.

### 4.4 Exact percentile templates

```sql
-- R09: native / shared scoped window builder; no pricing CTE.
WITH filtered AS (SELECT timestamp_ms,latency_ms,ttft_ms FROM usage_events
  WHERE F AND (latency_ms>0 OR ttft_ms>0)),
 samples AS (SELECT scope,bucket,metric_kind,positive_value
  FROM filtered CROSS JOIN requested_metrics CROSS JOIN requested_scopes
  /* exact bucket assignment; retain only positive values */),
 ranked AS (SELECT *,ROW_NUMBER() OVER(PARTITION BY scope,bucket,metric_kind
  ORDER BY positive_value) AS rn,
  COUNT(*) OVER(PARTITION BY scope,bucket,metric_kind) AS n FROM samples)
SELECT scope,bucket,latency_winner,ttft_winner FROM ranked
WHERE rn=(n*95+99) DIV 100 /* fold metric winners by scope,bucket */;
-- R10: MySQL native-off; all passes share one repeatable-read transaction.
SELECT timestamp_ms,latency_ms>0,ttft_ms>0 FROM usage_events WHERE F AND either_positive;
SELECT timestamp_ms,latency_ms FROM usage_events WHERE F AND latency_ms>0 ORDER BY latency_ms;
SELECT timestamp_ms,ttft_ms FROM usage_events WHERE F AND ttft_ms>0 ORDER BY ttft_ms;
```

R09 expands to `Q` samples for one scope, `2Q≤4N` for summary+buckets. Transfer is at most `B+1` four-field rows; one statement is not one physical scan/sort. R10 skips empty metric passes, transfers up to roughly `3N` narrow rows, retains `O(B)` Go state, and still orders values in the database. The timestamp-leading cover cannot provide global metric order across a timestamp range.

R11, oversized calendar fallback: in one snapshot, `SELECT DISTINCT timestamp_ms ... ORDER BY timestamp_ms`, construct bucket runs, optionally rank summary, then rank batches of complete bucket partitions using bounded interval relations. Discovery can transfer `O(N)` timestamps. Never split one bucket across percentile batches or average partial P95s; an unrepresentable oversized partition errors rather than truncates. Sources: `LAT:24–220`, `STREAM:22–175`.

### 4.5 Selectors, options, and bounded detail prefetch

```sql
-- R24: five raw SELECTs, all under broad time/search F_options.
SELECT DISTINCT analytics_model FROM usage_events WHERE F_options AND model_nonempty ORDER BY 1;
SELECT api_key,preferred_provider,auth_index,source_hash,MAX(source)
  FROM usage_events WHERE F_options GROUP BY api_key,preferred_provider,auth_index,source_hash;
SELECT DISTINCT preferred_provider FROM usage_events WHERE F_options AND provider_nonempty ORDER BY 1;
SELECT DISTINCT auth_file FROM usage_events WHERE F_options AND file_nonempty ORDER BY 1;
SELECT account,label,preferred_provider,auth_index,source_hash,MAX(source)
  FROM usage_events WHERE F_options AND any_identity
  GROUP BY account,label,preferred_provider,auth_index,source_hash;
-- D09: MySQL projected full-options data query, eight scalar subqueries.
WITH filtered_events AS (EVENT_SOURCE)
SELECT (SELECT COALESCE(JSON_ARRAYAGG(v),JSON_ARRAY())
        FROM (SELECT DISTINCT field_1 AS v FROM filtered_events WHERE field_1<>'') d),
       /* seven more distinct-producing scalar subqueries */ ...;
-- R21: direct detail; R34/D10 select narrow IDs first, then hydrate.
SELECT detail_columns FROM usage_events WHERE F AND cursor_condition
ORDER BY timestamp_ms DESC,id DESC LIMIT :P_plus_1;
SELECT event_id,timestamp_ms FROM EVENT_SOURCE WHERE raw_exists AND cursor_condition
ORDER BY timestamp_ms DESC,event_id DESC LIMIT :P_plus_1;
SELECT detail_columns FROM usage_events WHERE id IN (JSON_ID_RELATION)
ORDER BY timestamp_ms DESC,id DESC; /* trim sentinel before hydration: at most P */
```

`cursor_condition` is `timestamp<before OR (timestamp=before AND id<before_id)`; old clients without ID use timestamp-only behavior. Projection candidate and hydration reads share a transaction. `LIMIT` bounds output, not filtered candidate examination. Direct raw details select 53 expressions; hydration selects 50 and currently omits three client/network fields. Neither final page SELECT includes raw body JSON or failure body, although response metadata is variable-width.

Raw full options R25 instead issue eight distinct-value queries (provider/file/project/type/header kind/code/plan/trace), potentially plus account/API/model statistics when not reusable. D09 is one data statement, **not proof of one physical pass** over its eight distinct subqueries. Sources: `UE:1010–1220,2567–2748`, `UM/metadata_read.go:45–232`, `UM/event_page_read.go:52–205`, `DIA:500–519`.

### 4.6 Derived composition, coverage, and preparation

```sql
-- EVENT_SOURCE: ordinary projection plus uncovered live tail.
SELECT narrow_fields FROM usage_monitoring_event_projection_v1
 WHERE event_id<=:projection_C AND F [AND span_ID_conditions]
UNION ALL
SELECT normalized_fields FROM usage_events
 WHERE id>:projection_C AND F [AND span_ID_conditions];
-- A stored monitoring span; each branch is a separate data statement.
SELECT requested_dimensions,SUM(measures) FROM stored_daily_or_hourly
 WHERE revision=:revision AND bucket_range AND supported_filters GROUP BY requested_dimensions;
SELECT requested_dimensions,aggregate_measures FROM EVENT_SOURCE
 WHERE id>:span_C AND span_time GROUP BY requested_dimensions; -- uncovered T
SELECT requested_dimensions,aggregate_measures FROM EVENT_SOURCE
 WHERE id<=:span_C AND span_time AND codex_marker GROUP BY requested_dimensions; -- R, account only
-- Daily interiors + hourly fringes + exact event edges are disjoint.
-- Readiness and revision are separate statements, not implicit constants.
SELECT state_fields FROM state_table WHERE rollup_name=:name;
SELECT COALESCE(MAX(id),0) FROM usage_events;
SELECT p.model,t.threshold_tokens FROM model_prices p
 LEFT JOIN model_price_context_tiers t ON t.model=p.model ORDER BY p.model,t.threshold_tokens;
-- RawCoverage: full UTC days and exact partial-day archive edges.
SELECT SUM(event_count),MIN(first_ms),MAX(last_ms) FROM (
  SELECT daily_counts_and_extrema FROM usage_archive_deleted_coverage_daily WHERE full_days
  UNION ALL SELECT COUNT(*),MIN(timestamp_ms),MAX(timestamp_ms)
    FROM usage_archive_event_refs WHERE left_edge AND raw_deleted_at_ms IS NOT NULL
  UNION ALL SELECT COUNT(*),MIN(timestamp_ms),MAX(timestamp_ms)
    FROM usage_archive_event_refs WHERE right_edge AND raw_deleted_at_ms IS NOT NULL) coverage;
SELECT COUNT(*) FROM usage_events WHERE time_range; -- conditional coverage count
SELECT EXISTS(SELECT 1 FROM usage_archive_event_refs
 WHERE partial_hour AND raw_deleted_at_ms IS NOT NULL); -- hourly edge routing
```

A completed projection can omit the raw arm. A union's arms are one statement. Exact edge queries use their full boundary population, not a proportional allocation of hourly totals. Permanent core has an additional current-generation identity-ledger exclusion on raw contributions. A new ID with an old timestamp remains uncovered until its own source advances.

## 5. Raw/reference/native family catalog

Stable IDs R01–R35 cover all 35 raw-inventory families. “Page” includes conditional fallback; “shared” means another Include profile; “adjacent” means another consumer. Counts exclude authorization, coverage, price-book loads, readiness, transaction control, and earlier unavailable-source attempts unless explicitly included. Index codes: **TIME** timestamp-leading raw access; **PAGE** timestamp/ID order; **LAT** latency cover; **PRICE** existing model and model/threshold prefix keys; **PK** event-ID lookup. All require residual checks when filters need additional columns.

| ID / method, reachability, source | Template / SELECT count | Input → transfer; grouping/order | Access and correctness/fallback |
|---|---|---|---|
| **R01 raw_summary** — `AggregateWithFilter`; page summary/comparison, shared rolling; UE:492–529 | Scalar totals / 1 | `Nwindow→N→1×12`; no group sort | TIME; no pricing. Nonzero SQL latency average is not universally the positive-only percentile population. Scoped event-count fallback may also invoke this full aggregate. |
| **R02 model_stats_all** — `ModelStatsWithFilter(limit=0)`; every page summary cost/model stats; UE:532–557,595–629 | PRICE0, group `J`, calls DESC / 1 | `N` contributions, `G_model×19`; outer-filter pricing caveat | TIME+PRICE; preferred permanent/monitoring source can replace it. Summary+model_stats does not double-load within one POST. |
| **R03 model_stats_top_variant** — positive limit; shared only; UE:558–594 | PRICE0 → top P analytics models → all their `J` groups / 1 | Two group stages; output can exceed P | TIME+PRICE; baseline bounded branch sums raw input rather than normalized input. Page passes zero; do not copy this discrepancy. |
| **R04 timeline** — `TimelineWithFilter`; core, anomalies, selected key; UE:632–761; GR:17–91 | EVENTSTREAM or native GROUP / 1 | Stream `N×15`; native `G_bucket,J×23`; final B; orders timestamp/min(timestamp), identity | TIME+PRICE; permanent snapshot may replace it. Native unavailable calendar uses stream; errors return. |
| **R05 api_key_timeline** — `APIKeyTimelineWithFilter`; top-key detail; UE:764–888; GR:94–115 | R04 + key / 0 without a key/hash, otherwise 1 | Stream `N×16`; native `G_key,bucket,J×24`; final selected keys×B | TIME+PRICE; key selection is mandatory, no output SQL limit implied by top-four UI selection. |
| **R06 heatmap** — `HeatmapWithFilter`; heatmap/date; UE:1223–1355; GR:25–39,117–142 | EVENTSTREAM or native weekday/hour GROUP / 1 | Stream `N×15`; native `G_weekday,hour,key,provider,J×23`; group sort; service ≤168 cells | TIME+PRICE; always raw at baseline, never stored hourly. No date grain or unused reasoning/latency in native final SELECT. |
| **R07 hour_distribution** — `HourlyDistributionWithFilter`; shared only; UE:961–1007; GR:145–177 | Timestamp/token stream or local-hour GROUP / 1 | Stream `N×2`; native ≤24×3; local-hour order | TIME, no query-local pricing. Broad service plan still loads book. Native/calendar fallback rules apply. |
| **R08 credential_timeline** — `CredentialTimelineWithFilter`; selected credential; UE:1896–2333; GR:180–252 | Raw whole-hour GROUP plus 0–2 fringes, or whole-range raw helper / 1–3 | Stream edge `Nedge×25`; grouped/core `G_provenance,bucket,J×33`; final credential×B | TIME+PRICE; raw query-time hourly, not H+T. Preserve first-snapshot provenance and weighted latency. |
| **R09 percentile_scoped_window** — `LatencyMetricsWithFilter`; exact/legacy; LAT:24–58,166–220 | Scoped rank template / 1; 0 if neither scope | `Q` or `2Q≤4N` rank rows → ≤`(B+1)×4`; partition sorts | LAT conditional cover; raw only, exact positive independent populations; no pricing CTE. |
| **R10 percentile_mysql_reference_stream** — native off; STREAM:22–175 | Count-population pass + nonempty metric streams / 1–3 | Up to ~3N narrow transferred rows, O(B) Go state; DB value sorts | LAT conditional cover; repeatable-read snapshot, cancellation/mismatch fails without partial answer. |
| **R11 percentile_oversized_calendar_map** — LAT:61–164 | Distinct timestamp discovery + optional summary + complete-bucket batches / 1+optional 1+batches | Discovery up to N timestamps; repeated bounded rank queries; B final rows | LAT; ≤4,000 intervals per batch, not a bound on all work or transfer. No split-percentile averaging. |
| **R12 percentile_sqlite_summary_legacy** — `LatencySummaryWithFilter`; direct non-MySQL caller; UE:896–943 | Two positive branches, rank by metric / 1 | Q ranked rows → ≤2×2 | LAT; current service uses combined R09/R10 instead. MySQL wrapper delegates. |
| **R13 channel_stats** — `ChannelModelStatsWithFilter`; Overview fallback; UE:1357–1432; S:2431–2541 | PRICE0, group auth-index×J / 1 raw | `G_channel,J×27`, calls order, MAX display fields | TIME+PRICE; first tries account-derived stats; reuse account result if both sections requested. MAX metadata is not latest provenance. |
| **R14 account_stats** — `AccountModelStatsWithFilter`; indirect channel/shared account/full options; UE:1479–1564 | PRICE0, account/provenance×J / 1 | `G_account,J×32`, last-seen/calls order | TIME+PRICE; monitoring preferred. Canonical account-window key is not this display grouping. |
| **R15 credential_stats** — `CredentialModelStatsWithFilter`; Credentials; UE:1804–1893 | PRICE0, credential/file/index/source-hash×J / 1 | `G_credential,J×32`, last-seen/calls order | TIME+PRICE; directly raw, no daily/hourly substitution. Preserve fallback identity and representative maxima. |
| **R16 api_key_stats** — `APIKeyModelStatsWithFilter`; four tabs; UE:2336–2417 | PRICE0, key/account/provenance×J / 1 | `G_key,context,J×30`, last-seen/calls order; not one row/key | TIME+PRICE; monitoring preferred, final service folds contexts. |
| **R17 failure_sources** — `FailureSourcesWithFilter`; shared; UE:1435–1476 | Group source-hash/index, HAVING failure_count>0 / 1 | `N→G_source×10`, failure/last-seen sort | TIME; no price joins. Do not prefilter failures and lose success denominator/average. |
| **R18 recent_failures** — `RecentFailuresWithFilter`; shared; UE:2482–2555 | Wide detail WHERE failed=1 ORDER timestamp/id LIMIT P / 0 if P≤0, else 1 | ≤P×22, variable metadata; sparse matches can examine Nwindow | PAGE; forces failure inclusion. No baseline failed/time composite. |
| **R19 task_buckets** — `TaskBucketsWithFilter`; shared/full summary; UE:2420–2479 | Group timestamp-text/source/index, distinct lists, ORDER last_seen LIMIT 500 / 1 | N grouped before cap; ≤500×19 plus variable lists | TIME; heuristic tasks, not session identity. GROUP_CONCAT capped at configured 16 MiB, not unlimited exact set transport. |
| **R20 event_count** — `EventsCountWithFilter`; shared events_page; UE:2558–2564; S:2574–2585 | COUNT WHERE F / 0 if reused, else 1 raw | Nwindow candidates → 1 integer; no cursor/limit | TIME can cover time-only count. Summary reuse, aggregate, projection, and archive branches exist; preview never requests this total. |
| **R21 event_page_and_preview** — `EventsPageWithFilter`; page preview/shared pagination; UE:2567–2748 | Ordered keyset detail prefetch / 1 | ≤(P+1)×53; UI P=12; candidate visits unbounded by P | PAGE; archive/time-only/unsupported projection use raw policy. Full page default 100/max 50,000; preview default 20/max 100 for other clients. |
| **R22 active_days** — `ActiveDaysWithFilter`; full summary only; UE:2880–2902 | Timestamp SELECT then local-date set / 1 | N×1 → one day count; no SQL sort | TIME conditional cover; active local days are not elapsed UTC days. |
| **R23 zero_token_models** — `ZeroTokenModelsWithFilter`; full summary only; UE:2905–2928 | DISTINCT analytics model, total_tokens=0, failed=0 / 1 | Distinct/sort over matches → model strings | TIME; raw-model index is not the normalized model expression; empty labels removed. |
| **R24 filter_selectors** — `FilterSelectorValuesWithFilter`; all tabs fallback; UE:1055–1220 | Five SELECT template / 5 | Revisit broad Nwindow up to five times; 3 distinct sets plus key×5/account×6 tuples | TIME; no token/pricing work in these queries; full option route suppressed by precedence. |
| **R25 filter_options_legacy** — `FilterOptionValuesWithFilter`; shared/old server; UE:1010–1053; S:2282–2349 | Eight independent DISTINCT SELECTs / 8, potentially +3 stat families | Variable distinct sets, trace cardinality may approach N; per-field order | TIME; do not charge to current selectors. Reuse stats only when scopes match. |
| **R26 account_window_stats_adjacent** — `AccountWindowModelStats`; adjacent batch; UE:1567–1801 | PRICE0 JOIN target VALUES(request_index,from,to,key,legacy_key), GROUP request_index,J / 1+identity probes | Across target windows/history-sensitive identity evidence → `G_target,J×21` | TIME+PRICE; no injected common base range. Legacy alias requires fail-closed provenance checks; not page work. |
| **R27 header_snapshots_adjacent** — `LatestHeaderSnapshots`; UE:2751–2877 | Candidate headers → ROW_NUMBER per fallback identity → latest P / 1 | Since-only history candidates ranked before cap → ≤P×23 | TIME; variable response metadata; no page call, preview header fields are already in details. |
| **R28 aggregate_between_adjacent** — `AggregateBetween`; AG:121–173 | Time-only scalar aggregate / 1 | Nwindow→1×18, including LC/sample count | TIME; dashboard/shared reader, not extra page summary SQL. |
| **R29 top_models_between_adjacent** — `TopModelsBetween`; AG:175–248 | PRICEF → top P models → all bands / 1 | Two groups/sorts, output may exceed P; default P=5 | TIME+PRICE; explicit early time filter. SQLite plan test is not a native-plan guarantee. |
| **R30 model_stats_between_adjacent** — `ModelStatsBetween`; AG:251–314 | PRICE0, time range, group J / 1 | `G_model×19`, calls/identity order | TIME+PRICE; not additional to page R02. |
| **R31 recent_failures_between_adjacent** — `RecentFailuresBetween`; AG:317–392 | Failed/time detail, ordered LIMIT P / 1 | ≤P×22; default P=5 | PAGE; direct snapshot-provider output differs from filtered fallback attribution. |
| **R32 relative_bucket_timeline_adjacent** — `BucketTimelineBetween` / `HourlyTimelineBetween`; AG:395–433 | GROUP integer((timestamp-from)/size) / 1 | Nwindow→relative buckets×5, bucket order | TIME; relative buckets are not local calendar buckets. |
| **R33 model_usage_summary_adjacent** — `ModelUsageSummary`; usageevent/model_usage.go:10–75 | Latest-P sample → requested/resolved UNION groups; separate all-history COUNT / 2 | Sample ≤min(Nall,P), default 50,000; count still Nall-scale; model rows×3 | PAGE/TIME; model-price endpoint, not page bootstrap price load; statements do not share a snapshot. |
| **R34 projection_page_raw_hydration_boundary** — UM/event_page_read.go:52–205 | Narrow candidate prefetch + bounded raw ID hydration / 1+0/1, preparation extra | ≤P+1 two-field candidates + ≤P×50 details; final keyset order | Projection time/model/account candidates + raw PK; require live raw existence and one snapshot; not all-window wide transfer. |
| **R35 price_catalog_snapshot** — `LoadAll[Tx]`; BOOK:53–220 | Models; context tiers; service tiers / **3 per invocation**, including empty books | M×15 + Kc×12 + Ks×13; full dictionaries ordered by their identities; base includes raw_json | PRICE keys; all actual page POSTs load prices at baseline. Standalone percentiles are exempt; independent/rejected snapshots may repeat the book. |

## 6. Derived and retained-source catalog

### 6.1 Grains, readiness, and exact source composition

Permanent core grain is `UTC hour × analytics model × billing model × service tier × outcome`; pricing hourly adds `pricing model × context band × structure revision`. It supports model/outcome/time, **not** key/provider/credential/header/search filters. It has no exact latency distribution or historical TTFT. Timeline additionally requires whole UTC hours to map losslessly to local buckets.

Monitoring account/API-key daily and optional hourly grain retains revision/bucket, account/label, both providers, account-ID, auth-index, source/hash, auth-file, API-key, executor type, analytics/billing/pricing models, service tier, band, and outcome. Account rows exclude particular Codex-marker provenance; API-key rows do not. Supported stats filters include identities/models/providers/request type/outcome, but not text, project, headers, minimum latency, or cache status. Those require projection/raw event composition.

Ordinary projection readiness checks schema and coverage against global `MAX(raw.id)`; it does **not** check structure revision/status. Daily stats check schema/current full stats revision and not-clearing; optional hourly stats require compatible schema/revision and `ready`/`catching_up`. Retained edge/recovery/rebuild checks are stronger and distinct. Missing permanent-core/pricing/daily/projection state generally propagates an error; optional hourly explicitly treats `sql.ErrNoRows` as unavailable. Do not generalize “missing state means safe fallback.”

For `s` stored spans and `e` exact event spans, monitoring aggregate/model/account readers normally issue four preparation SELECTs (projection state, raw high-water, daily state, price structure), plus optional hourly state, then **`3s+e`** data queries: stored, uncovered `T`, and covered marker `R` per stored span. API-key readers use **`2s+e`**. Logical contribution is `D/H + T + R + E`, not a physical scan guarantee. Preparation currently also occurs on some branches that ultimately use projection-only aggregation.

Selectors prepare projection state/high-water/metadata state plus optional hourly state, then **`2s+e`** stored+tail/edge queries. Stored selectors require unfiltered, include-failed scope; search/scoping uses event-level projection. Source: `UM/hourly.go:43–245`, `stats_read.go:90–171,187–603`, `core_stats_read.go:51–291`, `event_stats_read.go:14–257`, `metadata_read.go:128–237`.

### 6.2 Stable derived/control families

| ID / method and source | Statements / normalized shape | Scope, transfer, order, access | Correctness / fallback |
|---|---|---|---|
| **D01 state/high-water** — `projectionReadState`, `statsReadState`, hourly/state readers; UM files above | Point state SELECTs; `MAX(id)`; separate D02 revision | One state row each/global scalar; name-prefix keys and raw PK endpoint; not Nwindow-scoped | Readiness differs by source as above; independent watermarks cannot be substituted. |
| **D02 structure revision** — `StructureRevision`; PRICE:479–504; UM/types.go:28–48 | 1 model LEFT JOIN context-threshold SELECT / invocation | ≤M+Kc two-field rows, model/threshold order; PRICE keys | Full persisted revision wraps catalog structure with model/identity/Codex versions; stats adds `StatsBuildVersion`. Not a rate-cache version. |
| **D03 permanent core** — `LoadRowsTx`; CORE:330–437,616–802 | State + stored + raw tail; +0–2 exact edge aggregates | Aligned ordinary path 3 SELECTs; H_core+T+E → bucket/core groups with additive metrics; collapse buckets when possible; time key/ledger/PK | Ledger exclusion prevents replay double-counting. Clearing uses raw rather than stored. Deleted edges need D13 proof/source. |
| **D04 pricing hourly** — `LoadHourlyRowsTx`; PRICE:771–953 | State + D02 + stored + banded tail; +0–2 priced edges | Aligned ordinary path 4 SELECTs; H_pricing+T+E or revision fallback window → J/outcome/bucket groups; revision/time + PRICE | Per-event bands; stale structure/clearing may require full-range reclassification. Query errors terminal. |
| **D05 combined hourly snapshot** — `LoadUsageHourlyPricingSnapshot`; STORE:704–775 | D03+D04+R35 in one read transaction | **10 SELECTs** ordinary aligned nonempty eligible path; **10+2e** with e partial edges, before proofs/recovery/outer coverage | Core/pricing compared at bucket/model/billing/tier/outcome across 16 conserved metrics. Recovery must pass again; otherwise coverage error. |
| **D06 monitoring aggregate/model** — `LoadAggregate`, `LoadModelStats`; UM/event_stats_read.go, core_stats_read.go | Preparation + `3s+e`, or projection aggregate | D/H+T+R+E or V; scalar totals/model×J groups; stored revision/time and projection access | Price/state preparation on projection-only branch is currently redundant, not absent. No R omission when tail is empty. |
| **D07 account/channel stats** — `LoadAccountStats`; UM/stats_read.go:90–121,187–445 | Preparation + `3s+e`, or projection grouping; channel fold in Go | Account/provenance×J, roughly 31 fields; latest/calls order; D/H and V sources | Account exclusions require R. Channel derivation conserves weighted latency, not average of averages. |
| **D08 API-key stats** — `LoadAPIKeyStats`; UM/stats_read.go:123–153,466–603 | Preparation + `2s+e`, or projection grouping | API/context×J, roughly 30 fields; revision/time and explicit-key hourly digest candidates | API-key storage includes Codex-marker population; do not add account R again. |
| **D09 full options / selectors** — `LoadFilterOptions`, `LoadFilterSelectors`; UM/metadata_read.go:45–237 | Options: state+high-water+1 eight-subquery data SELECT; selectors: preparation + `2s+e` or event grouping | Options one row with 8 variable arrays; selectors G_selector tuples then Go dedup; projection/revision-bucket keys | Different families and scopes; one array row can be large. Stored selector dimensions do not cover all full-option fields. |
| **D10 projection count/page** — UM/event_page_read.go:18–205; S:1565–1623,2574–2596 | Count ordinarily 2 preparation+1 count; page 2 preparation+candidate+0/1 hydration | V candidates → scalar or ≤P+1 narrow IDs, ≤P wide rows; R34 boundary | Time-only prefers raw. Raw-existence proof excludes deleted details. Count may instead reuse summary or derived aggregate; archive policy counts live raw. Preview has no total query. |
| **D11 latest headers** — UM/metadata_read.go:382–498; adjacent | Metadata state + stored latest + ranked raw tail / 3 | Stored identity snapshots + T header candidates → ≤P merged snapshots; timestamp/ID and identity keys | Raw tail uses ROW_NUMBER per identity; merge/relimit preserves newest timestamp/ID. Not page bootstrap. |
| **D12 account windows / lifetime** — UM/account_window_read.go:18–315; PRICE:1073–1226; adjacent | Window-target batches of 100 over stored/event spans; identity preparation extra. Lifetime: state/revision + stored account rows + priced raw tail | Request-index×J window groups, or account×J lifetime groups; account/revision/digest/window keys | Lifetime rows have no time grain. Overlapping targets intentionally reuse events; account-window aliases need D16 evidence. |
| **D13 retained partial edges** — `VerifyRetainedEdgeTx`, `RetainedEdgeSource`; PROJ:24–74 | Per deleted edge: state + missing/above-coverage EXISTS proof, then caller's aggregate | Live slice UNION deleted refs JOIN projection by ID; time/ref and projection PK keys; narrow event evidence | Narrower proof than D15; helper lacks API-key/executor fields, so not a drop-in heatmap source. |
| **D14 pricing recovery** — `LoadHourlyRowsFromEventsTx`, account recovery; RECOVER:29–166 | Normally projection state + event-derived priced aggregate, then caller reconciliation | Raw authoritative UNION compatible actually-deleted projection; may examine full range; hourly joins ID+timestamp, account arm scopes key | Live-identity anti-join prevents duplication; core's 16-metric per-group conservation is mandatory. Ordinary projection availability is insufficient. |
| **D15 verified retained rebuild** — RET:25–115 | Conditional chain: deleted-reference EXISTS, state, invalid-evidence EXISTS, raw MAX(id), archived MAX(original ID); return UNION source | Proof can examine archive-wide A despite small later batch; PK/hash/ref probes; no request-time scope bound | Checks missing fields, timestamp mismatch, ID collisions, coverage, restored identity. EXISTS avoids reference multiplicity. Fail closed; never silently rebuild only surviving raw. |
| **D16 legacy identity evidence** — usageevent/account_identity_evidence.go:11–188; account_identity.go:29–197; adjacent | State/schema/high-water; capped tail-size probe LIMIT 1001; 3 stored evidence reads and up to 3 tail aggregates, else 3 raw-history forms | Per applicable distinct Codex target; identity/provenance chronology groups; may reach credential history outside window | Tail eligibility cap is 1,000, not truncated answers. Unavailable/too-large tail uses exact history fallback. Dedicated evidence prefix key; removed SQLite NOT INDEXED is not a MySQL plan. |
| **D17 coverage** — `RawCoverage`, `RawEventCount`, `HasDeletedRaw`; ARCH:595–672 | 1 union aggregate/range; +1 raw count if deleted; cross-range warnings can add counts; up to 2 partial-hour EXISTS probes | Daily rows+archive edge refs → 3 fields; raw Nwindow→1 count; daily PK/ref timestamp/raw timestamp | Time-only, not dropdown-filtered. Main always, comparison/preview conditionally; full-summary rolling is off-page. |

D05 pricing recovery preserves an existing cross-source integrity contract; it does not imply every future identity-filtered heatmap must add a core comparison that cannot represent its filter. The comparison-specific service exception remains: incomplete pricing coverage with deleted previous-period history suppresses that comparison rather than failing an otherwise valid current response (`S:1382–1405`). Other attempted-query errors are not fallback invitations.

## 7. Auxiliary and maintenance SQL

### 7.1 Prefetch is an execution graph, not a new query

Baseline service order is coverage → eligible hourly snapshot → price book if needed → model groups if needed → bounded background prefetch alongside foreground work → wait/merge → dependent timeline/anomaly/detail response construction. Prefetch schedules percentiles, distribution, heatmap, dimensions, selectors/options, and applicable shared sections. Each scheduled method expands into the families above; do not count a “prefetch SELECT” in addition to its constituent statements.

Sibling failure cancels foreground/background work. A waiting percentile job acquires its separate slot before shared query capacity. Request splitting can improve time-to-core while increasing total repeated reads. Sources: `S:37–130,1068–1338`, `EXEC:59–85,233–299`.

Illustrative successful counts, **not traces**, including one authorization settings read and excluding transaction control:

| Request and assumptions | SELECTs |
|---|---|
| Heatmap-only/date POST, no deleted history | 1 auth + 1 coverage + 3 prices + 1 heatmap = **6** |
| Selectors-only, wildcard makes projection unsupported before preparation, no deletions | 1 auth + 1 coverage + 3 currently unused prices + 5 raw selectors = **10** |
| Time-only preview, no deletions in main/preview ranges | 1 auth + 2 coverage + 3 currently unused prices + 1 raw page = **7** |

Unavailable hourly snapshots can already have loaded prices, after which the service loads them again; comparison can load another snapshot/book. Three price SELECTs means **per invocation**, not universally per POST.

### 7.2 Auxiliary/control families

| ID / source | SQL shape, count, scope, output, access, and contract |
|---|---|
| **A01 authorization/settings** — [setting/repository.go](../../apps/manager-server/internal/repository/setting/repository.go):305–318; [setup/service.go](../../apps/manager-server/internal/service/setup/service.go):70–103; [managerconfig/service.go](../../apps/manager-server/internal/service/managerconfig/service.go) | Bound `SELECT ... FROM settings WHERE key=?`; one record/read using key prefix. Each authorized Manager request loads admin credential. Setup/info/config add conditional config/setup/bootstrap reads; complete environment config can avoid setup reads. Never expose values in catalog/fixtures. |
| **A02 aliases** — apikeyalias/repository.go:28–45 | 1 `SELECT api_key_hash,alias,updated_at_ms FROM api_key_aliases ORDER BY alias COLLATE ...,api_key_hash`; all aliases, 3 fields, independent of N. Key-hash index does not supply alias ordering. |
| **A03 frontend prices** — LOAD:181–201; BOOK:223–326 | GET invokes R35 plus authorization. Conditional empty-book migration PUT reads 3 existing price tables, verifies retained rebuild on structural change, deletes service/context/model tables, inserts replacement M+K rows, then reloads book. Catalog-sized writes plus possible A-scale proof; not every mount. |
| **A04 metadata/config proxies** — [monitoringMetaService.ts](../../apps/web/src/features/monitoring/services/monitoringMetaService.ts); [proxy/service.go](../../apps/manager-server/internal/service/proxy/service.go) | Upstream auth-file/channel/config HTTP, preceded by A01 setup/auth point reads as needed. Payload is not queried from usage events. Cached availability is shared; page refresh differs from shell Refresh All. |

### 7.3 Maintenance families supporting the read contract

These are dependencies/background or explicit lifecycle operations, **not statements to add to every page request**. Write counts vary with batches and branch outcomes; source-derived limits on enumerated rows do not bound every proof/operator.

| ID / source | Normalized statement family and work | Integrity / index / maintenance consequence |
|---|---|---|
| **M01 permanent core catch-up** — CORE:115–287,440–613 | Admitted transaction: state update/lock, state+high-water, ordered ID batch LIMIT batch_limit, `INSERT SELECT GROUP BY` additive upsert, ledger insert/refresh, batch extrema, checkpoint advance | Default batch_limit=1,000; PK enumeration, core group digest, ledger identity keys. Watermark and additions commit atomically; replay identities excluded. |
| **M02 pricing catch-up/rebuild** — PRICE:155–438,597–752 | State/structure/target; bounded IDs; two banded aggregate writes (hourly and lifetime account); revision clearing then rebuild | Classification paid during maintenance. A→B→A clears old target revision before additive rebuild. Prove retained source before destructive transition; no missing-history fallback. |
| **M03 monitoring catch-up** — UM/repository.go:99–349,455–533; hourly.go:16–40 | Projection upsert; daily account/API aggregates; selectors/latest-header rank/upsert; identity evidence; optional hourly account/API/selectors | Hourly enabling maintains three hourly tables, not only heatmap. Statistics/metadata retained proofs can be A-scale even during idle/bounded turns. |
| **M04 metadata/accounting migration** — usageevent/backfill.go:28–89,160–232; datamigration/repository.go:685–834 | ID-paged JSON/substring candidate discovery; conditional metadata updates and projection/header refresh; publish new generations, batch-clear/reset affected aggregate states | LIMIT caps candidates returned, not JSON examined. Accounting changes must invalidate all affected core/pricing/daily/hourly measures. |
| **M05 structural price edits** — BOOK:223–269,466–475 | Read book; compare structure; conditional retained rebuild proof; model/tier writes | Rate-only edits use existing bands, structural edits can fail closed. Process-local price writer gate coordinates readers, not arbitrary external/multiprocess writes. |
| **M06 archive export/publication** — ARCH:876–1102,1240–1512 | Preview count/high-water/extrema/estimated bytes and hash validation; bounded raw ID export with anti-join and byte budget; exact segment ID/hash/time validation; ledger/ref/segment inserts | Wide payload transfer is intentional here, not analytics. PK/range, identity and segment/run keys; export limit does not erase proof work. |
| **M07 retained deletion handoff** — ARCH:1726–2017; usagearchive/bulk.go:12–125 | Validate all required generations/coverage; enumerate candidates; repair projection holes/time mismatches; prove batch handoff; delete raw, detach ledger, mark refs, update daily coverage/progress atomically | Bulk uses at most 200 identities and approximately 1 MiB statement budget with affected-count checks; portable path issues per-candidate statements. Deletion must not strand unrebuildable retained history. |
| **M08 generation cleanup** — mysql/maintenance.go:15–98; hourly_cleanup.go:25–113 | Minute maintenance deletes ≤1,000 rows/transaction. Hourly uses bounded digest `<current`/`>current` probes and chosen identities; older pricing/daily use `revision<>? LIMIT ?` | Full revision residuals retain collision cases. Delete count is bounded; examined candidates are not proven bounded by LIMIT. No speculative claim of a measured full scan. |

Worker scheduling uses 1,000-event batches, up to ten batches per turn, 30-second rescans, ingestion wakes, round-robin continuation, and idle-task tracking (`worker/usage_pricing_rollup.go:16–47,91–184`). Read aggregation tails generally have no fixed row cap; adding LIMIT would undercount. D16's evidence eligibility cap is a distinct, exact-fallback exception.

MySQL pool size ten, repeatable-read, and one admitted writer per connector before checkout are application policy (`mysql/db.go`, `mysql/driver.go`, `sqlwrite/sqlwrite.go`), not an inherent single-writer limitation of MySQL. Long proof/banding work can occupy the permit. Removing writer serialization requires its own race/correctness audit.

## 8. Index catalog

### 8.1 Translation and identity rules

DDL is the frozen baseline plus numbered migrations. Below, `U[...]` means the declared unique BINARY(32) SHA-256 key over the complete byte-length-framed identity, not prefix uniqueness. `L[...]` is the separate nonunique lookup key. Prefix numbers are **characters**, not fixed stored bytes. Unmarked parts are ascending. Digest and prefix lookups do not authorize dropping complete-value comparison.

The generator converts TEXT to LONGTEXT, full text/composite uniqueness to stored digests, and ordinary lookups to at most 16 parts with prefix `min(191,floor(700/parts))`. Expression keys become `CAST(expression AS CHAR(prefix))`. Ordinary partial-index predicates and index-level NOCASE behavior do not survive identically. Generated foreign-key digests can need additional InnoDB supporting trees. Sources: `GEN:237–345,414–433`, `DIA:418–610`.

### 8.2 Raw and price indexes

| Table / declared key | Exact key order or identity | Role and cost |
|---|---|---|
| usage_events PRIMARY | `id` | Clustered detail/ID-tail access. |
| idx_usage_events_timestamp | `timestamp_ms` | Implicit InnoDB PK suffix yields timestamp/ID order; time counts and keyset pages. |
| idx_usage_events_ts_latency_ttft_cover | `timestamp_ms,latency_ms,ttft_ms` | Narrow time/metric cover only when all residual dependencies are covered; not global latency order or a heatmap cover. |
| idx_usage_events_auth_index / endpoint / model / request_id | Respective `auth_index(191)`, `endpoint(191)`, `model(191)`, `request_id(191)` | Single-attribute prefix candidates; raw model differs from normalized analytics model. |
| idx_usage_events_header_error_kind / header_trace_id / header_quota_recover | `header_error_kind(191)`; `header_trace_id(191)`; `header_quota_recover_at_ms` | Shared diagnostic consumers; not a general multi-filter/time key. |
| idx_usage_events_latest_request_auth_file | `auth_file_snapshot(175),auth_index(175),timestamp_ms DESC,id DESC` | Latest credential requests; missing auth-index equality leaves a gap before time; binary/case-insensitive query mismatch matters. |
| idx_usage_events_latest_request_source | `source(175),auth_index(175),timestamp_ms DESC,id DESC` | Legacy latest-request identity; same prefix/case caveats. |
| uq_usage_events_b8b08a605b / lookup_b8b08a605b | `U[event_hash]`; `L[event_hash(191)]` | Complete identity uniqueness plus text lookup; one stored digest. Raw ingestion maintains **13 declared secondary trees** plus PK. |
| model_prices uq/lookup_1bd5e0b656 | `U[model]`; `L[model(191)]` | Three pricing existence joins already have model-leading lookup; not automatically a digest unique seek. |
| model_price_context_tiers uq/lookup_54362d4166 | `U[model,threshold_tokens]`; `L[model(191),threshold_tokens]`; FK digest of model | Model equality then threshold range supports correlated MAX candidates; prefix residuals remain. Two generated digests and FK-supporting access. |
| model_price_service_tiers uq/lookup_449ede9b49 | `U[model,mode,service_tier]`; `L[model(191),mode(191),service_tier(191)]`; FK model digest | Full catalog load; service pricing in Go, not another event-scale join. |

Sources: `DDL:224–243,570–652,1145–1185`. Keep **both timestamp indexes**: latency/TTFT intervene before the implicit ID in the cover, so it cannot replace timestamp-only page ordering. Conversely timestamp-only cannot cover both metric samples. Adding explicit `(timestamp_ms,id)` duplicates existing effective ordering; adding another model/threshold key is not a missing-index fix.

### 8.3 Derived, projection, and state indexes

For compactness, define exact ordered grains: `ACCOUNT19=(structure_revision,bucket_ms,account_snapshot,auth_label_snapshot,provider,auth_provider_snapshot,auth_account_id_snapshot,auth_index,source,source_hash,auth_file_snapshot,api_key_hash,executor_type,model,billing_model,pricing_model,service_tier,context_threshold_tokens,failed)`; `KEY19` moves api_key_hash directly after bucket_ms and otherwise keeps that order. `SELECTOR9=(bucket_ms,model,api_key_hash,provider,auth_file_snapshot,account_snapshot,auth_label_snapshot,auth_index,source_hash)`.

| Table / keys | Exact order/prefix/hash role | Access and maintenance |
|---|---|---|
| usage_hourly_aggregate_v1 | `U[bucket_ms,model,billing_model,service_tier,failed]`; lookup_73ff90a7f0 `(bucket_ms,model(140),billing_model(140),service_tier(140),failed)` | Time-leading stored H_core access, not model-leading selective access or covering measures; two trees + digest/group. DDL:676–702. |
| usage_pricing_hourly_rollups_v1 | idx_usage_pricing_hourly_bucket `(structure_revision(191),bucket_ms)`; `U[revision,bucket,model,billing,pricing,tier,threshold,failed]`; lookup_63d1defc84 uses that order with all text prefixes 87 | Three trees + digest/group; active/obsolete generations add storage. Narrow revision/time key is not automatically redundant with wide lookup. DDL:974–1004. |
| usage_monitoring_event_projection_v1 | PK `(event_id)`; timestamp `(timestamp_ms DESC,event_id DESC)`; model `(analytics_model(191),timestamp_ms DESC,event_id DESC)`; account-window `(account_key(191),timestamp_ms,event_id)` | Three secondary trees + retained event-scale rows. Wrapped model predicates are not the tested bare equality; multi-model IN does not imply global time order. DDL:811–854. |
| usage_monitoring_event_search_v1 | No index: view of projection event_id/search_text | No MySQL trigram write cost or trigram acceleration. DDL:1247. |
| account daily | idx_usage_monitoring_account_daily_bucket `(structure_revision(191),bucket_ms)`; credential window `(structure_revision(175),CAST(TRIM(auth_file_snapshot) AS CHAR(175)),CAST(TRIM(auth_index) AS CHAR(175)),bucket_ms)`; legacy substitutes source; `U[ACCOUNT19]`; lookup_59a0a8acf7 first 16 ACCOUNT19 parts, text prefixes 43 | Five explicit trees + digest; lookup omits tier/threshold/outcome. Actual bare TRIM predicates do not expression-match the CAST keys. DDL:719–763. |
| API-key daily | idx_usage_monitoring_api_key_daily_bucket `(structure_revision(191),bucket_ms)`; `U[KEY19]`; lookup_56791d9e86 first 16 KEY19 parts, text prefixes 43 | Three trees + digest; key after time range is not key-leading selective access. Codex inclusion differs from account daily. DDL:766–808. |
| selector daily | idx_usage_monitoring_selector_daily_bucket `(bucket_ms)`; selector_revision_bucket `(model_format_revision(191),bucket_ms)`; `U[SELECTOR9]`; lookup_6d4f2abfc1 SELECTOR9 order with text prefixes 77 | Four trees + digest; revision is not in daily unique identity, so lifecycle replacement matters. DDL:914–932. |
| account hourly | uq_hourly_identity `(_identity_hash)`; idx_hourly_revision_bucket `(_revision_hash,bucket_ms)`; idx_hourly_bucket `(bucket_ms)`; idx_hourly_api_key_bucket `(_revision_hash,_api_key_hash,bucket_ms)`; idx_hourly_credential_bucket `(_revision_hash,_credential_hash,bucket_ms)`; idx_hourly_legacy_bucket `(_revision_hash,_legacy_hash,bucket_ms)` | Identity hashes full ACCOUNT19; credential/legacy hash trimmed file-or-source plus auth-index. Five 32-byte stored digests = 160 logical bytes before overhead; six trees. HDDL:8–57. |
| API-key hourly | Identity U over KEY19; same identity, revision/bucket, bucket, revision/API-key/bucket key names as above | Three stored digests = 96 logical bytes; four trees. Explicit APIKeyHashes emits digest+full-value predicates; SearchAPIKeyHash does not. HDDL:60–105. |
| selector hourly | Identity U over `(model_format_revision,SELECTOR9)`; same four key names/order as API-key hourly | Three digests/four trees. Current stored selector reader emits revision/time, not API-key digest: no selective page use of API-key-leading key. Not authorization to remove it. HDDL:108–128. |
| core/pricing/monitoring state | `U[aggregate_name]` + lookup_25bab408e5 `(aggregate_name(191))`; `U[rollup_name]` + lookup_99c2dedcb3 or lookup_5e727d506c `(rollup_name(191))` | Small named rows; repeated query count matters more than another key. DDL:655–673,886–902,1007–1025. |

Hourly validation compares exact index definitions; ad hoc index changes can prevent startup. Migration 002 is immutable. Stable counter updates do not logically change identity keys, but inserts/deletes/rebuilds pay digest computation, index maintenance, redo, and buffer occupancy. H/D can approach event count; no storage-byte total or speedup follows from the schema alone.

### 8.4 Retention, auxiliary, and adjacent-consumer keys

| Table | Keys/order/prefixes | Role / cost |
|---|---|---|
| usage_archive_deleted_coverage_daily | PK `(utc_day)` | Full-day coverage aggregate; no secondary tree. DDL:350–356. |
| usage_archive_event_refs | run_deleted `(run_id(191),raw_deleted_at_ms,raw_event_id)`; timestamp_deleted `(timestamp_ms,raw_deleted_at_ms)`; `U[event_hash]` + lookup `(event_hash(191))`; `U[run_id,raw_event_id]` + lookup `(run_id(191),raw_event_id)`; FK digests for event hash and run/segment | Six declared keys + FK support; four stored digests. Time-leading cover serves edge count/extrema/exists, not a deleted-first global seek. DDL:1188–1208. |
| usage_event_identity_ledger | bucket `(bucket_ms)`; raw_event_id `(raw_event_id)`; `U[event_hash]` + lookup_23c5cd6c85 `(event_hash(191))` | Permanent replay/membership evidence; four trees+digest/event identity, retained beyond raw deletion. DDL:553–567. |
| usage_codex_legacy_identity_evidence_v1 | `U[structure_revision,physical_kind,physical_file,auth_index,provider,auth_provider_snapshot,auth_account_id_snapshot,auth_project_id_snapshot,account_snapshot]`; lookup_6105aa8b5e same order, text prefixes 77 | File/index uniqueness hashes use case folding; complete provenance/chronology evidence. DDL:440–455. |
| usage_archive_runs / segments | Runs: `(status(191),updated_at_ms)`, `U[id]`, lookup `(id(191))`. Segments: `(run_id(191),last_event_id)`, `U[run_id,sequence]`, lookup `(run_id(191),sequence)`, `U[file_name]`, lookup `(file_name(191))`, FK run digest | Export/deletion lifecycle access, not heatmap access. DDL:359–424. |
| api_key_aliases / settings | `U[api_key_hash]`, lookup_b6a02e3848 `(api_key_hash(191))`; `U[key]`, lookup_910a982be5 `(key(191))` | Small dictionary/point reads; no alias-order index. DDL:163–170,303–310. |
| usage_pricing_account_rollups_v1 | `(structure_revision(191),account_key(191))`; `U[revision,account_key,model,billing_model,pricing_model,service_tier,threshold]`; same-order lookup, text prefixes 100 | Lifetime account pricing, no arbitrary time grain. DDL:935–971. |
| usage_account_model_rollups | `(auth_index(191))`, `(last_seen_ms)`; `U[account_key,model,billing_model,service_tier]`; same-order lookup text prefixes 175 | Adjacent lifetime history, not current page source. DDL:313–347. |
| usage_dashboard_hourly_rollups | `U[bucket_ms,model,billing_model,service_tier]`; lookup same order, text prefixes 175 | Legacy dashboard representation, not permanent core/pricing replacement. DDL:459–486. |
| usage_monitoring_header_latest_v1 | `(timestamp_ms DESC,event_id DESC)`; `U[snapshot_key]`; lookup `(snapshot_key(191))` | Latest identity snapshots; row replacement/ranking maintenance. DDL:857–883. |
| usage_rollup_checkpoints / usage_rollup_rebuild_state | Each `U[name]` plus lookup `(name(191))` | Adjacent lifecycle state; do not remove because this page does not select it. DDL:1028–1048. |

No speculative index migration is justified by this inventory. Predicate alignment must preserve full normalization/OR/identity semantics. A future rare-failure `(failed,timestamp)` experiment is a separate workload decision, not a heatmap fix. A very wide heatmap cover adds write cost without eliminating dynamic price-band work.

## 9. SQLite lineage and portability

Commit hashes below identify local history and can be inspected with `git show <hash> -- <source-relative-path>`. They document algorithms, not an inference about author intent or an assertion that SQLite is inherently faster. Historical anchor `a67ed883` predates the MySQL port.

| Commit(s) | Evidence and design lesson |
|---|---|
| `5519abdf` | Initial dashboard hourly rollup; additive counters plus latency sum/sample count, ID checkpoint and atomic batch upsert. See historical aggregate/rollup repository and [July operations report](../../apps/docs/en/operations/performance-optimization-2026-07-10.md). Late old-timestamp events explain ID rather than timestamp coverage. |
| `26b8f166` | Active-tab Include shaping and independently scoped selectors; UI-M historical matrix. Avoid requesting hidden work before optimizing it. |
| `f40ad0c2`, `b7273340` | Compact summary removes tasks, rolling aggregation, active days, zero-token enumeration, and implicit summary percentile work; historical S summary branch. Compact is not approximate. |
| `27bea18a` | Project existing hourly rows to required model/day grain before transfer instead of loading all hourly dimensions. The operations report's allocation improvement concerns its fixture, not native MySQL. |
| `983cc0d3` | Credential query-time whole-hour SQL aggregation plus exact fringes and selected credential; UE historical timeline implementation. Reduces transfer without claiming stored-hourly input. |
| `ab915442`, `9199452f` | Permanent versioned hourly core, identity ledger, coverage, and analytics integration; CORE/HR. |
| `512f6249`, `3b1338bc`, `bcacdcee` | Per-request pricing bands, durable priced sufficient statistics, consistent snapshot reader with uncovered raw usage; PRICE/HR/STORE. |
| `4bbc76bc` | Distinguish requested/analytics/billing/pricing identity and rebuild dependent structures; MODEL/ID/projection/rollups. Model distinctions are correctness, not optional grouping overhead. |
| `ec2be8d2` | Narrow monitoring projection, daily statistics/selectors, keyset candidates before detail hydration, exact edges/tails, latest headers, trigram search; UM/PROJ and SQLite migrations. A nonmidnight rolling 24h interval may contain no complete UTC day. |
| `63c5ab74` | Batched account-window reads with stable identity and daily/projection/raw composition; UM/account_window_read.go. |
| `6d6c2a72`, `f68f8524`, `f26eb449` | Revision-aware models, resumable clearing, rebuild fallback correctness; UM/PRICE lifecycle code. |
| `6e0172ef` | Archived full hours stay on permanent rollups; retained evidence handles partial edges. |
| `2e10b1a8`, `cb741990`, `fe3c7569` | Recovery prefers intact raw; account predicates drive projection access; hourly retained joins include timestamp plus ID. See RECOVER. |
| `f08169855e15dce9f645245ee01a23c47806d5ef` | Adds managed latency-covering index registration while retaining timestamp index. See [covering-index study](usage-latency-covering-index.zh-CN.md), [recorded results](usage-latency-covering-index.results.json), and usageevent/latency_performance_test.go. |
| `bfc6b9ce`, `7266b5c3` | MySQL port boundary; subsequent native analytics/optional monitoring-hourly structures. See DIA/DDL/HDDL/GR/LAT. |
| `87d975a2` | Heatmap request isolation and native removal of date grain/unused reasoning+latency; event pricing and aggregate sorting remain. It is not a completed native-performance acceptance result. |

SQLite's embedded event iteration still pays SQL/driver/Go work, but not MySQL result-protocol/network transfer. Native aggregation can exchange `N` event rows for `G` groups; when `G≈N`, wider grouped rows need not save bytes. SQLite's full text keys became LONGTEXT prefixes/digests; deterministic model functions became expanded expressions; `NOT INDEXED` was stripped rather than translated into forced PRIMARY access. Planner pushdown, CTE reuse, and expression matching must be evaluated for actual compiled SQL.

The FTS5 external-content trigram index (`sqlite/migrate.go:2072–2079`) became the MySQL projection view, losing substring acceleration. Ordinary word FULLTEXT is not equivalent. MySQL AVG casting, integer DIV, JSON_TABLE, expression-cast indexes, and collation translation preserve selected compatibility behavior, not identical access paths or arbitrary Unicode case semantics.

The covering-index study demonstrates cheaper narrow sample access, remaining exact-percentile sorting, loss of covering benefit under extra dimension/status filters, and real build/write/WAL/storage costs. It distinguishes scan gains from smaller end-to-end gains. Its local synthetic measurements are neither MySQL estimates nor NFS measurements. Historical bucket P95 streamed and sorted in Go; current shared scoped ranking and native-off ordered-stream strategies differ, so old measurements do not validate current plans.

SQLite WAL/shared-memory/locking and file placement are deployment concerns separate from query algorithms. A database server can remove application-side direct SQLite-file access over network storage when deployed with appropriate storage; that does not remove repeated scans/pricing/grouping. This repository evidence supplies no quantified NFS comparison and no reason to attribute a particular operator's runtime to the database brand.

## 10. Recommended design priorities

These are **proposals**, not claims of implemented code. Start with demonstrably unnecessary work and bounded alternatives; do not require a further production-diagnostics cycle.

1. **Make dependency planning section-specific.** Pure selectors/previews do not consume prices, yet baseline planning acquires the price gate and reads all three tables. Respect selector precedence, preserve pricing for legacy full options/mixed requests, and retain coverage. Give the page alias-only frontend loading so it neither fetches unused prices nor triggers accidental migration there.
2. **Remove repeated preparation within valid read contexts.** Move stats preparation behind stored-stats eligibility and selector metadata preparation behind stored-selector eligibility. Reuse price structure/book and compatible contributions within a consistent request snapshot. Do not substitute a cross-request cache keyed only by structural revision; rates/service rules are excluded from that revision.
3. **Remove the second priced timeline without changing anomalies.** Core/exact separation is useful, but exact+anomaly rebuilds additive data. A contract may combine existing core with exact metrics, or provide server-owned reuse. Preserve previous-existing-bucket semantics, exact P95, thresholds, missing values, score/tie order, top-50 cap, interval metadata, coverage, and generation fencing. Existing frontend average-latency anomaly helper is not equivalent.
4. **Specialize the heatmap's narrow filtered source.** Keep its current weekday/hour grain and already-early filters. Select only required identity/token/filter columns; use explicit base filters in remaining PRICE0 consumers where equivalent. Do not claim wildcard payloads currently spill, or that narrowing alone fixes a measured timeout.
5. **Compare two bounded pricing alternatives offline.** (a) Narrow unpriced raw stream plus one immutable request book, cached model-tuple resolution, and existing joint-grain additive/cost fold: O(N) narrow network transfer and O(G) Go state, less SQL join/group work. (b) Narrow SQL aggregation with nonoverlapping tier intervals derived from Kc rows: `input>lower AND input<=next`, open last interval, explicit base -1. Joining every lower threshold is wrong; interval joins can also be slower than indexed scalar MAX.
6. **Align MySQL search/predicates with actual keys.** Compare direct time-scoped projected LIKE against the FTS-shaped view subquery. Remove redundant wrappers only where complete-value/NULL/case semantics remain equivalent. Explicitly resolve the raw/projected Codex search discrepancy before source substitution; do not silently rebrand it as equivalence.
7. **Reuse existing API-key hourly rows conditionally, before inventing storage.** That grain includes heatmap contributors/pricing and includes the account-excluded Codex population. A guarded reader could replace event pricing over N with H_api plus exact T+E. Require enabled/populated compatible state/full revision, supported filters, independent watermarks, lossless timezone mapping, and exact source coverage. If hourly is disabled, include initial catch-up and maintenance of all three tables in cost. H≈N remains a counterexample.
8. **Preserve raw-complete behavior first for a new hourly heatmap.** Stored rows retain archived usage while baseline heatmap is raw-only with warnings. An initial guard can restrict acceleration to transactionally established raw-complete ranges. Retained heatmap support is an explicit contract extension requiring complete contributor/filter evidence, compatible edges, and source-aware coverage; the existing retained-edge helper is insufficient.
9. **Make access choices explicit only after evidence.** An upper tail ID U clarifies membership but does not itself improve performance: repeatable-read already bounds visibility. Small tails may suit PK access; stale tails/narrow windows may suit timestamp access. Do not cap exact tail results, force SQLite access assumptions onto MySQL, add a giant cover, or delete apparently overlapping indexes speculatively.

The response exposes separate model/key/provider contributor totals, not their joint distribution. Separate marginal aggregation is a possible later experiment, but changing grain before cost/clamping is not algebraically safe for every admitted token vector. Likewise, removing heatmap SQL ordering requires explicit repository/output and floating tie-order decisions; credential first-provenance ordering must not be removed indiscriminately.

## 11. Immutable correctness invariants

- **Population:** half-open bounds; exact timezone/DST/fractional-offset membership; no pretending disconnected buckets are contiguous previews. Preserve current date-option semantics unless deliberately corrected and tested separately.
- **Identity/filtering:** authoritative requested-model normalization, billing versus rate-source distinction, full long-string identity, provider/account OR matching, credential fallback, search wildcard/hash OR behavior, and scope-specific selector semantics. Existing cross-path discrepancies must be documented rather than silently changed.
- **Band selection:** classify each event before summing; greatest threshold strictly below normalized input; equality stays lower; `-1` classified base differs from `0` legacy/unclassified. Tier overrides apply to base rates, not cumulatively or only to excess tokens.
- **Monetary behavior:** configured zero differs from absent; positive context tier precedes service-tier pricing; preserve explicit service rules, compatibility multipliers, supported built-in missing-price fallbacks, cache-rate fallbacks, and five legacy `I>272000` components.
- **Aggregation order:** normalize compatibility cache and long-context measures per event, then merge additive values at the existing final pricing/contributor grain **before** cost/clamping. Event-wise pricing then summation can differ: `max(sum(input)-sum(cache),0)` is not always `sum(max(input-cache,0))`.
- **Latency:** independent positive latency/TTFT populations and exact nearest rank; missing is not zero; never average P95s. Preserve family-specific average populations and sum/sample weighting. Retained projection lacks historical TTFT; do not fabricate it.
- **Composition:** independent watermarks, late old-timestamp IDs, disjoint spans/tails/edges, covered Codex R where required, replay ledger exclusions, and current complete revision identity. No truncating exact tails to meet a budget.
- **Retention:** raw authority when live identity exists; retained-only inclusion requires the appropriate proof; no duplicated archive references; repair/prove before deletion; fail closed on unreconstructable required history. Ordinary projection readiness, edge proof, recovery conservation, and full rebuild proof are different contracts.
- **Snapshot/errors:** preserve price consistency, same-snapshot multi-pass percentiles and candidate hydration, and 16-metric per-core-group recovery conservation. Unsupported/unavailable can fall back; attempted SQL/cancellation errors return. Preserve the deleted-history degraded-comparison exception.
- **Page contracts:** keyset timestamp+ID, preview count semantics, live-detail existence, stable contributor selection/ties, bounded request scheduling, independent optional failure handling with core prerequisite, and response-generation fencing. Separate POSTs are not one database snapshot.

## 12. Bounded offline acceptance and CI

### 12.1 What baseline CI does and does not prove

[pr-check.yml](../../.github/workflows/pr-check.yml) and [test-mysql.sh](../../bin/ci/test-mysql.sh) provision MySQL 8.4 and exercise ordinary tests, integration/recovery, and schema generation. Ordinary `go test` does not execute benchmarks. Default `store.New` fixtures leave native switches off; shared service tests on MySQL therefore do not prove all native/hourly-enabled service routes execute.

Direct native compiled-query differential tests, explicitly enabled hourly fixtures, percentile routing/cancellation fake-driver tests, pricing/retention tests, and SQLite plan tests provide valuable coverage. Small fixtures/shared production helpers do not establish independent semantic truth or moderate-data native performance. No baseline native EXPLAIN/work-counter/materialization regression gate was identified.

The baseline heatmap benchmark adds 7-/30-day windows, tiers, many keys/providers/models, complete catch-up/warmup, and the normal deadline. It lacks a 24-hour heatmap, realistic persisted payloads, filter/retention/revision/late-in-window matrices, and independent cell/contributor pricing assertions; native reads must be explicitly enabled. The rolling benchmark's exact profile omits anomalies, core omits comparison, and browser pair is only core+selectors. Those are not the complete page graph.

### 12.2 Acceptance matrix, with bounded synthetic data

Do broad semantics on small fixtures, then selected moderate sentinels rather than a full Cartesian product. All sizes below are **proposed fixture budgets**, not measured thresholds or production cardinalities.

| Tier | Proposed bounded fixture / purpose |
|---|---|
| Semantic | Roughly 512–2,048 hand-checked/seeded events; all routes, boundaries, identities, generations, and failure contracts. |
| Moderate sentinel | 16,384 events in a nonmidnight 24h interval; redistribute the same N over 7d/30d to separate event count from range length. |
| Fixed-density history | 60,000 events over 60 days, supporting main and previous 30-day windows; record actual Nwindow/N/T/R/H/G. |
| Extended opt-in | Start at 100,000 events, larger only with explicit time/disk caps; many-generation, concurrency, index build/write/storage comparisons. |

Use frozen bounds/seed/checksum, independently distributed dimensions, compressible and high-cardinality controls, skew/long tails, out-of-window history, tied timestamps, and permuted ingestion order. Try empty, approximately 1 KiB, approximately 4 KiB, and bounded heavy-tail synthetic payloads; record actual persisted OCTET_LENGTH after normalization/redaction. Price metadata width is a separate variable.

Required cases:

- Actual six-tab Include profiles, selectors, comparison, exact+anomalies, selected key/credential, top-key batching, preview, date heatmap, and legacy combined behavior; assert frontend serialization matches backend fixtures.
- MySQL native off/on × monitoring-hourly off/on, independent permanent-hourly option, default versus configured stores, progressive versus combined requests; assert **route taken**, not just flags.
- UTC and fixed/fractional offsets; New York spring/fall, Lord Howe fractional transitions, Kathmandu, partial-hour/day bounds, from−1/from/to−1/to, disconnected membership, oversized calendar fallback.
- Every supported filter and representative intersections; no/one/selective/broad matches; long common prefixes, case/multibyte values, empty identities, provider disagreement, Codex provenance, huge JSON lists, short/common/rare/wildcard/separator searches and hash OR.
- Price-model fallback order; missing configured book with built-in fallback; explicit zero; multiple tiers at threshold−1/threshold/threshold+1; noncumulative overrides; service/context precedence; normalized cache; legacy long-context boundary; grouped clamp counterexamples.
- Ready, unavailable, pending, independently lagging, clearing, failed, schema/revision mismatch, obsolete generations, A→B→A, late old-hour IDs, restart/replay/checkpoint rollback, rate-only versus structural edits.
- Retained low/high/all raw deletion, restored live identity, duplicate archive evidence, missing retained row, timestamp mismatch, ID collision; exercise edge, recovery, rebuild, and raw-only coverage separately.

### 12.3 Oracles, work capture, and release gates

Use three checks: independent in-memory oracle, existing reference, candidate. The oracle must not call the SQL band classifier, bucket planner, or merge implementation being tested. Retain hand-authored pricing/identity boundary expectations. Compare every cell/bucket/contributor, integer measure, exact percentile, selector set/scope, cursor, coverage/error, and representative provenance; use declared tolerance only for appropriate floating results, not missing rows or wrong bands.

Capture actual **compiled** SQL/bind types at the MySQL driver seam, normalized family fingerprint, statement/transaction counts, chosen source/generation/watermarks, transferred rows/bytes, full row-consumption time, allocations, and end-to-end time-to-core/time-to-all. On disposable fixtures, collect representative EXPLAIN FORMAT=JSON and separately timed bounded EXPLAIN ANALYZE; record engine/session settings. Optional server rows-examined/sent, sort/temp counters and loops are useful offline, not a prerequisite for production instrumentation. Missing instruments are “unavailable,” never zero.

Structural gates should check bounded intended time/generation/ID sources before expensive work, no multiplicative joins, required narrow projections, correct groups/winners, and source/statement budgets. Do not ban all sorts/temp tables, force one index for every distribution, or reject scans of tiny price books. Distinguish estimates from actual counters and global concurrency deltas from per-statement attribution.

Run sequential cases, actual page-section overlap, two viewers plus ingestion, overload, cancellation while queued/executing/consuming, concurrent price edits, late insertion around snapshot boundaries, and forced query failure followed by cheap read recovery. Use barriers rather than sleeps; verify rows/transactions/connections/permits release. Separate POSTs may legitimately observe different generations during mutation.

A focused native stage can reuse existing MySQL CI with package concurrency one, e.g. `GOMAXPROCS=2`, one medium fixture at a time, explicit row/payload/disk/setup/request/job caps, synthetic-only diagnostics, failure collection, and cleanup. Record exact server image/version; preserve isolated disposable test databases. Required native acceptance must not silently skip when its database is absent.

Release criteria are semantic parity, actual intended native route, no silent loss of retained history, moderate 24h/7d/30d completion within the **unchanged 20-second service deadline**, no unintended all-history/payload-width-sensitive work, expected statement/transfer grain, cancellation recovery, and justified maintenance/storage costs for any new structure. Calibrate stricter timing budgets on a fixed runner; do not report p95/p99 from a few benchmark means. No production diagnostic cycle is required before implementing and evaluating these bounded changes.

## 13. Implementation deltas

### 13.1 SQL redesign developed on 2026-11-13

These changes were developed on `perf/usage-analytics-mysql-redesign` and prepared for merge on 2026-11-14. Sections 1–12 remain the **87d975a2 baseline inventory**. The entries below describe actual source changes and pre-merge validation, not a measured production speedup or confirmation of image publication. Release status must be established from the corresponding CI run and image receipt.

| Families | Implemented difference | Preserved contract |
|---|---|---|
| Priced raw R02–R06, R08, R13–R16 and shared R03/R26/R29/R30 | Explicit allowlisted field sets, source filters before pricing, catalog-only context intervals, guarded model fallback joins. No production event-dependent tier `MAX` remains in these builders. | Full model equality and fallback precedence, exact per-event bands, existing grouped sufficient statistics and monetary calculation, ordering/provenance. |
| Limited-model R03/R29 | Choose model counts from narrow filtered raw data before pricing/token sums for selected models. Two bounded source scans in one statement, without reuse of materialized event-grain pricing. | Excluded-model token overflow cannot fail a limited result; returned-group overflow remains an error. Top-model default limit and ties unchanged. Limited input totals now correctly use normalized input. |
| Credential R08 | MySQL multipart whole-hour/fringe reads use one read-only repeatable-read transaction. Filter bindings precede calendar-map bindings. | Exact population, first-snapshot metadata, fractional/DST mappings, terminal query/scan/cancel/commit errors; no retry against another source after failure. |
| D04, D06–D08, D12–D14 | Shared catalog-interval classification and narrow fields; priced read source arms aggregate before union and merge at their existing final grain. | Independent source watermarks, archive/live identity preference, retained-proof distinctions, weighted latency, full pricing bands, Codex residuals, and grouped monetary clamping. |
| D06–D09 preparation | Price-structure/statistics-state reads occur only when stored statistics are eligible; selector metadata state is deferred until stored-selector eligibility. | Projection-state and high-water reads stay in the transaction; unsupported sources and attempted-query errors remain distinct. |
| Projected filters/search | Bare normalized model equality/list predicates; MySQL applies projected `search_text LIKE` directly in the time-scoped source instead of the SQLite FTS-shaped view subquery. | SQLite eligible FTS5 path unchanged; wildcard/control fallback, case behavior, search/hash OR, provider/account alternatives, hidden-marker policy, cursor and raw-existence checks retained. |
| R35/D05 books | `LoadRates[Tx]` reads ten base rate fields rather than management metadata, plus complete context/service tiers. `LoadAnalyticsModelPrices` uses it. An unavailable core snapshot returns before unnecessary pricing/book reads, after required edge validation. | Three catalog SELECTs remain; no cross-request cache, no rate/structure conflation, available snapshot/recovery conservation and transaction boundaries retained. Management `LoadAll` still includes metadata. |
| Section dependencies | Distinguish book consumption from price-snapshot gating. Pure selectors, previews, explicit percentiles, distribution/failure/task sections avoid both. Full event pages retain the gate but skip an unused book. | Selector precedence and mixed priced/legacy requests remain correct; raw coverage checks are not removed. |
| A01/page bootstrap | Analytics uses the shared alias-only hook, rather than the generic usage/price loader. | Auth/base/availability resets, stale-response fencing and alias notifications retained. Analytics mounting no longer GETs/PUTs/migrates browser model prices; other management consumers retain that behavior. |
| Progressive exact/anomalies | Capability 2 core supplies a versioned anomaly basis; exact POST requests percentiles only. Client completes the existing anomaly calculation from core additive changes and later exact latency values. | Capability 0/1 behavior remains; strict thresholds, ordering, severity, top-50 cap and bucket metadata are tested. Core/exact are explicitly different snapshots with separate coverage, not an atomic combined result. |

Primary new implementation files:

- [Shared context intervals](../../apps/manager-server/internal/usagepricingsql/context.go)
- [Raw pricing projections/builders](../../apps/manager-server/internal/repository/usageevent/pricing_sql.go)
- [Monitoring pricing/source-arm aggregation](../../apps/manager-server/internal/repository/usagemonitoring/pricing_sql.go)
- [Structured retained source arms](../../apps/manager-server/internal/repository/usageprojection/source_arms.go)
- [Server anomaly basis](../../apps/manager-server/internal/service/monitoring/anomalies.go)
- [Client anomaly completion](../../apps/web/src/features/usage-analytics/usageAnalyticsAnomalies.ts)
- [Alias-only loading](../../apps/web/src/features/monitoring/hooks/useApiKeyAliases.ts)

### 13.2 Selected pricing transformation and work bounds

The replacement relation contains only configured context tiers:

```sql
pricing_context_intervals AS (
  SELECT model, threshold_tokens,
         LEAD(threshold_tokens) OVER (
           PARTITION BY model ORDER BY threshold_tokens
         ) AS next_threshold_tokens
  FROM model_price_context_tiers
)
```

Each filtered event left-joins at most one interval by full model identity and:

```sql
normalized_input > threshold_tokens
AND (normalized_input <= next_threshold_tokens
     OR next_threshold_tokens IS NULL)
```

No match is band `-1`. For thresholds 100/200, inputs 100/101/200/201 map to `-1/100/100/200`. Classification happens before token aggregation; no `threshold+1` arithmetic or multiplication of event rows across every lower tier.

The windowed relation is `Kc` narrow catalog rows rather than `N` event rows. Removing the select-list correlated subquery makes the ordinary single-use event chain structurally mergeable, but **MySQL's actual merge/materialization strategy still requires the authored native plan test**. Interval matching is not asserted to be constant-time. Final grouping/sorting and `G≈N` cases remain; native output is still O(G), compatibility transfer O(N). No native elapsed-time improvement is claimed from SQL structure alone.

The model/book data can be tiny while events and contributor groups grow; resolving static tier boundaries once per SQL statement is the intended reduction. Existing pricing/identity/index semantics remain preferable to a second application pricing engine or a new persistent cube before measurements justify either.

### 13.3 Capability 2 anomaly semantics

New clients request `anomaly_basis_version:1` with a core timeline. The server acknowledges version/semantics and emits a response-unique `core_id` plus every adjacent-existing-bucket candidate, including candidates that can become anomalous only through latency. The exact request contains only percentile scopes and therefore no longer rebuilds a priced timeline.

The client fences completion by full request generation and core ID, rejects missing acknowledgment/required exact fields, and combines the server's five additive changes with exact latency change using the existing thresholds and stable ranking. Empty candidates/timeline are explicit. A later exact snapshot can legitimately differ after ingestion/retention; the UI exposes core and exact coverage separately. Legacy server-owned anomalies remain available to old clients.

### 13.4 Explicit limits and unchanged design decisions

- **No schema, index, stored generation, or optimization-default changes.** Both raw timestamp indexes remain. Native grouped reads still follow the existing explicit option; bulk/hourly remain independently opt-in. SQL pricing improvements also affect compatibility/reference consumers.
- **No aggregate-backed heatmap is added.** The first fix reduces unnecessary request-time work over modest event populations. Existing hourly API-key storage remains a possible later optimization, not a prerequisite or a silently enabled source.
- **Retained maintenance remains an event-grain union exception.** `VerifiedRetainedEventSourceTx` is retained for monitoring/pricing catch-up with archived data, including ordinary catch-up after deletion and rebuilds. Its complete proof and multiset semantics are preserved. This is not on the usage-analytics page read path, but the separate account-history endpoint can invoke catch-up explicitly; it is not universally background-only. Read-arm aggregation does not imply every maintenance path is aggregate-first.
- Unpriced count/page/selector source unions retain their existing semantics. Exact P95 algorithms and retained-history safety checks are not approximated or removed.
- Structural revisions are not used as full-price cache keys. No cross-request price/result cache or automatic timeout retry is introduced.

### 13.5 Validation evidence and pending acceptance

Implemented regressions include an independent frozen scalar-MAX reference, hand-established tier and monetary cases, returned/excluded overflow, all five long-context measures, two-arm window bindings, retained/live precedence, terminal failures, and credential snapshot/calendar cases. The raw, derived, rate-loader/store, and analyticsbench offline package checks passed, with focused race tests and vet. The final read-only cross-component inspection found no confirmed reachable integration defect.

**Full integrated backend validation remains incomplete.** The final `go test -p 1 -count=1 -json ./...` and changed-package vet attempts were refused before workload execution by the configured runner's selected-Go executable identity check (`RCH selected tool unavailable`, executable SHA256 mismatch). Earlier attempts also encountered inherited `GOCACHE=off`. No full-suite test results, compilation pass, or native runtime skips were observed in that final attempt. Prior package passes are not a substitute for the full suite. Runner checks were not bypassed or reconfigured.

Frontend production changes passed a focused 13-file/212-test run and scoped ESLint; final-tree TypeScript checking passed. A later test-only stale-alias A→B→A regression was added. Its final execution and final changed-file lint/format reruns were initially blocked before execution by configured runner capacity; those refusals are **not passes**. Four unchanged files have pre-existing broad-directory formatting warnings.

Native acceptance is authored but **not executed** without a disposable DSN:

- `TestPricingNativeIntervalPlansAndExecution`: actual compiled pricing SQL, independent old/new execution, persisted payload width, prepared statements, EXPLAIN JSON/ANALYZE and same-connection work counters through the fixture-only helper.
- Credential native tests: explicit native/reference calendar paths, repeatable-read mutation, grouped edges and terminal errors.
- `TestMySQLAnalyticsPageSentinel`: 16,384-event fixture, unchanged service deadline, payload/cardinality controls, exact cell/contributor/pricing and percentile oracles. Low-cardinality 24h G=350 versus high-cardinality G=7,168 are **fixture enumeration counts, not native timing measurements**; equal-N duration comparisons share the same dimension profile.
- `TestAnalyticsPageFrozenHongKong24Hours`: frozen 24h request shape with half-open boundary evidence.
- `BenchmarkMySQLAnalyticsExtendedPageMatrix`: optional extended synthetic cases, not part of ordinary benchmark-free `go test` execution.

The existing disposable-MySQL CI suite already executes the new ordinary native tests when its DSN is configured. Offline skips must not be mistaken for native acceptance. No production tests, rollout, settings changes, or manual schema edits were performed during development. The user authorized merge/push on 2026-11-14; subsequent CI results are separate evidence from the pre-merge validation above.

When extending this entry, keep production payloads, credentials, personal identities, private machine paths, and real request identifiers out of the catalog and synthetic evidence.
