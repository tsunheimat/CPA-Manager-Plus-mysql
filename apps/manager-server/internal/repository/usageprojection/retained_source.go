package usageprojection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

// retainedRebuildColumns contains the complete pricing and monitoring evidence,
// including the unmodified Codex provenance marker. latency_ms is intentionally
// nullable; every other retained column must be present (empty text is valid).
var retainedRebuildColumns = []string{
	"timestamp_ms", "model", "requested_model", "resolved_model", "service_tier", "failed",
	"input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens", "cache_tokens",
	"cache_read_tokens", "cache_creation_tokens", "normalized_total_input_tokens", "total_tokens", "latency_ms",
	"provider", "auth_index", "source", "source_hash", "account_snapshot", "auth_label_snapshot",
	"auth_file_snapshot", "auth_provider_snapshot", "auth_project_id_snapshot", "auth_account_id_snapshot",
	"api_key_hash", "executor_type",
}

// VerifiedRetainedEventSourceTx returns a SQL FROM expression and the maximum
// original event ID for a duplicate-free live-plus-retained rebuild. Live rows
// are authoritative. Projection rows contribute only with matching deletion
// evidence and compatible coverage; orphan projections are never used.
//
// The source and its proof are valid only in the caller's transaction. Call on
// every batch, including for already-covered IDs: there is no durable mutation
// fence that would make a cached proof safe. A retained hole returns
// ErrRetainedCoverageIncomplete, never a raw-only fallback. This source retains
// pricing/monitoring additive fields, not historical TTFT or raw request bodies.
func VerifiedRetainedEventSourceTx(ctx context.Context, tx *sql.Tx) (string, int64, error) {
	var hasDeletedRaw bool
	if err := tx.QueryRowContext(ctx, `select exists (
		select 1 from usage_archive_event_refs where raw_deleted_at_ms is not null
	)`).Scan(&hasDeletedRaw); err != nil {
		return "", 0, err
	}
	if !hasDeletedRaw {
		var latestID int64
		err := tx.QueryRowContext(ctx, `select coalesce(max(id), 0) from usage_events`).Scan(&latestID)
		return "usage_events", latestID, err
	}

	var schemaVersion int
	var revision, status string
	var coverageID int64
	if err := tx.QueryRowContext(ctx, `select schema_version, structure_revision, status, coverage_event_id
		from usage_monitoring_rollup_state where rollup_name = 'projection_v1'`).Scan(
		&schemaVersion, &revision, &status, &coverageID,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", 0, fmt.Errorf("%w: retained usage projection state is unavailable", ErrRetainedCoverageIncomplete)
		}
		return "", 0, err
	}
	if schemaVersion != 1 || revision != usageidentity.MonitoringProjectionStructureRevision() || status == "clearing" {
		return "", 0, fmt.Errorf("%w: retained usage projection is incompatible or rebuilding", ErrRetainedCoverageIncomplete)
	}

	incompleteFields := []string{
		"archived.raw_event_id <= 0", "archived.raw_event_id > ?", "p.event_id is null",
		"archived.timestamp_ms <> p.timestamp_ms",
		// An original ID cannot name a different live event. Do not merge two
		// identities at the same checkpoint position after a damaged import.
		"exists (select 1 from usage_events collision where collision.id = archived.raw_event_id)",
	}
	for _, column := range retainedRebuildColumns {
		if column != "latency_ms" {
			incompleteFields = append(incompleteFields, "p."+column+" is null")
		}
	}
	var incomplete bool
	if err := tx.QueryRowContext(ctx, `select exists (
		select 1 from usage_archive_event_refs archived
		left join `+EventTable+` p on p.event_id = archived.raw_event_id
		where archived.raw_deleted_at_ms is not null
			and not exists (select 1 from usage_events live where live.event_hash = archived.event_hash)
			and (`+strings.Join(incompleteFields, " or ")+`)
	)`, coverageID).Scan(&incomplete); err != nil {
		return "", 0, err
	}
	if incomplete {
		return "", 0, fmt.Errorf("%w: retained usage projection is missing or inconsistent for a deleted event", ErrRetainedCoverageIncomplete)
	}

	// EXISTS, rather than a reference join, ensures one row per original ID
	// even if multiple archive references describe the same retained event.
	source := fmt.Sprintf(`(select e.id, e.%s from usage_events e
		union all
		select p.event_id as id, p.%s from %s p
		where exists (
			select 1 from usage_archive_event_refs archived
			where archived.raw_event_id = p.event_id
				and archived.timestamp_ms = p.timestamp_ms
				and archived.raw_deleted_at_ms is not null
				and not exists (select 1 from usage_events live where live.event_hash = archived.event_hash)
		))`, strings.Join(retainedRebuildColumns, ", e."), strings.Join(retainedRebuildColumns, ", p."), EventTable)
	// Do not take MAX over the UNION relation: SQLite/MySQL can materialize
	// every live and projected event even on an idle catch-up. The proof above
	// already established that every absent archived identity has its original
	// projected ID. Read the live indexed high-water and retained refs directly.
	var liveID, retainedID int64
	if err := tx.QueryRowContext(ctx, `select coalesce(max(id), 0) from usage_events`).Scan(&liveID); err != nil {
		return "", 0, err
	}
	if err := tx.QueryRowContext(ctx, `select coalesce(max(archived.raw_event_id), 0)
		from usage_archive_event_refs archived where archived.raw_deleted_at_ms is not null
		and not exists (select 1 from usage_events live where live.event_hash = archived.event_hash)`).Scan(&retainedID); err != nil {
		return "", 0, err
	}
	return source, max(liveID, retainedID), nil
}
