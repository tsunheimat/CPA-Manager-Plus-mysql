package usagepricing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

var retainedPricingColumns = []string{
	"timestamp_ms", "model", "requested_model", "resolved_model", "service_tier", "failed",
	"input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens", "cache_tokens",
	"cache_read_tokens", "cache_creation_tokens", "normalized_total_input_tokens", "total_tokens",
	"provider", "auth_index", "source", "source_hash", "account_snapshot", "auth_label_snapshot",
	"auth_file_snapshot", "auth_provider_snapshot", "auth_project_id_snapshot", "auth_account_id_snapshot",
}

var ErrRetainedPricingHistoryIncomplete = errors.New("retained pricing history is incomplete")

// LoadHourlyRowsFromEventsTx bypasses a deficient pricing cache without writing
// to it. Intact raw events are authoritative; the compatible retained projection
// contributes only archive-deleted events up to its watermark.
// The caller must compare the result with the permanent core snapshot before
// treating it as complete. A projection watermark alone is not proof of data.
func (r *repository) LoadHourlyRowsFromEventsTx(ctx context.Context, tx *sql.Tx, filter HourlyFilter) ([]HourlyRow, error) {
	arms, err := retainedEventSourceArmsTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	query, args := hourlyStatementFromArms(filter, filter.FromMS, filter.ToMS, 0, false, arms)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grouped := map[hourlyKey]*HourlyRow{}
	if err := scanAndMergeHourlyRows(rows, grouped); err != nil {
		return nil, err
	}
	return sortedHourlyRows(grouped), nil
}

// LoadAccountRowsFromEventsTx uses the same retained event source and exact
// pricing bands as hourly recovery. The retained projection arm is scoped by
// account_key before entering the pricing CTE so degraded account-history reads
// can use the projection's account index instead of scanning retained history.
// The caller must verify core coverage.
func (r *repository) LoadAccountRowsFromEventsTx(ctx context.Context, tx *sql.Tx, accountKeys []string) ([]AccountRow, error) {
	keys := normalizeValues(accountKeys)
	if len(keys) == 0 {
		return []AccountRow{}, nil
	}
	arms, err := retainedAccountEventSourceArmsTx(ctx, tx, keys)
	if err != nil {
		return nil, err
	}
	grouped := map[accountKey]*AccountRow{}
	if err := mergeAccountRowsFromArms(ctx, tx, 0, keys, grouped, arms); err != nil {
		return nil, err
	}
	return sortedAccountRows(grouped), nil
}

// VerifyRetainedPricingRebuildSourceTx proves that every raw event removed by
// the supported archive flow is still available from the compatible monitoring
// projection. Structural model-price mutations call this before commit so an
// unrecoverable pricing revision can never become active.
func VerifyRetainedPricingRebuildSourceTx(ctx context.Context, tx *sql.Tx) error {
	_, _, err := retainedPricingRebuildSourceTx(ctx, tx)
	return err
}

// retainedPricingRebuildSourceTx preserves the public pricing error contract
// while sharing the same proof/source as monitoring rebuilds.
func retainedPricingRebuildSourceTx(ctx context.Context, tx *sql.Tx) (string, int64, error) {
	source, targetID, err := usageprojection.VerifiedRetainedEventSourceTx(ctx, tx)
	if errors.Is(err, usageprojection.ErrRetainedCoverageIncomplete) {
		return "", 0, fmt.Errorf("%w: %w", ErrRetainedPricingHistoryIncomplete, err)
	}
	return source, targetID, err
}

// retainedEventSourceTx prefers the original raw event whenever it still exists.
// Only genuinely deleted archive records may use a compatible retained projection.
// This avoids losing an intact raw event when its projection is missing or stale.
func retainedEventSourceTx(ctx context.Context, tx *sql.Tx) (string, error) {
	arms, err := retainedEventSourceArmsTx(ctx, tx)
	if err != nil {
		return "", err
	}
	source, _ := usageprojection.UnionEventSources(arms)
	return source, nil
}

func retainedEventSourceArmsTx(ctx context.Context, tx *sql.Tx) ([]usageprojection.EventSourceArm, error) {
	coverageID, compatible, err := retainedProjectionCoverageTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !compatible || coverageID <= 0 {
		return []usageprojection.EventSourceArm{{From: "usage_events"}}, nil
	}
	// Hourly recovery has no account/provenance dependency. Keep the archive
	// join (including its multiplicity) and live-hash preference unchanged.
	columns := []string{"timestamp_ms", "model", "requested_model", "resolved_model", "service_tier", "failed",
		"input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens", "cache_tokens", "cache_read_tokens",
		"cache_creation_tokens", "normalized_total_input_tokens", "total_tokens", "latency_ms"}
	return []usageprojection.EventSourceArm{
		{From: fmt.Sprintf(`(select e.id, e.%s from usage_events e)`, strings.Join(columns, ", e."))},
		{From: fmt.Sprintf(`(select p.event_id as id, p.%s
			from usage_archive_event_refs archived
			join %s p on p.event_id = archived.raw_event_id and archived.timestamp_ms = p.timestamp_ms
			where archived.raw_deleted_at_ms is not null and p.event_id <= %d
				and not exists (select 1 from usage_events live where live.event_hash = archived.event_hash))`,
			strings.Join(columns, ", p."), usageprojection.EventTable, coverageID)},
	}, nil
}

func retainedAccountEventSourceTx(ctx context.Context, tx *sql.Tx, accountKeys []string) (string, []any, error) {
	arms, err := retainedAccountEventSourceArmsTx(ctx, tx, accountKeys)
	if err != nil {
		return "", nil, err
	}
	source, args := usageprojection.UnionEventSources(arms)
	return source, args, nil
}

func retainedAccountEventSourceArmsTx(ctx context.Context, tx *sql.Tx, accountKeys []string) ([]usageprojection.EventSourceArm, error) {
	coverageID, compatible, err := retainedProjectionCoverageTx(ctx, tx)
	if err != nil {
		return nil, err
	}
	if !compatible || coverageID <= 0 {
		return []usageprojection.EventSourceArm{{From: "usage_events"}}, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(accountKeys)), ",")
	args := make([]any, 0, len(accountKeys))
	for _, key := range accountKeys {
		args = append(args, key)
	}
	// Drive archived candidates from the account index. The archive timestamp
	// index verifies actual deletion without scanning every archived credential.
	return []usageprojection.EventSourceArm{
		{From: fmt.Sprintf(`(select e.id, e.%s from usage_events e)`, strings.Join(retainedPricingColumns, ", e."))},
		{From: fmt.Sprintf(`(select p.event_id as id, p.%s
			from %s p
			where p.account_key in (%s) and p.event_id <= %d
				and exists (
					select 1 from usage_archive_event_refs archived
					where archived.timestamp_ms = p.timestamp_ms
						and archived.raw_event_id = p.event_id
						and archived.raw_deleted_at_ms is not null
						and not exists (select 1 from usage_events live where live.event_hash = archived.event_hash)
				))`, strings.Join(retainedPricingColumns, ", p."), usageprojection.EventTable, placeholders, coverageID), Args: args},
	}, nil
}

func retainedProjectionCoverageTx(ctx context.Context, tx *sql.Tx) (int64, bool, error) {
	var schemaVersion int
	var revision, status string
	var coverageID int64
	err := tx.QueryRowContext(ctx, `select schema_version, structure_revision, status, coverage_event_id
		from usage_monitoring_rollup_state where rollup_name = 'projection_v1'`).Scan(
		&schemaVersion, &revision, &status, &coverageID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	compatible := schemaVersion == 1 &&
		revision == usageidentity.MonitoringProjectionStructureRevision() &&
		status != "clearing"
	return coverageID, compatible, nil
}
