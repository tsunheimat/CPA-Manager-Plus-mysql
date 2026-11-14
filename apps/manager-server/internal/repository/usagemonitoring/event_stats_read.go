package usagemonitoring

import (
	"context"
	"database/sql"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func (r *repository) LoadAggregate(ctx context.Context, filter AnalyticsFilter) (Aggregate, State, bool, error) {
	if !SupportsEventProjectionFilter(filter) {
		return Aggregate{}, State{}, false, nil
	}
	tx, err := sqlwrite.BeginTx(ctx, r.db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Aggregate{}, State{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	state, available, projectionComplete, err := projectionReadState(ctx, tx)
	if err != nil || !available {
		return Aggregate{}, state, available, err
	}
	var aggregate Aggregate
	if SupportsStatsFilter(filter) {
		statsState, revision, dailyAvailable, stateErr := statsReadState(ctx, tx)
		if stateErr != nil {
			return Aggregate{}, state, false, stateErr
		}
		aggregate, err = r.loadComposedAggregate(ctx, tx, statsState, dailyAvailable, state.CoverageEventID, projectionComplete, revision, filter)
	} else {
		aggregate, err = loadFilteredAggregate(ctx, tx, state.CoverageEventID, filter,
			eventSourceOptions{ProjectionComplete: projectionComplete, DirectProjectedSearch: mysql.IsMySQL(r.db)})
	}
	if err != nil {
		return Aggregate{}, state, false, err
	}
	if err := tx.Commit(); err != nil {
		return Aggregate{}, state, false, err
	}
	return aggregate, state, true, nil
}

// Keep this reader's original AVG population/arithmetic rather than converting
// it to an integer latency SUM merely to share the composed-reader scanner.
func loadFilteredAggregate(ctx context.Context, tx *sql.Tx, coverageID int64, filter AnalyticsFilter, options eventSourceOptions) (Aggregate, error) {
	source, args := filteredEventSourceSQL(filter, coverageID,
		`p.failed, p.normalized_total_input_tokens, p.output_tokens, p.reasoning_tokens, p.cached_tokens, p.cache_tokens,
		p.cache_read_tokens, p.cache_creation_tokens, p.total_tokens, p.latency_ms`,
		`e.failed, coalesce(e.normalized_total_input_tokens, e.input_tokens, 0), coalesce(e.output_tokens, 0),
		coalesce(e.reasoning_tokens, 0), coalesce(e.cached_tokens, 0), coalesce(e.cache_tokens, 0),
		coalesce(e.cache_read_tokens, 0), coalesce(e.cache_creation_tokens, 0), coalesce(e.total_tokens, 0), e.latency_ms`, options)
	var aggregate Aggregate
	err := tx.QueryRowContext(ctx, `with filtered_events as (`+source+`)
	select count(*), coalesce(sum(case when failed = 0 then 1 else 0 end), 0),
		coalesce(sum(case when failed = 1 then 1 else 0 end), 0),
		coalesce(sum(normalized_total_input_tokens), 0), coalesce(sum(output_tokens), 0), coalesce(sum(reasoning_tokens), 0),
		coalesce(sum(max(max(cached_tokens, cache_tokens) - max(cache_read_tokens, 0) - max(cache_creation_tokens, 0), 0)), 0),
		coalesce(sum(cache_read_tokens), 0), coalesce(sum(cache_creation_tokens), 0), coalesce(sum(total_tokens), 0),
		avg(nullif(latency_ms, 0)), coalesce(sum(case when total_tokens = 0 and failed = 0 then 1 else 0 end), 0)
	from filtered_events`, args...).Scan(&aggregate.TotalCalls, &aggregate.SuccessCalls, &aggregate.FailureCalls,
		&aggregate.InputTokens, &aggregate.OutputTokens, &aggregate.ReasoningTokens, &aggregate.CachedTokens,
		&aggregate.CacheReadTokens, &aggregate.CacheCreationTokens, &aggregate.TotalTokens, &aggregate.AvgLatencyMS, &aggregate.ZeroTokenCalls)
	return aggregate, err
}

func (r *repository) LoadModelStats(ctx context.Context, filter AnalyticsFilter) ([]ModelStat, State, bool, error) {
	if !SupportsEventProjectionFilter(filter) {
		return nil, State{}, false, nil
	}
	tx, err := sqlwrite.BeginTx(ctx, r.db, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, State{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	state, available, projectionComplete, err := projectionReadState(ctx, tx)
	if err != nil || !available {
		return nil, state, available, err
	}
	var stats []ModelStat
	if SupportsStatsFilter(filter) {
		statsState, revision, dailyAvailable, stateErr := statsReadState(ctx, tx)
		if stateErr != nil {
			return nil, state, false, stateErr
		}
		stats, err = r.loadComposedModelStats(ctx, tx, statsState, dailyAvailable, state.CoverageEventID, projectionComplete, revision, filter)
	} else {
		grouped := map[dailyModelStatKey]*ModelStat{}
		err = mergeProjectedModelStats(ctx, tx, state.CoverageEventID, projectionComplete, filter,
			eventSourceOptions{DirectProjectedSearch: mysql.IsMySQL(r.db)}, grouped)
		stats = sortedDailyModelStats(grouped)
	}
	if err != nil {
		return nil, state, false, err
	}
	if err := tx.Commit(); err != nil {
		return nil, state, false, err
	}
	return stats, state, true, nil
}

func projectionReadState(ctx context.Context, tx *sql.Tx) (State, bool, bool, error) {
	state, err := stateQuery(ctx, tx, ProjectionRollupName)
	if err != nil {
		return State{}, false, false, err
	}
	if state.SchemaVersion != SchemaVersion {
		return state, false, false, nil
	}
	latestID, err := latestEventID(ctx, tx)
	if err != nil {
		return State{}, false, false, err
	}
	return state, true, state.CoverageEventID >= latestID, nil
}
