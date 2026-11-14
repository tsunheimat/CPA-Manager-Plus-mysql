package usagemonitoring

import (
	"fmt"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usageidentity"
)

type eventSourceOptions struct {
	AfterID            int64
	UseAfter           bool
	MaxID              int64
	UseMax             bool
	CodexMarkerOnly    bool
	BeforeMS           int64
	BeforeID           int64
	ProjectionComplete bool
	RequireRawEvent    bool
	// MySQL's search relation is a plain view, not SQLite's trigram index.
	DirectProjectedSearch bool
}

// eventSourceArm keeps each source's predicates and bindings together. Priced
// readers aggregate arms independently; unpriced consumers retain the UNION API.
type eventSourceArm struct {
	SQL  string
	Args []any
}

func filteredEventSourceSQL(
	filter AnalyticsFilter,
	coverageEventID int64,
	projectionSelect string,
	rawSelect string,
	options eventSourceOptions,
) (string, []any) {
	arms := filteredEventSourceArms(filter, coverageEventID, projectionSelect, rawSelect, options)
	queries := make([]string, 0, len(arms))
	var args []any
	for _, arm := range arms {
		queries = append(queries, arm.SQL)
		args = append(args, arm.Args...)
	}
	return strings.Join(queries, " union all "), args
}

func filteredEventSourceArms(filter AnalyticsFilter, coverageEventID int64, projectionSelect, rawSelect string, options eventSourceOptions) []eventSourceArm {
	projectionConditions, projectionArgs := eventFilterConditionsWithSearch(filter, "p.", true, options.DirectProjectedSearch)
	rawConditions, rawArgs := eventFilterConditions(filter, "e.", false)
	if options.UseAfter {
		projectionConditions = append(projectionConditions, "p.event_id > ?")
		projectionArgs = append(projectionArgs, options.AfterID)
		rawConditions = append(rawConditions, "e.id > ?")
		rawArgs = append(rawArgs, options.AfterID)
	}
	if options.UseMax {
		projectionConditions = append(projectionConditions, "p.event_id <= ?")
		projectionArgs = append(projectionArgs, options.MaxID)
		rawConditions = append(rawConditions, "e.id <= ?")
		rawArgs = append(rawArgs, options.MaxID)
	}
	if options.CodexMarkerOnly {
		projectionConditions = append(projectionConditions, codexAccountDailyExcludedSQL("p"))
		rawConditions = append(rawConditions, codexAccountDailyExcludedSQL("e"))
	}
	if options.BeforeMS > 0 {
		if options.BeforeID > 0 {
			projectionConditions = append(projectionConditions, "(p.timestamp_ms < ? or (p.timestamp_ms = ? and p.event_id < ?))")
			projectionArgs = append(projectionArgs, options.BeforeMS, options.BeforeMS, options.BeforeID)
			rawConditions = append(rawConditions, "(e.timestamp_ms < ? or (e.timestamp_ms = ? and e.id < ?))")
			rawArgs = append(rawArgs, options.BeforeMS, options.BeforeMS, options.BeforeID)
		} else {
			projectionConditions = append(projectionConditions, "p.timestamp_ms < ?")
			projectionArgs = append(projectionArgs, options.BeforeMS)
			rawConditions = append(rawConditions, "e.timestamp_ms < ?")
			rawArgs = append(rawArgs, options.BeforeMS)
		}
	}
	if options.RequireRawEvent {
		projectionConditions = append(
			projectionConditions,
			"exists (select 1 from usage_events raw_event where raw_event.id = p.event_id)",
		)
	}

	arms := []eventSourceArm{{
		SQL: fmt.Sprintf(`select %s from usage_monitoring_event_projection_v1 p
			where p.event_id <= ? and %s`, projectionSelect, strings.Join(projectionConditions, " and ")),
		Args: append([]any{coverageEventID}, projectionArgs...),
	}}
	if !options.ProjectionComplete {
		arms = append(arms, eventSourceArm{
			SQL:  fmt.Sprintf(`select %s from usage_events e where e.id > ? and %s`, rawSelect, strings.Join(rawConditions, " and ")),
			Args: append([]any{coverageEventID}, rawArgs...),
		})
	}
	return arms
}

func eventFilterConditions(filter AnalyticsFilter, prefix string, projected bool) ([]string, []any) {
	return eventFilterConditionsWithSearch(filter, prefix, projected, false)
}

func eventFilterConditionsWithSearch(filter AnalyticsFilter, prefix string, projected, directSearch bool) ([]string, []any) {
	column := func(name string) string { return prefix + name }
	conditions := []string{column("timestamp_ms") + " >= ?", column("timestamp_ms") + " < ?"}
	args := []any{filter.FromMS, filter.ToMS}

	query := strings.TrimSpace(strings.ToLower(filter.SearchQuery))
	hash := strings.TrimSpace(strings.ToLower(filter.SearchAPIKeyHash))
	if query != "" {
		like := "%" + query + "%"
		searchConditions := make([]string, 0, len(usageprojection.SearchColumns)+1)
		if projected {
			if likePattern, ok := usageprojection.SearchIndexLikePattern(query); ok && !directSearch {
				searchConditions = append(searchConditions, fmt.Sprintf(
					"%s in (select rowid from %s where search_text like ?)",
					column("event_id"),
					usageprojection.SearchIndexTable,
				))
				args = append(args, likePattern)
			} else {
				searchConditions = append(searchConditions, column("search_text")+" like ?")
				args = append(args, like)
			}
		} else {
			for _, searchColumn := range usageprojection.SearchColumns {
				expression := column(searchColumn)
				if searchColumn == "analytics_model" {
					expression = usageidentity.SQLRequestAnalyticsModelExpression(column("model"), column("requested_model"))
				} else if searchColumn == "auth_project_id_snapshot" {
					expression = usageidentity.SQLProjectIDSnapshotExpression(prefix)
				}
				searchConditions = append(searchConditions, fmt.Sprintf("lower(coalesce(%s, '')) like ?", expression))
				args = append(args, like)
			}
		}
		if hash != "" {
			searchConditions = append(searchConditions, "lower(coalesce("+column("api_key_hash")+", '')) = ?")
			args = append(args, hash)
		}
		conditions = append(conditions, "("+strings.Join(searchConditions, " or ")+")")
	} else if hash != "" {
		conditions = append(conditions, "lower(coalesce("+column("api_key_hash")+", '')) = ?")
		args = append(args, hash)
	}

	addInCondition := func(expression string, values []string) {
		normalized := normalizeFilterValues(values)
		if len(normalized) == 0 {
			return
		}
		conditions = append(conditions, fmt.Sprintf("coalesce(%s, '') in (select value from json_each(?))", expression))
		args = append(args, encodeJSONFilterValues(normalized))
	}
	models := normalizeModelFilterValues(filter.Models)
	if projected {
		switch len(models) {
		case 0:
		case 1:
			conditions = append(conditions, column("analytics_model")+" = ?")
			args = append(args, models[0])
		default:
			conditions = append(conditions, column("analytics_model")+" in (select value from json_each(?))")
			args = append(args, encodeJSONFilterValues(models))
		}
	} else {
		addInCondition(usageidentity.SQLRequestAnalyticsModelExpression(column("model"), column("requested_model")), models)
	}
	addProviderEventCondition(filter.Providers, prefix, &conditions, &args)
	addAccountEventCondition(filter.Accounts, prefix, &conditions, &args)
	credentialExpr := fmt.Sprintf("coalesce(nullif(%sauth_file_snapshot, ''), nullif(%sauth_index, ''), nullif(%ssource_hash, ''), nullif(%ssource, ''), '-')", prefix, prefix, prefix, prefix)
	addInCondition(credentialExpr, filter.CredentialIDs)
	addInCondition(column("auth_file_snapshot"), filter.AuthFiles)
	addInCondition(column("auth_index"), filter.AuthIndices)
	addInCondition(column("api_key_hash"), filter.APIKeyHashes)
	addInCondition(column("source_hash"), filter.SourceHashes)
	addInCondition(usageidentity.SQLProjectIDSnapshotExpression(prefix), filter.ProjectIDs)
	addInCondition(column("executor_type"), filter.RequestTypes)
	addInCondition(column("header_error_kind"), filter.HeaderErrorKinds)
	addInCondition(column("header_error_code"), filter.HeaderErrorCodes)
	addInCondition(column("header_quota_plan_type"), filter.HeaderQuotaPlans)
	addInCondition(column("header_trace_id"), filter.HeaderTraceIDs)
	if !filter.IncludeFailed {
		conditions = append(conditions, column("failed")+" = 0")
	}
	if filter.FailedOnly {
		conditions = append(conditions, column("failed")+" = 1")
	}
	if filter.MinLatencyMS > 0 {
		conditions = append(conditions, column("latency_ms")+" >= ?")
		args = append(args, filter.MinLatencyMS)
	}
	cacheHitCondition := strings.Join([]string{
		"(coalesce(" + column("cached_tokens") + ", 0) > 0",
		"or coalesce(" + column("cache_tokens") + ", 0) > 0",
		"or coalesce(" + column("cache_read_tokens") + ", 0) > 0",
		"or coalesce(" + column("cache_creation_tokens") + ", 0) > 0)",
	}, " ")
	switch strings.TrimSpace(strings.ToLower(filter.CacheStatus)) {
	case "hit":
		conditions = append(conditions, cacheHitCondition)
	case "miss":
		conditions = append(conditions, "not "+cacheHitCondition)
	case "read":
		conditions = append(conditions, "coalesce("+column("cache_read_tokens")+", 0) > 0")
	case "creation":
		conditions = append(conditions, "coalesce("+column("cache_creation_tokens")+", 0) > 0")
	}
	return conditions, args
}

func addProviderEventCondition(values []string, prefix string, conditions *[]string, args *[]any) {
	normalized := normalizeLowerFilterValues(values)
	if len(normalized) == 0 {
		return
	}
	encoded := encodeJSONFilterValues(normalized)
	providerConditions := []string{
		"lower(coalesce(" + prefix + "provider, '')) in (select value from json_each(?))",
		"lower(coalesce(" + prefix + "auth_provider_snapshot, '')) in (select value from json_each(?))",
	}
	*conditions = append(*conditions, "("+strings.Join(providerConditions, " or ")+")")
	for range providerConditions {
		*args = append(*args, encoded)
	}
}

func addAccountEventCondition(values []string, prefix string, conditions *[]string, args *[]any) {
	normalized := normalizeLowerFilterValues(values)
	if len(normalized) == 0 {
		return
	}
	encoded := encodeJSONFilterValues(normalized)
	accountConditions := []string{
		"lower(coalesce(" + prefix + "account_snapshot, '')) in (select value from json_each(?))",
		"lower(coalesce(" + prefix + "auth_label_snapshot, '')) in (select value from json_each(?))",
		"lower(coalesce(" + prefix + "source, '')) in (select value from json_each(?))",
		"lower(coalesce(" + prefix + "auth_index, '')) in (select value from json_each(?))",
	}
	*conditions = append(*conditions, "("+strings.Join(accountConditions, " or ")+")")
	for range accountConditions {
		*args = append(*args, encoded)
	}
}
