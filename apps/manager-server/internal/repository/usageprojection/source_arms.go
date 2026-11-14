package usageprojection

import (
	"fmt"
	"strings"
)

// EventSourceArm is a single live or retained source with its own bindings.
// Callers must retain the source-specific proof; arms are not interchangeable.
// From is an internal SQL FROM expression, never request-supplied SQL.
type EventSourceArm struct {
	From string
	Args []any
}

// AggregateColumn describes the existing output grain of an aggregate query.
// An empty Merge groups by the column; otherwise Merge is sum, min, or max.
// Both fields are internal SQL identifiers, not request values.
type AggregateColumn struct {
	Name  string
	Merge string
}

// MergeAggregateQueries unions only aggregate rows and restores the original
// statement grain in SQL. In particular, sums remain database sums: splitting
// sources must not turn an out-of-range result into unchecked Go int64 addition.
func MergeAggregateQueries(queries []string, columns []AggregateColumn) string {
	if len(queries) == 1 {
		return queries[0]
	}
	names := make([]string, 0, len(columns))
	selected := make([]string, 0, len(columns))
	grouped := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
		if column.Merge == "" {
			selected = append(selected, column.Name)
			grouped = append(grouped, column.Name)
		} else {
			selected = append(selected, column.Merge+"("+column.Name+") as "+column.Name)
		}
	}
	fields := strings.Join(names, ", ")
	ctes := make([]string, 0, len(queries)+1)
	unions := make([]string, 0, len(queries))
	for i, query := range queries {
		name := fmt.Sprintf("aggregate_arm_%d", i)
		ctes = append(ctes, name+" ("+fields+") as ("+query+")")
		unions = append(unions, "select "+fields+" from "+name)
	}
	ctes = append(ctes, "aggregate_contributions as ("+strings.Join(unions, " union all ")+")")
	query := "with " + strings.Join(ctes, ", ") + " select " + strings.Join(selected, ", ") + " from aggregate_contributions"
	if len(grouped) > 0 {
		query += " group by " + strings.Join(grouped, ", ")
	}
	return query
}

// UnionEventSources preserves the event-grain interface for unpriced consumers.
// Priced readers aggregate the structured arms before combining contributions.
func UnionEventSources(arms []EventSourceArm) (string, []any) {
	if len(arms) == 1 {
		return arms[0].From, arms[0].Args
	}
	queries := make([]string, 0, len(arms))
	var args []any
	for _, arm := range arms {
		queries = append(queries, "select * from "+arm.From+" source_arm")
		args = append(args, arm.Args...)
	}
	return "(" + strings.Join(queries, " union all ") + ")", args
}
