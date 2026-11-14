package usageprojection

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

func TestAggregateArmsMergeInSQLAndPreserveOverflowErrors(t *testing.T) {
	db := retainedSourceFixture(t)
	columns := []AggregateColumn{{Name: "identity"}, {Name: "calls", Merge: "sum"}, {Name: "representative", Merge: "max"}}
	queries := []string{`select ? as identity, ? as calls, ? as representative`, `select ?, ?, ?`}
	query := MergeAggregateQueries(queries, columns)
	args := []any{"same", int64(2), "alpha", "same", int64(3), "zeta"}
	compiled, bound, err := mysql.CompileSQL(query, args...)
	if err != nil || !reflect.DeepEqual(bound, args) {
		t.Fatalf("compile=%s bindings=%v err=%v", compiled, bound, err)
	}
	var identity, representative string
	var calls int64
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&identity, &calls, &representative); err != nil || identity != "same" || calls != 5 || representative != "zeta" {
		t.Fatalf("row=%s,%d,%s err=%v", identity, calls, representative, err)
	}
	args[1], args[4] = int64(math.MaxInt64), int64(1)
	if err := db.QueryRowContext(context.Background(), query, args...).Scan(&identity, &calls, &representative); err == nil || !strings.Contains(err.Error(), "integer overflow") {
		t.Fatalf("split-source SUM overflow was swallowed: calls=%d err=%v", calls, err)
	}
}

func TestRetainedEdgeArmsKeepTheirOriginalMultiset(t *testing.T) {
	db := retainedSourceFixture(t)
	// Unlike verified rebuilds, this legacy edge source keeps duplicate archive
	// references and does not exclude a live hash. Do not strengthen its proof
	// accidentally while changing where pricing/aggregation occurs.
	frozen := `select id from usage_events where timestamp_ms >= 1000 and timestamp_ms < 4000
	 union all select p.event_id from usage_archive_event_refs archived
	 join usage_monitoring_event_projection_v1 p on p.event_id=archived.raw_event_id
	 where archived.raw_deleted_at_ms is not null and archived.timestamp_ms >= 1000 and archived.timestamp_ms < 4000`
	queries := []string{frozen, "select id from " + RetainedEdgeSource(1000, 4000) + " e"}
	for _, query := range queries {
		rows, err := db.QueryContext(context.Background(), query)
		if err != nil {
			t.Fatal(err)
		}
		got := map[int64]int{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			got[id]++
		}
		err = rows.Err()
		rows.Close()
		if err != nil || !reflect.DeepEqual(got, map[int64]int{1: 2, 2: 2, 3: 1}) {
			t.Fatalf("multiset=%v err=%v", got, err)
		}
	}
}
