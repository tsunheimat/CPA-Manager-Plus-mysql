package usageevent

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestResponseMetadataBackfillCandidateNoOpDoesNotBlockReadiness(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	repo := New(db)

	// Insert a row that matches SQL candidate predicate (raw_json contains 'X-Ratelimit-Limit-Requests'
	// while $.rate_limit.requests.limit is null in metadata_json), but already has response_metadata_json = '{}'
	// and raw_json doesn't actually contain valid response header structures.
	rawJSON := `{"prompt":"User asked about X-Ratelimit-Limit-Requests"}`
	_, err = sqlwrite.ExecContext(ctx, db, `insert into usage_events (
		event_hash, timestamp_ms, timestamp, model, raw_json, response_metadata_json, created_at_ms
	) values ('hash-1', 1700000000000, '2023-11-14T22:13:20Z', 'gpt-4o', ?, '{}', 1700000000000)`, rawJSON)
	if err != nil {
		t.Fatalf("insert usage_event: %v", err)
	}

	// Verify the SQL candidate query actually matches this row
	rows, err := db.QueryContext(ctx, responseMetadataBackfillSelect, int64(0), 10)
	if err != nil {
		t.Fatalf("query candidates: %v", err)
	}
	defer rows.Close()
	candidateCount := 0
	for rows.Next() {
		candidateCount++
	}
	if candidateCount != 1 {
		t.Fatalf("expected 1 SQL candidate, got %d", candidateCount)
	}

	// Verify BackfillResponseMetadata does not update the no-op row
	updated, err := repo.BackfillResponseMetadata(ctx, 100)
	if err != nil {
		t.Fatalf("BackfillResponseMetadata: %v", err)
	}
	if updated != 0 {
		t.Fatalf("expected 0 updates for no-op candidate, got %d", updated)
	}

	// Verify ResponseMetadataBackfillPending returns false
	pending, err := repo.ResponseMetadataBackfillPending(ctx)
	if err != nil {
		t.Fatalf("ResponseMetadataBackfillPending: %v", err)
	}
	if pending {
		t.Fatalf("expected pending = false for no-op candidate, got true")
	}
}

func TestResponseMetadataBackfillMatchesHeaderNamesIgnoringCase(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, header := range []string{"x-ratelimit-limit-requests", "X-RaTeLiMiT-LiMiT-ReQuEsTs"} {
		raw := `{"response_headers":{"` + header + `":"123"}}`
		if _, err := sqlwrite.ExecContext(ctx, db, `insert into usage_events(event_hash,timestamp_ms,timestamp,model,raw_json,response_metadata_json,created_at_ms) values(?,0,'time','model',?,'{}',0)`, header, raw); err != nil {
			t.Fatal(err)
		}
	}
	updated, err := New(db).BackfillResponseMetadata(ctx, 10)
	if err != nil || updated != 2 {
		t.Fatalf("updated=%d err=%v", updated, err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `select count(*) from usage_events where json_extract(response_metadata_json,'$.rate_limit.requests.limit')=123`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("backfilled header count=%d err=%v", count, err)
	}
}
