package deadletter

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestInsertCancellationReleasesAdmissionAndPreservesPayload(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "deadletters.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := New(db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := repo.Insert(ctx, "not-inserted", "canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled insert: %v", err)
	}
	ctx = context.Background()
	const payload, reason = `{"message":"界 '?' ; --"}`, "strict validation failed"
	if err := repo.Insert(ctx, payload, reason); err != nil {
		t.Fatalf("insert after cancellation: %v", err)
	}
	if count, err := repo.Count(ctx); err != nil || count != 1 {
		t.Fatalf("count after cancellation: %d %v", count, err)
	}
	var gotPayload, gotError string
	var created int64
	if err := db.QueryRowContext(ctx, "select payload,error,created_at_ms from dead_letter_events").Scan(&gotPayload, &gotError, &created); err != nil || gotPayload != payload || gotError != reason || created <= 0 {
		t.Fatalf("bound payload changed: %q %q %d %v", gotPayload, gotError, created, err)
	}
}
