package usageevent

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestBulkBudgetDoesNotRejectSingleRowsOrLegacyDuplicates(t *testing.T) {
	ctx := context.Background()
	event := usage.Event{EventHash: fmt.Sprintf("%064x", 1001), TimestampMS: 1000, Timestamp: "synthetic", Model: "model", InputTokens: 1, TotalTokens: 1, CreatedAtMS: 1, FailBody: strings.Repeat("x", (2<<20)+1)}
	referenceDB, err := testdb.Open(t, filepath.Join(t.TempDir(), "reference.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer referenceDB.Close()
	if result, err := New(referenceDB).InsertBatch(ctx, []usage.Event{event}); err != nil || result.Inserted != 1 {
		t.Fatal(result, err)
	}
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint(legacy), func(t *testing.T) {
			conn := &bulkTestConn{raw: map[string]bulkIdentity{}, ledger: map[string]int64{}, nextID: 100}
			candidate := event
			if legacy {
				candidate.EventHash = "historical-noncanonical"
				conn.raw[candidate.EventHash] = bulkIdentity{7, 1000, 1}
			}
			db := sql.OpenDB(bulkTestConnector{conn})
			defer db.Close()
			repo := New(db).(*repository)
			prepared, err := repo.prepareUsageEvent(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if bulkArgsSize(preparedInsertArgs(prepared)) <= bulkMaxBytes {
				t.Fatal("fixture does not exceed batch budget")
			}
			result, err := repo.insertPreparedBulk(ctx, []preparedUsageEvent{prepared}, model.InsertResult{})
			wantInserted, wantSkipped := 1, 0
			if legacy {
				wantInserted, wantSkipped = 0, 1
			}
			if err != nil || !conn.committed || result.Inserted != wantInserted || result.Skipped != wantSkipped {
				t.Fatal(result, err)
			}
		})
	}
}
