package usageevent

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestPreparedInsertColumnContractAndBulkBounds(t *testing.T) {
	args := preparedInsertArgs(preparedUsageEvent{})
	if len(args) != len(strings.Split(usageInsertColumns, ",")) || len(args) != 65 {
		t.Fatalf("insert columns=%d args=%d", len(strings.Split(usageInsertColumns, ",")), len(args))
	}
	if bulkMaxRows*len(args) >= 65535 {
		t.Fatal("native placeholder limit exceeded")
	}
	if bulkArgsSize([]any{strings.Repeat("界", 100)}) < 300 {
		t.Fatal("encoded bytes undercounted")
	}
	repo := New(nil).(*repository)
	if repo.mysqlBulk || repo.mysqlAnalytics {
		t.Fatal("native acceleration must default off")
	}
}

// Deferred acceptance: CPA_TEST_MYSQL_DSN selects a disposable test schema.
// This test never provisions a server and is skipped in ordinary SQLite runs.
func TestMySQLBulkIdentityAndAtomicFailures(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("native MySQL acceptance explicitly deferred; set CPA_TEST_MYSQL_DSN")
	}
	ctx := context.Background()
	for _, phase := range []string{"success", "claim", "raw", "attach"} {
		t.Run(phase, func(t *testing.T) {
			db, err := testdb.Open(t, filepath.Join(t.TempDir(), "bulk"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := sqlwrite.ExecContext(ctx, db, mysql.Native(query), args...); err != nil {
					t.Fatal(err)
				}
			}
			event := func(id int) usage.Event {
				return usage.Event{EventHash: fmt.Sprintf("%064x", id), TimestampMS: 1000 + int64(id), Timestamp: "synthetic", Model: strings.Repeat("界", 100) + "Model", AuthFileSnapshot: strings.Repeat("認証", 70) + ".json", InputTokens: 100, TotalTokens: 100, CreatedAtMS: 1}
			}
			repo := New(db, WithMySQLBulk(true))
			archived := event(1)
			if _, err := repo.InsertBatch(ctx, []usage.Event{archived}); err != nil {
				t.Fatal(err)
			}
			exec(`delete from usage_events where event_hash=?`, archived.EventHash)
			exec(`update usage_event_identity_ledger set raw_event_id=null where event_hash=?`, archived.EventHash)
			legacy := event(2)
			legacy.EventHash = "legacy-noncanonical"
			exec(`insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms) values(?,77,'legacy','old',17)`, legacy.EventHash)
			switch phase {
			case "claim":
				exec(`create trigger fail_claim before insert on usage_event_identity_ledger for each row signal sqlstate '45000' set message_text='claim failed'`)
			case "raw":
				exec(`create trigger fail_raw before insert on usage_events for each row signal sqlstate '45000' set message_text='raw failed'`)
			case "attach":
				exec(`create trigger fail_attach before update on usage_event_identity_ledger for each row signal sqlstate '45000' set message_text='attach failed'`)
			}
			a, b := event(3), event(4)
			result, err := repo.InsertBatch(ctx, []usage.Event{archived, a, a, b, legacy})
			if phase != "success" {
				if err == nil || result.Inserted != 0 || result.Skipped != 0 || len(result.InsertedEventHashes) != 0 {
					t.Fatalf("failed batch returned partial result=%#v err=%v", result, err)
				}
				var raw, ledger int
				if err := db.QueryRow(`select count(*) from usage_events`).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`select count(*) from usage_event_identity_ledger`).Scan(&ledger); err != nil {
					t.Fatal(err)
				}
				if raw != 1 || ledger != 1 {
					t.Fatalf("partial commit raw=%d ledger=%d", raw, ledger)
				}
				return
			}
			if err != nil || result.Inserted != 2 || result.Skipped != 3 || !reflect.DeepEqual(result.InsertedEventHashes, []string{a.EventHash, b.EventHash}) {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			var resurrected int
			if err := db.QueryRow(`select count(*) from usage_events where event_hash=?`, archived.EventHash).Scan(&resurrected); err != nil || resurrected != 0 {
				t.Fatal("archived identity resurrected", resurrected, err)
			}
			var ts, created int64
			if err := db.QueryRow(`select timestamp_ms,first_seen_at_ms from usage_event_identity_ledger where event_hash=?`, legacy.EventHash).Scan(&ts, &created); err != nil || ts != 77 || created != 17 {
				t.Fatal("legacy evidence replaced", ts, created, err)
			}
			again, err := repo.InsertBatch(ctx, []usage.Event{a, b, legacy, archived})
			if err != nil || again.Inserted != 0 || again.Skipped != 4 {
				t.Fatal(again, err)
			}
		})
	}
}
