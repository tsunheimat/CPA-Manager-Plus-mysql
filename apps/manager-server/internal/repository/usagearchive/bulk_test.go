package usagearchive

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageevent"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestArchiveBulkChunkBoundsAndFullIdentityBindings(t *testing.T) {
	refs := make([]RecordRef, 517)
	for i := range refs {
		refs[i] = RecordRef{EventID: int64(i + 1), EventHash: fmt.Sprintf("%064x", i+1)}
	}
	sizes := []int{}
	visited := 0
	if err := archiveRefChunks(refs, func(chunk []RecordRef) error {
		sizes = append(sizes, len(chunk))
		visited += len(chunk)
		pairs, args := archivePairs(chunk)
		query, bindings, err := mysql.CompileSQL(`delete from usage_events where (id,event_hash) in (`+pairs+`)`, args...)
		if err != nil {
			return err
		}
		if len(bindings) != len(chunk)*2 || strings.Count(query, "?") != len(bindings) {
			t.Fatal("identity parameter mismatch")
		}
		for i, ref := range chunk {
			if bindings[i*2] != ref.EventID || bindings[i*2+1] != ref.EventHash {
				t.Fatal("identity ordering mismatch")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if visited != len(refs) || !reflect.DeepEqual(sizes, []int{200, 200, 117}) {
		t.Fatal(visited, sizes)
	}
	if err := archiveRefChunks([]RecordRef{{EventID: 1, EventHash: strings.Repeat("界", archiveBulkBytes)}}, func([]RecordRef) error { t.Fatal("oversized identity reached execution"); return nil }); err == nil {
		t.Fatal("missing encoded-byte bound")
	}
}

func TestMySQLBulkArchivePublicationDeletionRollback(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("native MySQL acceptance explicitly deferred; set CPA_TEST_MYSQL_DSN")
	}
	ctx := context.Background()
	for _, phase := range []string{"success", "publish", "raw", "ledger", "reference"} {
		t.Run(phase, func(t *testing.T) {
			db := openArchiveTestDB(t)
			repo := New(db, WithMySQLBulk(true))
			events := make([]usage.Event, 205)
			for i := range events {
				events[i] = auditOrdinaryEvent(fmt.Sprintf("bulk-archive-%d", i), 1000+int64(i), int64(i+1))
			}
			if _, err := usageevent.New(db, usageevent.WithMySQLBulk(true)).InsertBatch(ctx, events); err != nil {
				t.Fatal(err)
			}
			catchUpHourlyAggregate(t, ctx, db, 10000)
			catchUpDeleteReadiness(t, ctx, db, 11000)
			run, err := repo.CreateRun(ctx, "native-bulk", 2000, 12000)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginArchive(ctx, run.ID, 12001); err != nil {
				t.Fatal(err)
			}
			records, err := repo.Records(ctx, run.ID, 0, 300, 1<<30)
			if err != nil {
				t.Fatal(err)
			}
			// Failure after the first chunk proves no partial application batch commits.
			trigger := ""
			switch phase {
			case "publish":
				trigger = `create trigger block_publish before insert on usage_archive_event_refs for each row begin if new.raw_event_id > 200 then signal sqlstate '45000' set message_text='blocked'; end if; end`
			case "raw":
				trigger = `create trigger block_raw before delete on usage_events for each row begin if old.id > 200 then signal sqlstate '45000' set message_text='blocked'; end if; end`
			case "ledger":
				trigger = `create trigger block_ledger before update on usage_event_identity_ledger for each row begin if new.raw_event_id is null and old.raw_event_id > 200 then signal sqlstate '45000' set message_text='blocked'; end if; end`
			case "reference":
				trigger = `create trigger block_reference before update on usage_archive_event_refs for each row begin if new.raw_deleted_at_ms is not null and old.raw_event_id > 200 then signal sqlstate '45000' set message_text='blocked'; end if; end`
			}
			if trigger != "" {
				if _, err := testdb.Admin(t, db).ExecContext(ctx, trigger); err != nil {
					t.Fatal(err)
				}
			}
			_, err = repo.RecordSegment(ctx, run.ID, archiveTestSegment(run.ID, records), archiveRecordRefs(records), 12002)
			if phase == "publish" {
				if err == nil {
					t.Fatal("publication failure ignored")
				}
				var refs, segments int
				if err := db.QueryRow(`select count(*) from usage_archive_event_refs`).Scan(&refs); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`select count(*) from usage_archive_segments`).Scan(&segments); err != nil {
					t.Fatal(err)
				}
				if refs != 0 || segments != 0 {
					t.Fatal("partial publication", refs, segments)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.MarkArchived(ctx, run.ID, "digest", run.ID+"/manifest.json", "manifest", 12003); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginVerification(ctx, run.ID, 12004); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.MarkVerified(ctx, run.ID, 12005); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.BeginDelete(ctx, run.ID, 12006); err != nil {
				t.Fatal(err)
			}
			result, err := repo.DeleteBatch(ctx, run.ID, 300, 12007)
			if phase == "success" {
				if err != nil || result.Deleted != 205 || !result.Completed {
					t.Fatal(result, err)
				}
				return
			}
			if err == nil || result.Deleted != 0 {
				t.Fatal("delete failure ignored", result, err)
			}
			var raw, detached, deleted int
			if err := db.QueryRow(`select count(*) from usage_events`).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`select count(*) from usage_event_identity_ledger where raw_event_id is null`).Scan(&detached); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`select count(*) from usage_archive_event_refs where raw_deleted_at_ms is not null`).Scan(&deleted); err != nil {
				t.Fatal(err)
			}
			if raw != 205 || detached != 0 || deleted != 0 {
				t.Fatal("partial deletion committed", raw, detached, deleted)
			}
		})
	}
}
