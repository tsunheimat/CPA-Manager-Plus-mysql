package mysqlmigrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/security"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestMySQLMigrationPreservesDataAndSequenceWithoutChangingSource(t *testing.T) {
	ctx := context.Background()
	dsn := testdb.DSN(t, t.TempDir())
	path := filepath.Join(t.TempDir(), "source.sqlite")
	db, err := sqliterepo.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	key, err := security.NewProtector([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(db, key)
	if err := st.SaveSetup(ctx, store.Setup{CPAUpstreamURL: "http://cpa.example:8317", ManagementKey: "secret"}); err != nil {
		t.Fatal(err)
	}
	event := usage.Event{EventHash: fmt.Sprintf("%x", sha256.Sum256([]byte("original"))), Timestamp: "2026-01-01T00:00:00Z", TimestampMS: 1767225600000, Model: "model", InputTokens: 100, TotalTokens: 100}
	if _, err := st.InsertEvents(ctx, []usage.Event{event}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("update sqlite_sequence set seq=1000 where name='usage_events'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("update usage_event_identity_ledger set aggregate_structure_revision='copied-default-column'"); err != nil {
		t.Fatal(err)
	}
	st.Close()
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{DBDriver: "mysql", DBDSN: dsn}
	var output bytes.Buffer
	if err := Copy(ctx, path, cfg, &output); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(original, after) {
		t.Fatal("source database changed")
	}
	target, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	for _, name := range []string{"stats_hourly_v1", "metadata_hourly_v1"} {
		var status, revision string
		var backfill, coverage, targetID, processed int64
		if err := target.QueryRow(`select status,structure_revision,backfill_last_event_id,coverage_event_id,target_event_id,processed_events from usage_monitoring_rollup_state where rollup_name=?`, name).Scan(&status, &revision, &backfill, &coverage, &targetID, &processed); err != nil || status != "pending" || revision != "" || backfill != 0 || coverage != 0 || targetID != 0 || processed != 0 {
			t.Fatalf("destination-only state lost or inherited coverage: %s %s %s %d %d %d %d %v", name, status, revision, backfill, coverage, targetID, processed, err)
		}
	}
	migrated := store.New(target, key)
	setup, ok, err := migrated.LoadSetup(ctx)
	if err != nil || !ok || setup.ManagementKey != "secret" {
		t.Fatalf("encrypted settings not preserved: ok=%v err=%v", ok, err)
	}
	var id int64
	var revision string
	if err := target.QueryRow("select aggregate_structure_revision from usage_event_identity_ledger where event_hash=?", event.EventHash).Scan(&revision); err != nil || revision != "copied-default-column" {
		t.Fatalf("defaulted text column lost: %q %v", revision, err)
	}
	if err := target.QueryRow("select id from usage_events where event_hash=?", event.EventHash).Scan(&id); err != nil || id != 1 {
		t.Fatalf("original id=%d err=%v", id, err)
	}
	event.EventHash = fmt.Sprintf("%x", sha256.Sum256([]byte("next")))
	if _, err := migrated.InsertEvents(ctx, []usage.Event{event}); err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow("select id from usage_events where event_hash=?", event.EventHash).Scan(&id); err != nil || id <= 1000 {
		t.Fatalf("sequence reused id=%d err=%v", id, err)
	}
	if err := Copy(ctx, path, cfg, &output); err == nil || !strings.Contains(err.Error(), "empty database") {
		t.Fatalf("nonempty target accepted: %v", err)
	}
}

func TestMySQLMigrationRollsBackInvalidSourceRows(t *testing.T) {
	ctx := context.Background()
	dsn := testdb.DSN(t, t.TempDir())
	path := filepath.Join(t.TempDir(), "source.sqlite")
	db, err := sqliterepo.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`insert into settings(key,value,updated_at_ms) values('must-rollback','sentinel',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`insert into usage_events(event_hash,timestamp_ms,timestamp,model,total_tokens,created_at_ms) values(?,1,'time','model','not-an-integer',1)`, strings.Repeat("f", 64)); err != nil {
		t.Fatal(err)
	}
	db.Close()
	var output bytes.Buffer
	if err := Copy(ctx, path, config.Config{DBDriver: "mysql", DBDSN: dsn}, &output); err == nil {
		t.Fatal("invalid SQLite integer was coerced during import")
	}
	target, err := mysqlrepo.OpenNative(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var count int
	if err := target.QueryRow("select count(*) from usage_events").Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial events committed: %d %v", count, err)
	}
	if err := target.QueryRow("select count(*) from settings where `key`='must-rollback'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial settings committed: %d %v", count, err)
	}
}
