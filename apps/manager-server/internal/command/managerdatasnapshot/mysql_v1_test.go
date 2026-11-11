package managerdatasnapshot

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLV1SnapshotRestoresFilesAndKeepsPhysicalV2(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, dsn := testdb.OpenMySQLV1(t, dir)
	cfg := config.Config{DBDriver: "mysql", DBDSN: dsn, DataKeyPath: filepath.Join(dir, "data.key"), UsageArchiveDir: filepath.Join(dir, "archives")}
	if err := os.WriteFile(cfg.DataKeyPath, []byte("v1-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.UsageArchiveDir, 0700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(cfg.UsageArchiveDir, "archived.jsonl.gz")
	if err := os.WriteFile(archive, []byte("v1-archive"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"insert into settings(`key`,value,updated_at_ms) values('encrypted','v1-ciphertext',1)",
		"insert into usage_events(id,event_hash,timestamp_ms,timestamp,model,created_at_ms) values(9000,'archived',1,'time','model',1)",
		"insert into usage_event_identity_ledger(event_hash,raw_event_id,timestamp_ms,bucket_ms,first_seen_at_ms,updated_at_ms) values('archived',null,1,0,1,1)",
		"delete from usage_events",
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := filepath.Join(dir, "v1-snapshot")
	var output bytes.Buffer
	if err := runMySQL(ctx, options{Action: "create", SnapshotDir: snapshot}, cfg, &output); err != nil {
		t.Fatal(err)
	}
	upgraded, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	upgraded.Close()
	if _, err := db.Exec("update settings set value='current-ciphertext' where `key`='encrypted'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("update usage_monitoring_rollup_state set status='ready',structure_revision='current-generation',coverage_event_id=10000 where rollup_name='stats_hourly_v1'"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.DataKeyPath, []byte("current-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(archive, []byte("current-archive"), 0600); err != nil {
		t.Fatal(err)
	}
	checks := 0
	err = restoreMySQL(ctx, db, cfg, snapshot, func() error {
		checks++
		if checks == 2 {
			return errors.New("lost ownership after file replacement")
		}
		return nil
	})
	if err == nil || checks != 2 {
		t.Fatalf("expected late rollback: %d %v", checks, err)
	}
	for path, want := range map[string]string{cfg.DataKeyPath: "current-key", archive: "current-archive"} {
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want {
			t.Fatalf("v1 rollback files: %s %q %v", path, data, err)
		}
	}
	var coverage int64
	if err := db.QueryRow("select coverage_event_id from usage_monitoring_rollup_state where rollup_name='stats_hourly_v1'").Scan(&coverage); err != nil || coverage != 10000 {
		t.Fatalf("v1 rollback changed v2 state: %d %v", coverage, err)
	}
	if err := runMySQL(ctx, options{Action: "restore", SnapshotDir: snapshot}, cfg, &output); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow("select version from manager_schema_version where id=1").Scan(&version); err != nil || version != 2 {
		t.Fatalf("physical version changed: %d %v", version, err)
	}
	var value string
	if err := db.QueryRow("select value from settings where `key`='encrypted'").Scan(&value); err != nil || value != "v1-ciphertext" {
		t.Fatalf("ciphertext changed: %q %v", value, err)
	}
	for path, want := range map[string]string{cfg.DataKeyPath: "v1-key", archive: "v1-archive"} {
		data, err := os.ReadFile(path)
		if err != nil || string(data) != want {
			t.Fatalf("v1 restore files: %s %q %v", path, data, err)
		}
	}
	for _, name := range []string{"stats_hourly_v1", "metadata_hourly_v1"} {
		var status, revision string
		if err := db.QueryRow("select status,structure_revision,coverage_event_id from usage_monitoring_rollup_state where rollup_name=?", name).Scan(&status, &revision, &coverage); err != nil || status != "pending" || revision != "" || coverage != 0 {
			t.Fatalf("restored inherited coverage: %s %s %s %d %v", name, status, revision, coverage, err)
		}
	}
	var ledger int
	if err := db.QueryRow("select count(*) from usage_event_identity_ledger where event_hash='archived' and raw_event_id is null").Scan(&ledger); err != nil || ledger != 1 {
		t.Fatalf("ledger lost: %d %v", ledger, err)
	}
	result, err := db.Exec("insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms) values('new',2,'time','model',2)")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil || id <= 9000 {
		t.Fatalf("archived high water reused: %d %v", id, err)
	}
	if _, err := os.Stat(pendingPath(cfg)); !os.IsNotExist(err) {
		t.Fatal("completed restore left a file journal")
	}
}
