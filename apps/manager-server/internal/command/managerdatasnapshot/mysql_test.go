package managerdatasnapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/database"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLSnapshotRoundTripAndIntegrity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, dir), DataKeyPath: filepath.Join(dir, "data.key"), UsageArchiveDir: filepath.Join(dir, "archives")}
	db, err := mysqlrepo.Open(ctx, cfg.DBDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("insert into settings(`key`,value,updated_at_ms) values('test','before',0)"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.DataKeyPath, []byte("original-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(cfg.UsageArchiveDir, "nested"), 0700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(cfg.UsageArchiveDir, "nested", "day.jsonl.gz")
	if err := os.WriteFile(archive, []byte{0, 1, 2, 255}, 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot")
	var out bytes.Buffer
	if err := runMySQL(ctx, options{Action: "create", SnapshotDir: snapshot}, cfg, &out); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("update settings set value='after' where `key`='test'"); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(cfg.DataKeyPath, []byte("changed-key"), 0600)
	os.WriteFile(archive, []byte("changed"), 0600)
	if err := runMySQL(ctx, options{Action: "restore", SnapshotDir: snapshot}, cfg, &out); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRow("select value from settings where `key`='test'").Scan(&value); err != nil || value != "before" {
		t.Fatalf("restored value %q: %v", value, err)
	}
	key, _ := os.ReadFile(cfg.DataKeyPath)
	content, _ := os.ReadFile(archive)
	if string(key) != "original-key" || !bytes.Equal(content, []byte{0, 1, 2, 255}) {
		t.Fatal("local files did not restore")
	}
	if _, err := os.Stat(pendingPath(cfg)); !os.IsNotExist(err) {
		t.Fatal("restore journal was not cleared")
	}
	os.WriteFile(filepath.Join(snapshot, "mysql-data"), []byte("corrupt"), 0600)
	if err := runMySQL(ctx, options{Action: "restore", SnapshotDir: snapshot}, cfg, &out); err == nil {
		t.Fatal("corrupt backup accepted")
	}
	if err := db.QueryRow("select value from settings where `key`='test'").Scan(&value); err != nil || value != "before" {
		t.Fatal("corrupt backup changed live data")
	}
	if err := restore(ctx, filepath.Join(dir, "untouched.sqlite"), cfg.DataKeyPath, snapshot); err == nil {
		t.Fatal("MySQL backup accepted as SQLite")
	}
	if err := deleteSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
}
func TestMySQLRestoreJournalRecoversCommittedAndRolledBackFiles(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "rollback", true: "commit"}[committed], func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, dir), DataKeyPath: filepath.Join(dir, "data.key")}
			db, err := mysqlrepo.Open(ctx, cfg.DBDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			native, err := mysqlrepo.OpenNative(ctx, cfg.DBDSN)
			if err != nil {
				t.Fatal(err)
			}
			defer native.Close()
			fingerprint, err := mysqlFingerprint(ctx, native)
			if err != nil {
				t.Fatal(err)
			}
			item := mysqlJournalItem{Target: cfg.DataKeyPath, Old: filepath.Join(dir, "old-key"), Existed: true}
			os.WriteFile(item.Target, []byte("new"), 0600)
			os.WriteFile(item.Old, []byte("old"), 0600)
			j := mysqlJournal{Fingerprint: fingerprint, Token: "committed-token", Items: []mysqlJournalItem{item}}
			data, _ := json.Marshal(j)
			os.WriteFile(pendingPath(cfg), data, 0600)
			if opened, err := database.Open(ctx, cfg); err == nil {
				opened.Close()
				t.Fatal("startup ignored interrupted restore")
			}
			if committed {
				if _, err := db.Exec("insert into settings(`key`,value,updated_at_ms) values(?,?,0)", restoreTokenKey, j.Token); err != nil {
					t.Fatal(err)
				}
			}
			if err := recoverMySQLFiles(ctx, native, cfg); err != nil {
				t.Fatal(err)
			}
			content, _ := os.ReadFile(item.Target)
			want := "old"
			if committed {
				want = "new"
			}
			if string(content) != want {
				t.Fatalf("key=%q want %q", content, want)
			}
		})
	}
}

func TestMySQLSnapshotRestoresEmptyDatabaseAndDeletedHighWaterMark(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, dir), DataKeyPath: filepath.Join(dir, "source.key"), UsageArchiveDir: filepath.Join(dir, "source-archives")}
	db, err := mysqlrepo.Open(ctx, cfg.DBDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"insert into usage_events(id,event_hash,timestamp_ms,timestamp,model,created_at_ms) values(5000,'deleted',0,'time','model',0)",
		"delete from usage_events",
		"insert into settings(`key`,value,updated_at_ms) values('restore-test','你好',0)",
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(cfg.DataKeyPath, []byte("source-key"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot")
	var out bytes.Buffer
	if err := runMySQL(ctx, options{Action: "create", SnapshotDir: snapshot}, cfg, &out); err != nil {
		t.Fatal(err)
	}
	target := cfg
	target.DBDSN = testdb.DSN(t, dir+"-target")
	target.DataKeyPath = filepath.Join(dir, "target.key")
	target.UsageArchiveDir = filepath.Join(dir, "target-archives")
	if err := runMySQL(ctx, options{Action: "restore", SnapshotDir: snapshot}, target, &out); err != nil {
		t.Fatal(err)
	}
	restored, err := mysqlrepo.Open(ctx, target.DBDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	result, err := restored.Exec("insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms) values('new',0,'time','model',0)")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil || id <= 5000 {
		t.Fatalf("deleted high-water mark lost: id=%d err=%v", id, err)
	}
	var value string
	if err := restored.QueryRow("select value from settings where `key`='restore-test'").Scan(&value); err != nil || value != "你好" {
		t.Fatalf("restored value=%q err=%v", value, err)
	}
	key, err := os.ReadFile(target.DataKeyPath)
	if err != nil || string(key) != "source-key" {
		t.Fatalf("restored key=%q err=%v", key, err)
	}
}

func TestMySQLSnapshotOwnershipLossRollsBackFilesAndRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, dir), DataKeyPath: filepath.Join(dir, "data.key"), UsageArchiveDir: filepath.Join(dir, "archives")}
	db, err := mysqlrepo.Open(ctx, cfg.DBDSN)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = mysqlrepo.OpenNative(ctx, cfg.DBDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("insert into settings(`key`,value,updated_at_ms) values('state','backup',0)"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.DataKeyPath, []byte("backup-key"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(cfg.UsageArchiveDir, 0700); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(cfg.UsageArchiveDir, "segment.gz")
	if err := os.WriteFile(archive, []byte("backup-archive"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := filepath.Join(dir, "snapshot")
	if err := createMySQL(ctx, db, cfg, snapshot, func() error { return errors.New("lost ownership") }); err == nil {
		t.Fatal("published a backup without ownership")
	}
	if _, err := os.Stat(snapshot); !os.IsNotExist(err) {
		t.Fatal("inconsistent backup was published")
	}
	if err := createMySQL(ctx, db, cfg, snapshot, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("update settings set value='current' where `key`='state'"); err != nil {
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
			return errors.New("lost ownership after replacing files")
		}
		return nil
	})
	if err == nil || checks != 2 {
		t.Fatalf("expected a late ownership failure: checks=%d err=%v", checks, err)
	}
	var value string
	if err := db.QueryRow("select value from settings where `key`='state'").Scan(&value); err != nil || value != "current" {
		t.Fatalf("rows not rolled back: %q %v", value, err)
	}
	for path, want := range map[string]string{cfg.DataKeyPath: "current-key", archive: "current-archive"} {
		content, err := os.ReadFile(path)
		if err != nil || string(content) != want {
			t.Fatalf("file not rolled back: %s %q %v", path, content, err)
		}
	}
	if _, err := os.Stat(pendingPath(cfg)); !os.IsNotExist(err) {
		t.Fatal("resolved restore left a journal")
	}
}
