package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLBootstrapRestartAndMissingDataKey(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, dir), DataDir: dir, DataKeyPath: filepath.Join(dir, "data.key"), AdminKey: "cpamp_test_mysql_admin", UsageArchiveDir: filepath.Join(dir, "usage-archives")}
	first, err := New(ctx, cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Bootstrap.DataKeyCreated {
		t.Fatal("initial data key was not created")
	}
	if err := first.Store.SaveSetup(ctx, store.Setup{CPAUpstreamURL: "http://cpa.example:8317", ManagementKey: "encrypted-secret"}); err != nil {
		t.Fatal(err)
	}
	first.Store.Close()
	second, err := New(ctx, cfg, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if second.Bootstrap.DataKeyCreated {
		t.Fatal("data key regenerated on restart")
	}
	second.Store.Close()
	if err := os.Remove(cfg.DataKeyPath); err != nil {
		t.Fatal(err)
	}
	if _, err := New(ctx, cfg, Options{}); err == nil || !strings.Contains(err.Error(), "data key is missing") {
		t.Fatalf("missing-key error = %v", err)
	}
	if _, err := os.Stat(cfg.DataKeyPath); !os.IsNotExist(err) {
		t.Fatal("missing data key was replaced")
	}
}
