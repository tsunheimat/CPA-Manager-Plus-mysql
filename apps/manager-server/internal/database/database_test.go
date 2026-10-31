package database_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/command/adminreset"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/command/cpaconnection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/database"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/processlock"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/security"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLExclusiveManagerLock(t *testing.T) {
	cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, t.TempDir())}
	ctx := context.Background()
	first, err := database.Acquire(ctx, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := database.Acquire(ctx, &cfg)
	if second != nil {
		second.Close()
	}
	if !errors.Is(err, processlock.ErrLocked) {
		first.Close()
		t.Fatalf("second process was admitted: %v", err)
	}
	if err := first.Check(ctx); err != nil {
		first.Close()
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := database.Acquire(ctx, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := third.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMySQLRecoveryCommands(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	cfg := config.Config{DBDriver: "mysql", DBDSN: testdb.DSN(t, dir), DataDir: dir, DataKeyPath: filepath.Join(dir, "data.key")}
	key, _, err := security.LoadOrCreateDataKey("", cfg.DataKeyPath)
	if err != nil {
		t.Fatal(err)
	}
	protector, err := security.NewProtector(key)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.OpenConfigured(ctx, cfg, protector)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	t.Setenv("CPA_MANAGER_CONFIG", filepath.Join(dir, "config.json"))
	t.Setenv("USAGE_DB_DRIVER", "mysql")
	t.Setenv("USAGE_DB_DSN", cfg.DBDSN)
	t.Setenv("CPA_MANAGER_DATA_KEY_PATH", cfg.DataKeyPath)
	t.Setenv("CPA_UPSTREAM_URL", "")
	t.Setenv("CPA_MANAGEMENT_KEY", "")
	t.Setenv("CPA_MANAGER_DATA_KEY", "")
	secret := filepath.Join(dir, "management-key")
	if err := os.WriteFile(secret, []byte("recovered-management-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := cpaconnection.Run(ctx, []string{"--cpa-base-url", "http://cpa.example:8317", "--management-key-file", secret}, &out, &out); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := st.LoadManagerConfig(ctx)
	if err != nil || !ok || stored.CPAConnection.ManagementKey != "recovered-management-secret" {
		t.Fatalf("connection recovery failed: ok=%v err=%v", ok, err)
	}
	if err := adminreset.Run(ctx, []string{"--admin-key", "cpamp_mysql_recovered_admin_key_123456"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	credential, ok, err := st.LoadAdminCredential(ctx)
	if err != nil || !ok || credential.Version == 0 {
		t.Fatalf("admin recovery failed: ok=%v err=%v", ok, err)
	}
	lock, err := database.Acquire(ctx, &cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := adminreset.Run(ctx, []string{"--admin-key", "cpamp_mysql_changed_admin_key_123456"}, &out, &out); !errors.Is(err, processlock.ErrLocked) {
		t.Fatalf("recovery ignored live manager lock: %v", err)
	}
}
