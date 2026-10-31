package httpapi

import (
	"net/http"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLStatusAndMaintenance(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set")
	}
	cfg := testutil.NewConfig(t)
	st := testutil.NewStore(t, cfg)
	handler := New(cfg, st, collector.NewManager(cfg, st)).Handler()
	rr := testutil.Request(t, handler, http.MethodGet, "/status", "", testutil.AdminKey)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"dbDriver":"mysql"`) || strings.Contains(rr.Body.String(), cfg.DBDSN) || strings.Contains(rr.Body.String(), `"dbPath"`) {
		t.Fatalf("unexpected public status: %s", rr.Body.String())
	}
	rr = testutil.Request(t, handler, http.MethodGet, "/v0/management/usage/maintenance", "", testutil.AdminKey)
	if rr.Code != http.StatusOK {
		t.Fatalf("maintenance: %d %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), `"driver":"mysql"`) || !strings.Contains(rr.Body.String(), `"compact_requires_stopped_server":true`) {
		t.Fatalf("unexpected MySQL maintenance capabilities: %s", rr.Body.String())
	}
}
