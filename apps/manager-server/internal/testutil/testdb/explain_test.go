package testdb_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usagepricingsql"
)

// Verifies the administration helper's result contract. Production reader-plan
// acceptance remains in usageevent's same-package private-builder tests.
func TestMySQLExplainHelperCompiledBindingsAndWork(t *testing.T) {
	if !testdb.MySQL() {
		t.Skip("CPA_TEST_MYSQL_DSN is not set")
	}
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "plan.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	query := `with ` + usagepricingsql.ContextIntervalsCTE("bands") + ` select threshold_tokens from bands where model = ?`
	plan := testdb.ExplainMySQL(t, ctx, db, true, query, "plan-only-bound-value")
	if !json.Valid(plan.Explain) || !strings.Contains(plan.SQL, "lead(") || strings.Contains(plan.SQL, "plan-only-bound-value") {
		t.Fatalf("invalid compiled plan evidence: %+v", plan)
	}
	if plan.Analyze == "" || plan.AnalyzeElapsed <= 0 || len(plan.Work) != 13 {
		t.Fatalf("missing actual execution/work evidence: %+v", plan)
	}
	if _, ok := plan.Work["Handler_read_key"]; !ok {
		t.Fatal("missing session handler counter")
	}
}
