package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func TestCompoundTriggerFixturesRequireAdministrativeConnection(t *testing.T) {
	ctx := context.Background()
	for _, query := range []string{
		`create trigger reject_second_usage_stage before insert on usage_cache_accounting_v2_changes for each row begin if new.event_id=2 then signal sqlstate '45000' set message_text='blocked'; end if; end`,
		`create trigger fail_identity_checkpoint before update on usage_monitoring_rollup_state for each row begin if new.rollup_name = 'codex_legacy_identity_v1' and new.coverage_event_id > 0 then signal sqlstate '45000' set message_text = 'checkpoint failure'; end if; end`,
	} {
		db, owner, physical := admissionDB(t)
		for _, runtimeSQL := range []string{query, Native(query)} {
			if _, err := sqlwrite.ExecContext(ctx, db, runtimeSQL); err == nil || !strings.Contains(err.Error(), "multiple SQL statements") {
				t.Fatalf("runtime guard relaxed for compound fixture: %v", err)
			}
		}
		if len(physical.queries) != 0 || len(owner.writer) != 0 {
			t.Fatal("rejected fixture reached physical pool or leaked admission")
		}
		// OpenNative likewise exposes the unwrapped administrative connector.
		// This offline control proves pass-through only, not server DDL syntax.
		admin := sql.OpenDB(admissionConnector{physical})
		if _, err := admin.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
		admin.Close()
		if len(physical.queries) != 1 || physical.queries[0] != query {
			t.Fatal("administrative fixture was changed or split")
		}
	}
}
