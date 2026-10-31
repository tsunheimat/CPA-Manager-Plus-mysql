package mysql_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestMySQLBackupRejectsInvalidRowsAndRollsBack(t *testing.T) {
	ctx := context.Background()
	dsn := testdb.DSN(t, t.TempDir())
	schema, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema.Close()
	db, err := mysqlrepo.OpenNative(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"insert into settings(`key`,value,updated_at_ms) values('state','before',0)",
		"insert into model_prices(model,prompt_per_1m,completion_per_1m,cache_per_1m,updated_at_ms) values('parent',1.25,2.5,0,0)",
		"insert into model_price_context_tiers(model,threshold_tokens) values('parent',100)",
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	var backup bytes.Buffer
	if err := mysqlrepo.WriteBackup(ctx, db, &backup); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("update settings set value='after' where `key`='state'"); err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(backup.Bytes()))
	var header mysqlrepo.BackupHeader
	if err := dec.Decode(&header); err != nil {
		t.Fatal(err)
	}
	var invalid bytes.Buffer
	enc := json.NewEncoder(&invalid)
	if err := enc.Encode(header); err != nil {
		t.Fatal(err)
	}
	for _, table := range header.Tables {
		for i := int64(0); i < table.Rows; i++ {
			var row [][]byte
			if err := dec.Decode(&row); err != nil {
				t.Fatal(err)
			}
			if table.Name == "model_price_context_tiers" {
				for c, name := range table.Columns {
					if name == "model" {
						row[c] = []byte("missing-parent")
					}
				}
			}
			if err := enc.Encode(row); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tc := range []struct {
		name      string
		data      []byte
		cancelled bool
	}{
		{"foreign-key", invalid.Bytes(), false},
		{"truncated", backup.Bytes()[:backup.Len()-3], false},
		{"trailing", append(append([]byte{}, backup.Bytes()...), []byte("{}")...), false},
		{"cancelled", backup.Bytes(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			restore, err := mysqlrepo.BeginRestore(testCtx, db, bytes.NewReader(tc.data))
			if err == nil {
				restore.Close()
				t.Fatal("invalid restore succeeded")
			}
			if tc.name == "foreign-key" && !strings.Contains(err.Error(), "foreign key") {
				t.Fatalf("unexpected rejection: %v", err)
			}
			var value string
			if err := db.QueryRow("select value from settings where `key`='state'").Scan(&value); err != nil || value != "after" {
				t.Fatalf("failed restore changed live data: %q %v", value, err)
			}
			if _, err := db.Exec("insert into model_price_context_tiers(model,threshold_tokens) values('missing-parent',200)"); err == nil {
				t.Fatal("foreign key enforcement disabled after failed restore")
			}
		})
	}
}
