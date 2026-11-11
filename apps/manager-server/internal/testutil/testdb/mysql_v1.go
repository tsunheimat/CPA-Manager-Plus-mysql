package testdb

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

// OpenMySQLV1 installs only the frozen historical schema in a new isolated test
// database. It deliberately returns a native administrative pool for fixture DDL.
// It never downgrades an existing schema and skips without CPA_TEST_MYSQL_DSN.
func OpenMySQLV1(t testing.TB, path string) (*sql.DB, string) {
	t.Helper()
	dsn := DSN(t, path)
	db, err := mysqlrepo.OpenNative(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	var count int
	if err := db.QueryRow("select count(*) from information_schema.tables where table_schema=database()").Scan(&count); err != nil || count != 0 {
		t.Fatalf("v1 fixture requires an empty isolated database: %d %v", count, err)
	}
	_, source, _, _ := runtime.Caller(0)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(source), "../../repository/mysql/schema.sql"))
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range strings.Split(string(data), "-- statement")[1:] {
		if _, err := db.ExecContext(context.Background(), strings.TrimSpace(part)); err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []string{
		"create table manager_schema_version(id bigint primary key, version bigint not null) engine=InnoDB",
		"insert into manager_schema_version(id,version) values(1,1)",
	} {
		if _, err := db.ExecContext(context.Background(), query); err != nil {
			t.Fatal(err)
		}
	}
	return db, dsn
}
