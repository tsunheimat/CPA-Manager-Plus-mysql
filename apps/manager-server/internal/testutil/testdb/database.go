// Package testdb runs shared repository contracts against SQLite by default,
// or an isolated MySQL schema when CPA_TEST_MYSQL_DSN is set. The supplied DSN's
// database is never modified. The test user needs CREATE/DROP DATABASE rights.
package testdb

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

var paths = struct {
	sync.Mutex
	dsn   map[string]string
	pools map[*sql.DB]string
}{dsn: map[string]string{}, pools: map[*sql.DB]string{}}

func MySQL() bool { return os.Getenv("CPA_TEST_MYSQL_DSN") != "" }

func SQL(db *sql.DB, sqlite, mysql string) string {
	if mysqlrepo.IsMySQL(db) {
		return mysql
	}
	return sqlite
}

func Open(t testing.TB, path string) (*sql.DB, error) {
	t.Helper()
	if !MySQL() {
		return sqliterepo.Open(path)
	}
	dsn := DSN(t, path)
	db, err := mysqlrepo.Open(context.Background(), dsn)
	if err != nil {
		return nil, err
	}
	paths.Lock()
	paths.pools[db] = dsn
	paths.Unlock()
	t.Cleanup(func() {
		paths.Lock()
		delete(paths.pools, db)
		paths.Unlock()
	})
	return db, nil
}

// Admin is an explicitly native administrative TEST connection for fixture DDL
// such as compound CREATE TRIGGER bodies. Runtime pools keep their multi-statement
// guard. Only pools registered by Open can resolve an isolated schema; never fall
// back to the environment's base DSN or silently skip a fixture. SQLite uses its
// existing test connection. The caller's testing scope closes the native pool.
func Admin(t testing.TB, db *sql.DB) *sql.DB {
	t.Helper()
	if !mysqlrepo.IsMySQL(db) {
		return db
	}
	admin, err := openFixtureAdmin(context.Background(), db, mysqlrepo.OpenNative)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	return admin
}

func openFixtureAdmin(ctx context.Context, db *sql.DB, open func(context.Context, string) (*sql.DB, error)) (*sql.DB, error) {
	paths.Lock()
	dsn, ok := paths.pools[db]
	paths.Unlock()
	if !ok {
		return nil, fmt.Errorf("administrative fixture connection requires a pool opened by testdb.Open")
	}
	return open(ctx, dsn)
}

func DSN(t testing.TB, path string) string {
	t.Helper()
	paths.Lock()
	defer paths.Unlock()
	if dsn := paths.dsn[path]; dsn != "" {
		return dsn
	}
	base := os.Getenv("CPA_TEST_MYSQL_DSN")
	if base == "" {
		t.Skip("CPA_TEST_MYSQL_DSN is not set")
	}
	cfg, err := mysqldriver.ParseDSN(base)
	if err != nil {
		t.Fatal("invalid CPA_TEST_MYSQL_DSN")
	}
	admin, err := mysqlrepo.OpenNative(context.Background(), base)
	if err != nil {
		t.Fatal(err)
	}
	var id [10]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	name := "cpamp_test_" + hex.EncodeToString(id[:])
	if _, err := admin.Exec("create database `" + name + "` character set utf8mb4 collate utf8mb4_0900_bin"); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.DBName = name
	dsn := cfg.FormatDSN()
	paths.dsn[path] = dsn
	t.Cleanup(func() {
		if _, err := admin.Exec("drop database `" + name + "`"); err != nil {
			t.Errorf("drop isolated MySQL test schema: %v", err)
		}
		admin.Close()
		paths.Lock()
		delete(paths.dsn, path)
		paths.Unlock()
	})
	return dsn
}

// PrepareIndexes performs SQLite's deferred DDL. MySQL installs these indexes
// synchronously with the schema, so it has no post-listen DDL phase.
func PrepareIndexes(ctx context.Context, db *sql.DB) error {
	if mysqlrepo.IsMySQL(db) {
		return nil
	}
	return sqliterepo.RunDerivedStartupMaintenance(ctx, db)
}

// ExecScript is for fixture setup only. Production connections intentionally
// reject multi-statements. Bind each statement's placeholders independently.
func ExecScript(ctx context.Context, db *sql.DB, script string, args ...any) (sql.Result, error) {
	var result sql.Result
	start, bindings := 0, 0
	var quote byte
	for i := 0; i <= len(script); i++ {
		if i < len(script) {
			c := script[i]
			if quote != 0 {
				if c == quote {
					if i+1 < len(script) && script[i+1] == quote {
						i++
					} else {
						quote = 0
					}
				}
				continue
			}
			if c == '\'' || c == '"' || c == '`' {
				quote = c
				continue
			}
			if c == '?' {
				bindings++
			}
			if c != ';' {
				continue
			}
		}
		statement := strings.TrimSpace(script[start:i])
		start = i + 1
		if statement == "" {
			continue
		}
		if bindings > len(args) {
			return nil, fmt.Errorf("fixture SQL argument count mismatch")
		}
		var err error
		result, err = sqlwrite.ExecContext(ctx, db, statement, args[:bindings]...)
		if err != nil {
			return nil, err
		}
		args = args[bindings:]
		bindings = 0
	}
	if len(args) > 0 {
		return nil, fmt.Errorf("unused fixture SQL arguments")
	}
	return result, nil
}
