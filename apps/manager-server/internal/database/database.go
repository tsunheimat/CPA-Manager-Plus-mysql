// Package database owns runtime database selection, startup inspection and the
// exclusive Manager Server process lock. Repositories receive database/sql DBs.
package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/processlock"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
)

func Driver(cfg config.Config) string {
	if cfg.DBDriver == "" {
		return "sqlite"
	}
	return cfg.DBDriver
}

func Open(ctx context.Context, cfg config.Config) (*sql.DB, error) {
	if err := checkPendingRestore(cfg); err != nil {
		return nil, err
	}
	switch Driver(cfg) {
	case "sqlite":
		return sqliterepo.Open(cfg.DBPath)
	case "mysql":
		return mysqlrepo.Open(ctx, cfg.DBDSN)
	default:
		return nil, fmt.Errorf("unsupported database driver %q", cfg.DBDriver)
	}
}

func InspectConnection(ctx context.Context, cfg config.Config) (sqliterepo.PersistedCPAConnectionStorageInspection, error) {
	if Driver(cfg) == "sqlite" {
		return sqliterepo.InspectPersistedCPAConnectionStorage(ctx, cfg.DBPath)
	}
	db, err := mysqlrepo.OpenNative(ctx, cfg.DBDSN)
	if err != nil {
		return sqliterepo.PersistedCPAConnectionStorageInspection{}, err
	}
	defer db.Close()
	var exists bool
	if err := db.QueryRowContext(ctx, "select exists(select 1 from information_schema.tables where table_schema=database() and table_name='settings')").Scan(&exists); err != nil {
		return sqliterepo.PersistedCPAConnectionStorageInspection{}, err
	}
	if !exists {
		return sqliterepo.PersistedCPAConnectionStorageInspection{}, nil
	}
	return sqliterepo.InspectCPAConnectionStorage(ctx, db)
}

func RequireDataKey(ctx context.Context, cfg config.Config) error {
	if err := checkPendingRestore(cfg); err != nil {
		return err
	}
	if Driver(cfg) == "sqlite" {
		return sqliterepo.RequireExistingDataKeyForEncryptedCPAConnection(ctx, cfg.DBPath, cfg.DataKey, cfg.DataKeyPath)
	}
	inspection, err := InspectConnection(ctx, cfg)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cfg.DataKey) != "" {
		return nil
	}
	if _, err := os.Stat(cfg.DataKeyPath); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat data key: %w", err)
	}
	if inspection.HasEncryptedConnection {
		return fmt.Errorf("encrypted CPA connection exists but data key is missing at %s", cfg.DataKeyPath)
	}
	return nil
}

func checkPendingRestore(cfg config.Config) error {
	if Driver(cfg) != "mysql" {
		return nil
	}
	_, err := os.Lstat(cfg.DataKeyPath + ".restore-pending.json")
	if err == nil {
		return errors.New("an interrupted MySQL restore must be recovered before startup: run manager-data-snapshot recover with the same database and data-key configuration")
	}
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// RequireExistingMySQL prevents recovery commands from accidentally initializing
// an empty or unrelated database.
func RequireExistingMySQL(ctx context.Context, cfg config.Config) error {
	db, err := mysqlrepo.OpenNative(ctx, cfg.DBDSN)
	if err != nil {
		return err
	}
	defer db.Close()
	var count int
	if err := db.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema=database() and table_name in ('settings','usage_events','manager_schema_version')").Scan(&count); err != nil {
		return err
	}
	if count != 3 {
		return errors.New("configured MySQL database is not an initialized CPA Manager Plus database")
	}
	return nil
}

type Lock struct {
	file *processlock.Lock
	db   *sql.DB
	conn *sql.Conn
	name string
}

func Acquire(ctx context.Context, cfg *config.Config) (*Lock, error) {
	if Driver(*cfg) == "sqlite" {
		file, err := processlock.Acquire(cfg.DBPath)
		if err != nil {
			return nil, err
		}
		cfg.DBPath = file.DatabasePath()
		return &Lock{file: file}, nil
	}
	db, err := mysqlrepo.OpenNative(ctx, cfg.DBDSN)
	if err != nil {
		return nil, err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	lock := &Lock{db: db, conn: conn}
	var name string
	if err := conn.QueryRowContext(ctx, "select database()").Scan(&name); err != nil {
		lock.Close()
		return nil, err
	}
	lock.name = fmt.Sprintf("cpamp-process-%x", sha256.Sum256([]byte(name)))[:60]
	var acquired int
	if err := conn.QueryRowContext(ctx, "select get_lock(?,0)", lock.name).Scan(&acquired); err != nil {
		lock.Close()
		return nil, err
	}
	if acquired != 1 {
		lock.Close()
		return nil, processlock.ErrLocked
	}
	return lock, nil
}

// Check must be called periodically while a MySQL process is running. A lost
// lock connection must stop the process rather than silently admit two managers.
func (l *Lock) Check(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}
	var held bool
	if err := l.conn.QueryRowContext(ctx, "select coalesce(is_used_lock(?)=connection_id(),0)", l.name).Scan(&held); err != nil {
		return fmt.Errorf("MySQL process lock connection lost: %w", err)
	}
	if !held {
		return errors.New("MySQL process lock was lost")
	}
	return nil
}
func (l *Lock) Close() error {
	if l.file != nil {
		return l.file.Close()
	}
	var errs []error
	if l.conn != nil {
		_, err := l.conn.ExecContext(context.Background(), "do release_lock(?)", l.name)
		errs = append(errs, err, l.conn.Close())
	}
	if l.db != nil {
		errs = append(errs, l.db.Close())
	}
	return errors.Join(errs...)
}
