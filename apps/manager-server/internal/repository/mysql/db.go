package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

//go:generate go run ./internal/schemagen

//go:embed schema.sql
var schema string

const schemaVersion = 1

func parseDSN(dsn string) (*mysqldriver.Config, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return nil, errors.New("invalid MySQL DSN; expected user:password@tcp(host:3306)/database")
	}
	if strings.TrimSpace(cfg.DBName) == "" {
		return nil, errors.New("MySQL DSN must name an existing database")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	cfg.ClientFoundRows = true // SQLite UPDATE counts matched rows, including no-op updates.
	cfg.InterpolateParams = true
	cfg.MultiStatements = false
	cfg.AllowAllFiles = false
	if err := cfg.Apply(mysqldriver.Charset("utf8mb4", "utf8mb4_bin")); err != nil {
		return nil, err
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	delete(cfg.Params, "charset")
	cfg.Params["collation_connection"] = "'utf8mb4_0900_bin'"
	cfg.Params["time_zone"] = "'+00:00'"
	cfg.Params["sql_mode"] = "'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_BACKSLASH_ESCAPES,PIPES_AS_CONCAT,ANSI_QUOTES'"
	cfg.Params["transaction_isolation"] = "'REPEATABLE-READ'"
	cfg.Params["autocommit"] = "1"
	cfg.Params["group_concat_max_len"] = "16777216"
	return cfg, nil
}

// OpenNative opens a bounded pool without changing the schema. It is used for
// startup inspection and administrative operations that must not create data.
func OpenNative(ctx context.Context, dsn string) (*sql.DB, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	base, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return nil, errors.New("invalid MySQL connection options")
	}
	db := sql.OpenDB(base)
	configurePool(db)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to MySQL: %w", err)
	}
	return db, nil
}

func configurePool(db *sql.DB) {
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(3 * time.Minute)
	db.SetConnMaxIdleTime(time.Minute)
}

func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	native, err := OpenNative(ctx, dsn)
	if err != nil {
		return nil, err
	}
	err = migrate(ctx, native)
	native.Close()
	if err != nil {
		return nil, err
	}
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	base, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return nil, errors.New("invalid MySQL connection options")
	}
	db := sql.OpenDB(&connector{base: base, writer: make(chan struct{}, 1)})
	configurePool(db)
	return db, nil
}

func IsMySQL(db *sql.DB) bool {
	if db == nil {
		return false
	}
	_, ok := db.Driver().(*dialectDriver)
	return ok
}

func migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var version, name string
	if err := conn.QueryRowContext(ctx, "select version(), database()").Scan(&version, &name); err != nil {
		return err
	}
	var major, minor int
	_, err = fmt.Sscanf(version, "%d.%d", &major, &minor)
	if err != nil || strings.Contains(strings.ToLower(version), "mariadb") || major < 8 || major == 8 && minor < 4 {
		return fmt.Errorf("MySQL 8.4 or newer is required (server reports %s)", version)
	}
	lockName := fmt.Sprintf("cpamp-schema-%x", sha256.Sum256([]byte(name)))[:60]
	var acquired int
	if err := conn.QueryRowContext(ctx, "select get_lock(?, 30)", lockName).Scan(&acquired); err != nil {
		return err
	}
	if acquired != 1 {
		return errors.New("MySQL schema migration is already running")
	}
	defer conn.ExecContext(context.Background(), "do release_lock(?)", lockName)
	var tables, markers int
	if err := conn.QueryRowContext(ctx, `select count(*), coalesce(sum(table_name='manager_schema_version'),0)
		from information_schema.tables where table_schema=database()`).Scan(&tables, &markers); err != nil {
		return err
	}
	if tables > 0 && markers == 0 {
		return errors.New("refusing to initialize a non-empty MySQL database without a Manager schema marker; use an empty dedicated database")
	}
	if _, err := conn.ExecContext(ctx, "create table if not exists manager_schema_version (id bigint primary key, version bigint not null) engine=InnoDB"); err != nil {
		return err
	}
	var current int
	err = conn.QueryRowContext(ctx, "select version from manager_schema_version where id=1").Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if current > schemaVersion {
		return fmt.Errorf("MySQL schema version %d is newer than supported version %d", current, schemaVersion)
	}
	if current == schemaVersion {
		return nil
	}
	// MySQL DDL commits implicitly. Every statement is restartable; write the
	// version only after every table, index, view, and seed has succeeded.
	for i, part := range strings.Split(schema, "-- statement") {
		if i == 0 {
			continue
		}
		query := strings.TrimSpace(part)
		if query == "" {
			continue
		}
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("MySQL schema statement %d: %w", i, err)
		}
	}
	_, err = conn.ExecContext(ctx, "insert into manager_schema_version(id,version) values(1,?) on duplicate key update version=?", schemaVersion, schemaVersion)
	return err
}
