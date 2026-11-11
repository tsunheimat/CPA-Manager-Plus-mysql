package store

import (
	"context"
	"database/sql"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

// MySQLOptions are independently opt-in until native integration acceptance.
// They do not disable mandatory admission or retained-history safety checks.
type MySQLOptions struct {
	AnalyticsEnabled bool
	BulkEnabled      bool
	HourlyEnabled    bool
}

func (s *Store) ConfiguredMySQLOptions() MySQLOptions {
	if s == nil {
		return MySQLOptions{}
	}
	return s.mysqlOptions
}

func (s *Store) UsageMonitoringHourlyEnabled() bool {
	return s != nil && s.mysqlOptions.HourlyEnabled && mysqlrepo.IsMySQL(s.db)
}

func (s *Store) CatchUpUsageMonitoringHourlyStats(ctx context.Context, limit int, nowMS int64) (UsageMonitoringCatchUpResult, error) {
	if !s.UsageMonitoringHourlyEnabled() {
		return UsageMonitoringCatchUpResult{}, nil
	}
	ready, err := s.UsageCacheAccountingMigrationReady(ctx)
	if err != nil {
		return UsageMonitoringCatchUpResult{}, err
	}
	if !ready {
		return UsageMonitoringCatchUpResult{Pending: true}, nil
	}
	return s.UsageMonitoring.CatchUpHourlyStats(ctx, limit, nowMS)
}

func (s *Store) CatchUpUsageMonitoringHourlyMetadata(ctx context.Context, limit int, nowMS int64) (UsageMonitoringCatchUpResult, error) {
	if !s.UsageMonitoringHourlyEnabled() {
		return UsageMonitoringCatchUpResult{}, nil
	}
	ready, err := s.UsageCacheAccountingMigrationReady(ctx)
	if err != nil {
		return UsageMonitoringCatchUpResult{}, err
	}
	if !ready {
		return UsageMonitoringCatchUpResult{Pending: true}, nil
	}
	return s.UsageMonitoring.CatchUpHourlyMetadata(ctx, limit, nowMS)
}

func (s *Store) MySQLWriteAdmissionStats() (mysqlrepo.AdmissionStats, bool) {
	if s == nil {
		return mysqlrepo.AdmissionStats{}, false
	}
	return mysqlrepo.WriteAdmissionStats(s.db)
}

// DatabasePoolStats contains no connection strings or query parameters. Deltas
// observed across a request are shared pool observations, not its own waits.
func (s *Store) DatabasePoolStats() sql.DBStats {
	if s == nil || s.db == nil {
		return sql.DBStats{}
	}
	return s.db.Stats()
}
