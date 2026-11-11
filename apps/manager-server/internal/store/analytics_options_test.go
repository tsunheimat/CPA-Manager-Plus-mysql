package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageaggregate"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
)

func TestConfiguredMySQLOptionsPreserveOptInAndSQLiteNoop(t *testing.T) {
	ctx := context.Background()
	for _, enabled := range []bool{false, true} {
		st, err := OpenConfigured(ctx, config.Config{DBDriver: "sqlite", DBPath: filepath.Join(t.TempDir(), "usage.sqlite"), MySQLAnalyticsEnabled: enabled, MySQLBulkEnabled: enabled, MySQLHourlyEnabled: enabled})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if got := st.ConfiguredMySQLOptions(); got != (MySQLOptions{AnalyticsEnabled: enabled, BulkEnabled: enabled, HourlyEnabled: enabled}) {
			t.Fatalf("configured options=%+v", got)
		}
		if st.UsageMonitoringHourlyEnabled() {
			t.Fatal("SQLite enabled MySQL hourly tasks")
		}
		// Disabled/no-op hourly paths must not touch repositories or source state.
		st.UsageMonitoring = nil
		for _, catchUp := range []func(context.Context, int, int64) (UsageMonitoringCatchUpResult, error){st.CatchUpUsageMonitoringHourlyStats, st.CatchUpUsageMonitoringHourlyMetadata} {
			if result, err := catchUp(ctx, 1, 1); err != nil || result.Pending || result.Processed != 0 {
				t.Fatalf("disabled task=%+v err=%v", result, err)
			}
		}
		legacy := New(st.db)
		if legacy.ConfiguredMySQLOptions() != (MySQLOptions{}) {
			t.Fatal("legacy constructor enabled native paths")
		}
	}
}

type availableAggregateRepository struct{ usageaggregate.Repository }

func (r *availableAggregateRepository) LoadRowsTx(context.Context, *sql.Tx, usageaggregate.Filter) ([]usageaggregate.Row, usageaggregate.State, bool, error) {
	return nil, usageaggregate.State{}, true, nil
}

type failedPricingRepository struct {
	usagepricing.Repository
	err       error
	recovered bool
}

func (r *failedPricingRepository) LoadHourlyRowsTx(context.Context, *sql.Tx, usagepricing.HourlyFilter) ([]usagepricing.HourlyRow, usagepricing.State, bool, error) {
	return nil, usagepricing.State{}, false, r.err
}
func (r *failedPricingRepository) LoadHourlyRowsFromEventsTx(context.Context, *sql.Tx, usagepricing.HourlyFilter) ([]usagepricing.HourlyRow, error) {
	r.recovered = true
	return nil, nil
}

func TestHourlyPricingTerminalReadDoesNotTriggerRecovery(t *testing.T) {
	st, err := openPortableTestStore(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	st.UsageAggregates = &availableAggregateRepository{Repository: st.UsageAggregates}
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, errors.New("database connection failed"), ErrUsagePricingCoverageIncomplete} {
		pricing := &failedPricingRepository{Repository: st.UsagePricing, err: want}
		st.UsagePricing = pricing
		_, err := st.LoadUsageHourlyPricingSnapshot(context.Background(), UsageHourlyAggregateFilter{}, UsagePricingHourlyFilter{})
		if !errors.Is(err, want) || pricing.recovered {
			t.Fatalf("err=%v recovered=%v", err, pricing.recovered)
		}
	}
}
