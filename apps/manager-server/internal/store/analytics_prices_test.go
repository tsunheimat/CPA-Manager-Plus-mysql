package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageaggregate"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

type snapshotAggregateStub struct {
	usageaggregate.Repository
	available bool
	err       error
	endTx     bool
	calls     int
	tx        *sql.Tx
}

func (r *snapshotAggregateStub) LoadRowsTx(_ context.Context, tx *sql.Tx, _ usageaggregate.Filter) ([]usageaggregate.Row, usageaggregate.State, bool, error) {
	r.calls++
	r.tx = tx
	if r.endTx {
		if err := tx.Rollback(); err != nil {
			return nil, usageaggregate.State{}, false, err
		}
	}
	return nil, usageaggregate.State{}, r.available, r.err
}

type snapshotPriceSpy struct {
	modelprice.Repository
	full, fullTx, rates, ratesTx int
	lastTx                       *sql.Tx
	err                          error
}

func (r *snapshotPriceSpy) LoadAll(ctx context.Context) (map[string]model.ModelPrice, error) {
	r.full++
	return r.Repository.LoadAll(ctx)
}
func (r *snapshotPriceSpy) LoadAllTx(ctx context.Context, tx *sql.Tx) (map[string]model.ModelPrice, error) {
	r.fullTx++
	return r.Repository.LoadAllTx(ctx, tx)
}
func (r *snapshotPriceSpy) LoadRates(ctx context.Context) (map[string]model.ModelPrice, error) {
	r.rates++
	return r.Repository.LoadRates(ctx)
}
func (r *snapshotPriceSpy) LoadRatesTx(ctx context.Context, tx *sql.Tx) (map[string]model.ModelPrice, error) {
	r.ratesTx++
	r.lastTx = tx
	if r.err != nil {
		return nil, r.err
	}
	return r.Repository.LoadRatesTx(ctx, tx)
}

func TestUnavailableHourlySnapshotSkipsPricingAndBook(t *testing.T) {
	st, err := openPortableTestStore(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	core := &snapshotAggregateStub{}
	st.UsageAggregates = core
	// Any use of either missing dependency panics; the core and edge checks
	// must run first, and unavailable core must not touch either dependency.
	st.UsagePricing, st.ModelPrices = nil, nil
	got, err := st.LoadUsageHourlyPricingSnapshot(context.Background(), UsageHourlyAggregateFilter{}, UsagePricingHourlyFilter{})
	if err != nil || got.AggregateAvailable || got.PricingAvailable || got.Prices != nil || got.PricingRows != nil || core.calls != 1 {
		t.Fatalf("snapshot=%+v core calls=%d err=%v", got, core.calls, err)
	}
	if err := core.tx.Commit(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("unavailable snapshot left transaction open: %v", err)
	}
	for _, want := range []error{context.Canceled, context.DeadlineExceeded, errors.New("core query failed")} {
		core.err = want
		if _, err := st.LoadUsageHourlyPricingSnapshot(context.Background(), UsageHourlyAggregateFilter{}, UsagePricingHourlyFilter{}); !errors.Is(err, want) {
			t.Fatalf("core error lost: %v, want %v", err, want)
		}
	}
	core.err, core.endTx = nil, true
	if _, err := st.LoadUsageHourlyPricingSnapshot(context.Background(), UsageHourlyAggregateFilter{}, UsagePricingHourlyFilter{}); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("unavailable commit error lost: %v", err)
	}
}

func TestUnavailableHourlySnapshotStillValidatesDeletedEdges(t *testing.T) {
	st, err := openPortableTestStore(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	core := &snapshotAggregateStub{}
	st.UsageAggregates, st.UsagePricing, st.ModelPrices = core, nil, nil
	ctx := context.Background()
	if _, err := sqlwrite.ExecContext(ctx, st.db, `delete from usage_monitoring_rollup_state where rollup_name = 'projection_v1'`); err != nil {
		t.Fatal(err)
	}
	for _, filter := range []UsageHourlyAggregateFilter{
		{FromMS: 1000, ToMS: 7201000, LeftEdgeDeleted: true},
		{FromMS: 1000, ToMS: 7201000, RightEdgeDeleted: true},
	} {
		if _, err := st.LoadUsageHourlyPricingSnapshot(ctx, filter, UsagePricingHourlyFilter{}); !errors.Is(err, ErrUsagePricingCoverageIncomplete) {
			t.Fatalf("missing edge proof was bypassed: %v", err)
		}
	}
	if core.calls != 0 {
		t.Fatal("core ran before required edge validation")
	}
}

func TestAnalyticsBookAndSnapshotsUseRatesWithoutChangingManagement(t *testing.T) {
	ctx := context.Background()
	st, err := openPortableTestStore(t, filepath.Join(t.TempDir(), "usage.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveModelPrices(ctx, map[string]ModelPrice{"model-a": {Prompt: 1, PromptConfigured: true, Source: "manual", RawJSON: `{"wide":"metadata"}`}}); err != nil {
		t.Fatal(err)
	}
	spy := &snapshotPriceSpy{Repository: st.ModelPrices}
	st.ModelPrices = spy
	full, err := st.LoadModelPrices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rates, err := st.LoadAnalyticsModelPrices(ctx)
	if err != nil || rates["model-a"].Prompt != 1 || rates["model-a"].RawJSON != "" || full["model-a"].RawJSON == "" || spy.full != 1 || spy.rates != 1 {
		t.Fatalf("book routing full=%+v rates=%+v spy=%+v err=%v", full, rates, spy, err)
	}
	const fromMS int64 = 1_800_000_000_000
	if _, err := st.InsertEvents(ctx, []usage.Event{snapshotTestEvent("rates-snapshot", fromMS+1000, 100)}); err != nil {
		t.Fatal(err)
	}
	catchUpUsagePricingSnapshot(t, ctx, st)
	core := &snapshotAggregateStub{available: true}
	// Empty, available core with empty window avoids changing coverage proofs.
	st.UsageAggregates = core
	for i := 0; i < 2; i++ {
		snapshot, err := st.LoadUsageHourlyPricingSnapshot(ctx,
			UsageHourlyAggregateFilter{FromMS: fromMS + 3600000, ToMS: fromMS + 7200000},
			UsagePricingHourlyFilter{FromMS: fromMS + 3600000, ToMS: fromMS + 7200000})
		if err != nil || !snapshot.AggregateAvailable || !reflect.DeepEqual(snapshot.Prices, rates) || spy.lastTx != core.tx {
			t.Fatalf("transactional rates snapshot=%+v err=%v", snapshot, err)
		}
	}
	account, err := st.LoadUsagePricingAccountSnapshot(ctx, nil)
	if err != nil || !reflect.DeepEqual(account.Prices, rates) || spy.ratesTx != 3 || spy.fullTx != 0 {
		t.Fatalf("account=%+v ratesTx=%d fullTx=%d err=%v", account, spy.ratesTx, spy.fullTx, err)
	}
	want := errors.New("rates scan failed")
	spy.err = want
	if _, err := st.LoadUsageHourlyPricingSnapshot(ctx,
		UsageHourlyAggregateFilter{FromMS: fromMS + 3600000, ToMS: fromMS + 7200000},
		UsagePricingHourlyFilter{FromMS: fromMS + 3600000, ToMS: fromMS + 7200000}); !errors.Is(err, want) {
		t.Fatalf("book error lost: %v", err)
	}
}
