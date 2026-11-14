package modelprice_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

func TestLoadRatesPreservesCostFieldsAndOmitsOnlyMetadata(t *testing.T) {
	ctx := context.Background()
	db, repo := openTestDB(t)
	synced := int64(1234)
	book := map[string]model.ModelPrice{
		"模型-" + strings.Repeat("long-", 50): {
			Prompt: 0, Completion: 2, Cache: 3, CacheRead: 4, CacheCreation: 5,
			PromptConfigured: true, CompletionConfigured: true, CacheReadConfigured: true, CacheCreationConfigured: true,
			Source: "manual", SourceModelID: "upstream-model", RawJSON: `{"metadata":"` + strings.Repeat("x", 64*1024) + `"}`, SyncedAtMS: &synced,
			ContextTiers: []model.ModelPriceContextTier{
				{ThresholdTokens: 200, Completion: 7, CacheCreation: 0, CompletionConfigured: true, CacheCreationConfigured: true},
				{ThresholdTokens: 100, Prompt: 6, Cache: 0, CacheRead: 8, PromptConfigured: true, CacheConfigured: true, CacheReadConfigured: true},
			},
			ServiceTiers: []model.ModelPriceServiceTier{
				{Mode: "fast", ServiceTier: "priority", Prompt: 9, Completion: 10, Cache: 11, CacheRead: 0, CacheCreation: 12,
					PromptConfigured: true, CompletionConfigured: true, CacheConfigured: true, CacheReadConfigured: true, CacheCreationConfigured: true},
			},
		},
		"unset": {Prompt: 1, Cache: 2},
	}
	if err := repo.ReplaceAll(ctx, book); err != nil {
		t.Fatal(err)
	}
	full, err := repo.LoadAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]model.ModelPrice, len(full))
	for key, price := range full {
		if key != "unset" && (price.RawJSON == "" || price.Source == "" || price.UpdatedAtMS == 0 || price.SyncedAtMS == nil) {
			t.Fatalf("management metadata was lost: %+v", price)
		}
		price.Source, price.SourceModelID, price.RawJSON = "", "", ""
		price.UpdatedAtMS, price.SyncedAtMS = 0, nil
		want[key] = price
	}
	got, err := repo.LoadRates(ctx)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("rates=%+v want=%+v err=%v", got, want, err)
	}
	if model.ModelPriceStructureRevision(full) != model.ModelPriceStructureRevision(got) {
		t.Fatal("rates changed pricing structure")
	}

	// Both entry points use the caller's transaction, including uncommitted rates.
	tx, err := sqlwrite.BeginTx(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `update model_prices set prompt_per_1m = 23 where model = ?`, "unset"); err != nil {
		t.Fatal(err)
	}
	inTx, err := repo.LoadRatesTx(ctx, tx)
	if err != nil || inTx["unset"].Prompt != 23 {
		t.Fatalf("transaction rates=%+v err=%v", inTx, err)
	}
	fullTx, err := repo.LoadAllTx(ctx, tx)
	if err != nil || fullTx["unset"].Prompt != 23 {
		t.Fatalf("management transaction rates=%+v err=%v", fullTx, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, err := repo.LoadRates(ctx)
	if err != nil || !reflect.DeepEqual(after, want) {
		t.Fatalf("rollback or book isolation failed: %+v err=%v", after, err)
	}
}

func TestLoadRatesDoesNotSelectManagementColumns(t *testing.T) {
	ctx := context.Background()
	db, repo := openTestDB(t)
	if err := repo.ReplaceAll(ctx, map[string]model.ModelPrice{"m": {Prompt: 1}}); err != nil {
		t.Fatal(err)
	}
	// Renaming every metadata column makes any accidental projection fail on
	// both engines, rather than merely asserting zero-valued response fields.
	for _, column := range []string{"source", "source_model_id", "raw_json", "updated_at_ms", "synced_at_ms"} {
		if _, err := sqlwrite.ExecContext(ctx, db, `alter table model_prices rename column `+column+` to unused_`+column); err != nil {
			t.Fatal(err)
		}
	}
	prices, err := repo.LoadRates(ctx)
	if err != nil || prices["m"].Prompt != 1 {
		t.Fatalf("narrow rates=%+v err=%v", prices, err)
	}
	if _, err := repo.LoadAll(ctx); err == nil {
		t.Fatal("management query must still select metadata")
	}
}

func TestLoadRatesEmptyCancellationAndChildQueryErrors(t *testing.T) {
	ctx := context.Background()
	db, repo := openTestDB(t)
	if prices, err := repo.LoadRates(ctx); err != nil || prices == nil || len(prices) != 0 {
		t.Fatalf("empty rates=%v err=%v", prices, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := repo.LoadRates(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LoadRatesTx(canceled, tx); !errors.Is(err, context.Canceled) {
		t.Fatalf("transaction cancellation lost: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.LoadRatesTx(ctx, tx); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("closed transaction error lost: %v", err)
	}
	if _, err := sqlwrite.ExecContext(ctx, db, `alter table model_price_service_tiers rename to unavailable_service_tiers`); err != nil {
		t.Fatal(err)
	}
	if prices, err := repo.LoadRates(ctx); err == nil || prices != nil {
		t.Fatalf("child query error became a partial book: %v err=%v", prices, err)
	}
}
