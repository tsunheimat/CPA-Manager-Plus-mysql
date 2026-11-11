package apikeyalias

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func TestAliasWritesPreserveBatchAtomicityAndPostCommitAdmission(t *testing.T) {
	db, err := testdb.Open(t, filepath.Join(t.TempDir(), "aliases.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := New(db)
	ctx := context.Background()
	a, b, c := strings.Repeat("a", 64), strings.Repeat("b", 64), strings.Repeat("c", 64)
	if err := repo.UpsertMany(ctx, []model.APIKeyAlias{{APIKeyHash: a, Alias: "Primary"}}, nil, false); err != nil {
		t.Fatal(err)
	}
	// The first row is valid but the second conflicts with a stored alias.
	// The already-executed first write must roll back along with the batch.
	if err := repo.UpsertMany(ctx, []model.APIKeyAlias{{APIKeyHash: b, Alias: "Secondary"}, {APIKeyHash: c, Alias: "PRIMARY"}}, nil, false); err == nil {
		t.Fatal("alias collision accepted")
	}
	aliases, err := repo.LoadAll(ctx)
	if err != nil || len(aliases) != 1 || aliases[0].APIKeyHash != a {
		t.Fatalf("partial alias batch committed: %+v %v", aliases, err)
	}
	// A completed/rolled-back transaction must not retain the writer permit.
	if err := repo.UpsertMany(ctx, []model.APIKeyAlias{{APIKeyHash: b, Alias: "Primary"}}, []string{b}, true); err != nil {
		t.Fatalf("orphan replacement after rollback: %v", err)
	}
	aliases, err = repo.LoadAll(ctx)
	if err != nil || len(aliases) != 1 || aliases[0].APIKeyHash != b {
		t.Fatalf("orphan alias replacement: %+v %v", aliases, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := repo.Delete(canceled, b); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled alias delete: %v", err)
	}
	if err := repo.Delete(ctx, b); err != nil {
		t.Fatalf("post-commit autocommit delete: %v", err)
	}
	aliases, err = repo.LoadAll(ctx)
	if err != nil || len(aliases) != 0 {
		t.Fatalf("remaining aliases: %+v %v", aliases, err)
	}
}
