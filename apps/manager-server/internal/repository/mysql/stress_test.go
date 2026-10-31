package mysql_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

func TestMySQLConcurrentIngestionDeduplicationAndRestart(t *testing.T) {
	dsn := testdb.DSN(t, t.TempDir())
	ctx := context.Background()
	db, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(db)
	defer st.Close()
	const workers, batches, batchSize = 8, 10, 64
	started := time.Now()
	var wg sync.WaitGroup
	failures := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				events := make([]usage.Event, batchSize)
				for i := range events {
					n := (w*batches+b)*batchSize + i
					sum := sha256.Sum256([]byte(fmt.Sprint(n)))
					ts := int64(1800000000000 + n)
					events[i] = usage.Event{EventHash: fmt.Sprintf("%x", sum), TimestampMS: ts, Timestamp: time.UnixMilli(ts).UTC().Format(time.RFC3339Nano), Model: "model-你好", TotalTokens: 3, InputTokens: 1, OutputTokens: 2, CreatedAtMS: ts}
				}
				for repeat := 0; repeat < 2; repeat++ {
					if _, err := st.InsertEvents(ctx, events); err != nil {
						failures <- err
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := mysqlrepo.Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var count, tokens, ledger int64
	if err := reopened.QueryRow("select count(*),sum(total_tokens) from usage_events").Scan(&count, &tokens); err != nil {
		t.Fatal(err)
	}
	if err := reopened.QueryRow("select count(*) from usage_event_identity_ledger").Scan(&ledger); err != nil {
		t.Fatal(err)
	}
	want := int64(workers * batches * batchSize)
	if count != want || ledger != want || tokens != want*3 {
		t.Fatalf("count=%d ledger=%d tokens=%d want=%d", count, ledger, tokens, want)
	}
	t.Logf("%d unique events, %d duplicate attempts, %d concurrent producers, elapsed %s", want, want, workers, time.Since(started))
}
