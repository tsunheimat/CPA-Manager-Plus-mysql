package testdb

import (
	"context"
	"database/sql"
	"errors"
	"testing"
)

func TestFixtureAdminUsesOnlyRegisteredIsolatedSchema(t *testing.T) {
	// No native connections are made. In particular the environment base DSN
	// must never become the fixture target when a runtime pool is unregistered.
	t.Setenv("CPA_TEST_MYSQL_DSN", "must-not-be-used")
	runtime, admin := &sql.DB{}, &sql.DB{}
	paths.Lock()
	paths.pools[runtime] = "isolated-test-schema"
	paths.Unlock()
	t.Cleanup(func() {
		paths.Lock()
		delete(paths.pools, runtime)
		paths.Unlock()
	})
	calls := 0
	opener := func(ctx context.Context, dsn string) (*sql.DB, error) {
		calls++
		if dsn != "isolated-test-schema" || ctx == nil {
			t.Fatalf("wrong native test target: %q", dsn)
		}
		return admin, nil
	}
	got, err := openFixtureAdmin(context.Background(), runtime, opener)
	if err != nil || got != admin || got == runtime || calls != 1 {
		t.Fatalf("native admin selection=%p calls=%d err=%v", got, calls, err)
	}
	if _, err := openFixtureAdmin(context.Background(), &sql.DB{}, opener); err == nil || calls != 1 {
		t.Fatalf("unregistered pool silently used a base DSN: calls=%d err=%v", calls, err)
	}
	failure := errors.New("native connection failed")
	if _, err := openFixtureAdmin(context.Background(), runtime, func(context.Context, string) (*sql.DB, error) { return nil, failure }); !errors.Is(err, failure) {
		t.Fatalf("native fixture failure hidden: %v", err)
	}
}
