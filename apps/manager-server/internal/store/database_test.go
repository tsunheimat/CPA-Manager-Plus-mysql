package store

import (
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/security"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil/testdb"
)

func openPortableTestStore(t *testing.T, path string, protector ...*security.Protector) (*Store, error) {
	t.Helper()
	db, err := testdb.Open(t, path)
	if err != nil {
		return nil, err
	}
	return New(db, protector...), nil
}
