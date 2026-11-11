package deadletter

import (
	"context"
	"database/sql"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/sqlwrite"
)

type Repository interface {
	Insert(ctx context.Context, payload string, errText string) error
	Count(ctx context.Context) (int64, error)
}

type repository struct {
	db *sql.DB
}

func New(db *sql.DB) Repository {
	return &repository{db: db}
}

func (r *repository) Insert(ctx context.Context, payload string, errText string) error {
	_, err := sqlwrite.ExecContext(
		ctx, r.db,
		`insert into dead_letter_events(payload, error, created_at_ms) values(?, ?, ?)`,
		payload,
		errText,
		time.Now().UnixMilli(),
	)
	return err
}

func (r *repository) Count(ctx context.Context) (int64, error) {
	var count int64
	if err := r.db.QueryRowContext(ctx, `select count(*) from dead_letter_events`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
