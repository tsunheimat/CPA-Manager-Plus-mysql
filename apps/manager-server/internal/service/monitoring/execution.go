package monitoring

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

var (
	ErrAnalyticsOverloaded       = errors.New("analytics request capacity exhausted")
	ErrAnalyticsDeadlineExceeded = errors.New("analytics deadline exceeded")
)

type Options struct {
	HourlyRollupEnabled bool
	Timeout             time.Duration
	MaxConcurrent       int
	// QueueSize zero uses the default. Set DisableQueue to reject immediately.
	QueueSize        int
	DisableQueue     bool
	AdmissionTimeout time.Duration
}

func OptionsFromConfig(cfg config.Config) Options {
	return Options{HourlyRollupEnabled: cfg.DashboardHourlyRollupEnabled,
		Timeout: cfg.AnalyticsTimeout, MaxConcurrent: cfg.AnalyticsMaxConcurrent,
		QueueSize: cfg.AnalyticsQueueSize, AdmissionTimeout: cfg.AnalyticsAdmissionTimeout,
		DisableQueue: cfg.AnalyticsMaxConcurrent > 0 && cfg.AnalyticsQueueSize == 0}
}

func NewWithOptions(st *store.Store, options Options) *Service {
	s := New(st, options.HourlyRollupEnabled)
	s.execution = newAnalyticsExecution(options, st.DatabasePoolStats().MaxOpenConnections)
	return s
}

type analyticsExecution struct {
	timeout          time.Duration
	admissionTimeout time.Duration
	requests         chan struct{}
	population       chan struct{}
	// Shared across requests, not just a limit per request. Each admitted
	// request has one foreground path; background work can use only two slots.
	queries     chan struct{}
	percentiles chan struct{}
	accepted    atomic.Int64
	rejected    atomic.Int64
	deadlines   atomic.Int64
}

func newAnalyticsExecution(o Options, poolLimit ...int) *analyticsExecution {
	if o.Timeout <= 0 {
		o.Timeout = config.DefaultAnalyticsTimeout
	}
	if o.MaxConcurrent <= 0 {
		o.MaxConcurrent = config.DefaultAnalyticsMaxConcurrent
	}
	o.MaxConcurrent = min(o.MaxConcurrent, 8)
	// Keep at least two pool connections outside analytics when the configured
	// pool permits it. We never expand the pool to meet analytics demand.
	if len(poolLimit) > 0 && poolLimit[0] > 0 {
		o.MaxConcurrent = min(o.MaxConcurrent, max(1, poolLimit[0]-analyticsPrefetchConcurrency-2))
	}
	if o.QueueSize <= 0 {
		o.QueueSize = config.DefaultAnalyticsQueueSize
	}
	o.QueueSize = min(o.QueueSize, 64)
	if o.DisableQueue {
		o.QueueSize = 0
	}
	if o.AdmissionTimeout <= 0 {
		o.AdmissionTimeout = config.DefaultAnalyticsAdmissionTimeout
	}
	return &analyticsExecution{timeout: o.Timeout, admissionTimeout: o.AdmissionTimeout,
		requests: make(chan struct{}, o.MaxConcurrent), population: make(chan struct{}, o.MaxConcurrent+o.QueueSize),
		queries: make(chan struct{}, analyticsPrefetchConcurrency), percentiles: make(chan struct{}, 1)}
}

func (e *analyticsExecution) admit(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case e.population <- struct{}{}:
	default:
		e.rejected.Add(1)
		return nil, ErrAnalyticsOverloaded
	}
	timer := time.NewTimer(e.admissionTimeout)
	defer timer.Stop()
	select {
	case e.requests <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-e.requests
			<-e.population
			return nil, err
		}
		e.accepted.Add(1)
		var once sync.Once
		return func() { once.Do(func() { <-e.requests; <-e.population }) }, nil
	case <-ctx.Done():
		<-e.population
		return nil, ctx.Err()
	case <-timer.C:
		<-e.population
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e.rejected.Add(1)
		return nil, ErrAnalyticsOverloaded
	}
}

func acquireQuery(ctx context.Context, gate chan struct{}) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-gate
			return nil, err
		}
		return func() { <-gate }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Diagnostics is served only on the authenticated monitoring route. All
// counters are process-local, bounded-cardinality operational observations.
type Diagnostics struct {
	Active                  int                `json:"active"`
	Queued                  int                `json:"queued"`
	MaxConcurrent           int                `json:"max_concurrent"`
	QueueSize               int                `json:"queue_size"`
	BackgroundQueriesActive int                `json:"background_queries_active"`
	BackgroundQueriesMax    int                `json:"background_queries_max"`
	PercentilesActive       int                `json:"percentiles_active"`
	TimeoutMS               int64              `json:"timeout_ms"`
	AdmissionTimeoutMS      int64              `json:"admission_timeout_ms"`
	Accepted                int64              `json:"accepted"`
	Rejected                int64              `json:"rejected"`
	Deadlines               int64              `json:"deadlines"`
	PoolInUse               int                `json:"pool_in_use"`
	PoolIdle                int                `json:"pool_idle"`
	PoolMax                 int                `json:"pool_max"`
	PoolWaitCountShared     int64              `json:"pool_wait_count_shared"`
	PoolWaitMSShared        int64              `json:"pool_wait_ms_shared"`
	MySQLAnalyticsEnabled   bool               `json:"mysql_analytics_enabled"`
	MySQLBulkEnabled        bool               `json:"mysql_bulk_enabled"`
	MySQLHourlyEnabled      bool               `json:"mysql_hourly_enabled"`
	Writer                  *writerDiagnostics `json:"writer,omitempty"`
}

type writerDiagnostics struct {
	Waiting      int64  `json:"waiting"`
	Active       int    `json:"active"`
	Acquired     uint64 `json:"acquired"`
	Canceled     uint64 `json:"canceled"`
	WaitMSShared int64  `json:"wait_ms_shared"`
	HoldMSShared int64  `json:"hold_ms_shared"`
}

func (s *Service) Diagnostics() Diagnostics {
	e := s.execution
	active, population := len(e.requests), len(e.population)
	pool := s.store.DatabasePoolStats()
	options := s.store.ConfiguredMySQLOptions()
	result := Diagnostics{Active: active, Queued: max(0, population-active), MaxConcurrent: cap(e.requests), QueueSize: cap(e.population) - cap(e.requests),
		BackgroundQueriesActive: len(e.queries), BackgroundQueriesMax: cap(e.queries), PercentilesActive: len(e.percentiles),
		TimeoutMS: e.timeout.Milliseconds(), AdmissionTimeoutMS: e.admissionTimeout.Milliseconds(),
		Accepted: e.accepted.Load(), Rejected: e.rejected.Load(), Deadlines: e.deadlines.Load(),
		PoolInUse: pool.InUse, PoolIdle: pool.Idle, PoolMax: pool.MaxOpenConnections, PoolWaitCountShared: pool.WaitCount, PoolWaitMSShared: pool.WaitDuration.Milliseconds(),
		MySQLAnalyticsEnabled: options.AnalyticsEnabled, MySQLBulkEnabled: options.BulkEnabled, MySQLHourlyEnabled: options.HourlyEnabled}
	if writer, ok := s.store.MySQLWriteAdmissionStats(); ok {
		result.Writer = &writerDiagnostics{Waiting: writer.Waiting, Active: writer.Active, Acquired: writer.Acquired, Canceled: writer.Canceled,
			WaitMSShared: writer.WaitDuration.Milliseconds(), HoldMSShared: writer.HoldDuration.Milliseconds()}
	}
	return result
}

type requestIDKey struct{}

// WithRequestID accepts a server-generated correlation ID, never SQL inputs.
// Defend the log boundary as well as the HTTP boundary against injected labels.
func WithRequestID(ctx context.Context, id string) context.Context {
	if len(id) > 64 {
		id = ""
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			id = ""
			break
		}
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

func analyticsOutcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrAnalyticsOverloaded):
		return "overloaded"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, ErrAnalyticsDeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, store.ErrUsagePricingCoverageIncomplete), errors.Is(err, store.ErrUsagePricingRecoveryFailed),
		errors.Is(err, usageprojection.ErrRetainedCoverageIncomplete), errors.Is(err, usagepricing.ErrRetainedPricingHistoryIncomplete):
		return "coverage_error"
	default:
		return "query_error"
	}
}

// stage is always a fixed internal label; SQL, raw errors, search strings and
// parameter values must never be logged here.
func logAnalyticsStage(ctx context.Context, stage string, started time.Time, err error) {
	id, _ := ctx.Value(requestIDKey{}).(string)
	log.Printf("analytics request_id=%s stage=%s duration_ms=%d outcome=%s", id, stage, time.Since(started).Milliseconds(), analyticsOutcome(err))
}

func analyticsReadSource[T any](ctx context.Context, stage string, derived func() (T, bool, error), raw func() (T, error)) (rows T, available bool, err error) {
	started := time.Now()
	source := "derived"
	defer func() {
		logAnalyticsStage(ctx, stage, started, err)
		id, _ := ctx.Value(requestIDKey{}).(string)
		log.Printf("analytics request_id=%s stage=%s source=%s outcome=%s", id, stage, source, analyticsOutcome(err))
	}()
	if err = ctx.Err(); err != nil {
		return rows, false, err
	}
	rows, available, err = derived()
	if err != nil || available {
		return rows, available, err
	}
	if err = ctx.Err(); err != nil {
		return rows, false, err
	}
	source = "raw_unavailable_or_unsupported"
	rows, err = raw()
	return rows, false, err
}

func (s *Service) runAnalytics(ctx context.Context, req Request) (response Response, err error) {
	started := time.Now()
	poolBefore := s.store.DatabasePoolStats()
	ctx, cancel := context.WithTimeout(ctx, s.execution.timeout)
	defer cancel()
	defer func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			err = errors.Join(ErrAnalyticsDeadlineExceeded, context.DeadlineExceeded)
			response = Response{}
			s.execution.deadlines.Add(1)
		} else if err == nil && ctx.Err() != nil {
			err = ctx.Err()
			response = Response{}
		}
		logAnalyticsStage(ctx, "request", started, err)
		poolAfter := s.store.DatabasePoolStats()
		id, _ := ctx.Value(requestIDKey{}).(string)
		log.Printf("analytics request_id=%s pool_in_use=%d pool_idle=%d pool_wait_count_delta_shared=%d pool_wait_ms_delta_shared=%d timeline_rows=%d percentile_rows=%d event_rows=%d coverage_limited=%t", id,
			poolAfter.InUse, poolAfter.Idle, poolAfter.WaitCount-poolBefore.WaitCount, (poolAfter.WaitDuration - poolBefore.WaitDuration).Milliseconds(),
			len(response.Timeline), percentileRowCount(response.Percentiles), eventRowCount(response.Events), response.Coverage != nil)
	}()
	release, err := s.execution.admit(ctx)
	logAnalyticsStage(ctx, "admission", started, err)
	if err != nil {
		return Response{}, err
	}
	defer release()
	plan := planAnalytics(req.Include)
	if !plan.priceSnapshot {
		return s.analytics(ctx, req, plan)
	}
	gateStarted := time.Now()
	entered := false
	err = s.store.WithModelPriceSnapshotContext(ctx, func() error {
		entered = true
		logAnalyticsStage(ctx, "price_gate", gateStarted, nil)
		var queryErr error
		response, queryErr = s.analytics(ctx, req, plan)
		return queryErr
	})
	if !entered {
		logAnalyticsStage(ctx, "price_gate", gateStarted, err)
	}
	return response, err
}

func eventRowCount(value *EventsResponse) int {
	if value == nil {
		return 0
	}
	return len(value.Items)
}

func percentileRowCount(value *PercentilesResponse) int {
	if value == nil || value.Timeline == nil {
		return 0
	}
	return len(*value.Timeline)
}
