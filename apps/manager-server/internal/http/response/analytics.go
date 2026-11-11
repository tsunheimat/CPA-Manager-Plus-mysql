package response

import (
	"context"
	"errors"
	"net/http"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usagepricing"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/usageprojection"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

// AnalyticsError preserves typed terminal failures without exposing SQL,
// connection details or bound values to clients. The request ID ties the
// response to bounded-cardinality server diagnostics.
func AnalyticsError(w http.ResponseWriter, err error, requestID string) {
	status, code, message := http.StatusInternalServerError, "analytics_query_failed", "Analytics query failed"
	body := map[string]any{"request_id": requestID}
	switch {
	case errors.Is(err, monitoring.ErrAnalyticsOverloaded):
		status, code, message = http.StatusServiceUnavailable, "analytics_overloaded", "Analytics capacity is busy; retry shortly"
		w.Header().Set("Retry-After", "1")
		body["retry_after_ms"] = 1000
	case errors.Is(err, monitoring.ErrAnalyticsDeadlineExceeded), errors.Is(err, context.DeadlineExceeded):
		status, code, message = http.StatusGatewayTimeout, "analytics_deadline_exceeded", "Analytics request deadline exceeded"
	case errors.Is(err, context.Canceled):
		status, code, message = http.StatusRequestTimeout, "analytics_canceled", "Analytics request canceled"
	case errors.Is(err, store.ErrUsagePricingCoverageIncomplete), errors.Is(err, store.ErrUsagePricingRecoveryFailed),
		errors.Is(err, usageprojection.ErrRetainedCoverageIncomplete), errors.Is(err, usagepricing.ErrRetainedPricingHistoryIncomplete):
		status, code, message = http.StatusServiceUnavailable, "analytics_coverage_incomplete", "Historical analytics coverage is unavailable"
	}
	body["code"], body["error"] = code, message
	JSON(w, status, body)
}
