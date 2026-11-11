package response

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

func TestAnalyticsErrorStableStatusAndCodes(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("wrapped: %w", monitoring.ErrAnalyticsOverloaded), 503, "analytics_overloaded"},
		{monitoring.ErrAnalyticsDeadlineExceeded, 504, "analytics_deadline_exceeded"},
		{context.DeadlineExceeded, 504, "analytics_deadline_exceeded"},
		{context.Canceled, 408, "analytics_canceled"},
		{store.ErrUsagePricingCoverageIncomplete, 503, "analytics_coverage_incomplete"},
		{errors.New("password=private database SQL"), 500, "analytics_query_failed"},
	} {
		t.Run(tc.code, func(t *testing.T) {
			w := httptest.NewRecorder()
			AnalyticsError(w, tc.err, "server-request-123")
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			var result map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if result["code"] != tc.code || result["request_id"] != "server-request-123" {
				t.Fatalf("body=%v", result)
			}
			if strings.Contains(w.Body.String(), "private") {
				t.Fatal("leaked query details")
			}
			if tc.code == "analytics_overloaded" && (w.Header().Get("Retry-After") != "1" || result["retry_after_ms"] != float64(1000)) {
				t.Fatal("missing overload retry guidance")
			}
			if w.Header().Get("Content-Type") != "application/json" || w.Code == http.StatusOK {
				t.Fatal("not a structured error")
			}
		})
	}
}
