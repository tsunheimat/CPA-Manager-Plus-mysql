package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/app"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/modelprice"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/security"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/adminauth"
	monitoringsvc "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/monitoring"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
)

const analyticsAdminKey = "test-analytics-admin"

func analyticsTestHandler(t *testing.T, options monitoringsvc.Options) (*Handler, *store.Store) {
	t.Helper()
	st := newHandlerTestStore(t)
	credential, err := security.NewAdminCredential(analyticsAdminKey, "test")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SaveAdminCredential(context.Background(), credential); err != nil {
		t.Fatal(err)
	}
	return &Handler{App: &app.Context{AdminAuthService: adminauth.New(config.Config{}, st), MonitoringService: monitoringsvc.NewWithOptions(st, options)}}, st
}

func analyticsHTTPRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v0/management/monitoring/analytics", bytes.NewBufferString(`{"from_ms":1,"to_ms":2}`))
	r.Header.Set("Authorization", "Bearer "+analyticsAdminKey)
	return r
}

func TestAnalyticsControllerValidatesProfile(t *testing.T) {
	for _, profile := range []string{"", "core", "full"} {
		if err := validateRequest(monitoringsvc.Request{FromMS: 1, ToMS: 2, Include: monitoringsvc.Include{TimelineProfile: profile}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := validateRequest(monitoringsvc.Request{FromMS: 1, ToMS: 2, Include: monitoringsvc.Include{TimelineProfile: "typo"}}); err == nil {
		t.Fatal("accepted unsupported profile")
	}
}

func TestAnalyticsControllerAuthenticatedDiagnostics(t *testing.T) {
	h, _ := analyticsTestHandler(t, monitoringsvc.Options{Timeout: time.Second})
	for _, authorized := range []bool{false, true} {
		r := httptest.NewRequest(http.MethodGet, "/v0/management/monitoring/analytics/diagnostics", nil)
		if authorized {
			r.Header.Set("Authorization", "Bearer "+analyticsAdminKey)
		}
		w := httptest.NewRecorder()
		h.Handle(w, r)
		if !authorized && w.Code == http.StatusOK {
			t.Fatal("diagnostics exposed without auth")
		}
		if authorized && (w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"timeout_ms":1000`))) {
			t.Fatalf("diagnostics status=%d body=%s", w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	h.Handle(w, analyticsHTTPRequest())
	if w.Code != http.StatusOK {
		t.Fatalf("normal request failed: %s", w.Body.String())
	}
	if w.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request ID")
	}
}

type heldPriceRepository struct {
	modelprice.Repository
	entered chan struct{}
	release <-chan struct{}
}

func (r *heldPriceRepository) ReplaceAll(ctx context.Context, _ map[string]store.ModelPrice) error {
	close(r.entered)
	select {
	case <-r.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestAnalyticsControllerReturns503And504WithRequestCorrelation(t *testing.T) {
	h, st := analyticsTestHandler(t, monitoringsvc.Options{Timeout: 500 * time.Millisecond, MaxConcurrent: 1, DisableQueue: true})
	release := make(chan struct{})
	prices := &heldPriceRepository{Repository: st.ModelPrices, entered: make(chan struct{}), release: release}
	st.ModelPrices = prices
	writerDone := make(chan error, 1)
	go func() { writerDone <- st.SaveModelPrices(context.Background(), nil) }()
	<-prices.entered
	defer func() {
		close(release)
		if err := <-writerDone; err != nil {
			t.Error(err)
		}
	}()
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { w := httptest.NewRecorder(); h.Handle(w, analyticsHTTPRequest()); first <- w }()
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for h.App.MonitoringService.Diagnostics().Active == 0 {
		select {
		case <-deadline:
			t.Fatal("analytics request never admitted")
		case <-ticker.C:
		}
	}
	overload := httptest.NewRecorder()
	h.Handle(overload, analyticsHTTPRequest())
	if overload.Code != 503 || overload.Header().Get("Retry-After") != "1" {
		t.Fatalf("overload=%d %s", overload.Code, overload.Body.String())
	}
	var timedOut *httptest.ResponseRecorder
	select {
	case timedOut = <-first:
	case <-deadline:
		t.Fatal("deadline did not bound price wait")
	}
	if timedOut.Code != 504 {
		t.Fatalf("timeout=%d %s", timedOut.Code, timedOut.Body.String())
	}
	for _, tc := range []struct {
		w    *httptest.ResponseRecorder
		code string
	}{{overload, "analytics_overloaded"}, {timedOut, "analytics_deadline_exceeded"}} {
		var body map[string]any
		if err := json.Unmarshal(tc.w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if body["code"] != tc.code || body["request_id"] == "" || body["request_id"] != tc.w.Header().Get("X-Request-ID") {
			t.Fatalf("uncorrelated error=%v", body)
		}
	}
	if h.App.MonitoringService.Diagnostics().Active != 0 {
		t.Fatal("timed out request retained admission")
	}
}
