package setup

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestInfoAdvertisesConfiguredAnalyticsSections(t *testing.T) {
	service, _, _ := newSetupService(t, nil, nil)
	for _, enabled := range []bool{true, false} {
		service.cfg.AnalyticsProgressiveEnabled = enabled
		info, err := service.Info(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(info)
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			if info.AnalyticsSectionsVersion != 1 || !strings.Contains(string(data), `"analyticsSectionsVersion":1`) {
				t.Fatalf("capability missing: %s", data)
			}
		} else if info.AnalyticsSectionsVersion != 0 || strings.Contains(string(data), "analyticsSectionsVersion") {
			t.Fatalf("disabled capability advertised: %s", data)
		}
	}
}
