package monitoring

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"
)

func TestAnalyticsQueryStagesDistinguishAdmissionFromRead(t *testing.T) {
	for _, phase := range []string{"success", "read_error", "shared_admission_deadline", "percentile_admission_deadline"} {
		t.Run(phase, func(t *testing.T) {
			var output bytes.Buffer
			previous := log.Writer()
			log.SetOutput(&output)
			defer log.SetOutput(previous)
			blocked := strings.Contains(phase, "admission_deadline")
			timeout := 2 * time.Second
			if blocked {
				timeout = 30 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			group := newAnalyticsQueryGroup(ctx, 1)
			defer group.Close()
			group.execution = newAnalyticsExecution(Options{})
			stage := "heatmap"
			if phase == "shared_admission_deadline" {
				for range cap(group.execution.queries) {
					group.execution.queries <- struct{}{}
				}
			} else if phase == "percentile_admission_deadline" {
				stage = "percentiles"
				group.execution.percentiles <- struct{}{}
			}
			called := false
			group.GoStage(stage, func(context.Context) error {
				called = true
				if phase == "read_error" {
					return errors.New("password=do-not-log search=private")
				}
				return nil
			})
			err := group.Wait()
			if blocked {
				if called || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("blocked read called=%t err=%v", called, err)
				}
			} else if !called || (err != nil) != (phase == "read_error") {
				t.Fatalf("read called=%t err=%v", called, err)
			}
			logs := output.String()
			for _, suffix := range []string{"_admission", "_prefetch"} {
				if strings.Count(logs, "stage="+stage+suffix+" ") != 1 {
					t.Fatalf("expected exactly one %s observation: %s", suffix, logs)
				}
			}
			if strings.Contains(logs, "stage="+stage+"_read ") == blocked {
				t.Fatalf("read observation must exist only after admission: %s", logs)
			}
			if blocked && strings.Count(logs, "outcome=deadline") != 2 {
				t.Fatalf("admission deadline misattributed to read: %s", logs)
			}
			if phase == "read_error" && strings.Count(logs, "outcome=query_error") != 2 {
				t.Fatalf("missing read/prefetch error classification: %s", logs)
			}
			if strings.Contains(logs, "do-not-log") || strings.Contains(logs, "private") {
				t.Fatalf("query error leaked private inputs: %s", logs)
			}
			if len(group.semaphore) != 0 {
				t.Fatal("query admission leaked local slot")
			}
		})
	}
}
