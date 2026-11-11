package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestV1RestoreRecoversStaleCachedSequenceFromArchiveEvidence(t *testing.T) {
	input := historicalBackup(t)
	dec := json.NewDecoder(bytes.NewReader(input))
	var header BackupHeader
	if err := dec.Decode(&header); err != nil {
		t.Fatal(err)
	}
	for i := range header.Tables {
		if header.Tables[i].Name == "usage_events" {
			header.Tables[i].NextID = 1 // v1's cached information_schema observation
		}
	}
	var stale bytes.Buffer
	if err := json.NewEncoder(&stale).Encode(header); err != nil {
		t.Fatal(err)
	}
	stale.Write(input[dec.InputOffset():])
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	db, backend := restoreDB(t)
	restore, err := BeginRestore(context.Background(), db, &stale)
	if err != nil {
		t.Fatal(err)
	}
	defer restore.Close()
	if !strings.Contains(strings.Join(backend.writes, "\n"), "alter table `usage_events` auto_increment=7001") {
		t.Fatal("all-archived legacy restore could reuse event IDs below its coverage")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("legacy stream spool survived BeginRestore: %v %v", files, err)
	}
}

func TestV1RestorePreflightFailureRemovesPrivateSpoolBeforeDestinationAccess(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	input := historicalBackup(t)
	// A complete header followed by a truncated body must fail before even
	// consulting the destination. nil db makes that ordering observable.
	if _, err := BeginRestore(context.Background(), nil, bytes.NewReader(input[:len(input)-12])); err == nil {
		t.Fatal("truncated historical backup accepted")
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("failed legacy spool leaked: %v %v", files, err)
	}
}

func TestLegacySequenceEvidenceIncludesRetainedCoverageButNotOtherIDs(t *testing.T) {
	for _, pair := range [][2]string{
		{"usage_events", "id"}, {"usage_archive_event_refs", "raw_event_id"},
		{"usage_monitoring_event_projection_v1", "event_id"}, {"usage_archive_segments", "last_event_id"},
		{"usage_pricing_rollup_state", "coverage_event_id"}, {"usage_monitoring_rollup_state", "backfill_last_event_id"},
		{"usage_hourly_aggregate_state", "target_event_id"}, {"usage_rollup_checkpoints", "last_event_id"},
	} {
		if !legacyEventSequenceColumn(pair[0], pair[1]) {
			t.Errorf("lost durable event high-water evidence: %v", pair)
		}
	}
	for _, pair := range [][2]string{{"usage_archive_runs", "id"}, {"quota_snapshots", "id"}, {"usage_events", "timestamp_ms"}} {
		if legacyEventSequenceColumn(pair[0], pair[1]) {
			t.Errorf("unrelated value treated as event high-water: %v", pair)
		}
	}
}
