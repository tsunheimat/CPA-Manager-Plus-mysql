package managerdatasnapshot

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	mysqlrepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/mysql"
)

// Read the checked-in layouts only to build offline fixture streams; the actual
// preflight independently validates them with the repository's schema parser.
func snapshotPreflightHeader(t *testing.T, version int) mysqlrepo.BackupHeader {
	t.Helper()
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(source), "../../repository/mysql")
	paths := []string{filepath.Join(root, "schema.sql")}
	if version == 2 {
		paths = append(paths, filepath.Join(root, "migrations/002_monitoring_hourly.sql"))
	}
	h := mysqlrepo.BackupHeader{Version: 1, SchemaVersion: version, Tables: []mysqlrepo.BackupTable{{Name: "manager_schema_version", Columns: []string{"id", "version"}, Rows: 1}}}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var table *mysqlrepo.BackupTable
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "create table if not exists `") {
				name := strings.Split(line, "`")[1]
				h.Tables = append(h.Tables, mysqlrepo.BackupTable{Name: name})
				table = &h.Tables[len(h.Tables)-1]
			} else if strings.HasPrefix(line, ")") {
				table = nil
			} else if table != nil && strings.HasPrefix(line, "`") && !strings.Contains(line, " generated always as ") {
				table.Columns = append(table.Columns, strings.Split(line, "`")[1])
			}
		}
	}
	sort.Slice(h.Tables, func(i, j int) bool { return h.Tables[i].Name < h.Tables[j].Name })
	return h
}

func snapshotPreflightData(t *testing.T, h mysqlrepo.BackupHeader, row [][]byte, suffix string) []byte {
	t.Helper()
	var raw bytes.Buffer
	enc := json.NewEncoder(&raw)
	if err := enc.Encode(h); err != nil {
		t.Fatal(err)
	}
	if row != nil {
		if err := enc.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	raw.WriteString(suffix)
	var zipped bytes.Buffer
	gz := gzip.NewWriter(&zipped)
	if _, err := gz.Write(raw.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return zipped.Bytes()
}

func snapshotPreflightArchive(t *testing.T, unsafe bool) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	name := "segment.jsonl.gz"
	if unsafe {
		name = "../escape"
	}
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestMySQLDispatcherPreflightsBeforeDestinationUpgrade(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, kind := range []string{"valid", "missing-directory", "wrong-driver", "missing-entry", "mysql-data-checksum", "data-key-checksum", "archives-checksum", "format", "future-layout", "columns", "marker", "row-width", "row-count", "trailing-data", "gzip-trailer", "archive-trailer", "unsafe-archive", "pending-journal", "lost-ownership"} {
			t.Run(strconv.Itoa(version)+"/"+kind, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				spools := filepath.Join(root, "spools")
				if err := os.Mkdir(spools, 0700); err != nil {
					t.Fatal(err)
				}
				t.Setenv("TMPDIR", spools)
				cfg := config.Config{DataKeyPath: filepath.Join(root, "data.key"), UsageArchiveDir: filepath.Join(root, "archives")}
				writeTestFile(t, cfg.DataKeyPath, "live-key", 0600)
				if err := os.Mkdir(cfg.UsageArchiveDir, 0700); err != nil {
					t.Fatal(err)
				}
				liveArchive := filepath.Join(cfg.UsageArchiveDir, "live.gz")
				writeTestFile(t, liveArchive, "live-archive", 0600)
				snapshot := filepath.Join(root, "snapshot")
				if err := os.Mkdir(snapshot, 0700); err != nil {
					t.Fatal(err)
				}
				h := snapshotPreflightHeader(t, version)
				row := [][]byte{[]byte("1"), []byte(strconv.Itoa(version))}
				suffix := ""
				switch kind {
				case "format":
					h.Version = 99
				case "future-layout":
					h.SchemaVersion = 99
				case "columns":
					h.Tables[0].Columns[0] = "unknown"
				case "marker":
					row[1] = []byte("99")
				case "row-width":
					row = row[:1]
				case "row-count":
					row = nil
				case "trailing-data":
					suffix = "{}"
				}
				data := snapshotPreflightData(t, h, row, suffix)
				archive := snapshotPreflightArchive(t, kind == "unsafe-archive")
				if kind == "gzip-trailer" {
					data[len(data)-8] ^= 1 // checksum in manifest is still correct
				}
				if kind == "archive-trailer" {
					archive[len(archive)-8] ^= 1
				}
				m := manifest{Version: 2, Driver: "mysql", Files: map[string]manifestEntry{}}
				for name, payload := range map[string][]byte{"mysql-data": data, "data-key": []byte("snapshot-key"), "archives": archive} {
					entry, err := writeSnapshotPayload(ctx, filepath.Join(snapshot, name), func(w io.Writer) error { _, err := w.Write(payload); return err })
					if err != nil {
						t.Fatal(err)
					}
					if kind == name+"-checksum" {
						entry.SHA256 = strings.Repeat("0", 64)
					}
					m.Files[name] = entry
				}
				if kind == "wrong-driver" {
					m.Driver = "sqlite"
				}
				if kind == "missing-entry" {
					delete(m.Files, "data-key")
				}
				manifestJSON, err := json.Marshal(m)
				if err != nil {
					t.Fatal(err)
				}
				writeTestFile(t, filepath.Join(snapshot, "manifest.json"), string(manifestJSON), 0600)
				if kind == "missing-directory" {
					snapshot = filepath.Join(root, "absent")
				}
				if kind == "pending-journal" {
					writeTestFile(t, pendingPath(cfg), "recovery-evidence", 0600)
				}
				db := sql.OpenDB(&journalConnector{}) // read-only fingerprint fake
				defer db.Close()
				upgradeReached := false
				upgradeStopped := errors.New("stop at destination upgrade boundary")
				var stdout bytes.Buffer
				err = dispatchMySQL(ctx, options{Action: "restore", SnapshotDir: snapshot}, cfg, db, &stdout, func() error {
					if kind == "lost-ownership" {
						return errors.New("lock lost")
					}
					return nil
				}, func() error {
					upgradeReached = true
					return upgradeStopped
				})
				if err == nil || upgradeReached != (kind == "valid") || errors.Is(err, upgradeStopped) != (kind == "valid") {
					t.Fatalf("upgrade=%v error=%v", upgradeReached, err)
				}
				if stdout.Len() != 0 {
					t.Fatalf("failed restore reported success: %s", stdout.String())
				}
				requireTestFile(t, cfg.DataKeyPath, "live-key")
				requireTestFile(t, liveArchive, "live-archive")
				if kind == "pending-journal" {
					requireTestFile(t, pendingPath(cfg), "recovery-evidence")
				} else if _, err := os.Stat(pendingPath(cfg)); !os.IsNotExist(err) {
					t.Fatalf("preflight published a journal: %v", err)
				}
				stages, err := filepath.Glob(filepath.Join(root, ".cpamp-restore-*"))
				if err != nil || len(stages) != 0 {
					t.Fatalf("preflight leaked file stages: %v %v", stages, err)
				}
				entries, err := os.ReadDir(spools)
				if err != nil || len(entries) != 0 {
					t.Fatalf("preflight leaked row spool: %v %v", entries, err)
				}
			})
		}
	}
}
