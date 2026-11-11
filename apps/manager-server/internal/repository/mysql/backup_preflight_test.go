package mysql

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
)

func preflightStream(t *testing.T, version int) (BackupHeader, [][][]byte) {
	t.Helper()
	tables, err := versionedBackupLayout(version)
	if err != nil {
		t.Fatal(err)
	}
	var rows [][][]byte
	for i := range tables {
		if tables[i].Name == "manager_schema_version" {
			tables[i].Rows = 1
			rows = append(rows, [][]byte{[]byte("1"), []byte(strconv.Itoa(version))})
		}
		if tables[i].Name == "settings" {
			tables[i].Rows = 1
			rows = append(rows, [][]byte{[]byte("encrypted"), []byte("opaque-value"), []byte("0")})
		}
	}
	return BackupHeader{Version: 1, SchemaVersion: version, Tables: tables}, rows
}

func encodePreflightStream(t *testing.T, h BackupHeader, rows [][][]byte) []byte {
	t.Helper()
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	if err := enc.Encode(h); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := enc.Encode(row); err != nil {
			t.Fatal(err)
		}
	}
	return b.Bytes()
}

func TestRestorePreparationRejectsCorruptionBeforeDestinationAccess(t *testing.T) {
	for _, version := range []int{1, 2} {
		for _, kind := range []string{"missing-marker", "wrong-marker", "width", "missing-row", "extra-row", "truncated", "base64", "future-schema", "column-layout", "trailing-garbage"} {
			t.Run(strconv.Itoa(version)+"/"+kind, func(t *testing.T) {
				t.Setenv("TMPDIR", t.TempDir())
				h, rows := preflightStream(t, version)
				switch kind {
				case "missing-marker":
					for i := range h.Tables {
						if h.Tables[i].Name == "manager_schema_version" {
							h.Tables[i].Rows = 0
						}
					}
					rows = rows[1:]
				case "wrong-marker":
					rows[0][1] = []byte("99")
				case "width":
					rows[1] = rows[1][:1]
				case "missing-row":
					rows = rows[:1]
				case "extra-row":
					rows = append(rows, rows[1])
				case "future-schema":
					h.SchemaVersion = 99
				case "column-layout":
					h.Tables[0].Columns[0] = "unknown"
				}
				input := encodePreflightStream(t, h, rows)
				switch kind {
				case "truncated":
					input = input[:len(input)-3]
				case "base64":
					input = bytes.Replace(input, []byte(`"b3BhcXVlLXZhbHVl"`), []byte(`"!invalid!"`), 1)
				case "trailing-garbage":
					input = append(input, '{')
				}
				// A nil DB makes any premature destination inspection a hard failure.
				if restore, err := BeginRestore(context.Background(), nil, bytes.NewReader(input)); err == nil || restore != nil {
					t.Fatalf("invalid input accepted: %v", err)
				}
				entries, err := os.ReadDir(os.TempDir())
				if err != nil || len(entries) != 0 {
					t.Fatalf("failed preflight leaked spool: %v %v", entries, err)
				}
			})
		}
	}
}

func TestPreparedRestorePrivateSpoolSurvivesInputChangeAndCloses(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	h, rows := preflightStream(t, 2)
	input := encodePreflightStream(t, h, rows)
	prepared, err := PrepareRestore(context.Background(), bytes.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	info, err := prepared.rows.Stat()
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("non-private spool: %v %v", info, err)
	}
	for i := range input {
		input[i] = '!'
	}
	db, backend := restoreDB(t)
	restored, err := prepared.BeginRestore(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	restored.Close()
	if !backend.rolledBack || !strings.Contains(strings.Join(backend.writes, "\n"), "insert into `settings`") {
		t.Fatal("validated spool not restored transactionally")
	}
	path := prepared.rows.Name()
	prepared.Close()
	prepared.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("spool not removed: %v", err)
	}
	if _, err := prepared.BeginRestore(context.Background(), db); err == nil {
		t.Fatal("closed prepared restore reused")
	}
}
