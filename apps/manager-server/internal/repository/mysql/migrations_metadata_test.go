package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
)

const metadataAccountTable = "usage_monitoring_account_hourly_rollups_v1"

// These information_schema rows are literal fixtures, not schemaTables output.
// They cover the representations already supported by the comparator, not a
// claim about what a particular native server returns. The native round-trip
// test records that evidence independently, before startup validation can fail.
func hourlyMetadataFixtures() map[[2]string][]driver.Value {
	return map[[2]string][]driver.Value{
		{metadataAccountTable, "auth_account_id_snapshot"}: {
			"auth_account_id_snapshot", "longtext", "NO", "(_utf8mb4'')", "", "DEFAULT_GENERATED", "utf8mb4_0900_bin",
		},
		{"usage_monitoring_api_key_hourly_rollups_v1", "auth_account_id_snapshot"}: {
			"auth_account_id_snapshot", "longtext", "NO", "(_utf8mb4'')", "", "DEFAULT_GENERATED", "utf8mb4_0900_bin",
		},
		{"usage_monitoring_selector_hourly_rollups_v1", "model_format_revision"}: {
			"model_format_revision", "longtext", "NO", "(_utf8mb4'')", "", "DEFAULT_GENERATED", "utf8mb4_0900_bin",
		},
		{metadataAccountTable, "structure_revision"}: {
			"structure_revision", "longtext", "NO", nil, "", "", "utf8mb4_0900_bin",
		},
		{metadataAccountTable, "calls"}: {
			"calls", "bigint", "NO", "0", "", "", "",
		},
		{metadataAccountTable, "_revision_hash"}: {
			"_revision_hash", "binary(32)", "YES", nil, "(unhex(sha2(`structure_revision`,256)))", "STORED GENERATED", "",
		},
	}
}

func TestHourlySchemaAcceptsLiteralMetadata(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"empty", ""},
		{"quoted", "''"},
		{"parenthesized", "('')"},
		{"nested_parentheses", "((('')))"},
		{"utf8mb3", "_utf8mb3''"},
		{"utf8mb4", "_utf8mb4''"},
		{"parenthesized_utf8mb3", "(_utf8mb3'')"},
		{"parenthesized_utf8mb4", "(_utf8mb4'')"},
	} {
		for _, extra := range []string{"", "DEFAULT_GENERATED"} {
			t.Run(tc.name+"/extra="+extra, func(t *testing.T) {
				conn, b := migrationDB(t)
				b.columnMetadata = hourlyMetadataFixtures()
				for _, row := range b.columnMetadata {
					if row[1] == "longtext" && row[3] != nil {
						row[3], row[5] = tc.value, extra
					}
				}
				ctx := context.Background()
				if err := upgradeSchema(ctx, conn, 1); err != nil {
					t.Fatal(err)
				}
				if b.version != 2 || b.states["stats_hourly_v1"] != "pending" || b.states["metadata_hourly_v1"] != "pending" {
					t.Fatalf("successful validation did not publish seeds and marker: version=%d states=%v", b.version, b.states)
				}
				b.states["stats_hourly_v1"] = "complete:retained-hourly-progress"
				before, creates := maps.Clone(b.states), b.creates
				if err := upgradeSchema(ctx, conn, 2); err != nil {
					t.Fatal(err)
				}
				if b.creates != creates || !reflect.DeepEqual(b.states, before) {
					t.Fatal("v2 metadata validation recreated tables or reset progress")
				}
			})
		}
	}
}

func assertHourlyMetadataRejected(t *testing.T, column string, field int, value driver.Value, attribute string) {
	t.Helper()
	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			conn, b := migrationDB(t)
			b.columnMetadata = hourlyMetadataFixtures()
			b.states["stats_v1"] = "complete:retained-daily-progress"
			ctx := context.Background()
			if version == 2 {
				if err := upgradeSchema(ctx, conn, 1); err != nil {
					t.Fatal(err)
				}
				b.states["stats_hourly_v1"] = "complete:retained-hourly-progress"
			}
			b.columnMetadata[[2]string{metadataAccountTable, column}][field] = value
			before, creates := maps.Clone(b.states), b.creates
			err := upgradeSchema(ctx, conn, version)
			if err == nil || !strings.Contains(err.Error(), "incompatible column") || !strings.Contains(err.Error(), attribute+"(") {
				t.Fatalf("metadata drift was not diagnosed: %v", err)
			}
			if b.version != version || !reflect.DeepEqual(b.states, before) {
				t.Fatalf("invalid metadata advanced the marker or seeded/reset progress: version=%d states=%v", b.version, b.states)
			}
			if version == 2 && b.creates != creates {
				t.Fatal("v2 restart attempted DDL despite drift")
			}
		})
	}
}

func TestHourlySchemaRejectsLiteralDefaultMetadata(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value driver.Value
	}{
		{"sql_null", nil},
		{"null_expression", "NULL"},
		{"quoted_null", "'NULL'"},
		{"nonempty", "'x'"},
		{"nonempty_charset", "(_utf8mb4'x')"},
		{"raw_space", " "},
		{"quoted_space", "' '"},
		{"charset_space", "(_utf8mb4' ')"},
		{"tab", "'\t'"},
		{"numeric", "0"},
		{"parenthesized_numeric", "(0)"},
		{"quoted_numeric", "('0')"},
		{"expression", "concat('','')"},
		{"cast_expression", "cast('' as char)"},
		{"unknown_charset", "_binary''"},
		{"unclosed_parenthesis", "(''"},
		{"extra_parenthesis", "'')"},
		{"unclosed_quote", "'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertHourlyMetadataRejected(t, "auth_account_id_snapshot", 3, tc.value, "default")
		})
	}
	// Empty string is not an absent default; numeric defaults remain exact and
	// do not inherit the special comparison for the DDL's empty text default.
	t.Run("unexpected_empty_default", func(t *testing.T) {
		assertHourlyMetadataRejected(t, "structure_revision", 3, "", "default")
	})
	for _, value := range []driver.Value{nil, "", "00", "(0)", "'0'", "0.0", "0 + 0", "1"} {
		t.Run(fmt.Sprintf("numeric_default_%v", value), func(t *testing.T) {
			assertHourlyMetadataRejected(t, "calls", 3, value, "default")
		})
	}
}

func TestHourlySchemaRejectsLiteralAttributeMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, column, attribute string
		field                   int
		value                   driver.Value
	}{
		{"name", "auth_account_id_snapshot", "name", 0, "different_column"},
		{"type", "auth_account_id_snapshot", "type", 1, "varchar(64)"},
		{"nullable", "auth_account_id_snapshot", "nullable", 2, "YES"},
		{"extra", "auth_account_id_snapshot", "extra", 5, "auto_increment"},
		{"collation", "auth_account_id_snapshot", "collation", 6, "utf8mb4_bin"},
		{"missing_collation", "auth_account_id_snapshot", "collation", 6, ""},
		{"unexpected_generation", "auth_account_id_snapshot", "generation_canonical", 4, "concat('','')"},
		{"numeric_type", "calls", "type", 1, "bigint unsigned"},
		{"generated_type", "_revision_hash", "type", 1, "binary(16)"},
		{"generated_nullable", "_revision_hash", "nullable", 2, "NO"},
		{"generated_default", "_revision_hash", "default", 3, ""},
		{"generated_virtual", "_revision_hash", "extra", 5, "VIRTUAL GENERATED"},
		{"generated_missing_extra", "_revision_hash", "extra", 5, ""},
		{"generated_wrong_column", "_revision_hash", "generation_canonical", 4, "unhex(sha2(`api_key_hash`,256))"},
		{"generated_wrong_nesting", "_revision_hash", "generation_canonical", 4, "unhex(sha2(concat(`structure_revision`,256)))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertHourlyMetadataRejected(t, tc.column, tc.field, tc.value, tc.attribute)
		})
	}
}

func TestHourlySchemaMetadataDiagnosticsPreserveRawDefault(t *testing.T) {
	conn, b := migrationDB(t)
	b.columnMetadata = hourlyMetadataFixtures()
	// A different attribute fails after the empty-default normalization succeeds.
	b.columnMetadata[[2]string{metadataAccountTable, "auth_account_id_snapshot"}][1] = "varchar(64)"
	err := upgradeSchema(context.Background(), conn, 1)
	if err == nil {
		t.Fatal("type drift accepted")
	}
	for _, part := range []string{
		"MySQL schema drift in " + metadataAccountTable,
		`name(actual="auth_account_id_snapshot" expected="auth_account_id_snapshot")`,
		`type(actual="varchar(64)" expected="longtext")`,
		`nullable(actual="NO" expected="NO")`,
		`default(actual.Valid=true actual="(_utf8mb4'')" compared="" expected.Valid=true expected="")`,
		`extra(actual="DEFAULT_GENERATED" expected="" or "DEFAULT_GENERATED")`,
		`collation(actual="utf8mb4_0900_bin" expected="utf8mb4_0900_bin")`,
		`generation_canonical(actual="" expected="")`,
	} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("missing metadata diagnostic %q in %v", part, err)
		}
	}
	b.columnMetadata[[2]string{metadataAccountTable, "auth_account_id_snapshot"}][3] = nil
	err = upgradeSchema(context.Background(), conn, 1)
	if err == nil || !strings.Contains(err.Error(), `default(actual.Valid=false actual="" compared="" expected.Valid=true expected="")`) {
		t.Fatalf("SQL NULL was not distinguishable from empty string: %v", err)
	}
}

func TestSchemaMetadataDiagnosticsBoundAndQuoteValues(t *testing.T) {
	value := strings.Repeat("\x00\n", 4096)
	quoted := schemaDiagnosticValue(value)
	if len(quoted) > 1200 || strings.ContainsAny(quoted, "\x00\n") || !strings.Contains(quoted, "bytes=8192 sha256=") {
		t.Fatalf("metadata diagnostic is unbounded or unescaped: length=%d", len(quoted))
	}
	if quoted == schemaDiagnosticValue(value[:len(value)-1]+"x") {
		t.Fatal("diagnostic hid a difference beyond the bounded prefix")
	}
	c := schemaColumn{name: value, kind: value, generated: value, defaultValue: sql.NullString{Valid: true, String: value}}
	actual := schemaColumnMetadata{
		name: value, kind: value, nullable: value, generated: value, extra: value, collation: value, defaultValue: c.defaultValue,
	}
	message := columnMismatch(c, actual, c.defaultValue).Error()
	if len(message) > 20000 || strings.ContainsAny(message, "\x00\n") {
		t.Fatalf("column diagnostic is unbounded or unescaped: length=%d", len(message))
	}
}
