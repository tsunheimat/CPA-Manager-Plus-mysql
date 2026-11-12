package mysql

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
)

func TestGenerationMetadataEscapedDigestSeparators(t *testing.T) {
	// Independent two-field framing fixture with the literal metadata encoding
	// observed from MySQL 8.4.13 in CI run 43136356338. The full unchanged
	// hourly DDL is covered separately by the native metadata round-trip test.
	const expected = "unhex(sha2(concat(octet_length(cast(`revision` as char character set utf8mb4)),':',cast(`revision` as char character set utf8mb4),';',octet_length(cast(`key` as char character set utf8mb4)),':',cast(`key` as char character set utf8mb4),';'),256))"
	const metadata = `(unhex(sha2(concat(length(cast(` + "`revision`" + ` as char charset utf8mb4)),_utf8mb4\':\',cast(` + "`revision`" + ` as char charset utf8mb4),_utf8mb4\';\',length(cast(` + "`key`" + ` as char charset utf8mb4)),_utf8mb4\':\',cast(` + "`key`" + ` as char charset utf8mb4),_utf8mb4\';\'),256)))`
	want := canonicalGeneration(expected)
	if got := canonicalMetadataGeneration(metadata); got != want {
		t.Fatalf("native metadata not equivalent:\nactual: %s\nexpected: %s", got, want)
	}
	if canonicalGeneration(metadata) == want {
		t.Fatal("metadata escape handling leaked into SQL canonicalization")
	}
	for _, tc := range []struct{ name, value string }{
		{"wrong_separator", strings.Replace(metadata, `_utf8mb4\':\'`, `_utf8mb4\';\'`, 1)},
		{"extra_space", strings.Replace(metadata, `_utf8mb4\':\'`, `_utf8mb4\': \'`, 1)},
		{"unknown_charset", strings.Replace(metadata, `_utf8mb4\':\'`, `_binary\':\'`, 1)},
		{"extra_backslash", strings.Replace(metadata, `_utf8mb4\':\'`, `_utf8mb4\':\\'`, 1)},
		{"wrong_length", strings.Replace(metadata, "length(", "char_length(", 1)},
		{"wrong_column", strings.ReplaceAll(metadata, "`revision`", "`other_revision`")},
		{"wrong_nesting", strings.Replace(metadata, `_utf8mb4\':\'`, `concat(_utf8mb4\':\',_utf8mb4\';\')`, 1)},
		{"wrong_bits", strings.Replace(metadata, ",256)", ",512)", 1)},
		{"quoted_metadata_text", "'" + strings.ReplaceAll(metadata, "'", "''") + "'"},
		{"malformed_quote", strings.Replace(metadata, `_utf8mb4\':\'`, `_utf8mb4\':\`, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if canonicalMetadataGeneration(tc.value) == want {
				t.Fatalf("changed digest frame accepted: %s", tc.value)
			}
		})
	}
	// Exercise the actual column comparator with independently specified rows.
	conn, backend := migrationDB(t)
	table := schemaTable{name: "metadata_digest_fixture", columns: []schemaColumn{{name: "_identity_hash", kind: "binary(32)", generated: expected}}}
	backend.tables[table.name] = table
	backend.columnMetadata = map[[2]string][]driver.Value{
		{table.name, "_identity_hash"}: {"_identity_hash", "binary(32)", "YES", nil, metadata, "STORED GENERATED", ""},
	}
	if err := validateTable(context.Background(), conn, table); err != nil {
		t.Fatalf("native generated-column metadata rejected: %v", err)
	}
	backend.columnMetadata[[2]string{table.name, "_identity_hash"}][4] = strings.Replace(metadata, `_utf8mb4\':\'`, `_utf8mb4\';\'`, 1)
	if err := validateTable(context.Background(), conn, table); err == nil || !strings.Contains(err.Error(), "generation_canonical(") {
		t.Fatalf("changed separator was not diagnosed: %v", err)
	}
}
