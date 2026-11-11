package worker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Fixture writers use the same runtime pools as production under testdb.Open.
// Keeping them behind sqlwrite makes the native admission tests runnable rather
// than failing during setup before they exercise their intended assertions.
func TestWorkerRuntimeFixtureWritesUsePreCheckoutAdmission(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatal("locate worker test sources", err)
	}
	fset := token.NewFileSet()
	sqliteExceptions := 0
	for _, path := range files {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		sqlwriteAlias := ""
		for _, spec := range file.Imports {
			importPath, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasSuffix(importPath, "/internal/sqlwrite") {
				sqlwriteAlias = "sqlwrite"
				if spec.Name != nil {
					sqlwriteAlias = spec.Name.Name
				}
			}
			if path == "quota_snapshot_legacy_migration_test.go" && (strings.HasSuffix(importPath, "/internal/testutil") || strings.HasSuffix(importPath, "/internal/testutil/testdb")) {
				t.Error("the explicit SQLite-only legacy migration fixture now imports a runtime DB helper; migrate its write before broadening the backend")
			}
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if receiver, ok := selector.X.(*ast.Ident); ok && receiver.Name == sqlwriteAlias {
					return true
				}
				switch selector.Sel.Name {
				case "Exec", "ExecContext", "Begin", "BeginTx", "Prepare", "PrepareContext", "Conn":
					// This historical migration fixture deliberately opens SQLite
					// directly in each of its three callers, not testdb.Open.
					if path == "quota_snapshot_legacy_migration_test.go" && fn.Name.Name == "seedWorkerLegacySnapshot" && selector.Sel.Name == "Exec" {
						sqliteExceptions++
						return true
					}
					t.Errorf("%s: fixture write/checkout must use sqlwrite (or document an admitted transaction/SQLite-only exception)", fset.Position(call.Pos()))
				case "Query", "QueryContext", "QueryRow", "QueryRowContext":
					queryArg := 0
					if strings.HasSuffix(selector.Sel.Name, "Context") {
						queryArg = 1
					}
					if queryArg >= len(call.Args) {
						return true
					}
					literal, ok := call.Args[queryArg].(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						t.Errorf("%s: nonliteral fixture query needs explicit write-routing review", fset.Position(call.Pos()))
						return true
					}
					query, err := strconv.Unquote(literal.Value)
					if err != nil || !strings.HasPrefix(strings.TrimSpace(strings.ToLower(query)), "select ") {
						t.Errorf("%s: non-SELECT fixture query must use sqlwrite or an explicit reviewed exception", fset.Position(call.Pos()))
					}
				}
				return true
			})
		}
	}
	if sqliteExceptions != 1 {
		t.Errorf("SQLite-only exception count = %d; review stale/new fixture exceptions", sqliteExceptions)
	}
}
