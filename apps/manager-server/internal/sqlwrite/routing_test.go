package sqlwrite_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// This intentionally checks source boundaries rather than allowing a driver
// fallback that would wait after checkout. Driver tests cover native/CTE/query/
// prepared bypasses at execution time. Test fixtures and native administrative
// pools have separate lifetimes and are excluded here, not globally exempted by
// the runtime driver.
func TestRuntimeWritesUsePreCheckoutAdmission(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(source))
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			// Commands own offline/exclusive native connections; SQLite owns its
			// backend-specific startup migration/maintenance implementation.
			for _, excluded := range []string{"command", "testutil", "sqlwrite", "repository/sqlite", "repository/mysql/internal"} {
				if rel == excluded {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		switch rel {
		case "repository/mysql/driver.go", "repository/mysql/db.go", "repository/mysql/backup.go", "repository/mysql/migrations.go":
			// Adapter internals and exclusive native schema/backup connections.
			// Enumerate files so future runtime MySQL helpers are not exempted.
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if rel == "repository/mysql/maintenance.go" && fn.Name.Name == "Compact" {
				continue
			} // native pool under offline process lock
			if rel == "database/database.go" {
				continue
			} // native pool for the exclusive process advisory lock
			trusted := map[string]bool{}
			// These private helpers accept only statements returned by their
			// sibling prepare*TierInsert(ctx, *sql.Tx) functions. Every caller
			// prepares them inside a sqlwrite transaction, never on a DB.
			if rel == "repository/modelprice/repository.go" && (fn.Name.Name == "insertContextTiers" || fn.Name.Name == "insertServiceTiers") {
				trusted["stmt"] = true
			}
			if fn.Type.Params != nil {
				for _, field := range fn.Type.Params.List {
					if isSQLTx(field.Type) {
						for _, name := range field.Names {
							trusted[name.Name] = true
						}
					}
				}
			}
			// Track transaction and transaction-statement values created by the
			// approved boundaries. Names alone (e.g. a DB called tx) are not proof.
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				assign, ok := node.(*ast.AssignStmt)
				if !ok || len(assign.Rhs) != 1 || len(assign.Lhs) == 0 {
					return true
				}
				call, ok := assign.Rhs[0].(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				name, ok := assign.Lhs[0].(*ast.Ident)
				if !ok {
					return true
				}
				if sel.Sel.Name == "BeginTx" && (ident(sel.X) == "sqlwrite" || readOnly(call)) {
					trusted[name.Name] = true
				}
				if (sel.Sel.Name == "PrepareContext" || sel.Sel.Name == "Prepare" || sel.Sel.Name == "StmtContext" || sel.Sel.Name == "Stmt") && trusted[ident(sel.X)] {
					trusted[name.Name] = true
				}
				return true
			})
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if ident(sel.X) == "sqlwrite" || trusted[ident(sel.X)] {
					return true
				}
				method := sel.Sel.Name
				queryIndex := 0
				if strings.HasSuffix(method, "Context") {
					queryIndex = 1
				}
				query := ""
				if len(call.Args) > queryIndex {
					query = literalPrefix(call.Args[queryIndex])
				}
				query = strings.ToLower(strings.TrimSpace(query))
				switch method {
				case "BeginTx":
					if readOnly(call) {
						return true
					}
				case "Begin":
				case "Exec", "ExecContext":
					// SQLite's deferred full-export snapshot is selected only after
					// the explicit MySQL read-only branch returned. No DML exemption.
					if rel == "repository/usageevent/stream.go" && fn.Name.Name == "WriteFullExportJSONL" && ident(sel.X) == "conn" && (query == "begin" || query == "commit" || query == "rollback") {
						return true
					}
				case "Prepare", "PrepareContext":
					if strings.HasPrefix(query, "select ") {
						return true
					}
				case "QueryContext", "QueryRowContext", "Query", "QueryRow":
					// Dynamic read builders are checked by the execution-time driver;
					// obvious write-returning calls cannot sneak past source routing.
					if !obviousWrite(query) {
						return true
					}
				default:
					return true
				}
				t.Errorf("%s: direct %s outside an admitted transaction; use sqlwrite", fset.Position(call.Pos()), method)
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A conservative literal-source guard, not a SQL interpreter. Strip comments
// and quoted values before recognizing explicit and CTE write-returning calls.
// Dynamic builders still fail closed at the runtime driver's execution boundary.
var sourceSQLQuotes = regexp.MustCompile("(?s)/\\*.*?\\*/|--[^\\r\\n]*|#[^\\r\\n]*|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`")
var sourceSQLWrites = regexp.MustCompile(`(?is)^\s*(insert|update|delete|replace|create|alter|drop|truncate)\b|^\s*with\b.*\)\s*(insert|update|delete|replace)\b|\bfor\s+(update|share)\b|\block\s+in\s+share\s+mode\b`)

func obviousWrite(query string) bool {
	return sourceSQLWrites.MatchString(sourceSQLQuotes.ReplaceAllString(query, " "))
}

func TestSourceRoutingRecognizesLiteralWriteReturningQueries(t *testing.T) {
	for _, query := range []string{
		"insert into t values(?) returning id", "/* read? */ update t set x=1",
		"with x as (select 1), y as (select * from x) delete from t",
		"with x as (select '(') insert into t select * from x",
		"select * from (select * from t for update) locked_rows",
	} {
		if !obviousWrite(query) {
			t.Errorf("missed source write: %s", query)
		}
	}
	for _, query := range []string{
		"select 1", "with x as (select 'for update') select * from x",
		"select ') update t set v=1'", "select 1 /* for update */",
	} {
		if obviousWrite(query) {
			t.Errorf("false source write: %s", query)
		}
	}
}

func ident(expr ast.Expr) string {
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}
func isSQLTx(expr ast.Expr) bool {
	p, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	s, ok := p.X.(*ast.SelectorExpr)
	return ok && ident(s.X) == "sql" && s.Sel.Name == "Tx"
}
func literalPrefix(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind == token.STRING {
			s, _ := strconv.Unquote(e.Value)
			return s
		}
	case *ast.BinaryExpr:
		if e.Op == token.ADD {
			return literalPrefix(e.X)
		}
	case *ast.CallExpr:
		if s, ok := e.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "Native" && len(e.Args) == 1 {
			return literalPrefix(e.Args[0])
		}
	}
	return ""
}
func readOnly(call *ast.CallExpr) bool {
	if len(call.Args) < 2 {
		return false
	}
	arg := call.Args[len(call.Args)-1]
	if u, ok := arg.(*ast.UnaryExpr); ok {
		arg = u.X
	}
	c, ok := arg.(*ast.CompositeLit)
	if !ok {
		return false
	}
	for _, elt := range c.Elts {
		if kv, ok := elt.(*ast.KeyValueExpr); ok && ident(kv.Key) == "ReadOnly" && ident(kv.Value) == "true" {
			return true
		}
	}
	return false
}
