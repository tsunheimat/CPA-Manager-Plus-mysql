package mysql

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

// Native marks exactly one internal statement for execution without SQLite
// translation. It changes neither the connection nor write admission. Parameters
// remain bound; never put request values in query text.
func Native(query string) string { return nativePrefix + query }

const nativePrefix = "\x00cpamp:mysql-native\x00"

// CompileSQL renders shared SQL and its (possibly expanded/reordered) bindings.
// Native statements retain their exact SQL and argument ordering.
func CompileSQL(query string, args ...any) (string, []any, error) {
	s, err := compile(query)
	if err != nil {
		return "", nil, err
	}
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	bound, err := s.bind(named)
	if err != nil {
		return "", nil, err
	}
	out := make([]any, len(bound))
	for i, arg := range bound {
		out[i] = arg.Value
	}
	return s.sql, out, nil
}

// Translate translates a shared SQL expression for inclusion in a native
// statement. Bound expressions should use CompileSQL instead, to retain bindings.
func Translate(expression string) (string, error) {
	tokens, err := lex(expression)
	if err != nil {
		return "", err
	}
	for _, token := range tokens {
		if token == "?" {
			return "", fmt.Errorf("Translate requires an unbound expression; use CompileSQL for parameters")
		}
	}
	return render(tokens)
}

// statementVerb finds the outer statement after a WITH list. The CTE bodies
// cannot change classification (a SELECT inside a write is still a write).
func statementVerb(tokens []string) (string, error) {
	if len(tokens) == 0 {
		return "", fmt.Errorf("empty SQL statement")
	}
	if !equal(tokens[0], "with") {
		return strings.ToLower(tokens[0]), nil
	}
	i := 1
	if i < len(tokens) && equal(tokens[i], "recursive") {
		i++
	}
	for i < len(tokens) {
		i++ // CTE name
		if i < len(tokens) && tokens[i] == "(" {
			end := closing(tokens, i)
			if end < 0 {
				break
			}
			i = end + 1
		}
		if i >= len(tokens) || !equal(tokens[i], "as") {
			break
		}
		i++
		if i < len(tokens) && equal(tokens[i], "not") {
			i++
		}
		if i < len(tokens) && equal(tokens[i], "materialized") {
			i++
		}
		end := closing(tokens, i)
		if end < 0 {
			break
		}
		i = end + 1
		if i < len(tokens) && tokens[i] == "," {
			i++
			continue
		}
		if i < len(tokens) {
			return strings.ToLower(tokens[i]), nil
		}
		break
	}
	return "", fmt.Errorf("invalid SQL WITH statement")
}

func hasSequence(tokens []string, sequence ...string) bool {
	for i := 0; i+len(sequence) <= len(tokens); i++ {
		match := true
		for j, word := range sequence {
			if !equal(tokens[i+j], word) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// Native INSERT modifiers do not disable strict duplicate-warning verification.
// UPDATE/DELETE IGNORE have the same coercion hazards and use the same savepoint.
func nativeIgnoresErrors(tokens []string) bool {
	if len(tokens) == 0 {
		return false
	}
	verb, err := statementVerb(tokens)
	if err != nil || verb != "insert" && verb != "update" && verb != "delete" {
		return false
	}
	start := 0
	if equal(tokens[0], "with") {
		start = topSequence(tokens, verb)
	}
	if start < 0 {
		return false
	}
	for _, token := range tokens[start+1:] {
		switch strings.ToLower(token) {
		case "low_priority", "high_priority", "delayed", "quick":
		case "ignore":
			return true
		default:
			return false
		}
	}
	return false
}

func classify(tokens []string) (write, control, ddl bool, err error) {
	verb, err := statementVerb(tokens)
	if err != nil {
		return false, false, false, err
	}
	switch verb {
	case "select":
		// Inspect nested query blocks too: a SELECT over a locking CTE/subquery
		// still participates in the writer's lock order. Quoted text is a single
		// token and cannot masquerade as one of these keyword sequences.
		write = hasSequence(tokens, "into") || hasSequence(tokens, "for", "update") || hasSequence(tokens, "for", "share") || hasSequence(tokens, "lock", "in", "share", "mode")
	case "show", "describe", "desc":
	case "insert", "update", "delete", "replace":
		write = true
	case "begin", "start", "commit", "rollback", "savepoint", "release", "xa", "lock", "unlock", "call":
		write, control = true, true
	case "set":
		write = true
		// SET PASSWORD commits implicitly; SET TRANSACTION/autocommit can
		// move transaction lifetime outside database/sql's ownership.
		control = hasSequence(tokens, "set", "password")
		for _, token := range tokens {
			if equal(strings.Trim(token, "`\""), "autocommit") || equal(token, "transaction") {
				control = true
			}
		}
	case "create", "alter", "drop", "truncate", "rename", "grant", "revoke", "optimize", "analyze", "repair":
		write, ddl = true, true
	default:
		// Unclassified vendor/admin commands may commit implicitly (FLUSH,
		// RESET, INSTALL, replication commands) or establish session state that
		// outlives admission. They belong on the exclusive native admin pool.
		// CALL is forbidden above even outside a tx: a procedure can leave its
		// own transaction open after the statement's permit would be returned.
		write, control = true, true
	}
	return
}
