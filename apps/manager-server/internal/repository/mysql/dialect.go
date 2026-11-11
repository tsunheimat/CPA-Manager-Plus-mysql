package mysql

// The repositories share a small SQL vocabulary. This renderer translates that
// vocabulary at the database/sql boundary, including prepared statements and
// transactions. It never interpolates values: placeholders remain placeholders.
// Schema DDL is native MySQL and is deliberately outside this renderer.

import (
	"fmt"
	"strconv"
	"strings"
)

type statement struct {
	sql            string
	ignore         bool
	bulkIgnore     bool
	returningID    bool
	parameterCount int
	bindings       []int
	write          bool
	txControl      bool
	ddl            bool
}

func compile(query string) (statement, error) {
	native := strings.HasPrefix(query, nativePrefix)
	query = strings.TrimPrefix(query, nativePrefix)
	tokens, err := lexSQL(query, native)
	if err != nil {
		return statement{}, err
	}
	if len(tokens) == 0 {
		return statement{}, fmt.Errorf("empty SQL statement")
	}
	write, control, ddl, err := classify(tokens)
	if err != nil {
		return statement{}, err
	}
	if !native && (ddl || equal(tokens[0], "set")) {
		// These statements, like Native, go to MySQL verbatim. In MySQL --
		// starts a comment only before whitespace/control; otherwise it is
		// arithmetic and must not hide parameters or transaction controls.
		tokens, err = lexSQL(query, true)
		if err != nil {
			return statement{}, err
		}
		write, control, ddl, err = classify(tokens)
		if err != nil {
			return statement{}, err
		}
	}
	parameterCount := 0
	for i, token := range tokens {
		if token == "?" {
			tokens[i] = "__cpamp_bound_" + strconv.Itoa(parameterCount) + "__"
			parameterCount++
		}
	}
	s := statement{parameterCount: parameterCount, write: write, txControl: control, ddl: ddl}
	if native || ddl || equal(tokens[0], "set") {
		s.sql = query
		s.bindings = make([]int, parameterCount)
		for i := range s.bindings {
			s.bindings[i] = i
		}
		if native && nativeIgnoresErrors(tokens) {
			s.ignore, s.bulkIgnore = true, true
		}
		return s, nil
	}
	if equal(tokens[0], "with") {
		if insert := topSequence(tokens, "insert"); insert >= 0 {
			selectAt := topSequence(tokens[insert:], "select")
			if selectAt < 0 {
				return statement{}, fmt.Errorf("unsupported CTE insert")
			}
			selectAt += insert
			reordered := append([]string{}, tokens[insert:selectAt]...)
			reordered = append(reordered, tokens[:insert]...)
			tokens = append(reordered, tokens[selectAt:]...)
		}
	}
	if equal(tokens[0], "update") {
		setAt := topSequence(tokens, "set")
		if setAt > 0 {
			end := len(tokens)
			for _, clause := range []string{"from", "where", "order", "limit"} {
				if at := topSequence(tokens, clause); at > setAt && at < end {
					end = at
				}
			}
			ordered, err := orderAssignments(tokens[setAt+1 : end])
			if err != nil {
				return statement{}, err
			}
			reordered := append([]string{}, tokens[:setAt+1]...)
			reordered = append(reordered, ordered...)
			tokens = append(reordered, tokens[end:]...)
		}
		setAt, fromAt, whereAt := topSequence(tokens, "set"), topSequence(tokens, "from"), topSequence(tokens, "where")
		if setAt > 0 && fromAt > setAt && whereAt > fromAt {
			// A comma join keeps SET placeholders before WHERE placeholders.
			// Qualify assignment targets because source columns share names.
			alias := tokens[setAt-1]
			reordered := append([]string{}, tokens[:setAt]...)
			reordered = append(reordered, ",")
			reordered = append(reordered, tokens[fromAt+1:whereAt]...)
			reordered = append(reordered, "set")
			for i, assignment := range split(tokens[setAt+1 : fromAt]) {
				if i > 0 {
					reordered = append(reordered, ",")
				}
				reordered = append(reordered, alias, ".")
				reordered = append(reordered, assignment...)
			}
			tokens = append(reordered, tokens[whereAt:]...)
		}
	}
	// Bounded rowid deletion maps directly to MySQL DELETE ... LIMIT.
	if len(tokens) > 12 && equal(tokens[0], "delete") && equal(tokens[1], "from") && equal(tokens[3], "where") && equal(tokens[4], "rowid") && equal(tokens[5], "in") && tokens[6] == "(" && equal(tokens[7], "select") && equal(tokens[8], "rowid") && equal(tokens[9], "from") && tokens[10] == tokens[2] && tokens[len(tokens)-1] == ")" {
		tokens = append(append([]string{}, tokens[:3]...), tokens[11:len(tokens)-1]...)
	}
	for _, token := range tokens {
		if equal(token, "json_group_array") && len(tokens) > 2 && equal(tokens[len(tokens)-2], "from") && equal(tokens[len(tokens)-1], "filtered_events") {
			tokens = tokens[:len(tokens)-2]
			break
		}
	}
	if len(tokens) >= 2 && equal(tokens[len(tokens)-2], "returning") && equal(tokens[len(tokens)-1], "id") {
		s.returningID = true
		tokens = tokens[:len(tokens)-2]
	}
	if len(tokens) > 2 && equal(tokens[0], "insert") && equal(tokens[1], "ignore") {
		s.ignore = true
		tokens = append(tokens[:1], tokens[2:]...)
	}
	if len(tokens) > 3 && equal(tokens[0], "insert") && equal(tokens[1], "or") && equal(tokens[2], "ignore") {
		s.ignore = true
		tokens = append(tokens[:1], tokens[3:]...)
	}
	if len(tokens) > 3 && equal(tokens[0], "insert") && equal(tokens[1], "or") && equal(tokens[2], "replace") {
		tokens = append([]string{"replace"}, tokens[3:]...)
	}
	conflict := topSequence(tokens, "on", "conflict")
	if conflict >= 0 {
		do := conflict + 2
		if do < len(tokens) && tokens[do] == "(" {
			do = closing(tokens, do) + 1
		}
		if do <= conflict || do+1 >= len(tokens) || !equal(tokens[do], "do") {
			return s, fmt.Errorf("unsupported conflict clause")
		}
		if equal(tokens[do+1], "nothing") {
			s.ignore = true
			tokens = tokens[:conflict]
		} else if do+2 < len(tokens) && equal(tokens[do+1], "update") && equal(tokens[do+2], "set") {
			assignments := tokens[do+3:]
			if where := topSequence(assignments, "where"); where >= 0 {
				// Updating event_id first preserves this guard: after it
				// succeeds, incoming and stored event IDs remain equal.
				if len(tokens) < 3 || !equal(tokens[2], "usage_monitoring_header_latest_v1") {
					return s, fmt.Errorf("unsupported conditional upsert")
				}
				condition := append([]string{}, assignments[where+1:]...)
				parts := split(assignments[:where])
				if len(parts) == 0 || !equal(parts[0][0], "event_id") {
					return s, fmt.Errorf("header upsert must assign event_id first")
				}
				assignments = nil
				for i, part := range parts {
					if i > 0 {
						assignments = append(assignments, ",")
					}
					assignments = append(assignments, part[:2]...)
					assignments = append(assignments, "if", "(")
					assignments = append(assignments, condition...)
					assignments = append(assignments, ",")
					assignments = append(assignments, part[2:]...)
					assignments = append(assignments, ",", tokens[2], ".", part[0], ")")
				}
			} else {
				assignments, err = orderAssignments(assignments)
				if err != nil {
					return s, err
				}
			}
			tokens = append(tokens[:conflict], append([]string{"on", "duplicate", "key", "update"}, assignments...)...)
			if s.returningID {
				tokens = append(tokens, ",", "id", "=", "last_insert_id", "(", "id", ")")
			}
		} else {
			return s, fmt.Errorf("unsupported conflict action")
		}
	}
	if s.ignore {
		// Single-row inserts use ordinary strict INSERT and handle only error
		// 1062. Bulk inserts need IGNORE; the connection checks every warning
		// within a savepoint so truncation and other errors cannot be hidden.
		values := topSequence(tokens, "values")
		s.bulkIgnore = values < 0 || values+1 >= len(tokens) || closing(tokens, values+1) != len(tokens)-1
		if s.bulkIgnore {
			tokens = append(tokens[:1], append([]string{"ignore"}, tokens[1:]...)...)
		}
	}
	s.sql, err = render(tokens)
	if err != nil {
		return s, err
	}
	rendered, err := lex(s.sql)
	if err != nil {
		return s, err
	}
	for i, token := range rendered {
		if strings.HasPrefix(token, "__cpamp_bound_") && strings.HasSuffix(token, "__") {
			index, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(token, "__cpamp_bound_"), "__"))
			if err != nil || index < 0 || index >= parameterCount {
				return s, fmt.Errorf("invalid SQL binding")
			}
			s.bindings = append(s.bindings, index)
			rendered[i] = "?"
		}
	}
	var sql strings.Builder
	for i, token := range rendered {
		if i > 0 && !(token == "(" && len(rendered[i-1]) > 0 && isWord(rendered[i-1][0])) {
			sql.WriteByte(' ')
		}
		sql.WriteString(token)
	}
	s.sql = sql.String()
	return s, err
}

func equal(a, b string) bool { return strings.EqualFold(a, b) }

func lex(query string) ([]string, error) { return lexSQL(query, false) }

func lexSQL(query string, native bool) ([]string, error) {
	var out []string
	ended := false
	for i := 0; i < len(query); {
		c := query[i]
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			i++
			continue
		}
		if c == ';' {
			ended = true
			i++
			continue
		}
		if c == '#' || c == '-' && i+1 < len(query) && query[i+1] == '-' && (!native || i+2 == len(query) || query[i+2] <= ' ') {
			for i < len(query) && query[i] != '\n' && query[i] != '\r' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(query) && query[i+1] == '*' {
			if i+2 < len(query) && query[i+2] == '!' {
				return nil, fmt.Errorf("executable SQL comments are not supported")
			}
			end := strings.Index(query[i+2:], "*/")
			if end < 0 {
				return nil, fmt.Errorf("unterminated SQL comment")
			}
			i += end + 4
			continue
		}
		if ended {
			return nil, fmt.Errorf("multiple SQL statements are not supported")
		}
		start := i
		if c == '\'' || c == '"' || c == '`' {
			i++
			closed := false
			for i < len(query) {
				if query[i] == c {
					i++
					if i < len(query) && query[i] == c {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, fmt.Errorf("unterminated SQL quote")
			}
		} else if c >= '0' && c <= '9' {
			for i < len(query) && (query[i] >= '0' && query[i] <= '9' || query[i] == '.' || query[i] == '_') {
				i++
			}
			out = append(out, strings.ReplaceAll(query[start:i], "_", ""))
			continue
		} else if isWord(c) {
			for i < len(query) && isWord(query[i]) {
				i++
			}
		} else {
			i++
			if i < len(query) && (query[start:i+1] == "||" || query[start:i+1] == "<=" || query[start:i+1] == ">=" || query[start:i+1] == "<>" || query[start:i+1] == "!=") {
				i++
			}
		}
		out = append(out, query[start:i])
	}
	return out, nil
}

func isWord(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

func closing(tokens []string, start int) int {
	if start >= len(tokens) || tokens[start] != "(" {
		return -1
	}
	depth := 0
	for i := start; i < len(tokens); i++ {
		if tokens[i] == "(" {
			depth++
		}
		if tokens[i] == ")" {
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func topSequence(tokens []string, sequence ...string) int {
	depth := 0
	for i, token := range tokens {
		if token == "(" {
			depth++
		}
		if token == ")" {
			depth--
		}
		if depth != 0 || i+len(sequence) > len(tokens) {
			continue
		}
		match := true
		for j, word := range sequence {
			if !equal(tokens[i+j], word) {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func split(tokens []string) [][]string {
	var parts [][]string
	for len(tokens) > 0 {
		i := topSequence(tokens, ",")
		if i < 0 {
			return append(parts, tokens)
		}
		parts = append(parts, tokens[:i])
		tokens = tokens[i+1:]
	}
	return parts
}

// SQLite evaluates all RHS expressions against the old row; MySQL evaluates
// assignments from left to right. Put consumers of an old column before its
// assignment (e.g. snapshot metadata before last_seen_ms).
func orderAssignments(tokens []string) ([]string, error) {
	parts := split(tokens)
	var out []string
	used := make([]bool, len(parts))
	for count := 0; count < len(parts); count++ {
		choice := -1
		for i, part := range parts {
			if used[i] || len(part) < 3 {
				continue
			}
			blocked := false
			for j, other := range parts {
				if i == j || used[j] {
					continue
				}
				for k := 2; k < len(other); k++ {
					if equal(other[k], part[0]) && !(k >= 2 && other[k-1] == "." && equal(other[k-2], "excluded")) {
						blocked = true
					}
				}
			}
			if !blocked {
				choice = i
				break
			}
		}
		if choice < 0 {
			return nil, fmt.Errorf("cyclic SQL assignments require an explicit MySQL implementation")
		} // Current repository upserts have no cycles.
		if len(out) > 0 {
			out = append(out, ",")
		}
		out = append(out, parts[choice]...)
		used[choice] = true
	}
	return out, nil
}

const schemaRelation = `(select table_name as name, 'table' as type, table_name as tbl_name from information_schema.tables where table_schema = database() union all select distinct index_name as name, 'index' as type, table_name as tbl_name from information_schema.statistics where table_schema = database()) as manager_schema`

func render(tokens []string) (string, error) {
	if len(tokens) > 1 && equal(tokens[0], "values") {
		var rows []string
		for i := 1; i < len(tokens); {
			end := closing(tokens, i)
			if end < 0 {
				return "", fmt.Errorf("unsupported VALUES table expression")
			}
			row, err := render(tokens[i+1 : end])
			if err != nil {
				return "", err
			}
			rows = append(rows, "select "+row)
			i = end + 1
			if i < len(tokens) && tokens[i] == "," {
				i++
			}
		}
		return strings.Join(rows, " union all "), nil
	}
	var out []string
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]
		word := strings.ToLower(token)
		if word == "not" && i+1 < len(tokens) && equal(tokens[i+1], "indexed") {
			i++
			continue
		}
		if word == "indexed" && i+2 < len(tokens) && equal(tokens[i+1], "by") {
			out = append(out, "force index ("+tokens[i+2]+")")
			i += 2
			continue
		}
		if i+1 < len(tokens) && tokens[i+1] == "(" && len(token) > 0 && isWord(token[0]) {
			end := closing(tokens, i+1)
			if end < 0 {
				return "", fmt.Errorf("unbalanced SQL parentheses")
			}
			inner := tokens[i+2 : end]
			args := split(inner)
			body, err := render(inner)
			if err != nil {
				return "", err
			}
			switch word {
			case "avg":
				body = "cast(" + body + " as double)"
			case "char":
				body += " using utf8mb4"
			case "cpamp_analytics_model":
				out = append(out, analyticsModelSQL(body))
				i = end
				continue
			case "length":
				word = "char_length"
			case "min", "max":
				if len(args) > 1 {
					if word == "min" {
						word = "least"
					} else {
						word = "greatest"
					}
				}
			case "json_patch":
				word = "json_merge_patch"
			case "json":
				out = append(out, "cast("+body+" as json)")
				i = end
				continue
			case "json_type":
				if len(args) == 2 {
					body = "json_extract(" + body + ")"
				}
			case "json_extract":
				json := "json_extract(" + body + ")"
				out = append(out, "(case when json_type("+json+") collate utf8mb4_0900_bin='NULL' then null else json_unquote("+json+") end collate utf8mb4_0900_bin)")
				i = end
				continue
			case "json_each":
				out = append(out, "json_table("+body+", '$[*]' columns(value longtext path '$')) as manager_json_values")
				i = end
				continue
			case "json_group_array":
				// Use a scalar subquery over DISTINCT values instead of
				// GROUP_CONCAT, whose configurable size limit can truncate JSON.
				if len(inner) != 2 || !equal(inner[0], "distinct") || end+2 >= len(tokens) || !equal(tokens[end+1], "filter") || tokens[end+2] != "(" {
					return "", fmt.Errorf("unsupported JSON aggregate")
				}
				filterEnd := closing(tokens, end+2)
				if filterEnd < 0 {
					return "", fmt.Errorf("unbalanced aggregate filter")
				}
				condition, err := render(tokens[end+3 : filterEnd])
				if err != nil {
					return "", err
				}
				col, err := render(inner[1:])
				if err != nil {
					return "", err
				}
				out = append(out, "(select coalesce(json_arrayagg(v), json_array()) from (select distinct "+col+" as v from filtered_events "+condition+") as manager_distinct_values)")
				i = filterEnd
				continue
			case "unixepoch":
				if len(inner) == 1 && equal(inner[0], "'subsec'") {
					out = append(out, "unix_timestamp(current_timestamp(3))")
					i = end
					continue
				}
			case "cast":
				as := topSequence(inner, "as")
				if as >= 0 && as+1 < len(inner) {
					expr, err := render(inner[:as])
					if err != nil {
						return "", err
					}
					if equal(inner[as+1], "integer") {
						out = append(out, "cast(truncate("+expr+", 0) as signed)")
						i = end
						continue
					}
					if equal(inner[as+1], "real") {
						body = expr + " as double"
					}
					if equal(inner[as+1], "blob") {
						body = expr + " as binary"
					}
					if equal(inner[as+1], "text") {
						body = expr + " as char character set utf8mb4"
					}
				}
			}
			fragment := word + "(" + body + ")"
			if word == "in" && len(inner) > 0 && equal(inner[0], "select") && topSequence(inner, "limit") >= 0 {
				fragment = "in (select * from (" + body + ") as manager_limited_values)"
			}
			if (word == "from" || word == "join") && len(inner) > 0 && (equal(inner[0], "select") || equal(inner[0], "with")) {
				if end+1 == len(tokens) || isClause(tokens[end+1]) {
					fragment += " as manager_derived"
				}
			}
			out = append(out, fragment)
			i = end
			continue
		}
		if word == "excluded" && i+2 < len(tokens) && tokens[i+1] == "." {
			out = append(out, "values(`"+tokens[i+2]+"`)")
			i += 2
			continue
		}
		switch word {
		case "stored", "window":
			token = "`" + word + "`"
		case "sqlite_master", "sqlite_schema":
			token = schemaRelation
		case "key":
			if i == 0 || !(equal(tokens[i-1], "duplicate") || equal(tokens[i-1], "primary") || equal(tokens[i-1], "foreign")) {
				token = "`key`"
			}
		case "nocase":
			token = "utf8mb4_0900_as_ci"
		case "/":
			token = "div" // Repository SQL divisions operate on integer counters/timestamps.
		case "glob":
			if i+1 < len(tokens) && tokens[i+1] == "'*[^0-9a-f]*'" {
				out = append(out, "regexp", "'[^0-9a-f]'")
				i++
				continue
			}
			if i+1 < len(tokens) && tokens[i+1] == "'*[^!-~]*'" {
				out = append(out, "regexp", "'[^!-~]'")
				i++
				continue
			}
			return "", fmt.Errorf("unsupported GLOB pattern")
		case "pragma", "returning":
			return "", fmt.Errorf("SQLite-only SQL requires an explicit MySQL implementation")
		}
		out = append(out, token)
	}
	return strings.Join(out, " "), nil
}

// Equivalent to usageidentity.AnalyticsModel, including signed zero, leading
// zeroes, and the int64 boundary. String comparisons avoid overflow warnings
// when an untrusted model suffix contains an arbitrarily large integer.
func analyticsModelSQL(value string) string {
	suffix := "substring_index(left(" + value + ", char_length(" + value + ")-1), '(', -1)"
	digits := "trim(leading '0' from trim(leading '+' from " + suffix + "))"
	known := "lower(" + suffix + ") in ('none','auto','-1','minimal','low','medium','high','xhigh','max')"
	number := "(regexp_like(" + suffix + ", '^[+]?[0-9]+\\z', 'c') and (char_length(" + digits + ") < 19 or (char_length(" + digits + ") = 19 and " + digits + " collate utf8mb4_0900_bin <= '9223372036854775807')))"
	negativeZero := "regexp_like(" + suffix + ", '^-0+\\z', 'c')"
	return "(case when right(" + value + ",1)=')' and char_length(" + value + ")>char_length(" + suffix + ")+2 and (" + known + " or " + number + " or " + negativeZero + ") then left(" + value + ",char_length(" + value + ")-char_length(" + suffix + ")-2) else " + value + " end)"
}

func isClause(token string) bool {
	switch strings.ToLower(token) {
	case "where", "group", "order", "limit", "having", "union", "join", "left", "inner", "on", ")":
		return true
	}
	return false
}
