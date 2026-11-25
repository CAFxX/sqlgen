package sqlgen

import (
	"strings"

	"github.com/CAFxX/sqlgen/runtime"
)

// Decomposability: a statement with one collection parameter may be split into chunks (one statement per
// chunk, results concatenated) only when its result IS the concatenation of the per-chunk results. The
// generator decides this lexically and conservatively; when it cannot be sure, the statement is not
// decomposable and is never chunked (a collection that does not fit one statement is then an error, see
// runtime.ErrCollectionTooLarge). The rule (DOCS section 3.2):
//
//   - The collection must be either the VALUES list of a top-level INSERT, or a positive `IN (...)` list
//     that is a whole conjunct of the top-level WHERE clause of a SELECT, UPDATE or DELETE (possibly
//     inside plain parenthesised AND groups). An IN list under NOT, OR, CASE, a comparison, an ON / SET /
//     projection clause, or inside a sub-query, CTE body or derived table is not decomposable.
//   - The top level of the statement (and every boolean group enclosing the IN list) must contain none of:
//     an aggregate function call, GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT, OFFSET, FETCH, a window
//     (OVER / WINDOW), a set operation (UNION, INTERSECT, EXCEPT). For an IN list, OR is also rejected
//     at those levels. Sub-queries that do not contain the collection (a scalar sub-query in the projection,
//     an EXISTS in another conjunct, a CTE body) are evaluated independently of the chunking and are ignored.
//
// User-defined aggregate functions are not recognised (only the built-in ones of the supported engines).

type dtokKind uint8

const (
	dtWord  dtokKind = iota // keyword or identifier, lowercased
	dtValue                 // literal, quoted identifier or bound parameter
	dtOpen                  // (
	dtClose                 // )
	dtComma                 // ,
	dtOp                    // any other punctuation run
	dtColl                  // the chunked collection
)

type dtok struct {
	kind dtokKind
	text string
}

// aggregateFuncs are the built-in aggregate functions of the supported engines. min/max are listed even
// though SQLite also has scalar two-argument forms: classifying those as aggregates is the safe mistake.
var aggregateFuncs = setOf(
	"count", "sum", "avg", "min", "max", "total", "array_agg", "string_agg", "group_concat", "json_agg", "jsonb_agg",
	"json_object_agg", "jsonb_object_agg", "json_arrayagg", "json_objectagg", "json_group_array", "json_group_object",
	"bool_and", "bool_or", "every", "bit_and", "bit_or", "bit_xor", "stddev", "stddev_pop", "stddev_samp", "std",
	"variance", "var_pop", "var_samp", "listagg", "xmlagg", "percentile_cont", "percentile_disc", "mode", "corr",
	"covar_pop", "covar_samp", "any_value", "range_agg", "range_intersect_agg", "array_concat_agg",
)

// nonDecomposableWords are the clauses that make a level's result depend on rows outside one chunk.
var nonDecomposableWords = setOf(
	"group", "having", "distinct", "order", "limit", "offset", "fetch", "over", "window", "union", "intersect", "except",
)

// clauseWords end a WHERE conjunct when walking backwards (the first one found must be WHERE).
var clauseWords = setOf(
	"select", "from", "where", "set", "on", "using", "join", "values", "returning", "having", "group", "order", "limit",
	"into", "update", "delete", "insert", "with", "when", "then", "else", "case",
)

// leftOperandBadWords may not appear in the left operand of the IN predicate (they would change its meaning).
var leftOperandBadWords = setOf("not", "case", "when", "then", "else", "end", "exists", "select", "is", "like", "between", "and", "or", "in", "any", "all", "some", "null")

// lexDecomp flattens the root block of a statement into tokens. Optional blocks are inlined (a flag may
// turn them on), the SelectCols projection is replaced by its column expressions, the chunked collection
// becomes a dtColl token and every other placeholder a dtValue.
func lexDecomp(cs *compiledStmt) []dtok {
	var toks []dtok
	var walk func(blk *parsedBlock, root bool)
	walk = func(blk *parsedBlock, root bool) {
		for i, seg := range blk.Segments {
			toks = lexDecompSQL(toks, seg)
			if i >= len(blk.Slots) {
				continue
			}
			sl := blk.Slots[i]
			switch sl.Kind {
			case runtime.SlotSelectCols:
				toks = lexDecompSQL(toks, strings.Join(cs.SelectColExprs, ", "))
			case runtime.SlotOptBlock:
				if sl.Inner != nil {
					walk(sl.Inner, false)
				}
			case runtime.SlotSlice, runtime.SlotTupleSlice:
				if root && sl.ParamIdx == cs.BatchParamIdx {
					toks = append(toks, dtok{kind: dtColl})
				} else {
					toks = append(toks, dtok{kind: dtValue})
				}
			case runtime.SlotScalar, runtime.SlotQuery, runtime.SlotSeek:
				toks = append(toks, dtok{kind: dtValue})
			case runtime.SlotOrder:
				toks = append(toks, dtok{kind: dtWord, text: "order"})
			}
		}
	}
	walk(&cs.Root, true)
	return toks
}

const opBytes = "+-*/<>=~!@#%^&|:;[]{}"

// lexDecompSQL appends the tokens of a piece of SQL text (comments dropped, literals and quoted
// identifiers as dtValue).
func lexDecompSQL(toks []dtok, s string) []dtok {
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case isSpaceByte(c):
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return toks
			}
			i += 2 + end + 2
		case c == '\'' || c == '"' || c == '`':
			j := i + 1
			for j < len(s) {
				if s[j] == c {
					if j+1 < len(s) && s[j+1] == c { // doubled quote
						j += 2
						continue
					}
					break
				}
				if s[j] == '\\' && c != '"' {
					j++
				}
				j++
			}
			toks = append(toks, dtok{kind: dtValue})
			i = j + 1
		case c == '$':
			if tag, ok := matchDollarQuoteTag(s, i); ok {
				end := strings.Index(s[i+len(tag):], tag)
				toks = append(toks, dtok{kind: dtValue})
				if end < 0 {
					return toks
				}
				i += len(tag) + end + len(tag)
				continue
			}
			j := i + 1
			for j < len(s) && s[j] >= '0' && s[j] <= '9' {
				j++
			}
			toks = append(toks, dtok{kind: dtValue})
			i = max(j, i+1)
		case isIdentStartByte(c):
			j := i + 1
			for j < len(s) && (isIdentContByte(s[j]) || s[j] == '$') {
				j++
			}
			toks = append(toks, dtok{kind: dtWord, text: strings.ToLower(s[i:j])})
			i = j
		case c >= '0' && c <= '9':
			j := i + 1
			for j < len(s) && (isIdentContByte(s[j]) || s[j] == '.') {
				j++
			}
			toks = append(toks, dtok{kind: dtValue})
			i = j
		case c == '(':
			toks = append(toks, dtok{kind: dtOpen})
			i++
		case c == ')':
			toks = append(toks, dtok{kind: dtClose})
			i++
		case c == ',':
			toks = append(toks, dtok{kind: dtComma})
			i++
		case c == '?':
			toks = append(toks, dtok{kind: dtValue})
			i++
		case c == '.':
			toks = append(toks, dtok{kind: dtOp, text: "."})
			i++
		default:
			j := i + 1
			for j < len(s) && strings.IndexByte(opBytes, s[j]) >= 0 {
				j++
			}
			toks = append(toks, dtok{kind: dtOp, text: s[i:j]})
			i = j
		}
	}
	return toks
}

// decomposable reports whether the CanBatch statement cs may be chunked, and if not, why.
func decomposable(cs *compiledStmt) (bool, string) {
	toks := lexDecomp(cs)
	ci := -1
	for i, t := range toks {
		if t.kind == dtColl {
			ci = i
			break
		}
	}
	if ci < 0 {
		return false, "the collection is not at the top level of the statement"
	}
	// match[i] is the index of the matching paren of an open/close token; stack holds the groups enclosing ci.
	match := make([]int, len(toks))
	var open, stack []int
	for i, t := range toks {
		switch t.kind {
		case dtOpen:
			open = append(open, i)
		case dtClose:
			if len(open) == 0 {
				return false, "unbalanced parentheses"
			}
			o := open[len(open)-1]
			open = open[:len(open)-1]
			match[o], match[i] = i, o
		}
		if i == ci {
			stack = append(stack, open...)
		}
	}
	if len(open) != 0 {
		return false, "unbalanced parentheses"
	}
	// levelToks returns the indexes of the tokens directly inside the group opened at o (o = -1: top level),
	// nested groups represented by their open paren.
	levelToks := func(o int) []int {
		lo, hi := 0, len(toks)
		if o >= 0 {
			lo, hi = o+1, match[o]
		}
		var out []int
		for i := lo; i < hi; i++ {
			out = append(out, i)
			if toks[i].kind == dtOpen {
				i = match[i]
			}
		}
		return out
	}
	checkLevel := func(o int, orToo bool) (bool, string) {
		lvl := levelToks(o)
		for k, i := range lvl {
			t := toks[i]
			if t.kind != dtWord {
				continue
			}
			if t.text == "distinct" && isDistinctFromPredicate(toks, lvl, k) {
				continue // a IS [NOT] DISTINCT FROM b: a row-wise comparison
			}
			if nonDecomposableWords[t.text] {
				w := strings.ToUpper(t.text)
				if w == "GROUP" || w == "ORDER" {
					w += " BY"
				}
				return false, "it uses " + w
			}
			if orToo && t.text == "or" {
				return false, "the collection is combined with OR"
			}
			if aggregateFuncs[t.text] && k+1 < len(lvl) && toks[lvl[k+1]].kind == dtOpen && (k == 0 || toks[lvl[k-1]].text != ".") {
				return false, "it uses the aggregate function " + t.text + "()"
			}
		}
		return true, ""
	}

	if cs.batchIsValues() {
		if len(stack) != 0 {
			return false, "the VALUES list is not at the top level"
		}
		return checkLevel(-1, false)
	}
	switch cs.StmtVerb {
	case runtime.VerbSelect, runtime.VerbUpdate, runtime.VerbDelete:
	default:
		return false, "the collection is not an IN list of a SELECT, UPDATE or DELETE"
	}
	if ci > 0 && toks[ci-1].kind == dtWord && toks[ci-1].text == "values" {
		if len(stack) == 1 && cs.StmtVerb != runtime.VerbSelect {
			if ok, why := valuesJoinDecomposable(toks, match, stack[0]); ok {
				return checkLevel(-1, true)
			} else if why != "" {
				return false, why
			}
		}
		return false, "the VALUES list feeds a CTE, sub-query or derived table"
	}
	if len(stack) == 0 {
		return false, "the collection is not an IN list"
	}
	list := stack[len(stack)-1]
	if match[list] != ci+1 || list != ci-1 {
		return false, "the collection is not an IN list"
	}
	groups := stack[:len(stack)-1] // enclosing groups, outermost first
	// item is the token range [from, to] of the element containing the collection at the current level.
	from, to := list, match[list]
	for g := len(groups); g >= 0; g-- {
		o := -1
		if g > 0 {
			o = groups[g-1]
		}
		if ok, why := checkLevel(o, true); !ok {
			return false, why
		}
		lvl := levelToks(o)
		pos := -1
		for k, i := range lvl {
			if i == from {
				pos = k
				break
			}
		}
		if pos < 0 {
			return false, "internal: collection not found at its level"
		}
		start := pos
		if g == len(groups) {
			// The IN predicate: `<operand> IN (coll)`; walk back over the left operand.
			if pos == 0 || toks[lvl[pos-1]].text != "in" {
				return false, "the collection is not an IN list"
			}
			start = pos - 1
			for start > 0 {
				t := toks[lvl[start-1]]
				if t.kind == dtWord && t.text == "and" || t.kind == dtWord && clauseWords[t.text] {
					break
				}
				switch {
				case t.kind == dtWord && leftOperandBadWords[t.text]:
					return false, "the IN list is under " + strings.ToUpper(t.text)
				case t.kind == dtOp && t.text != "." && t.text != "::":
					return false, "the IN list is part of a larger expression"
				case t.kind == dtComma:
					return false, "the collection is not in the WHERE clause"
				}
				start--
			}
			if start == pos-1 {
				return false, "the IN list has no left operand"
			}
		} else {
			// A plain boolean group: no sub-query at its level.
			for _, i := range levelToks(groups[g]) {
				if t := toks[i]; t.kind == dtWord && (t.text == "select" || t.text == "values" || t.text == "with") {
					return false, "the collection is inside a sub-query"
				}
			}
		}
		// The element must be a whole conjunct: preceded by AND / WHERE / the group start, followed by AND /
		// the group end / a clause keyword.
		if start > 0 {
			prev := toks[lvl[start-1]]
			switch {
			case prev.kind == dtWord && prev.text == "and":
			case prev.kind == dtWord && prev.text == "where" && o == -1:
			default:
				return false, "the collection is not a conjunct of the WHERE clause"
			}
		} else if o == -1 {
			return false, "the collection is not in the WHERE clause"
		}
		if to+1 < len(toks) && (o == -1 || to+1 < match[o]) {
			next := toks[to+1]
			if !(next.kind == dtWord && (next.text == "and" || next.text == "returning" || next.text == "for" || nonDecomposableWords[next.text]) || next.kind == dtOp && next.text == ";") {
				return false, "the IN list is part of a larger expression"
			}
		}
		if o == -1 {
			// The first clause keyword before the conjunct must be WHERE.
			for k := start - 1; k >= 0; k-- {
				if t := toks[lvl[k]]; t.kind == dtWord && clauseWords[t.text] {
					if t.text == "from" && k > 0 && toks[lvl[k-1]].text == "distinct" {
						continue // IS [NOT] DISTINCT FROM
					}
					if t.text != "where" {
						return false, "the collection is not in the WHERE clause"
					}
					break
				}
			}
			break
		}
		from, to = o, match[o]
	}
	return true, ""
}

// outerJoinWords make a join keep rows that match no row of the VALUES source: every chunk would touch
// them again.
var outerJoinWords = setOf("left", "right", "full", "outer", "natural", "lateral")

// valuesJoinDecomposable reports whether the VALUES list in the group opened at o (the statement's only
// group around the collection) is a row source joined into a top-level UPDATE or DELETE, which is
// row-wise: each target row is changed by the source rows it matches, so a chunk of the source changes
// what those rows match. The accepted shapes are
//
//	UPDATE t SET ... FROM (VALUES coll) [AS] v[(cols)] WHERE ... t.c = v.c ...
//	DELETE FROM t USING (VALUES coll) [AS] v[(cols)] WHERE ... t.c = v.c ...
//	UPDATE t JOIN (VALUES coll) [AS] v[(cols)] ON t.c = v.c SET ...
//	WITH v[(cols)] AS (VALUES coll) UPDATE ... FROM v WHERE ... t.c = v.c ...   (and DELETE ... USING v)
//
// with an equality between a column of v and a column of another table as a conjunct of the top-level
// WHERE or ON, only inner joins, the CTE the statement's only one and v referenced as a table only by the
// statement's own FROM / USING / JOIN. The caller checks the top level for aggregates, GROUP BY, DISTINCT,
// ORDER BY, LIMIT, set operations and OR. why is "" when the VALUES list is not in one of these positions.
func valuesJoinDecomposable(toks []dtok, match []int, o int) (bool, string) {
	word := func(i int, w string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == dtWord && toks[i].text == w
	}
	isWord := func(i int) bool { return i >= 0 && i < len(toks) && toks[i].kind == dtWord }
	alias, aliasAt, cte := "", -1, false
	switch {
	case word(o-1, "from") || word(o-1, "using") || word(o-1, "join") || o > 0 && toks[o-1].kind == dtComma:
		i := match[o] + 1
		if word(i, "as") {
			i++
		}
		if !isWord(i) || clauseWords[toks[i].text] || nonDecomposableWords[toks[i].text] {
			return false, "the VALUES source has no alias"
		}
		alias, aliasAt = toks[i].text, i
	case word(o-1, "as") && word(0, "with"):
		// WITH v[(cols)] AS (VALUES coll) <UPDATE | DELETE>
		i := 1
		if !isWord(i) || toks[i].text == "recursive" {
			return false, ""
		}
		alias, aliasAt = toks[i].text, i
		i++
		if i < len(toks) && toks[i].kind == dtOpen {
			i = match[i] + 1
		}
		if i != o-1 {
			return false, ""
		}
		if !(word(match[o]+1, "update") || word(match[o]+1, "delete")) {
			return false, "the VALUES source is not the statement's only CTE"
		}
		cte = true
	default:
		return false, ""
	}
	depth := 0
	tableRefs, joinPred := 0, false
	clause := ""
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		switch t.kind {
		case dtOpen:
			depth++
			continue
		case dtClose:
			depth--
			continue
		}
		if t.kind != dtWord {
			continue
		}
		if depth == 0 {
			if outerJoinWords[t.text] {
				return false, "the VALUES source is joined with an outer join"
			}
			switch t.text {
			case "where", "on", "set", "from", "using", "join", "returning", "update", "delete":
				clause = t.text
			}
		}
		if t.text != alias || i == aliasAt {
			continue
		}
		if i+2 < len(toks) && toks[i+1].kind == dtOp && toks[i+1].text == "." && isWord(i+2) {
			// v.col: a join predicate when it is one side of a top-level equality in WHERE / ON whose other
			// side is a column of another table
			if depth == 0 && (clause == "where" || clause == "on") {
				if joinConjunct(toks, i, alias) {
					joinPred = true
				}
			}
			i += 2
			continue
		}
		// v as a table: only the statement's own FROM / USING / JOIN may name the CTE
		if !cte || depth != 0 || !(word(i-1, "from") || word(i-1, "using") || word(i-1, "join") || toks[i-1].kind == dtComma) {
			return false, "the VALUES source is used outside the join"
		}
		tableRefs++
	}
	if cte && tableRefs != 1 {
		return false, "the VALUES source is used outside the join"
	}
	if !joinPred {
		return false, "the VALUES source is not joined by an equality in WHERE / ON"
	}
	return true, ""
}

// colRef returns the token range [from, to] of the column reference (`col` or `tbl.col`) that starts
// (dir +1) or ends (dir -1) at toks[at], or ok false.
func colRef(toks []dtok, at, dir int) (from, to int, ok bool) {
	if at < 0 || at >= len(toks) || toks[at].kind != dtWord {
		return 0, 0, false
	}
	from, to = at, at
	if dir > 0 && at+2 < len(toks) && toks[at+1].kind == dtOp && toks[at+1].text == "." && toks[at+2].kind == dtWord {
		to = at + 2
	} else if dir < 0 && at-2 >= 0 && toks[at-1].kind == dtOp && toks[at-1].text == "." && toks[at-2].kind == dtWord {
		from = at - 2
	}
	return from, to, true
}

// joinConjunct reports whether alias.col at toks[i:i+3] is one side of an equality with a column of another
// table that is a whole conjunct (preceded by WHERE / ON / AND, followed by AND, a clause keyword, ";" or
// the end).
func joinConjunct(toks []dtok, i int, alias string) bool {
	isEq := func(k int) bool { return k >= 0 && k < len(toks) && toks[k].kind == dtOp && toks[k].text == "=" }
	var from, to int
	switch {
	case isEq(i + 3):
		f, t, ok := colRef(toks, i+4, +1)
		if !ok || toks[f].text == alias {
			return false
		}
		from, to = i, t
	case isEq(i - 1):
		f, _, ok := colRef(toks, i-2, -1)
		if !ok || toks[f].text == alias {
			return false
		}
		from, to = f, i+2
	default:
		return false
	}
	if p := from - 1; p < 0 || toks[p].kind != dtWord || !(toks[p].text == "where" || toks[p].text == "on" || toks[p].text == "and") {
		return false
	}
	n := to + 1
	return n >= len(toks) || toks[n].kind == dtWord && (toks[n].text == "and" || clauseWords[toks[n].text]) || toks[n].kind == dtOp && toks[n].text == ";"
}

// isDistinctFromPredicate reports whether the DISTINCT at lvl[k] is part of `IS [NOT] DISTINCT FROM`.
func isDistinctFromPredicate(toks []dtok, lvl []int, k int) bool {
	word := func(j int) string {
		if j < 0 || j >= len(lvl) || toks[lvl[j]].kind != dtWord {
			return ""
		}
		return toks[lvl[j]].text
	}
	if word(k+1) != "from" {
		return false
	}
	return word(k-1) == "is" || word(k-1) == "not" && word(k-2) == "is"
}
