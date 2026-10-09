package sql_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/rqlite/sql"
)

// walkIdentNames parses s and returns the sorted names of every Ident that
// Walk visits.
func walkIdentNames(tb testing.TB, s string) []string {
	tb.Helper()
	stmt, err := sql.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		tb.Fatal(err)
	}
	var names []string
	if _, err := sql.Walk(sql.VisitFunc(func(n sql.Node) (sql.Node, error) {
		if ident, ok := n.(*sql.Ident); ok {
			names = append(names, ident.Name)
		}
		return n, nil
	}), stmt); err != nil {
		tb.Fatal(err)
	}
	sort.Strings(names)
	return names
}

func assertWalkVisits(tb testing.TB, s string, want ...string) {
	tb.Helper()
	got := walkIdentNames(tb, s)
	for _, w := range want {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			tb.Errorf("Walk(%q) did not visit ident %q; visited %v", s, w, got)
		}
	}
}

// Ensure Walk descends into common table expressions.
func TestWalk_WithClause(t *testing.T) {
	assertWalkVisits(t, `WITH cte (c1) AS (SELECT a FROM t1) SELECT b FROM cte`, "cte", "c1", "a", "t1", "b")
	assertWalkVisits(t, `WITH RECURSIVE cte AS (SELECT a FROM t1) INSERT INTO t2 SELECT * FROM cte`, "a", "t1", "t2")
}

// Ensure Walk descends into scalar subqueries used as expressions.
func TestWalk_SelectExpr(t *testing.T) {
	assertWalkVisits(t, `SELECT (SELECT a FROM t1) FROM t2`, "a", "t1", "t2")
	assertWalkVisits(t, `UPDATE t2 SET x = (SELECT max(a) FROM t1)`, "a", "t1", "x")
}

// Ensure Walk visits the operand of IS NULL / NOT NULL.
func TestWalk_Null(t *testing.T) {
	assertWalkVisits(t, `SELECT * FROM t1 WHERE a IS NULL`, "a", "t1")
	assertWalkVisits(t, `SELECT * FROM t1 WHERE b NOT NULL AND c ISNULL`, "b", "c", "t1")
}

// Ensure Walk visits a COLLATE column constraint's name and collation.
func TestWalk_CollateConstraint(t *testing.T) {
	assertWalkVisits(t, `CREATE TABLE t1 (a TEXT CONSTRAINT c1 COLLATE NOCASE)`, "t1", "a", "c1", "NOCASE")
}

// Ensure Walk visits the schema and value of a PRAGMA.
func TestWalk_Pragma(t *testing.T) {
	assertWalkVisits(t, `PRAGMA main.foo = bar`, "main", "foo", "bar")
	assertWalkVisits(t, `PRAGMA foo(bar)`, "foo", "bar")
}
