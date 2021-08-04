package sqlparser_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/longbridgeapp/sqlparser"
)

func TestExprString(t *testing.T) {
	if got, want := sqlparser.ExprString(&sqlparser.NullLit{}), "NULL"; got != want {
		t.Fatalf("ExprString()=%q, want %q", got, want)
	} else if got, want := sqlparser.ExprString(nil), ""; got != want {
		t.Fatalf("ExprString()=%q, want %q", got, want)
	}
}

func TestSplitExprTree(t *testing.T) {
	t.Run("AND-only", func(t *testing.T) {
		AssertSplitExprTree(t, `x = 1 AND y = 2 AND z = 3`, []sqlparser.Expr{
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "x"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "1"}},
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "y"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "2"}},
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "z"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "3"}},
		})
	})

	t.Run("OR", func(t *testing.T) {
		AssertSplitExprTree(t, `x = 1 AND (y = 2 OR y = 3) AND z = 4`, []sqlparser.Expr{
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "x"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "1"}},
			&sqlparser.BinaryExpr{
				X:  &sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "y"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "2"}},
				Op: sqlparser.OR,
				Y:  &sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "y"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "3"}},
			},
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "z"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "4"}},
		})
	})

	t.Run("ParenExpr", func(t *testing.T) {
		AssertSplitExprTree(t, `x = 1 AND (y = 2 AND z = 3)`, []sqlparser.Expr{
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "x"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "1"}},
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "y"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "2"}},
			&sqlparser.BinaryExpr{X: &sqlparser.Ident{Name: "z"}, Op: sqlparser.EQ, Y: &sqlparser.NumberLit{Value: "3"}},
		})
	})
}

func AssertSplitExprTree(tb testing.TB, s string, want []sqlparser.Expr) {
	tb.Helper()
	if diff := deep.Equal(sqlparser.SplitExprTree(StripExprPos(sqlparser.MustParseExprString(s))), want); diff != nil {
		tb.Fatal("mismatch: \n" + strings.Join(diff, "\n"))
	}
}

func TestDeleteStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sqlparser.DeleteStatement{
		TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}, Alias: &sqlparser.Ident{Name: "tbl2"}},
	}, `DELETE FROM "tbl" AS "tbl2"`)

	AssertStatementStringer(t, &sqlparser.DeleteStatement{
		TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
	}, `DELETE FROM "tbl"`)

	AssertStatementStringer(t, &sqlparser.DeleteStatement{
		TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		Condition: &sqlparser.BoolLit{Value: true},
	}, `DELETE FROM "tbl" WHERE TRUE`)
}

func TestInsertStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName:     &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		DefaultValues: true,
	}, `INSERT INTO "tbl" DEFAULT VALUES`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName: &sqlparser.TableName{
			Name:  &sqlparser.Ident{Name: "tbl"},
			Alias: &sqlparser.Ident{Name: "x"},
		},
		DefaultValues: true,
	}, `INSERT INTO "tbl" AS "x" DEFAULT VALUES`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		Query: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `INSERT INTO "tbl" SELECT *`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		ColumnNames: []*sqlparser.Ident{
			{Name: "x"},
			{Name: "y"},
		},
		Expressions: []*sqlparser.Exprs{
			{Exprs: []sqlparser.Expr{&sqlparser.NullLit{}, &sqlparser.NullLit{}}},
			{Exprs: []sqlparser.Expr{&sqlparser.NullLit{}, &sqlparser.NullLit{}}},
		},
	}, `INSERT INTO "tbl" ("x", "y") VALUES (NULL, NULL), (NULL, NULL)`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName:     &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		DefaultValues: true,
		UpsertClause: &sqlparser.UpsertClause{
			DoNothing: true,
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES ON CONFLICT DO NOTHING`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName:     &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		DefaultValues: true,
		UpsertClause: &sqlparser.UpsertClause{
			Columns: []*sqlparser.IndexedColumn{
				{X: &sqlparser.Ident{Name: "x"}, Asc: true},
				{X: &sqlparser.Ident{Name: "y"}, Desc: true},
			},
			WhereExpr: &sqlparser.BoolLit{Value: true},
			Assignments: []*sqlparser.Assignment{
				{Columns: []*sqlparser.Ident{{Name: "x"}}, Expr: &sqlparser.NumberLit{Value: "100"}},
				{Columns: []*sqlparser.Ident{{Name: "y"}, {Name: "z"}}, Expr: &sqlparser.NumberLit{Value: "200"}},
			},
			UpdateWhereExpr: &sqlparser.BoolLit{Value: false},
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES ON CONFLICT ("x" ASC, "y" DESC) WHERE TRUE DO UPDATE SET "x" = 100, ("y", "z") = 200 WHERE FALSE`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName:         &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		DefaultValues:     true,
		OutputExpressions: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
	}, `INSERT INTO "tbl" DEFAULT VALUES RETURNING *`)

	AssertStatementStringer(t, &sqlparser.InsertStatement{
		TableName:     &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		DefaultValues: true,
		OutputExpressions: &sqlparser.OutputNames{
			&sqlparser.ResultColumn{Expr: &sqlparser.Ident{Name: "x"}, Alias: &sqlparser.Ident{Name: "y"}},
			&sqlparser.ResultColumn{Expr: &sqlparser.Ident{Name: "z"}},
		},
	}, `INSERT INTO "tbl" DEFAULT VALUES RETURNING "x" AS "y", "z"`)
}

func TestSelectStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{
			&sqlparser.ResultColumn{Expr: &sqlparser.Ident{Name: "x"}, Alias: &sqlparser.Ident{Name: "y"}},
			&sqlparser.ResultColumn{Expr: &sqlparser.Ident{Name: "z"}},
		},
	}, `SELECT "x" AS "y", "z"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Distinct: true,
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
			Expr: &sqlparser.Ident{Name: "x"},
		}},
	}, `SELECT DISTINCT "x"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		All:     true,
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Expr: &sqlparser.Ident{Name: "x"}}},
	}, `SELECT ALL "x"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Hint:    &sqlparser.Hint{Value: "HINT"},
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Expr: &sqlparser.Ident{Name: "x"}}},
	}, `SELECT /* HINT */ "x"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns:          &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		Condition:        &sqlparser.BoolLit{Value: true},
		GroupingElements: []sqlparser.Expr{&sqlparser.Ident{Name: "x"}, &sqlparser.Ident{Name: "y"}},
		HavingCondition:  &sqlparser.Ident{Name: "z"},
	}, `SELECT * FROM "tbl" WHERE TRUE GROUP BY "x", "y" HAVING "z"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.ParenSource{
			X:     &sqlparser.SelectStatement{Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}}},
			Alias: &sqlparser.Ident{Name: "tbl"},
		},
	}, `SELECT * FROM (SELECT *) AS "tbl"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.ParenSource{
			X: &sqlparser.SelectStatement{Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}}},
		},
	}, `SELECT * FROM (SELECT *)`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		Union:   true,
		Compound: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `SELECT * UNION SELECT *`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns:  &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		Union:    true,
		UnionAll: true,
		Compound: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `SELECT * UNION ALL SELECT *`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns:   &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		Intersect: true,
		Compound: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `SELECT * INTERSECT SELECT *`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		Except:  true,
		Compound: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `SELECT * EXCEPT SELECT *`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		OrderBy: []*sqlparser.OrderingTerm{
			{X: &sqlparser.Ident{Name: "x"}},
			{X: &sqlparser.Ident{Name: "y"}},
		},
	}, `SELECT * ORDER BY "x", "y"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		Limit:   &sqlparser.NumberLit{Value: "1"},
		Offset:  &sqlparser.NumberLit{Value: "2"},
	}, `SELECT * LIMIT 1 OFFSET 2`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.JoinClause{
			X:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "x"}},
			Operator: &sqlparser.JoinOperator{Comma: true},
			Y:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x", "y"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.JoinClause{
			X:          &sqlparser.TableName{Name: &sqlparser.Ident{Name: "x"}},
			Operator:   &sqlparser.JoinOperator{},
			Y:          &sqlparser.TableName{Name: &sqlparser.Ident{Name: "y"}},
			Constraint: &sqlparser.OnConstraint{X: &sqlparser.BoolLit{Value: true}},
		},
	}, `SELECT * FROM "x" JOIN "y" ON TRUE`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.JoinClause{
			X:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "x"}},
			Operator: &sqlparser.JoinOperator{Natural: true, Inner: true},
			Y:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "y"}},
			Constraint: &sqlparser.UsingConstraint{
				Columns: []*sqlparser.Ident{{Name: "a"}, {Name: "b"}},
			},
		},
	}, `SELECT * FROM "x" NATURAL INNER JOIN "y" USING ("a", "b")`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.JoinClause{
			X:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "x"}},
			Operator: &sqlparser.JoinOperator{Left: true},
			Y:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x" LEFT JOIN "y"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.JoinClause{
			X:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "x"}},
			Operator: &sqlparser.JoinOperator{Left: true, Outer: true},
			Y:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x" LEFT OUTER JOIN "y"`)

	AssertStatementStringer(t, &sqlparser.SelectStatement{
		Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		FromItems: &sqlparser.JoinClause{
			X:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "x"}},
			Operator: &sqlparser.JoinOperator{Cross: true},
			Y:        &sqlparser.TableName{Name: &sqlparser.Ident{Name: "y"}},
		},
	}, `SELECT * FROM "x" CROSS JOIN "y"`)
}

func TestUpdateStatement_String(t *testing.T) {
	AssertStatementStringer(t, &sqlparser.UpdateStatement{
		TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
		Assignments: []*sqlparser.Assignment{
			{Columns: []*sqlparser.Ident{{Name: "x"}}, Expr: &sqlparser.NumberLit{Value: "100"}},
			{Columns: []*sqlparser.Ident{{Name: "y"}}, Expr: &sqlparser.NumberLit{Value: "200"}},
		},
		Condition: &sqlparser.BoolLit{Value: true},
	}, `UPDATE "tbl" SET "x" = 100, "y" = 200 WHERE TRUE`)
}

func TestIdent_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.Ident{Name: "foo"}, `"foo"`)
	AssertExprStringer(t, &sqlparser.Ident{Name: "foo \" bar"}, `"foo "" bar"`)
}

func TestStringLit_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.StringLit{Value: "foo"}, `'foo'`)
	AssertExprStringer(t, &sqlparser.StringLit{Value: "foo ' bar"}, `'foo '' bar'`)
}

func TestNumberLit_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.NumberLit{Value: "123.45"}, `123.45`)
}

func TestBlobLit_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.BlobLit{Value: "0123abcd"}, `x'0123abcd'`)
}

func TestBoolLit_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.BoolLit{Value: true}, `TRUE`)
	AssertExprStringer(t, &sqlparser.BoolLit{Value: false}, `FALSE`)
}

func TestNullLit_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.NullLit{}, `NULL`)
}

func TestBindExpr_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.BindExpr{Name: "foo"}, `foo`)
}

func TestParenExpr_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.ParenExpr{X: &sqlparser.NullLit{}}, `(NULL)`)
}

func TestUnaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.UnaryExpr{Op: sqlparser.PLUS, X: &sqlparser.NumberLit{Value: "100"}}, `+100`)
	AssertExprStringer(t, &sqlparser.UnaryExpr{Op: sqlparser.MINUS, X: &sqlparser.NumberLit{Value: "100"}}, `-100`)
	AssertNodeStringerPanic(t, &sqlparser.UnaryExpr{X: &sqlparser.NumberLit{Value: "100"}}, `sqlparser.UnaryExpr.String(): invalid op ILLEGAL`)
}

func TestBinaryExpr_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.PLUS, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 + 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.MINUS, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 - 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.STAR, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 * 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.SLASH, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 / 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.REM, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 % 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.CONCAT, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 || 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.BETWEEN, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.Range{X: &sqlparser.NumberLit{Value: "2"}, Y: &sqlparser.NumberLit{Value: "3"}}}, `1 BETWEEN 2 AND 3`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NOTBETWEEN, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.BinaryExpr{Op: sqlparser.AND, X: &sqlparser.NumberLit{Value: "2"}, Y: &sqlparser.NumberLit{Value: "3"}}}, `1 NOT BETWEEN 2 AND 3`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.LSHIFT, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 << 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.RSHIFT, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 >> 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.BITAND, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 & 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.BITOR, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 | 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.LT, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 < 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.LG, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 <> 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.LE, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 <= 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.GT, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 > 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.GE, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 >= 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.EQ, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 = 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NE, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 != 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.IS, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 IS 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.ISNOT, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 IS NOT 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.IN, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.Exprs{Exprs: []sqlparser.Expr{&sqlparser.NumberLit{Value: "2"}}}}, `1 IN (2)`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NOTIN, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.Exprs{Exprs: []sqlparser.Expr{&sqlparser.NumberLit{Value: "2"}}}}, `1 NOT IN (2)`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.LIKE, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 LIKE 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NOTLIKE, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 NOT LIKE 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.GLOB, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 GLOB 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NOTGLOB, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 NOT GLOB 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.MATCH, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 MATCH 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NOTMATCH, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 NOT MATCH 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.REGEXP, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 REGEXP 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.NOTREGEXP, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 NOT REGEXP 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.AND, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 AND 2`)
	AssertExprStringer(t, &sqlparser.BinaryExpr{Op: sqlparser.OR, X: &sqlparser.NumberLit{Value: "1"}, Y: &sqlparser.NumberLit{Value: "2"}}, `1 OR 2`)
	AssertNodeStringerPanic(t, &sqlparser.BinaryExpr{}, `sqlparser.BinaryExpr.String(): invalid op ILLEGAL`)
}

func TestCaseExpr_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.CaseExpr{
		Operand: &sqlparser.Ident{Name: "foo"},
		Blocks: []*sqlparser.CaseBlock{
			{Condition: &sqlparser.NumberLit{Value: "1"}, Body: &sqlparser.BoolLit{Value: true}},
			{Condition: &sqlparser.NumberLit{Value: "2"}, Body: &sqlparser.BoolLit{Value: false}},
		},
		ElseExpr: &sqlparser.NullLit{},
	}, `CASE "foo" WHEN 1 THEN TRUE WHEN 2 THEN FALSE ELSE NULL END`)

	AssertExprStringer(t, &sqlparser.CaseExpr{
		Blocks: []*sqlparser.CaseBlock{
			{Condition: &sqlparser.NumberLit{Value: "1"}, Body: &sqlparser.BoolLit{Value: true}},
		},
	}, `CASE WHEN 1 THEN TRUE END`)
}

func TestExprs_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.Exprs{Exprs: []sqlparser.Expr{&sqlparser.NullLit{}}}, `(NULL)`)
	AssertExprStringer(t, &sqlparser.Exprs{Exprs: []sqlparser.Expr{&sqlparser.NullLit{}, &sqlparser.NullLit{}}}, `(NULL, NULL)`)
}

func TestQualifiedRef_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.QualifiedRef{Table: &sqlparser.Ident{Name: "tbl"}, Column: &sqlparser.Ident{Name: "col"}}, `"tbl"."col"`)
	AssertExprStringer(t, &sqlparser.QualifiedRef{Table: &sqlparser.Ident{Name: "tbl"}, Star: true}, `"tbl".*`)
}

func TestCall_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.Call{Name: &sqlparser.Ident{Name: "foo"}}, `foo()`)
	AssertExprStringer(t, &sqlparser.Call{Name: &sqlparser.Ident{Name: "foo"}, Star: true}, `foo(*)`)

	AssertExprStringer(t, &sqlparser.Call{
		Name:     &sqlparser.Ident{Name: "foo"},
		Distinct: true,
		Args: []sqlparser.Expr{
			&sqlparser.NullLit{},
			&sqlparser.NullLit{},
		},
	}, `foo(DISTINCT NULL, NULL)`)

	AssertExprStringer(t, &sqlparser.Call{
		Name: &sqlparser.Ident{Name: "foo"},
		Filter: &sqlparser.FilterClause{
			X: &sqlparser.BoolLit{Value: true},
		},
	}, `foo() FILTER (WHERE TRUE)`)
}

func TestExists_String(t *testing.T) {
	AssertExprStringer(t, &sqlparser.Exists{
		Select: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `EXISTS (SELECT *)`)

	AssertExprStringer(t, &sqlparser.Exists{
		Not: true,
		Select: &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
		},
	}, `NOT EXISTS (SELECT *)`)
}

func AssertExprStringer(tb testing.TB, expr sqlparser.Expr, s string) {
	tb.Helper()
	if str := expr.String(); str != s {
		tb.Fatalf("String()=%s, expected %s", str, s)
	} else if _, err := sqlparser.NewParser(strings.NewReader(str)).ParseExpr(); err != nil {
		tb.Fatalf("cannot parse string: %s; err=%s", str, err)
	}
}

func AssertStatementStringer(tb testing.TB, stmt sqlparser.Statement, s string) {
	tb.Helper()
	if str := stmt.String(); str != s {
		tb.Fatalf("String()=%s, expected %s", str, s)
	} else if _, err := sqlparser.NewParser(strings.NewReader(str)).ParseStatement(); err != nil {
		tb.Fatalf("cannot parse string: %s; err=%s", str, err)
	}
}

func AssertNodeStringerPanic(tb testing.TB, node sqlparser.Node, msg string) {
	tb.Helper()
	var r interface{}
	func() {
		defer func() { r = recover() }()
		_ = node.String()
	}()
	if r == nil {
		tb.Fatal("expected node stringer to panic")
	} else if r != msg {
		tb.Fatalf("recover()=%s, want %s", r, msg)
	}
}

// StripPos removes the position data from a node and its children.
// This function returns the root argument passed in.
func StripPos(root sqlparser.Node) sqlparser.Node {
	zero := reflect.ValueOf(sqlparser.Pos{})

	_ = sqlparser.Walk(sqlparser.VisitFunc(func(node sqlparser.Node) error {
		value := reflect.Indirect(reflect.ValueOf(node))
		for i := 0; i < value.NumField(); i++ {
			if field := value.Field(i); field.Type() == zero.Type() {
				field.Set(zero)
			}
		}
		return nil
	}), root)
	return root
}

func StripExprPos(root sqlparser.Expr) sqlparser.Expr {
	StripPos(root)
	return root
}
