package sqlparser_test

import (
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/longbridgeapp/sqlparser"
)

func TestParser_ParseStatement(t *testing.T) {
	t.Run("ErrNoStatement", func(t *testing.T) {
		AssertParseStatementError(t, `123`, `1:1: expected statement, found 123`)
	})

	t.Run("Select", func(t *testing.T) {
		AssertParseStatement(t, `SELECT * FROM tbl`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT /* hint */ * FROM tbl`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
			Hint: &sqlparser.Hint{
				Value: "hint",
			},
		})

		AssertParseStatement(t, `SELECT DISTINCT * FROM tbl`, &sqlparser.SelectStatement{
			Distinct: true,
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT ALL * FROM tbl`, &sqlparser.SelectStatement{
			All: true,
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT foo AS FOO, bar baz, tbl.* FROM tbl`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{
				&sqlparser.ResultColumn{
					Expr:  &sqlparser.Ident{Name: "foo"},
					Alias: &sqlparser.Ident{Name: "FOO"},
				},
				&sqlparser.ResultColumn{
					Expr:  &sqlparser.Ident{Name: "bar"},
					Alias: &sqlparser.Ident{Name: "baz"},
				},
				&sqlparser.ResultColumn{
					Expr: &sqlparser.QualifiedRef{
						Table: &sqlparser.Ident{Name: "tbl"},
						Star:  true,
					},
				},
			},
			FromItems: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl tbl2`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.TableName{
				Name:  &sqlparser.Ident{Name: "tbl"},
				Alias: &sqlparser.Ident{Name: "tbl2"},
			},
		})
		AssertParseStatement(t, `SELECT * FROM tbl AS tbl2`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.TableName{
				Name:  &sqlparser.Ident{Name: "tbl"},
				Alias: &sqlparser.Ident{Name: "tbl2"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM (SELECT *) AS tbl`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.ParenSource{
				X: &sqlparser.SelectStatement{
					Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
						Star: true,
					}},
				},
				Alias: &sqlparser.Ident{Name: "tbl"},
			},
		})

		AssertParseStatement(t, `SELECT * FROM foo, bar`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{Comma: true},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo JOIN bar`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo NATURAL JOIN bar`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{Natural: true},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo INNER JOIN bar ON true`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{Inner: true},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
				Constraint: &sqlparser.OnConstraint{
					X: &sqlparser.BoolLit{Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT JOIN bar USING (x, y)`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{Left: true},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
				Constraint: &sqlparser.UsingConstraint{
					Columns: []*sqlparser.Ident{
						{Name: "x"},
						{Name: "y"},
					},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM X INNER JOIN Y ON true INNER JOIN Z ON false`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "X"},
				},
				Operator: &sqlparser.JoinOperator{Inner: true},
				Y: &sqlparser.JoinClause{
					X: &sqlparser.TableName{
						Name: &sqlparser.Ident{Name: "Y"},
					},
					Operator: &sqlparser.JoinOperator{Inner: true},
					Y: &sqlparser.TableName{
						Name: &sqlparser.Ident{Name: "Z"},
					},
					Constraint: &sqlparser.OnConstraint{
						X: &sqlparser.BoolLit{Value: false},
					},
				},
				Constraint: &sqlparser.OnConstraint{
					X: &sqlparser.BoolLit{Value: true},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo LEFT OUTER JOIN bar`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{Left: true, Outer: true},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
			},
		})
		AssertParseStatement(t, `SELECT * FROM foo CROSS JOIN bar`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			FromItems: &sqlparser.JoinClause{
				X: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "foo"},
				},
				Operator: &sqlparser.JoinOperator{Cross: true},
				Y: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "bar"},
				},
			},
		})

		AssertParseStatement(t, `SELECT * WHERE true`, &sqlparser.SelectStatement{
			Columns:   &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
			Condition: &sqlparser.BoolLit{Value: true},
		})

		AssertParseStatement(t, `SELECT * GROUP BY foo, bar`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
			GroupingElements: []sqlparser.Expr{
				&sqlparser.Ident{Name: "foo"},
				&sqlparser.Ident{Name: "bar"},
			},
		})
		AssertParseStatement(t, `SELECT * GROUP BY foo HAVING true`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{Star: true}},
			GroupingElements: []sqlparser.Expr{
				&sqlparser.Ident{Name: "foo"},
			},
			HavingCondition: &sqlparser.BoolLit{Value: true},
		})

		AssertParseStatement(t, `SELECT * ORDER BY foo ASC, bar DESC`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			OrderBy: []*sqlparser.OrderingTerm{
				{X: &sqlparser.Ident{Name: "foo"}, Asc: true},
				{X: &sqlparser.Ident{Name: "bar"}, Desc: true},
			},
		})

		AssertParseStatement(t, `SELECT * LIMIT 1`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Limit: &sqlparser.NumberLit{Value: "1"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1 OFFSET 2`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Limit:  &sqlparser.NumberLit{Value: "1"},
			Offset: &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseStatement(t, `SELECT * LIMIT 1, 2`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Limit:  &sqlparser.NumberLit{Value: "1"},
			Offset: &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseStatement(t, `SELECT * UNION SELECT * ORDER BY foo`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Union: true,
			Compound: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Star: true,
				}},
			},
			OrderBy: []*sqlparser.OrderingTerm{
				{X: &sqlparser.Ident{Name: "foo"}},
			},
		})
		AssertParseStatement(t, `SELECT * UNION ALL SELECT *`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Union:    true,
			UnionAll: true,
			Compound: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Star: true,
				}},
			},
		})
		AssertParseStatement(t, `SELECT * INTERSECT SELECT *`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Intersect: true,
			Compound: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Star: true,
				}},
			},
		})
		AssertParseStatement(t, `SELECT * EXCEPT SELECT *`, &sqlparser.SelectStatement{
			Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
				Star: true,
			}},
			Except: true,
			Compound: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Star: true,
				}},
			},
		})

		AssertParseStatementError(t, `SELECT `, `1:7: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT 1+`, `1:9: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo,`, `1:11: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo AS`, `1:13: expected column alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo.* AS`, `1:14: expected semicolon or EOF, found 'AS'`)
		AssertParseStatementError(t, `SELECT foo FROM`, `1:15: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo INNER`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo CROSS`, `1:23: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo NATURAL`, `1:25: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT`, `1:22: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo LEFT OUTER`, `1:28: expected JOIN, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo,`, `1:18: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar ON`, `1:29: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING`, `1:32: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (`, `1:34: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x`, `1:35: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo JOIN bar USING (x,`, `1:36: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (`, `1:15: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM ((`, `1:16: expected table name or left paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT`, `1:21: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (tbl`, `1:18: expected right paren, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM (SELECT *) AS`, `1:27: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * FROM foo AS`, `1:20: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `SELECT foo WHERE`, `1:16: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP`, `1:14: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo bar`, `1:23: expected semicolon or EOF, found bar`)
		AssertParseStatementError(t, `SELECT * GROUP BY foo HAVING`, `1:28: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER`, `1:14: expected BY, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * ORDER BY 1,`, `1:20: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT`, `1:14: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1,`, `1:17: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * LIMIT 1 OFFSET`, `1:23: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `SELECT * UNION`, `1:14: expected SELECT, found 'EOF'`)
	})

	t.Run("Insert", func(t *testing.T) {
		AssertParseStatement(t, `INSERT INTO tbl (x, y) VALUES (1, 2)`, &sqlparser.InsertStatement{
			TableName: &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
			ColumnNames: []*sqlparser.Ident{
				{Name: "x"},
				{Name: "y"},
			},
			Expressions: []*sqlparser.Exprs{{
				Exprs: []sqlparser.Expr{
					&sqlparser.NumberLit{Value: "1"},
					&sqlparser.NumberLit{Value: "2"},
				},
			}},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) SELECT y`, &sqlparser.InsertStatement{
			TableName:   &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
			ColumnNames: []*sqlparser.Ident{{Name: "x"}},
			Query: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Expr: &sqlparser.Ident{Name: "y"},
				}},
			},
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) DEFAULT VALUES`, &sqlparser.InsertStatement{
			TableName:     &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
			ColumnNames:   []*sqlparser.Ident{{Name: "x"}},
			DefaultValues: true,
		})

		AssertParseStatement(t, `INSERT INTO tbl AS tbl2 (x) DEFAULT VALUES`, &sqlparser.InsertStatement{
			TableName: &sqlparser.TableName{
				Name:  &sqlparser.Ident{Name: "tbl"},
				Alias: &sqlparser.Ident{Name: "tbl2"},
			},
			ColumnNames:   []*sqlparser.Ident{{Name: "x"}},
			DefaultValues: true,
		})

		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y ASC, z DESC) DO NOTHING`, &sqlparser.InsertStatement{
			TableName:   &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
			ColumnNames: []*sqlparser.Ident{{Name: "x"}},
			Expressions: []*sqlparser.Exprs{{
				Exprs: []sqlparser.Expr{
					&sqlparser.NumberLit{Value: "1"},
				},
			}},
			UpsertClause: &sqlparser.UpsertClause{
				Columns: []*sqlparser.IndexedColumn{
					{X: &sqlparser.Ident{Name: "y"}, Asc: true},
					{X: &sqlparser.Ident{Name: "z"}, Desc: true},
				},
				DoNothing: true,
			},
		})
		AssertParseStatement(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (y) WHERE true DO UPDATE SET foo = 1, (bar, baz) = 2 WHERE false`, &sqlparser.InsertStatement{
			TableName:   &sqlparser.TableName{Name: &sqlparser.Ident{Name: "tbl"}},
			ColumnNames: []*sqlparser.Ident{{Name: "x"}},
			Expressions: []*sqlparser.Exprs{{
				Exprs: []sqlparser.Expr{
					&sqlparser.NumberLit{Value: "1"},
				},
			}},
			UpsertClause: &sqlparser.UpsertClause{
				Columns: []*sqlparser.IndexedColumn{
					{X: &sqlparser.Ident{Name: "y"}},
				},
				WhereExpr: &sqlparser.BoolLit{Value: true},
				DoUpdate:  true,
				Assignments: []*sqlparser.Assignment{
					{
						Columns: []*sqlparser.Ident{
							{Name: "foo"},
						},
						Expr: &sqlparser.NumberLit{Value: "1"},
					},
					{
						Columns: []*sqlparser.Ident{
							{Name: "bar"},
							{Name: "baz"},
						},
						Expr: &sqlparser.NumberLit{Value: "2"},
					},
				},
				UpdateWhereExpr: &sqlparser.BoolLit{Value: false},
			},
		})

		AssertParseStatementError(t, `INSERT`, `1:6: expected INTO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl AS`, `1:18: expected table alias, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl `, `1:16: expected DEFAULT VALUES, VALUES or SELECT, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (`, `1:17: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x`, `1:18: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x)`, `1:19: expected DEFAULT VALUES, VALUES or SELECT, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES`, `1:26: expected left paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (`, `1:28: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1`, `1:29: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) SELECT`, `1:26: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) DEFAULT`, `1:27: expected VALUES, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON`, `1:33: expected CONFLICT, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (`, `1:44: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x`, `1:45: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) WHERE`, `1:52: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x)`, `1:46: expected DO, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO`, `1:49: expected NOTHING or UPDATE SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE`, `1:56: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo`, `1:64: expected =, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo =`, `1:66: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET foo = 1 WHERE`, `1:74: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (`, `1:62: expected column name, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) ON CONFLICT (x) DO UPDATE SET (foo`, `1:65: expected comma or right paren, found 'EOF'`)
		AssertParseStatementError(t, `INSERT INTO tbl (x) VALUES (1) RETURNING`, `1:40: expected expression, found 'EOF'`)
	})

	t.Run("Update", func(t *testing.T) {
		AssertParseStatement(t, `UPDATE tbl SET x = 1, y = 2`, &sqlparser.UpdateStatement{
			TableName: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
			Assignments: []*sqlparser.Assignment{
				{
					Columns: []*sqlparser.Ident{{Name: "x"}},
					Expr:    &sqlparser.NumberLit{Value: "1"},
				},
				{
					Columns: []*sqlparser.Ident{{Name: "y"}},
					Expr:    &sqlparser.NumberLit{Value: "2"},
				},
			},
		})
		AssertParseStatement(t, `UPDATE tbl SET x = 1 WHERE y = 2`, &sqlparser.UpdateStatement{
			TableName: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
			Assignments: []*sqlparser.Assignment{{
				Columns: []*sqlparser.Ident{{Name: "x"}},
				Expr:    &sqlparser.NumberLit{Value: "1"},
			}},
			Condition: &sqlparser.BinaryExpr{
				X:  &sqlparser.Ident{Name: "y"},
				Op: sqlparser.EQ,
				Y:  &sqlparser.NumberLit{Value: "2"},
			},
		})

		AssertParseStatementError(t, `UPDATE`, `1:6: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl`, `1:10: expected SET, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET`, `1:14: expected column name or column list, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = `, `1:19: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = 1 WHERE`, `1:26: expected expression, found 'EOF'`)
		AssertParseStatementError(t, `UPDATE tbl SET x = 1 WHERE y =`, `1:30: expected expression, found 'EOF'`)
	})

	t.Run("Delete", func(t *testing.T) {
		AssertParseStatement(t, `DELETE FROM tbl`, &sqlparser.DeleteStatement{
			TableName: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
		})
		AssertParseStatement(t, `DELETE FROM tbl WHERE x = 1`, &sqlparser.DeleteStatement{
			TableName: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
			Condition: &sqlparser.BinaryExpr{
				X:  &sqlparser.Ident{Name: "x"},
				Op: sqlparser.EQ,
				Y:  &sqlparser.NumberLit{Value: "1"},
			},
		})
		AssertParseStatement(t, `DELETE FROM ONLY tbl`, &sqlparser.DeleteStatement{
			Only: true,
			TableName: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
		})
		/*
			AssertParseStatement(t, `DELETE FROM tbl USING tbl2, tbl3`, &sqlparser.DeleteStatement{
				TableName: &sqlparser.TableName{
					Name: &sqlparser.Ident{Name: "tbl"},
				},
				UsingList: []*sqlparser.TableName{
					&sqlparser.TableName{
						Name: &sqlparser.Ident{Name: "tbl"},
					},
					&sqlparser.TableName{
						Name: &sqlparser.Ident{Name: "tbl"},
					},
				},
			})*/
		AssertParseStatement(t, `DELETE FROM tbl WHERE CURRENT OF c_tasks`, &sqlparser.DeleteStatement{
			TableName: &sqlparser.TableName{
				Name: &sqlparser.Ident{Name: "tbl"},
			},
			CursorName: &sqlparser.Ident{Name: "c_tasks"},
		})

		AssertParseStatementError(t, `DELETE FROM`, `1:11: expected table name, found 'EOF'`)
		AssertParseStatementError(t, `DELETE FROM tbl WHERE`, `1:21: expected expression, found 'EOF'`)
	})
}

func TestParser_ParseExpr(t *testing.T) {
	t.Run("Ident", func(t *testing.T) {
		AssertParseExpr(t, `fooBAR_123'`, &sqlparser.Ident{Name: `fooBAR_123`})
	})
	t.Run("StringLit", func(t *testing.T) {
		AssertParseExpr(t, `'foo bar'`, &sqlparser.StringLit{Value: `foo bar`})
	})
	t.Run("BlobLit", func(t *testing.T) {
		AssertParseExpr(t, `x'0123'`, &sqlparser.BlobLit{Value: `0123`})
	})
	t.Run("Integer", func(t *testing.T) {
		AssertParseExpr(t, `123`, &sqlparser.NumberLit{Value: `123`})
	})
	t.Run("Float", func(t *testing.T) {
		AssertParseExpr(t, `123.456`, &sqlparser.NumberLit{Value: `123.456`})
	})
	t.Run("Null", func(t *testing.T) {
		AssertParseExpr(t, `NULL`, &sqlparser.NullLit{})
	})
	t.Run("Bool", func(t *testing.T) {
		AssertParseExpr(t, `true`, &sqlparser.BoolLit{Value: true})
		AssertParseExpr(t, `false`, &sqlparser.BoolLit{Value: false})
	})
	t.Run("Bind", func(t *testing.T) {
		AssertParseExpr(t, `$bar`, &sqlparser.BindExpr{Name: "$bar"})
	})
	t.Run("UnaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `-123`, &sqlparser.UnaryExpr{Op: sqlparser.MINUS, X: &sqlparser.NumberLit{Value: `123`}})
		AssertParseExprError(t, `-`, `1:1: expected expression, found 'EOF'`)
	})
	t.Run("QualifiedRef", func(t *testing.T) {
		AssertParseExpr(t, `tbl.col`, &sqlparser.QualifiedRef{
			Table:  &sqlparser.Ident{Name: "tbl"},
			Column: &sqlparser.Ident{Name: "col"},
		})
		AssertParseExpr(t, `"tbl"."col"`, &sqlparser.QualifiedRef{
			Table:  &sqlparser.Ident{Name: "tbl", Quoted: true, QuoteChar: `"`},
			Column: &sqlparser.Ident{Name: "col", Quoted: true, QuoteChar: `"`},
		})
		AssertParseExpr(t, "`tbl`.`col`", &sqlparser.QualifiedRef{
			Table:  &sqlparser.Ident{Name: "tbl", Quoted: true, QuoteChar: "`"},
			Column: &sqlparser.Ident{Name: "col", Quoted: true, QuoteChar: "`"},
		})
		AssertParseExprError(t, `tbl.`, `1:4: expected column name, found 'EOF'`)
	})
	t.Run("Exists", func(t *testing.T) {
		AssertParseExpr(t, `EXISTS (SELECT *)`, &sqlparser.Exists{
			Select: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Star: true,
				}},
			},
		})
		AssertParseExpr(t, `NOT EXISTS (SELECT *)`, &sqlparser.Exists{
			Not: true,
			Select: &sqlparser.SelectStatement{
				Columns: &sqlparser.OutputNames{&sqlparser.ResultColumn{
					Star: true,
				}},
			},
		})
		AssertParseExprError(t, `NOT`, `1:3: expected EXISTS, found 'EOF'`)
		AssertParseExprError(t, `EXISTS`, `1:6: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (`, `1:8: expected SELECT, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (SELECT`, `1:14: expected expression, found 'EOF'`)
		AssertParseExprError(t, `EXISTS (SELECT *`, `1:16: expected right paren, found 'EOF'`)
	})
	t.Run("BinaryExpr", func(t *testing.T) {
		AssertParseExpr(t, `1 + 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.PLUS,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 - 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.MINUS,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 * 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.STAR,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 / 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.SLASH,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 % 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.REM,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 || 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.CONCAT,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 << 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.LSHIFT,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 >> 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.RSHIFT,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 & 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.BITAND,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 | 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.BITOR,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 < 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.LT,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 <= 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.LE,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 <> 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.LG,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 > 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.GT,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 >= 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.GE,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 = 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.EQ,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 != 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NE,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `(1 + 2)'`, &sqlparser.ParenExpr{
			X: &sqlparser.BinaryExpr{
				X:  &sqlparser.NumberLit{Value: "1"},
				Op: sqlparser.PLUS,
				Y:  &sqlparser.NumberLit{Value: "2"},
			},
		})
		AssertParseExprError(t, `(`, `1:1: expected expression, found 'EOF'`)
		AssertParseExpr(t, `1 IS 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.IS,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 IS NOT 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.ISNOT,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 LIKE 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.LIKE,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT LIKE 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NOTLIKE,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 GLOB 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.GLOB,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT GLOB 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NOTGLOB,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 REGEXP 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.REGEXP,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT REGEXP 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NOTREGEXP,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 MATCH 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.MATCH,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExpr(t, `1 NOT MATCH 2'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NOTMATCH,
			Y:  &sqlparser.NumberLit{Value: "2"},
		})
		AssertParseExprError(t, `1 NOT TABLE`, `1:7: expected IN, LIKE, GLOB, REGEXP, MATCH, or BETWEEN, found 'TABLE'`)
		AssertParseExpr(t, `1 IN (2, 3)'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.IN,
			Y: &sqlparser.Exprs{
				Exprs: []sqlparser.Expr{
					&sqlparser.NumberLit{Value: "2"},
					&sqlparser.NumberLit{Value: "3"},
				},
			},
		})
		AssertParseExpr(t, `1 NOT IN (2, 3)'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NOTIN,
			Y: &sqlparser.Exprs{
				Exprs: []sqlparser.Expr{
					&sqlparser.NumberLit{Value: "2"},
					&sqlparser.NumberLit{Value: "3"},
				},
			},
		})
		AssertParseExprError(t, `1 IN 2`, `1:6: expected left paren, found 2`)
		AssertParseExprError(t, `1 IN (`, `1:6: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 IN (2 3`, `1:9: expected comma or right paren, found 3`)
		AssertParseExpr(t, `1 BETWEEN 2 AND 3'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.BETWEEN,
			Y: &sqlparser.Range{
				X: &sqlparser.NumberLit{Value: "2"},
				Y: &sqlparser.NumberLit{Value: "3"},
			},
		})
		AssertParseExpr(t, `1 NOT BETWEEN 2 AND 3'`, &sqlparser.BinaryExpr{
			X:  &sqlparser.NumberLit{Value: "1"},
			Op: sqlparser.NOTBETWEEN,
			Y: &sqlparser.Range{
				X: &sqlparser.NumberLit{Value: "2"},
				Y: &sqlparser.NumberLit{Value: "3"},
			},
		})
		AssertParseExprError(t, `1 BETWEEN`, `1:9: expected expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2`, `1:11: expected range expression, found 'EOF'`)
		AssertParseExprError(t, `1 BETWEEN 2 + 3`, `1:15: expected range expression, found 'EOF'`)
		AssertParseExprError(t, `1 + `, `1:4: expected expression, found 'EOF'`)
	})
	t.Run("Call", func(t *testing.T) {
		AssertParseExpr(t, `sum()`, &sqlparser.Call{
			Name: &sqlparser.Ident{Name: "sum"},
		})
		AssertParseExpr(t, `sum(*)`, &sqlparser.Call{
			Name: &sqlparser.Ident{Name: "sum"},
			Star: true,
		})
		AssertParseExpr(t, `sum(foo, 123)`, &sqlparser.Call{
			Name: &sqlparser.Ident{Name: "sum"},
			Args: []sqlparser.Expr{
				&sqlparser.Ident{Name: "foo"},
				&sqlparser.NumberLit{Value: "123"},
			},
		})
		AssertParseExpr(t, `sum(distinct 'foo')`, &sqlparser.Call{
			Name:     &sqlparser.Ident{Name: "sum"},
			Distinct: true,
			Args: []sqlparser.Expr{
				&sqlparser.StringLit{Value: "foo"},
			},
		})
		AssertParseExpr(t, `sum() filter (where true)`, &sqlparser.Call{
			Name: &sqlparser.Ident{Name: "sum"},
			Filter: &sqlparser.FilterClause{
				X: &sqlparser.BoolLit{Value: true},
			},
		})

		AssertParseExprError(t, `sum(`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum(*`, `1:5: expected right paren, found 'EOF'`)
		AssertParseExprError(t, `sum(foo foo`, `1:9: expected comma or right paren, found foo`)
		AssertParseExprError(t, `sum() filter`, `1:12: expected left paren, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (`, `1:14: expected WHERE, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (where`, `1:19: expected expression, found 'EOF'`)
		AssertParseExprError(t, `sum() filter (where true`, `1:24: expected right paren, found 'EOF'`)
	})

	t.Run("Case", func(t *testing.T) {
		AssertParseExpr(t, `CASE 1 WHEN 2 THEN 3 WHEN 4 THEN 5 ELSE 6 END`, &sqlparser.CaseExpr{
			Operand: &sqlparser.NumberLit{Value: "1"},
			Blocks: []*sqlparser.CaseBlock{
				{
					Condition: &sqlparser.NumberLit{Value: "2"},
					Body:      &sqlparser.NumberLit{Value: "3"},
				},
				{
					Condition: &sqlparser.NumberLit{Value: "4"},
					Body:      &sqlparser.NumberLit{Value: "5"},
				},
			},
			ElseExpr: &sqlparser.NumberLit{Value: "6"},
		})
		AssertParseExpr(t, `CASE WHEN 1 THEN 2 END`, &sqlparser.CaseExpr{
			Blocks: []*sqlparser.CaseBlock{
				{
					Condition: &sqlparser.NumberLit{Value: "1"},
					Body:      &sqlparser.NumberLit{Value: "2"},
				},
			},
		})
		AssertParseExprError(t, `CASE`, `1:4: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE 1`, `1:6: expected WHEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN`, `1:9: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1`, `1:11: expected THEN, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN`, `1:16: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2`, `1:18: expected WHEN, ELSE or END, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE`, `1:23: expected expression, found 'EOF'`)
		AssertParseExprError(t, `CASE WHEN 1 THEN 2 ELSE 3`, `1:25: expected END, found 'EOF'`)
	})
}

func TestError_Error(t *testing.T) {
	err := &sqlparser.Error{Msg: "test"}
	if got, want := err.Error(), `test`; got != want {
		t.Fatalf("Error()=%s, want %s", got, want)
	}
}

// ParseStatementOrFail parses a statement from s. Fail on error.
func ParseStatementOrFail(tb testing.TB, s string) sqlparser.Statement {
	tb.Helper()
	stmt, err := sqlparser.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		tb.Fatal(err)
	}
	return stmt
}

// AssertParseStatement asserts the value of the first parse of s.
func AssertParseStatement(tb testing.TB, s string, want sqlparser.Statement) {
	tb.Helper()
	stmt, err := sqlparser.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		tb.Fatal(err)
	} else if diff := deep.Equal(stmt, want); diff != nil {
		tb.Fatalf("mismatch:\n%s", strings.Join(diff, "\n"))
	}
}

// AssertParseStatementError asserts s parses to a given error string.
func AssertParseStatementError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := sqlparser.NewParser(strings.NewReader(s)).ParseStatement()
	if err == nil || err.Error() != want {
		tb.Fatalf("ParseStatement()=%q, want %q", err, want)
	}
}

// ParseExprOrFail parses a expression from s. Fail on error.
func ParseExprOrFail(tb testing.TB, s string) sqlparser.Expr {
	tb.Helper()
	stmt, err := sqlparser.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		tb.Fatal(err)
	}
	return stmt
}

// AssertParseExpr asserts the value of the first parse of s.
func AssertParseExpr(tb testing.TB, s string, want sqlparser.Expr) {
	tb.Helper()
	stmt, err := sqlparser.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		tb.Fatal(err)
	} else if diff := deep.Equal(stmt, want); diff != nil {
		tb.Fatalf("mismatch:\n%s", strings.Join(diff, "\n"))
	}
}

// AssertParseExprError asserts s parses to a given error string.
func AssertParseExprError(tb testing.TB, s string, want string) {
	tb.Helper()
	_, err := sqlparser.NewParser(strings.NewReader(s)).ParseExpr()
	if err == nil || err.Error() != want {
		tb.Fatalf("ParseExpr()=%q, want %q", err, want)
	}
}
