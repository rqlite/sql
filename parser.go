package sqlparser

import (
	"errors"
	"io"
	"strings"
)

var (
	// ErrNotImplemented not implemented
	ErrNotImplemented = errors.New("not implemented")
)

// Parser represents a SQL parser.
type Parser struct {
	l *Lexer

	pos   Pos    // current position
	tok   Token  // current token
	lit   string // current literal value
	full  bool   // buffer full
	binds int    // bind count
}

// NewParser returns a new instance of Parser that reads from r.
func NewParser(r io.Reader) *Parser {
	return &Parser{
		l: NewLexer(r),
	}
}

// ParseExprString parses s into an expression. Returns nil if s is blank.
func ParseExprString(s string) (Expr, error) {
	if s == "" {
		return nil, nil
	}
	return NewParser(strings.NewReader(s)).ParseExpr()
}

// MustParseExprString parses s into an expression. Panic on error.
func MustParseExprString(s string) Expr {
	expr, err := ParseExprString(s)
	if err != nil {
		panic(err)
	}
	return expr
}

func (p *Parser) ParseStatement() (stmt Statement, err error) {
	switch tok := p.peek(); tok {
	case EOF:
		return nil, io.EOF
	default:
		if stmt, err = p.parseStatement(); err != nil {
			return stmt, err
		}
	}

	// Read trailing semicolon or end of file.
	if tok := p.peek(); tok != EOF && tok != SEMI {
		return stmt, p.errorExpected(p.pos, p.tok, "semicolon or EOF")
	}
	p.lex()

	return stmt, nil
}

// parseStmt parses all statement types.
func (p *Parser) parseStatement() (Statement, error) {
	switch p.peek() {
	case SELECT:
		return p.parseSelectStatement(false)
	case INSERT:
		return p.parseInsertStatement()
	case UPDATE:
		return p.parseUpdateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "statement")
	}
}

func (p *Parser) parseIdent(desc string) (*Ident, error) {
	pos, tok, lit := p.lex()
	switch tok {
	case IDENT, QIDENT:
		return identByNameAndTok(lit, tok), nil
	default:
		return nil, p.errorExpected(pos, tok, desc)
	}
}

func (p *Parser) parseInsertStatement() (_ *InsertStatement, err error) {
	assert(p.peek() == INSERT)
	p.lex()

	var stmt InsertStatement

	if p.peek() != INTO {
		return &stmt, p.errorExpected(p.pos, p.tok, "INTO")
	}
	p.lex()

	// Parse table name & optional alias.
	if stmt.TableName, err = p.parseTableName(); err != nil {
		return &stmt, err
	}

	// Parse optional column list.
	if p.peek() == LP {
		p.lex()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &stmt, err
			}
			stmt.ColumnNames = append(stmt.ColumnNames, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.lex()
		}
		p.lex()
	}

	if p.peek() == OVERRIDING {
		p.lex()
		if p.peek() == SYSTEM {
			stmt.Overriding = "system"
		} else if p.peek() == USER {
			stmt.Overriding = "user"
		} else {
			return &stmt, p.errorExpected(p.pos, p.tok, "SYSTEM or USER")
		}
		if p.peek() != VALUE {
			return &stmt, p.errorExpected(p.pos, p.tok, "VALUE")
		}
		p.lex()
	}

	switch p.peek() {
	case DEFAULT:
		p.lex()
		if p.peek() != VALUES {
			return &stmt, p.errorExpected(p.pos, p.tok, "VALUES")
		}
		stmt.DefaultValues = true
		p.lex()
	case VALUES:
		p.lex()
		for {
			var exprs Exprs
			if p.peek() != LP {
				return &stmt, p.errorExpected(p.pos, p.tok, "left paren")
			}
			p.lex()

			for {
				expr, err := p.ParseExpr()
				if err != nil {
					return &stmt, err
				}
				exprs.Exprs = append(exprs.Exprs, expr)

				if p.peek() == RP {
					break
				} else if p.peek() != COMMA {
					return &stmt, p.errorExpected(p.pos, p.tok, "comma or right paren")
				}
				p.lex()
			}
			p.lex()
			stmt.Expressions = append(stmt.Expressions, &exprs)

			if p.peek() != COMMA {
				break
			}
			p.lex()
		}
	case SELECT:
		if stmt.Query, err = p.parseSelectStatement(false); err != nil {
			return &stmt, err
		}
	default:
		return &stmt, p.errorExpected(p.pos, p.tok, "DEFAULT VALUES, VALUES or SELECT")
	}

	// Parse optional upsert clause.
	if p.peek() == ON {
		if stmt.UpsertClause, err = p.parseUpsertClause(); err != nil {
			return &stmt, err
		}
	}

	if p.peek() == RETURNING {
		p.lex()
		if stmt.OutputExpressions, err = p.parseOutputNames(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseUpsertClause() (_ *UpsertClause, err error) {
	assert(p.peek() == ON)

	var clause UpsertClause

	// Parse "ON CONFLICT"
	p.lex()
	if p.peek() != CONFLICT {
		return &clause, p.errorExpected(p.pos, p.tok, "CONFLICT")
	}
	p.lex()

	// Parse optional indexed column list & WHERE conditional.
	if p.peek() == LP {
		p.lex()
		for {
			col, err := p.parseIndexedColumn()
			if err != nil {
				return &clause, err
			}
			clause.Columns = append(clause.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &clause, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.lex()
		}
		p.lex()

		if p.peek() == WHERE {
			p.lex()
			if clause.WhereExpr, err = p.ParseExpr(); err != nil {
				return &clause, err
			}
		}
	}

	// Parse "DO NOTHING" or "DO UPDATE SET".
	if p.peek() != DO {
		return &clause, p.errorExpected(p.pos, p.tok, "DO")
	}
	p.lex()

	// If next token is NOTHING, then read it and exit immediately.
	if p.peek() == NOTHING {
		p.lex()
		clause.DoNothing = true
		return &clause, nil
	} else if p.peek() != UPDATE {
		return &clause, p.errorExpected(p.pos, p.tok, "NOTHING or UPDATE SET")
	}

	// Otherwise parse "UPDATE SET"
	p.lex()
	clause.DoUpdate = true
	if p.peek() != SET {
		return &clause, p.errorExpected(p.pos, p.tok, "SET")
	}
	p.lex()

	// Parse list of assignments.
	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &clause, err
		}
		clause.Assignments = append(clause.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.lex()
	}

	// Parse WHERE after DO UPDATE SET.
	if p.peek() == WHERE {
		p.lex()
		if clause.UpdateWhereExpr, err = p.ParseExpr(); err != nil {
			return &clause, err
		}
	}

	return &clause, nil
}

func (p *Parser) parseIndexedColumn() (_ *IndexedColumn, err error) {
	var col IndexedColumn
	if col.X, err = p.ParseExpr(); err != nil {
		return &col, err
	}
	if p.peek() == ASC {
		p.lex()
		col.Asc = true
	} else if p.peek() == DESC {
		p.lex()
		col.Desc = true
	}
	return &col, nil
}

func (p *Parser) parseUpdateStatement() (_ *UpdateStatement, err error) {
	assert(p.peek() == UPDATE)
	p.lex()

	var stmt UpdateStatement

	if stmt.TableName, err = p.parseTableName(); err != nil {
		return &stmt, err
	}

	// Parse SET + list of assignments.
	if p.peek() != SET {
		return &stmt, p.errorExpected(p.pos, p.tok, "SET")
	}
	p.lex()

	for {
		assignment, err := p.parseAssignment()
		if err != nil {
			return &stmt, err
		}
		stmt.Assignments = append(stmt.Assignments, assignment)

		if p.peek() != COMMA {
			break
		}
		p.lex()
	}

	if p.peek() == WHERE {
		p.lex()
		if p.peek() == CURRENT {
			p.lex()
			if p.peek() != OF {
				return &stmt, p.errorExpected(p.pos, p.tok, "OF")
			}
			p.lex()

			if stmt.CursorName, err = p.parseIdent("cursor name"); err != nil {
				return &stmt, err
			}
		} else {
			if stmt.Condition, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	if p.peek() == RETURNING {
		p.lex()
		if stmt.OutputExpressions, err = p.parseOutputNames(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseDeleteStatement() (_ *DeleteStatement, err error) {
	assert(p.peek() == DELETE)
	p.lex()
	assert(p.peek() == FROM)
	p.lex()

	var stmt DeleteStatement

	if p.peek() == ONLY {
		stmt.Only = true
		p.lex()
	}

	if stmt.TableName, err = p.parseTableName(); err != nil {
		return &stmt, err
	}

	if p.peek() == STAR {
		stmt.TableStar = true
		p.lex()
	}

	if p.peek() == AS {
		p.lex()
	}
	if isIdentToken(p.peek()) {
		if stmt.Alias, err = p.parseIdent("alias"); err != nil {
			return &stmt, err
		}
	}

	if p.peek() == USING {
		p.lex()
		return &stmt, ErrNotImplemented
	}

	if p.peek() == WHERE {
		p.lex()
		if p.peek() == CURRENT {
			p.lex()
			if p.peek() != OF {
				return &stmt, p.errorExpected(p.pos, p.tok, "OF")
			}
			p.lex()

			if stmt.CursorName, err = p.parseIdent("cursor name"); err != nil {
				return &stmt, err
			}
		} else {
			if stmt.Condition, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	if p.peek() == RETURNING {
		p.lex()
		if stmt.OutputExpressions, err = p.parseOutputNames(); err != nil {
			return &stmt, err
		}
	}

	return &stmt, nil
}

func (p *Parser) parseAssignment() (_ *Assignment, err error) {
	var assignment Assignment

	// Parse either a single column (IDENT) or a column list (LP IDENT COMMA IDENT RP)
	if isIdentToken(p.peek()) {
		col, _ := p.parseIdent("column name")
		assignment.Columns = []*Ident{col}
	} else if p.peek() == LP {
		p.lex()
		for {
			col, err := p.parseIdent("column name")
			if err != nil {
				return &assignment, err
			}
			assignment.Columns = append(assignment.Columns, col)

			if p.peek() == RP {
				break
			} else if p.peek() != COMMA {
				return &assignment, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}
			p.lex()
		}
		p.lex()
	} else {
		return &assignment, p.errorExpected(p.pos, p.tok, "column name or column list")
	}

	if p.peek() != EQ {
		return &assignment, p.errorExpected(p.pos, p.tok, "=")
	}
	p.lex()

	if assignment.Expr, err = p.ParseExpr(); err != nil {
		return &assignment, err
	}

	return &assignment, nil
}

// parseSelectStatement parses a SELECT statement.
// If compounded is true, WITH, ORDER BY, & LIMIT/OFFSET are skipped.
func (p *Parser) parseSelectStatement(compounded bool) (_ *SelectStatement, err error) {
	var stmt SelectStatement

	if p.peek() != SELECT {
		return &stmt, p.errorExpected(p.pos, p.tok, "SELECT")
	}
	p.lex()

	// Parse optional hint
	if tok := p.peek(); tok == MLCOMMENT {
		_, _, lit := p.lex()
		stmt.Hint = &Hint{Value: lit}
	}

	// Parse optional "DISTINCT" or "ALL".
	if tok := p.peek(); tok == ALL {
		stmt.All = true
		p.lex()
	} else if tok == DISTINCT {
		stmt.Distinct = true
		p.lex()
	}

	// Parse result columns.
	if stmt.Columns, err = p.parseOutputNames(); err != nil {
		return &stmt, err
	}

	// Parse FROM clause.
	if p.peek() == FROM {
		p.lex()
		if stmt.FromItems, err = p.parseSource(); err != nil {
			return &stmt, err
		}
	}

	// Parse WHERE clause.
	if p.peek() == WHERE {
		p.lex()
		if stmt.Condition, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}
	}

	// Parse GROUP BY/HAVING clause.
	if p.peek() == GROUP {
		p.lex()
		if p.peek() != BY {
			return &stmt, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.lex()

		for {
			expr, err := p.ParseExpr()
			if err != nil {
				return &stmt, err
			}
			stmt.GroupingElements = append(stmt.GroupingElements, expr)

			if p.peek() != COMMA {
				break
			}
			p.lex()
		}

		// Parse optional HAVING clause.
		if p.peek() == HAVING {
			p.lex()
			if stmt.HavingCondition, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	// Optionally compound additional SELECT/VALUES.
	switch tok := p.peek(); tok {
	case UNION, INTERSECT, EXCEPT:
		if tok == UNION {
			p.lex()
			stmt.Union = true
			if p.peek() == ALL {
				p.lex()
				stmt.UnionAll = true
			}
		} else if tok == INTERSECT {
			p.lex()
			stmt.Intersect = true
		} else {
			p.lex()
			stmt.Except = true
		}

		if stmt.Compound, err = p.parseSelectStatement(true); err != nil {
			return &stmt, err
		}
	}

	// Parse ORDER BY clause.
	if !compounded && p.peek() == ORDER {
		p.lex()
		if p.peek() != BY {
			return &stmt, p.errorExpected(p.pos, p.tok, "BY")
		}
		p.lex()

		for {
			term, err := p.parseOrderingTerm()
			if err != nil {
				return &stmt, err
			}
			stmt.OrderBy = append(stmt.OrderBy, term)

			if p.peek() != COMMA {
				break
			}
			p.lex()
		}
	}

	// Parse LIMIT/OFFSET clause.
	// The offset is optional. Can be specified with COMMA or OFFSET.
	// e.g. "LIMIT 1 OFFSET 2" or "LIMIT 1, 2"
	if !compounded && p.peek() == LIMIT {
		p.lex()
		if stmt.Limit, err = p.ParseExpr(); err != nil {
			return &stmt, err
		}

		if tok := p.peek(); tok == OFFSET || tok == COMMA {
			p.lex()
			if stmt.Offset, err = p.ParseExpr(); err != nil {
				return &stmt, err
			}
		}
	}

	return &stmt, nil
}

func (p *Parser) parseOutputNames() (_ *OutputNames, err error) {
	var names OutputNames
	for {
		name, err := p.parseResultColumn()
		if err != nil {
			return &names, err
		}
		names = append(names, name)

		if p.peek() != COMMA {
			break
		}
		p.lex()
	}
	return &names, nil
}

func (p *Parser) parseResultColumn() (_ *ResultColumn, err error) {
	var col ResultColumn

	// An initial "*" returns all columns.
	if p.peek() == STAR {
		p.lex()
		col.Star = true
		return &col, nil
	}

	// Next can be either "EXPR [[AS] column-alias]" or "IDENT DOT STAR".
	// We need read the next element as an expression and then determine what next.
	if col.Expr, err = p.ParseExpr(); err != nil {
		return &col, err
	}

	// If we have a qualified ref w/ a star, don't allow an alias.
	if ref, ok := col.Expr.(*QualifiedRef); ok && ref.Star {
		return &col, nil
	}

	// If "AS" is next, the alias must follow.
	// Otherwise it can optionally be an IDENT alias.
	if p.peek() == AS {
		p.lex()
		if !isIdentToken(p.peek()) {
			return &col, p.errorExpected(p.pos, p.tok, "column alias")
		}
		col.Alias, _ = p.parseIdent("column alias")
	} else if isIdentToken(p.peek()) {
		col.Alias, _ = p.parseIdent("column alias")
	}

	return &col, nil
}

func (p *Parser) parseSource() (source Source, err error) {
	source, err = p.parseUnarySource()
	if err != nil {
		return source, err
	}

	for {
		// Exit immediately if not part of a join operator.
		switch p.peek() {
		case COMMA, NATURAL, LEFT, INNER, CROSS, JOIN:
		default:
			return source, nil
		}

		// Parse join operator.
		operator, err := p.parseJoinOperator()
		if err != nil {
			return source, err
		}
		y, err := p.parseUnarySource()
		if err != nil {
			return source, err
		}
		constraint, err := p.parseJoinConstraint()
		if err != nil {
			return source, err
		}

		// Rewrite last source to nest next join on right side.
		if lhs, ok := source.(*JoinClause); ok {
			source = &JoinClause{
				X:        lhs.X,
				Operator: lhs.Operator,
				Y: &JoinClause{
					X:          lhs.Y,
					Operator:   operator,
					Y:          y,
					Constraint: constraint,
				},
				Constraint: lhs.Constraint,
			}
		} else {
			source = &JoinClause{X: source, Operator: operator, Y: y, Constraint: constraint}
		}
	}
}

// parseUnarySource parses a quailfied table name or subquery but not a JOIN.
func (p *Parser) parseUnarySource() (source Source, err error) {
	switch p.peek() {
	case LP:
		return p.parseParenSource()
	case IDENT, QIDENT:
		return p.parseTableName()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "table name or left paren")
	}
}

func (p *Parser) parseJoinOperator() (*JoinOperator, error) {
	var op JoinOperator

	// Handle single comma join.
	if p.peek() == COMMA {
		p.lex()
		op.Comma = true
		return &op, nil
	}

	if p.peek() == NATURAL {
		p.lex()
		op.Natural = true
	}

	// Parse "LEFT", "LEFT OUTER", "INNER", or "CROSS"
	switch p.peek() {
	case LEFT:
		p.lex()
		op.Left = true
		if p.peek() == OUTER {
			p.lex()
			op.Outer = true
		}
	case INNER:
		p.lex()
		op.Inner = true
	case CROSS:
		p.lex()
		op.Cross = true
	}

	// Parse final JOIN.
	if p.peek() != JOIN {
		return &op, p.errorExpected(p.pos, p.tok, "JOIN")
	}
	p.lex()

	return &op, nil
}

func (p *Parser) parseJoinConstraint() (JoinConstraint, error) {
	switch p.peek() {
	case ON:
		return p.parseOnConstraint()
	case USING:
		return p.parseUsingConstraint()
	default:
		return nil, nil
	}
}

func (p *Parser) parseOnConstraint() (_ *OnConstraint, err error) {
	assert(p.peek() == ON)

	var con OnConstraint
	p.lex()
	if con.X, err = p.ParseExpr(); err != nil {
		return &con, err
	}
	return &con, nil
}

func (p *Parser) parseUsingConstraint() (*UsingConstraint, error) {
	assert(p.peek() == USING)

	var con UsingConstraint
	p.lex()

	if p.peek() != LP {
		return &con, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.lex()

	for {
		col, err := p.parseIdent("column name")
		if err != nil {
			return &con, err
		}
		con.Columns = append(con.Columns, col)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &con, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.lex()
	}
	p.lex()

	return &con, nil
}

func (p *Parser) parseParenSource() (_ *ParenSource, err error) {
	assert(p.peek() == LP)

	var source ParenSource
	p.lex()

	if p.peek() == SELECT {
		if source.X, err = p.parseSelectStatement(false); err != nil {
			return &source, err
		}
	} else {
		if source.X, err = p.parseSource(); err != nil {
			return &source, err
		}
	}

	if p.peek() != RP {
		return nil, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.lex()

	// Only parse aliases for nested select statements.
	if _, ok := source.X.(*SelectStatement); ok && (p.peek() == AS || isIdentToken(p.peek())) {
		if p.peek() == AS {
			p.lex()
		}
		if source.Alias, err = p.parseIdent("table alias"); err != nil {
			return &source, err
		}
	}

	return &source, nil
}

func (p *Parser) parseTableName() (_ *TableName, err error) {
	var tbl TableName

	if !isIdentToken(p.peek()) {
		return &tbl, p.errorExpected(p.pos, p.tok, "table name")
	}
	tbl.Name, _ = p.parseIdent("table name")

	// Parse optional table alias ("AS alias" or just "alias").
	if tok := p.peek(); tok == AS || isIdentToken(tok) {
		if p.peek() == AS {
			p.lex()
		}
		if tbl.Alias, err = p.parseIdent("table alias"); err != nil {
			return &tbl, err
		}
	}

	return &tbl, nil
}

func (p *Parser) ParseExpr() (expr Expr, err error) {
	return p.parseBinaryExpr(LowestPrec + 1)
}

func (p *Parser) parseOperand() (expr Expr, err error) {
	_, tok, lit := p.lex()
	switch tok {
	case IDENT, QIDENT:
		ident := identByNameAndTok(lit, tok)
		if p.peek() == DOT {
			return p.parseQualifiedRef(ident)
		} else if p.peek() == LP {
			return p.parseCall(ident)
		}
		return ident, nil
	case STRING:
		return &StringLit{Value: lit}, nil
	case BLOB:
		return &BlobLit{Value: lit}, nil
	case FLOAT, INTEGER:
		return &NumberLit{Value: lit}, nil
	case NULL:
		return &NullLit{}, nil
	case TRUE, FALSE:
		return &BoolLit{Value: tok == TRUE}, nil
	case BIND:
		p.binds += 1
		return &BindExpr{Name: lit, Pos: p.binds - 1}, nil
	case PLUS, MINUS:
		expr, err = p.parseOperand()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: tok, X: expr}, nil
	case LP:
		p.unlex()
		return p.parseParenExpr()
	case CASE:
		p.unlex()
		return p.parseCaseExpr()
	case NOT, EXISTS:
		p.unlex()
		return p.parseExists()
	default:
		return nil, p.errorExpected(p.pos, p.tok, "expression")
	}
}

func (p *Parser) parseBinaryExpr(prec1 int) (expr Expr, err error) {
	x, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	for {
		if p.peek().Precedence() < prec1 {
			return x, nil
		}

		_, op, err := p.lexBinaryOp()
		if err != nil {
			return nil, err
		}

		switch op {
		case IN, NOTIN:
			y, err := p.parseExprs()
			if err != nil {
				return x, err
			}
			x = &BinaryExpr{X: x, Op: op, Y: y}

		case BETWEEN, NOTBETWEEN:
			// Parsing the expression should yield a binary expression with AND op.
			// However, we don't want to conflate the boolean AND and the ranged AND
			// so we convert the expression to a Range.
			if rng, err := p.parseBinaryExpr(LowestPrec + 1); err != nil {
				return x, err
			} else if rng, ok := rng.(*BinaryExpr); !ok || rng.Op != AND {
				return x, p.errorExpected(p.pos, p.tok, "range expression")
			} else {
				x = &BinaryExpr{
					X:  x,
					Op: op,
					Y:  &Range{X: rng.X, Y: rng.Y},
				}
			}

		default:
			y, err := p.parseBinaryExpr(op.Precedence() + 1)
			if err != nil {
				return nil, err
			}
			x = &BinaryExpr{X: x, Op: op, Y: y}
		}
	}

}

func (p *Parser) parseExprs() (_ *Exprs, err error) {
	var list Exprs
	if p.peek() != LP {
		return &list, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.lex()

	for p.peek() != RP {
		x, err := p.ParseExpr()
		if err != nil {
			return &list, err
		}
		list.Exprs = append(list.Exprs, x)

		if p.peek() == RP {
			break
		} else if p.peek() != COMMA {
			return &list, p.errorExpected(p.pos, p.tok, "comma or right paren")
		}
		p.lex()
	}

	p.lex()

	return &list, nil
}

func (p *Parser) parseQualifiedRef(table *Ident) (_ *QualifiedRef, err error) {
	assert(p.peek() == DOT)

	var expr QualifiedRef
	expr.Table = table
	p.lex()

	if p.peek() == STAR {
		p.lex()
		expr.Star = true
	} else if isIdentToken(p.peek()) {
		_, tok, lit := p.lex()
		expr.Column = identByNameAndTok(lit, tok)
	} else {
		return &expr, p.errorExpected(p.pos, p.tok, "column name")
	}

	return &expr, nil
}

func (p *Parser) parseCall(name *Ident) (_ *Call, err error) {
	assert(p.peek() == LP)

	var expr Call
	expr.Name = name
	p.lex()

	// Parse argument list: either "*" or "[DISTINCT] expr, expr..."
	if p.peek() == STAR {
		p.lex()
		expr.Star = true
	} else {
		if p.peek() == DISTINCT {
			p.lex()
			expr.Distinct = true
		}
		for p.peek() != RP {
			arg, err := p.ParseExpr()
			if err != nil {
				return &expr, err
			}
			expr.Args = append(expr.Args, arg)

			if tok := p.peek(); tok == COMMA {
				p.lex()
			} else if tok != RP {
				return &expr, p.errorExpected(p.pos, p.tok, "comma or right paren")
			}

		}
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.lex()

	// Parse optional filter clause.
	if p.peek() == FILTER {
		if expr.Filter, err = p.parseFilterClause(); err != nil {
			return &expr, err
		}
	}

	return &expr, nil
}

func (p *Parser) parseFilterClause() (_ *FilterClause, err error) {
	assert(p.peek() == FILTER)

	var clause FilterClause
	p.lex()

	if p.peek() != LP {
		return &clause, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.lex()

	if p.peek() != WHERE {
		return &clause, p.errorExpected(p.pos, p.tok, "WHERE")
	}
	p.lex()

	if clause.X, err = p.ParseExpr(); err != nil {
		return &clause, err
	}

	if p.peek() != RP {
		return &clause, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.lex()

	return &clause, nil
}

func (p *Parser) parseOrderingTerm() (_ *OrderingTerm, err error) {
	var term OrderingTerm
	if term.X, err = p.ParseExpr(); err != nil {
		return &term, err
	}

	// Parse optional sort direction ("ASC" or "DESC")
	switch p.peek() {
	case ASC:
		p.lex()
		term.Asc = true
	case DESC:
		p.lex()
		term.Desc = true
	}

	// Parse optional "NULLS FIRST" or "NULLS LAST"
	if p.peek() == NULLS {
		p.lex()
		switch p.peek() {
		case FIRST:
			p.lex()
			term.NullsFirst = true
		case LAST:
			p.lex()
			term.NullsLast = true
		default:
			return &term, p.errorExpected(p.pos, p.tok, "FIRST or LAST")
		}
	}

	return &term, nil
}

func (p *Parser) parseParenExpr() (_ *ParenExpr, err error) {
	var expr ParenExpr
	p.lex()
	if expr.X, err = p.ParseExpr(); err != nil {
		return &expr, err
	}
	p.lex()
	return &expr, nil
}

func (p *Parser) parseCaseExpr() (_ *CaseExpr, err error) {
	assert(p.peek() == CASE)

	var expr CaseExpr
	p.lex()

	// Parse optional expression if WHEN is not next.
	if p.peek() != WHEN {
		if expr.Operand, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	// Parse one or more WHEN/THEN pairs.
	for {
		var blk CaseBlock
		if p.peek() != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN")
		}
		p.lex()

		if blk.Condition, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		if p.peek() != THEN {
			return &expr, p.errorExpected(p.pos, p.tok, "THEN")
		}
		p.lex()

		if blk.Body, err = p.ParseExpr(); err != nil {
			return &expr, err
		}

		expr.Blocks = append(expr.Blocks, &blk)

		if tok := p.peek(); tok == ELSE || tok == END {
			break
		} else if tok != WHEN {
			return &expr, p.errorExpected(p.pos, p.tok, "WHEN, ELSE or END")
		}
	}

	// Parse optional ELSE block.
	if p.peek() == ELSE {
		p.lex()
		if expr.ElseExpr, err = p.ParseExpr(); err != nil {
			return &expr, err
		}
	}

	if p.peek() != END {
		return &expr, p.errorExpected(p.pos, p.tok, "END")
	}
	p.lex()

	return &expr, nil
}

func (p *Parser) parseExists() (_ *Exists, err error) {
	assert(p.peek() == NOT || p.peek() == EXISTS)

	var expr Exists

	if p.peek() == NOT {
		p.lex()
		expr.Not = true
	}

	if p.peek() != EXISTS {
		return &expr, p.errorExpected(p.pos, p.tok, "EXISTS")
	}
	p.lex()

	if p.peek() != LP {
		return &expr, p.errorExpected(p.pos, p.tok, "left paren")
	}
	p.lex()

	if expr.Select, err = p.parseSelectStatement(false); err != nil {
		return &expr, err
	}

	if p.peek() != RP {
		return &expr, p.errorExpected(p.pos, p.tok, "right paren")
	}
	p.lex()

	return &expr, nil
}

func (p *Parser) lex() (Pos, Token, string) {
	if p.full {
		p.full = false
		return p.pos, p.tok, p.lit
	}

	p.pos, p.tok, p.lit = p.l.Lex()
	return p.pos, p.tok, p.lit
}

// lexBinaryOp performs a lex but combines multi-word operations into a single token.
func (p *Parser) lexBinaryOp() (Pos, Token, error) {
	pos, tok, _ := p.lex()
	switch tok {
	case IS:
		if p.peek() == NOT {
			p.lex()
			return pos, ISNOT, nil
		}
		return pos, IS, nil
	case NOT:
		switch p.peek() {
		case IN:
			p.lex()
			return pos, NOTIN, nil
		case LIKE:
			p.lex()
			return pos, NOTLIKE, nil
		case GLOB:
			p.lex()
			return pos, NOTGLOB, nil
		case REGEXP:
			p.lex()
			return pos, NOTREGEXP, nil
		case MATCH:
			p.lex()
			return pos, NOTMATCH, nil
		case BETWEEN:
			p.lex()
			return pos, NOTBETWEEN, nil
		default:
			return pos, tok, p.errorExpected(p.pos, p.tok, "IN, LIKE, GLOB, REGEXP, MATCH, or BETWEEN")
		}
	default:
		return pos, tok, nil
	}
}

func (p *Parser) peek() Token {
	if !p.full {
		p.lex()
		p.unlex()
	}
	return p.tok
}

func (p *Parser) unlex() {
	assert(!p.full)
	p.full = true
}

func (p *Parser) errorExpected(pos Pos, tok Token, msg string) error {
	msg = "expected " + msg
	if pos == p.pos {
		if p.tok.IsLiteral() {
			msg += ", found " + p.lit
		} else {
			msg += ", found '" + p.tok.String() + "'"
		}
	}
	return &Error{Pos: pos, Msg: msg}
}

func identByNameAndTok(lit string, tok Token) *Ident {
	quoted := tok == QIDENT
	if quoted {
		return &Ident{Name: lit[1 : len(lit)-1], Quoted: quoted, QuoteChar: lit[0:1]}
	}
	return &Ident{Name: lit, Quoted: false}
}

// Error represents a parse error.
type Error struct {
	Pos Pos
	Msg string
}

// Error implements the error interface.
func (e Error) Error() string {
	if e.Pos.IsValid() {
		return e.Pos.String() + ": " + e.Msg
	}
	return e.Msg
}
