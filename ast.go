package sqlparser

import (
	"bytes"
	"fmt"
	"strings"
)

type Node interface {
	node()
	fmt.Stringer
}

func (*Assignment) node()      {}
func (*BinaryExpr) node()      {}
func (*BindExpr) node()        {}
func (*BlobLit) node()         {}
func (*BoolLit) node()         {}
func (*Call) node()            {}
func (*CaseBlock) node()       {}
func (*CaseExpr) node()        {}
func (*DeleteStatement) node() {}
func (*Exists) node()          {}
func (*Exprs) node()           {}
func (*FilterClause) node()    {}
func (*Hint) node()            {}
func (*Ident) node()           {}
func (*IndexedColumn) node()   {}
func (*InsertStatement) node() {}
func (*JoinClause) node()      {}
func (*JoinOperator) node()    {}
func (*NullLit) node()         {}
func (*NumberLit) node()       {}
func (*OnConstraint) node()    {}
func (*OrderingTerm) node()    {}
func (*ParenExpr) node()       {}
func (*ParenSource) node()     {}
func (*QualifiedRef) node()    {}
func (*Range) node()           {}
func (*OutputNames) node()     {}
func (*ResultColumn) node()    {}
func (*SelectStatement) node() {}
func (*StringLit) node()       {}
func (*TableName) node()       {}
func (*Type) node()            {}
func (*UnaryExpr) node()       {}
func (*UpdateStatement) node() {}
func (*UpsertClause) node()    {}
func (*UsingConstraint) node() {}

type Statement interface {
	Node
	stmt()
}

func (*DeleteStatement) stmt() {}
func (*InsertStatement) stmt() {}
func (*SelectStatement) stmt() {}
func (*UpdateStatement) stmt() {}

// StatementSource returns the root statement for a statement.
func StatementSource(stmt Statement) Source {
	switch stmt := stmt.(type) {
	case *SelectStatement:
		return stmt.FromItems
	case *UpdateStatement:
		return stmt.TableName
	case *DeleteStatement:
		return stmt.TableName
	default:
		return nil
	}
}

type Expr interface {
	Node
	expr()
}

func (*BinaryExpr) expr()   {}
func (*BindExpr) expr()     {}
func (*BlobLit) expr()      {}
func (*BoolLit) expr()      {}
func (*Call) expr()         {}
func (*CaseExpr) expr()     {}
func (*Exists) expr()       {}
func (*Exprs) expr()        {}
func (*Ident) expr()        {}
func (*NullLit) expr()      {}
func (*NumberLit) expr()    {}
func (*ParenExpr) expr()    {}
func (*QualifiedRef) expr() {}
func (*Range) expr()        {}
func (*StringLit) expr()    {}
func (*UnaryExpr) expr()    {}

// ExprString returns the string representation of expr.
// Returns a blank string if expr is nil.
func ExprString(expr Expr) string {
	if expr == nil {
		return ""
	}
	return expr.String()
}

// SplitExprTree splits apart expr so it is a list of all AND joined expressions.
// For example, the expression "A AND B AND (C OR (D AND E))" would be split into
// a list of "A", "B", "C OR (D AND E)".
func SplitExprTree(expr Expr) []Expr {
	if expr == nil {
		return nil
	}

	var a []Expr
	splitExprTree(expr, &a)
	return a
}

func splitExprTree(expr Expr, a *[]Expr) {
	switch expr := expr.(type) {
	case *BinaryExpr:
		if expr.Op != AND {
			*a = append(*a, expr)
			return
		}
		splitExprTree(expr.X, a)
		splitExprTree(expr.Y, a)
	case *ParenExpr:
		splitExprTree(expr.X, a)
	default:
		*a = append(*a, expr)
	}
}

// Scope represents a context for name resolution.
// Names can be resolved at the current source or in parent scopes.
type Scope struct {
	Parent *Scope
	Source Source
}

// Source represents a table or subquery.
type Source interface {
	Node
	source()
}

func (*JoinClause) source()      {}
func (*ParenSource) source()     {}
func (*TableName) source()       {}
func (*SelectStatement) source() {}

// SourceName returns the name of the source.
// Only returns for TableName & ParenSource.
func SourceName(src Source) string {
	switch src := src.(type) {
	case *JoinClause, *SelectStatement:
		return ""
	case *ParenSource:
		return IdentName(src.Alias)
	case *TableName:
		return src.TableName()
	default:
		return ""
	}
}

// SourceList returns a list of scopes in the current scope.
func SourceList(src Source) []Source {
	var a []Source
	ForEachSource(src, func(s Source) bool {
		a = append(a, s)
		return true
	})
	return a
}

// ForEachSource calls fn for every source within the current scope.
// Stops iteration if fn returns false.
func ForEachSource(src Source, fn func(Source) bool) {
	forEachSource(src, fn)
}

func forEachSource(src Source, fn func(Source) bool) bool {
	if !fn(src) {
		return false
	}

	switch src := src.(type) {
	case *JoinClause:
		if !forEachSource(src.X, fn) {
			return false
		} else if !forEachSource(src.Y, fn) {
			return false
		}
	case *SelectStatement:
		if !forEachSource(src.FromItems, fn) {
			return false
		}
	}
	return true
}

// ResolveSource returns a source with the given name.
// This can either be the table name or the alias for a source.
func ResolveSource(root Source, name string) Source {
	var ret Source
	ForEachSource(root, func(src Source) bool {
		switch src := src.(type) {
		case *ParenSource:
			if IdentName(src.Alias) == name {
				ret = src
			}
		case *TableName:
			if src.TableName() == name {
				ret = src
			}
		}
		return ret == nil // continue until we find the matching source
	})
	return ret
}

// JoinConstraint represents either an ON or USING join constraint.
type JoinConstraint interface {
	Node
	joinConstraint()
}

func (*OnConstraint) joinConstraint()    {}
func (*UsingConstraint) joinConstraint() {}

type Constraint interface {
	Node
	constraint()
}

type Ident struct {
	Name      string // identifier name
	Quoted    bool   // true if double quoted
	QuoteChar string // " for postgresql, ` for mysql, etc
}

// String returns the string representation of the expression.
func (i *Ident) String() string {
	if i.Quoted {
		if i.QuoteChar == `"` {
			return i.QuoteChar + strings.ReplaceAll(i.Name, `"`, `""`) + i.QuoteChar
		}
		return i.QuoteChar + i.Name + i.QuoteChar
	}
	return i.Name
}

// IdentName returns the name of ident. Returns a blank string if ident is nil.
func IdentName(ident *Ident) string {
	if ident == nil {
		return ""
	}
	return ident.Name
}

type Type struct {
	Name      *Ident     // type name
	Lparen    Pos        // position of left paren (optional)
	Precision *NumberLit // precision (optional)
	Scale     *NumberLit // scale (optional)
	Rparen    Pos        // position of right paren (optional)
}

// String returns the string representation of the type.
func (t *Type) String() string {
	if t.Precision != nil && t.Scale != nil {
		return fmt.Sprintf("%s(%s,%s)", t.Name.Name, t.Precision.String(), t.Scale.String())
	} else if t.Precision != nil {
		return fmt.Sprintf("%s(%s)", t.Name.Name, t.Precision.String())
	}
	return t.Name.Name
}

type StringLit struct {
	Value string // literal value (without quotes)
}

// String returns the string representation of the expression.
func (lit *StringLit) String() string {
	return `'` + strings.Replace(lit.Value, `'`, `''`, -1) + `'`
}

type Hint struct {
	Value string
}

// String returns the string representation of the expression.
func (h *Hint) String() string {
	return `/* ` + h.Value + ` */`
}

type BlobLit struct {
	Value string // literal value
}

// String returns the string representation of the expression.
func (lit *BlobLit) String() string {
	return `x'` + lit.Value + `'`
}

type NumberLit struct {
	Value string // literal value
}

// String returns the string representation of the expression.
func (lit *NumberLit) String() string {
	return lit.Value
}

type NullLit struct {
}

// String returns the string representation of the expression.
func (lit *NullLit) String() string {
	return "NULL"
}

type BoolLit struct {
	Value bool // literal value
}

// String returns the string representation of the expression.
func (lit *BoolLit) String() string {
	if lit.Value {
		return "TRUE"
	}
	return "FALSE"
}

type BindExpr struct {
	Name string // binding name
	Pos  int    // binding position
}

// String returns the string representation of the expression.
func (expr *BindExpr) String() string {
	// TODO(BBJ): Support all bind characters.
	return expr.Name
}

type UnaryExpr struct {
	Op Token // operation
	X  Expr  // target expression
}

// String returns the string representation of the expression.
func (expr *UnaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return "+" + expr.X.String()
	case MINUS:
		return "-" + expr.X.String()
	default:
		panic(fmt.Sprintf("sqlparser.UnaryExpr.String(): invalid op %s", expr.Op))
	}
}

type BinaryExpr struct {
	X  Expr  // lhs
	Op Token // operator
	Y  Expr  // rhs
}

// String returns the string representation of the expression.
func (expr *BinaryExpr) String() string {
	switch expr.Op {
	case PLUS:
		return expr.X.String() + " + " + expr.Y.String()
	case MINUS:
		return expr.X.String() + " - " + expr.Y.String()
	case STAR:
		return expr.X.String() + " * " + expr.Y.String()
	case SLASH:
		return expr.X.String() + " / " + expr.Y.String()
	case REM:
		return expr.X.String() + " % " + expr.Y.String()
	case CONCAT:
		return expr.X.String() + " || " + expr.Y.String()
	case BETWEEN:
		return expr.X.String() + " BETWEEN " + expr.Y.String()
	case NOTBETWEEN:
		return expr.X.String() + " NOT BETWEEN " + expr.Y.String()
	case LSHIFT:
		return expr.X.String() + " << " + expr.Y.String()
	case RSHIFT:
		return expr.X.String() + " >> " + expr.Y.String()
	case BITAND:
		return expr.X.String() + " & " + expr.Y.String()
	case BITOR:
		return expr.X.String() + " | " + expr.Y.String()
	case LT:
		return expr.X.String() + " < " + expr.Y.String()
	case LG:
		return expr.X.String() + " <> " + expr.Y.String()
	case LE:
		return expr.X.String() + " <= " + expr.Y.String()
	case GT:
		return expr.X.String() + " > " + expr.Y.String()
	case GE:
		return expr.X.String() + " >= " + expr.Y.String()
	case EQ:
		return expr.X.String() + " = " + expr.Y.String()
	case NE:
		return expr.X.String() + " != " + expr.Y.String()
	case IS:
		return expr.X.String() + " IS " + expr.Y.String()
	case ISNOT:
		return expr.X.String() + " IS NOT " + expr.Y.String()
	case IN:
		return expr.X.String() + " IN " + expr.Y.String()
	case NOTIN:
		return expr.X.String() + " NOT IN " + expr.Y.String()
	case LIKE:
		return expr.X.String() + " LIKE " + expr.Y.String()
	case NOTLIKE:
		return expr.X.String() + " NOT LIKE " + expr.Y.String()
	case GLOB:
		return expr.X.String() + " GLOB " + expr.Y.String()
	case NOTGLOB:
		return expr.X.String() + " NOT GLOB " + expr.Y.String()
	case MATCH:
		return expr.X.String() + " MATCH " + expr.Y.String()
	case NOTMATCH:
		return expr.X.String() + " NOT MATCH " + expr.Y.String()
	case REGEXP:
		return expr.X.String() + " REGEXP " + expr.Y.String()
	case NOTREGEXP:
		return expr.X.String() + " NOT REGEXP " + expr.Y.String()
	case AND:
		return expr.X.String() + " AND " + expr.Y.String()
	case OR:
		return expr.X.String() + " OR " + expr.Y.String()
	default:
		panic(fmt.Sprintf("sqlparser.BinaryExpr.String(): invalid op %s", expr.Op))
	}
}

type CaseExpr struct {
	Operand  Expr         // optional condition after the CASE keyword
	Blocks   []*CaseBlock // list of WHEN/THEN pairs
	ElseExpr Expr         // expression used by default case
}

// String returns the string representation of the expression.
func (expr *CaseExpr) String() string {
	var buf bytes.Buffer
	buf.WriteString("CASE")
	if expr.Operand != nil {
		buf.WriteString(" ")
		buf.WriteString(expr.Operand.String())
	}
	for _, blk := range expr.Blocks {
		buf.WriteString(" ")
		buf.WriteString(blk.String())
	}
	if expr.ElseExpr != nil {
		buf.WriteString(" ELSE ")
		buf.WriteString(expr.ElseExpr.String())
	}
	buf.WriteString(" END")
	return buf.String()
}

type CaseBlock struct {
	Condition Expr // block condition
	Body      Expr // result expression
}

// String returns the string representation of the block.
func (b *CaseBlock) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", b.Condition.String(), b.Body.String())
}

type Exists struct {
	Not    bool
	Select *SelectStatement // select statement
}

// String returns the string representation of the expression.
func (expr *Exists) String() string {
	if expr.Not {
		return fmt.Sprintf("NOT EXISTS (%s)", expr.Select.String())
	}
	return fmt.Sprintf("EXISTS (%s)", expr.Select.String())
}

type Exprs struct {
	Exprs []Expr // list of expressions
}

// String returns the string representation of the expression.
func (l *Exprs) String() string {
	var buf bytes.Buffer
	buf.WriteString("(")
	for i, expr := range l.Exprs {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(expr.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type Range struct {
	X Expr // lhs expression
	Y Expr // rhs expression
}

// String returns the string representation of the expression.
func (r *Range) String() string {
	return fmt.Sprintf("%s AND %s", r.X.String(), r.Y.String())
}

type QualifiedRef struct {
	Table  *Ident // table name
	Star   bool
	Column *Ident // column name
}

// String returns the string representation of the expression.
func (r *QualifiedRef) String() string {
	if r.Star {
		return fmt.Sprintf("%s.*", r.Table.String())
	}
	return fmt.Sprintf("%s.%s", r.Table.String(), r.Column.String())
}

type Call struct {
	Name     *Ident // function name
	Star     bool
	Distinct bool
	Args     []Expr        // argument list
	Filter   *FilterClause // filter clause
}

// String returns the string representation of the expression.
func (c *Call) String() string {
	var buf bytes.Buffer
	buf.WriteString(c.Name.Name)
	buf.WriteString("(")
	if c.Star {
		buf.WriteString("*")
	} else {
		if c.Distinct {
			buf.WriteString("DISTINCT")
			if len(c.Args) != 0 {
				buf.WriteString(" ")
			}
		}
		for i, arg := range c.Args {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(arg.String())
		}
	}
	buf.WriteString(")")

	if c.Filter != nil {
		buf.WriteString(" ")
		buf.WriteString(c.Filter.String())
	}

	return buf.String()
}

type FilterClause struct {
	X Expr // filter expression
}

// String returns the string representation of the clause.
func (c *FilterClause) String() string {
	return fmt.Sprintf("FILTER (WHERE %s)", c.X.String())
}

type OrderingTerm struct {
	X Expr // ordering expression

	Asc  bool
	Desc bool

	NullsFirst bool
	NullsLast  bool
}

// String returns the string representation of the term.
func (t *OrderingTerm) String() string {
	var buf bytes.Buffer
	buf.WriteString(t.X.String())

	if t.Asc {
		buf.WriteString(" ASC")
	} else if t.Desc {
		buf.WriteString(" DESC")
	}

	if t.NullsFirst {
		buf.WriteString(" NULLS FIRST")
	} else if t.NullsLast {
		buf.WriteString(" NULLS LAST")
	}

	return buf.String()
}

type ColumnArg interface {
	Node
	columnArg()
}

// InsertStatement see http://www.postgres.cn/docs/12/sql-insert.html
type InsertStatement struct {
	TableName *TableName

	ColumnNames []*Ident
	Overriding  string

	DefaultValues bool
	Expressions   []*Exprs
	Query         *SelectStatement

	UpsertClause *UpsertClause

	OutputExpressions *OutputNames
}

// String returns the string representation of the statement.
func (s *InsertStatement) String() string {
	var buf bytes.Buffer

	buf.WriteString("INSERT")

	fmt.Fprintf(&buf, " INTO %s", s.TableName.String())

	if len(s.ColumnNames) != 0 {
		buf.WriteString(" (")
		for i, col := range s.ColumnNames {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	if s.DefaultValues {
		buf.WriteString(" DEFAULT VALUES")
	} else if s.Query != nil {
		fmt.Fprintf(&buf, " %s", s.Query.String())
	} else {
		buf.WriteString(" VALUES")
		for i := range s.Expressions {
			if i != 0 {
				buf.WriteString(",")
			}
			buf.WriteString(" (")
			for j, expr := range s.Expressions[i].Exprs {
				if j != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(expr.String())
			}
			buf.WriteString(")")
		}
	}

	if s.UpsertClause != nil {
		fmt.Fprintf(&buf, " %s", s.UpsertClause.String())
	}

	if s.OutputExpressions != nil {
		fmt.Fprintf(&buf, " RETURNING %s", s.OutputExpressions.String())
	}

	return buf.String()
}

type UpsertClause struct {
	Columns   []*IndexedColumn // optional indexed column list
	WhereExpr Expr             // optional conditional expression

	DoNothing       bool          // position of NOTHING keyword after DO
	DoUpdate        bool          // position of UPDATE keyword after DO
	Assignments     []*Assignment // list of column assignments
	UpdateWhereExpr Expr          // optional conditional expression for DO UPDATE SET
}

// String returns the string representation of the clause.
func (c *UpsertClause) String() string {
	var buf bytes.Buffer
	buf.WriteString("ON CONFLICT")

	if len(c.Columns) != 0 {
		buf.WriteString(" (")
		for i, col := range c.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")

		if c.WhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.WhereExpr.String())
		}
	}

	buf.WriteString(" DO")
	if c.DoNothing {
		buf.WriteString(" NOTHING")
	} else {
		buf.WriteString(" UPDATE SET ")
		for i := range c.Assignments {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(c.Assignments[i].String())
		}

		if c.UpdateWhereExpr != nil {
			fmt.Fprintf(&buf, " WHERE %s", c.UpdateWhereExpr.String())
		}
	}

	return buf.String()
}

// UpdateStatement see http://www.postgres.cn/docs/12/sql-update.html
type UpdateStatement struct {
	Only      bool
	TableName *TableName
	TableStar bool
	Alias     *Ident

	Assignments []*Assignment

	FromList []*TableName

	Condition  Expr
	CursorName *Ident

	OutputExpressions *OutputNames
}

// String returns the string representation of the clause.
func (s *UpdateStatement) String() string {
	var buf bytes.Buffer

	buf.WriteString("UPDATE ")
	if s.Only {
		buf.WriteString("ONLY ")
	}
	fmt.Fprintf(&buf, "%s", s.TableName.String())
	if s.TableStar {
		buf.WriteString(" *")
	}
	if s.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", s.Alias.String())
	}

	buf.WriteString(" SET ")
	for i := range s.Assignments {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(s.Assignments[i].String())
	}

	if len(s.FromList) > 0 {
		buf.WriteString(" From ")
		for i, name := range s.FromList {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(name.String())
		}
	}

	if s.Condition != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.Condition.String())
	} else if s.CursorName != nil {
		fmt.Fprintf(&buf, " WHERE CURRENT OF %s", s.CursorName.String())
	}

	if s.OutputExpressions != nil {
		fmt.Fprintf(&buf, " RETURNING %s", s.OutputExpressions.String())
	}

	return buf.String()
}

// DeleteStatement see http://www.postgres.cn/docs/12/sql-delete.html
type DeleteStatement struct {
	Only      bool
	TableName *TableName
	TableStar bool
	Alias     *Ident

	UsingList []*TableName

	Condition  Expr
	CursorName *Ident

	OutputExpressions *OutputNames
}

// String returns the string representation of the clause.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer

	buf.WriteString("DELETE FROM ")
	if s.Only {
		buf.WriteString("ONLY ")
	}
	fmt.Fprintf(&buf, "%s", s.TableName.String())
	if s.TableStar {
		buf.WriteString(" *")
	}
	if s.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", s.Alias.String())
	}

	if len(s.UsingList) > 0 {
		buf.WriteString(" USING ")
		for i, name := range s.UsingList {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(name.String())
		}
	}

	if s.Condition != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.Condition.String())
	} else if s.CursorName != nil {
		fmt.Fprintf(&buf, " WHERE CURRENT OF %s", s.CursorName.String())
	}

	if s.OutputExpressions != nil {
		fmt.Fprintf(&buf, " RETURNING %s", s.OutputExpressions.String())
	}

	return buf.String()
}

// Assignment is used within the UPDATE statement & upsert clause.
// It is similiar to an expression except that it must be an equality.
type Assignment struct {
	Columns []*Ident // column list
	Expr    Expr     // assigned expression
}

// String returns the string representation of the clause.
func (a *Assignment) String() string {
	var buf bytes.Buffer
	if len(a.Columns) == 1 {
		buf.WriteString(a.Columns[0].String())
	} else if len(a.Columns) > 1 {
		buf.WriteString("(")
		for i, col := range a.Columns {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(col.String())
		}
		buf.WriteString(")")
	}

	fmt.Fprintf(&buf, " = %s", a.Expr.String())
	return buf.String()
}

type IndexedColumn struct {
	X    Expr // column expression
	Asc  bool
	Desc bool
}

// String returns the string representation of the column.
func (c *IndexedColumn) String() string {
	if c.Asc {
		return fmt.Sprintf("%s ASC", c.X.String())
	} else if c.Desc {
		return fmt.Sprintf("%s DESC", c.X.String())
	}
	return c.X.String()
}

// SelectStatement see http://www.postgres.cn/docs/12/sql-select.html
type SelectStatement struct {
	All      bool
	Distinct bool
	Columns  *OutputNames // list of result columns in the SELECT clause

	FromItems Source

	Condition Expr

	GroupingElements []Expr
	HavingCondition  Expr

	Union     bool
	UnionAll  bool
	Intersect bool
	Except    bool
	Compound  *SelectStatement // compounded SELECT statement

	OrderBy []*OrderingTerm // terms of ORDER BY clause

	Limit  Expr
	Offset Expr // offset expression

	Hint *Hint
}

// String returns the string representation of the statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer

	buf.WriteString("SELECT ")
	if s.Hint != nil {
		fmt.Fprintf(&buf, "%s ", s.Hint.String())
	}

	if s.All {
		buf.WriteString("ALL ")
	} else if s.Distinct {
		buf.WriteString("DISTINCT ")
	}

	buf.WriteString(s.Columns.String())

	if s.FromItems != nil {
		fmt.Fprintf(&buf, " FROM %s", s.FromItems.String())
	}

	if s.Condition != nil {
		fmt.Fprintf(&buf, " WHERE %s", s.Condition.String())
	}

	if len(s.GroupingElements) != 0 {
		buf.WriteString(" GROUP BY ")
		for i, expr := range s.GroupingElements {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(expr.String())
		}

		if s.HavingCondition != nil {
			fmt.Fprintf(&buf, " HAVING %s", s.HavingCondition.String())
		}
	}

	// Write compound operator.
	if s.Compound != nil {
		switch {
		case s.Union:
			buf.WriteString(" UNION")
			if s.UnionAll {
				buf.WriteString(" ALL")
			}
		case s.Intersect:
			buf.WriteString(" INTERSECT")
		case s.Except:
			buf.WriteString(" EXCEPT")
		}

		fmt.Fprintf(&buf, " %s", s.Compound.String())
	}

	if len(s.OrderBy) != 0 {
		buf.WriteString(" ORDER BY ")
		for i, term := range s.OrderBy {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(term.String())
		}
	}

	if s.Limit != nil {
		fmt.Fprintf(&buf, " LIMIT %s", s.Limit.String())
		if s.Offset != nil {
			fmt.Fprintf(&buf, " OFFSET %s", s.Offset.String())
		}
	}

	return buf.String()
}

type OutputNames []*ResultColumn

func (on OutputNames) String() string {
	var buf bytes.Buffer
	for i, name := range on {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(name.String())
	}

	return buf.String()
}

type ResultColumn struct {
	Star  bool
	Expr  Expr   // column expression (may be "tbl.*")
	Alias *Ident // alias name
}

// String returns the string representation of the column.
func (c *ResultColumn) String() string {
	if c.Star {
		return "*"
	} else if c.Alias != nil {
		return fmt.Sprintf("%s AS %s", c.Expr.String(), c.Alias.String())
	}
	return c.Expr.String()
}

type TableName struct {
	Name  *Ident // table name
	Alias *Ident // optional table alias
}

// TableName returns the name used to identify n.
// Returns the alias, if one is specified. Otherwise returns the name.
func (n *TableName) TableName() string {
	if s := IdentName(n.Alias); s != "" {
		return s
	}
	return IdentName(n.Name)
}

// String returns the string representation of the table name.
func (n *TableName) String() string {
	var buf bytes.Buffer
	buf.WriteString(n.Name.String())
	if n.Alias != nil {
		fmt.Fprintf(&buf, " AS %s", n.Alias.String())
	}
	return buf.String()
}

type ParenSource struct {
	X     Source // nested source
	Alias *Ident // optional table alias (select source only)
}

// String returns the string representation of the source.
func (s *ParenSource) String() string {
	if s.Alias != nil {
		return fmt.Sprintf("(%s) AS %s", s.X.String(), s.Alias.String())
	}
	return fmt.Sprintf("(%s)", s.X.String())
}

type JoinClause struct {
	X          Source         // lhs source
	Operator   *JoinOperator  // join operator
	Y          Source         // rhs source
	Constraint JoinConstraint // join constraint
}

// String returns the string representation of the clause.
func (c *JoinClause) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s%s%s", c.X.String(), c.Operator.String(), c.Y.String())
	if c.Constraint != nil {
		fmt.Fprintf(&buf, " %s", c.Constraint.String())
	}
	return buf.String()
}

type JoinOperator struct {
	Comma   bool
	Natural bool
	Left    bool
	Outer   bool
	Inner   bool
	Cross   bool
}

// String returns the string representation of the operator.
func (op *JoinOperator) String() string {
	if op.Comma {
		return ", "
	}

	var buf bytes.Buffer
	if op.Natural {
		buf.WriteString(" NATURAL")
	}
	if op.Left {
		buf.WriteString(" LEFT")
		if op.Outer {
			buf.WriteString(" OUTER")
		}
	} else if op.Inner {
		buf.WriteString(" INNER")
	} else if op.Cross {
		buf.WriteString(" CROSS")
	}
	buf.WriteString(" JOIN ")

	return buf.String()
}

type OnConstraint struct {
	X Expr // constraint expression
}

// String returns the string representation of the constraint.
func (c *OnConstraint) String() string {
	return "ON " + c.X.String()
}

type UsingConstraint struct {
	Columns []*Ident // column list
}

// String returns the string representation of the constraint.
func (c *UsingConstraint) String() string {
	var buf bytes.Buffer
	buf.WriteString("USING (")
	for i, col := range c.Columns {
		if i != 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(col.String())
	}
	buf.WriteString(")")
	return buf.String()
}

type ParenExpr struct {
	X Expr // parenthesized expression
}

// String returns the string representation of the expression.
func (expr *ParenExpr) String() string {
	return fmt.Sprintf("(%s)", expr.X.String())
}
