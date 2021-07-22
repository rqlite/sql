package sqlparser

// A Visitor's Visit method is invoked for each node encountered by Walk.
// If the result visitor w is not nil, Walk visits each of the children
// of node with the visitor w, followed by a call of w.Visit(nil).
type Visitor interface {
	Visit(node Node) (w Visitor, err error)
	VisitEnd(node Node) error
}

// Walk traverses an AST in depth-first order: It starts by calling
// v.Visit(node); node must not be nil. If the visitor w returned by
// v.Visit(node) is not nil, Walk is invoked recursively with visitor
// w for each of the non-nil children of node, followed by a call of
// w.Visit(nil).
func Walk(v Visitor, node Node) error {
	return walk(v, node)
}

func walk(v Visitor, node Node) (err error) {
	// Visit the node itself
	if v, err = v.Visit(node); err != nil {
		return err
	} else if v == nil {
		return nil
	}

	// Visit node's children.
	switch n := node.(type) {
	case *Assignment:
		if err := walkIdentList(v, n.Columns); err != nil {
			return err
		}
		if err := walkExpr(v, n.Expr); err != nil {
			return err
		}

	case *SelectStatement:
		if err := walk(v, n.Columns); err != nil {
			return err
		}
		if n.FromItems != nil {
			if err := walk(v, n.FromItems); err != nil {
				return err
			}
		}
		if err := walkExpr(v, n.Condition); err != nil {
			return err
		}
		if err := walkExprs(v, n.GroupingElements); err != nil {
			return err
		}
		if err := walkExpr(v, n.HavingCondition); err != nil {
			return err
		}
		if n.Compound != nil {
			if err := walk(v, n.Compound); err != nil {
				return err
			}
		}
		for _, x := range n.OrderBy {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, n.Limit); err != nil {
			return err
		}
		if err := walkExpr(v, n.Offset); err != nil {
			return err
		}

	case *InsertStatement:
		if err := walk(v, n.TableName); err != nil {
			return err
		}
		if err := walkIdentList(v, n.ColumnNames); err != nil {
			return err
		}
		for _, x := range n.Expressions {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if n.Query != nil {
			if err := walk(v, n.Query); err != nil {
				return err
			}
		}
		if n.UpsertClause != nil {
			if err := walk(v, n.UpsertClause); err != nil {
				return err
			}
		}

	case *UpdateStatement:
		if n.TableName != nil {
			if err := walk(v, n.TableName); err != nil {
				return err
			}
		}
		for _, x := range n.Assignments {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, n.Condition); err != nil {
			return err
		}

	case *UpsertClause:
		if err := walkIndexedColumnList(v, n.Columns); err != nil {
			return err
		}
		if err := walkExpr(v, n.WhereExpr); err != nil {
			return err
		}
		for _, x := range n.Assignments {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, n.UpdateWhereExpr); err != nil {
			return err
		}

	case *DeleteStatement:
		if n.TableName != nil {
			if err := walk(v, n.TableName); err != nil {
				return err
			}
		}
		if err := walkExpr(v, n.Condition); err != nil {
			return err
		}

	case *ParenExpr:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}

	case *UnaryExpr:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}

	case *BinaryExpr:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}
		if err := walkExpr(v, n.Y); err != nil {
			return err
		}

	case *CaseBlock:
		if err := walkExpr(v, n.Condition); err != nil {
			return err
		}
		if err := walkExpr(v, n.Body); err != nil {
			return err
		}

	case *CaseExpr:
		if err := walkExpr(v, n.Operand); err != nil {
			return err
		}
		for _, x := range n.Blocks {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, n.ElseExpr); err != nil {
			return err
		}

	case *Exprs:
		if err := walkExprs(v, n.Exprs); err != nil {
			return err
		}

	case *QualifiedRef:
		if err := walkIdent(v, n.Table); err != nil {
			return err
		}
		if err := walkIdent(v, n.Column); err != nil {
			return err
		}

	case *Call:
		if err := walkIdent(v, n.Name); err != nil {
			return err
		}
		if err := walkExprs(v, n.Args); err != nil {
			return err
		}
		if n.Filter != nil {
			if err := walk(v, n.Filter); err != nil {
				return err
			}
		}

	case *FilterClause:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}

	case *OrderingTerm:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}

	case *Range:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}
		if err := walkExpr(v, n.Y); err != nil {
			return err
		}

	case *Exists:
		if n.Select != nil {
			if err := walk(v, n.Select); err != nil {
				return err
			}
		}

	case *ParenSource:
		if n.X != nil {
			if err := walk(v, n.X); err != nil {
				return err
			}
		}
		if err := walkIdent(v, n.Alias); err != nil {
			return err
		}

	case *TableName:
		if err := walkIdent(v, n.Name); err != nil {
			return err
		}
		if err := walkIdent(v, n.Alias); err != nil {
			return err
		}

	case *JoinClause:
		if n.X != nil {
			if err := walk(v, n.X); err != nil {
				return err
			}
		}
		if n.Operator != nil {
			if err := walk(v, n.Operator); err != nil {
				return err
			}
		}
		if n.Y != nil {
			if err := walk(v, n.Y); err != nil {
				return err
			}
		}
		if n.Constraint != nil {
			if err := walk(v, n.Constraint); err != nil {
				return err
			}
		}

	case *OnConstraint:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}

	case *UsingConstraint:
		if err := walkIdentList(v, n.Columns); err != nil {
			return err
		}

	case *ResultColumn:
		if err := walkExpr(v, n.Expr); err != nil {
			return err
		}
		if err := walkIdent(v, n.Alias); err != nil {
			return err
		}

	case *IndexedColumn:
		if err := walkExpr(v, n.X); err != nil {
			return err
		}

	case *Type:
		if err := walkIdent(v, n.Name); err != nil {
			return err
		}
		if n.Precision != nil {
			if err := walk(v, n.Precision); err != nil {
				return err
			}
		}
		if n.Scale != nil {
			if err := walk(v, n.Scale); err != nil {
				return err
			}
		}
	}

	// Revisit original node after its children have been processed.
	return v.VisitEnd(node)
}

// VisitFunc represents a function type that implements Visitor.
// Only executes on node entry.
type VisitFunc func(Node) error

// Visit executes fn. Walk visits node children if fn returns true.
func (fn VisitFunc) Visit(node Node) (Visitor, error) {
	if err := fn(node); err != nil {
		return nil, err
	}
	return fn, nil
}

// VisitEnd is a no-op.
func (fn VisitFunc) VisitEnd(node Node) error { return nil }

// VisitEndFunc represents a function type that implements Visitor.
// Only executes on node exit.
type VisitEndFunc func(Node) error

// Visit is a no-op.
func (fn VisitEndFunc) Visit(node Node) (Visitor, error) { return fn, nil }

// VisitEnd executes fn.
func (fn VisitEndFunc) VisitEnd(node Node) error { return fn(node) }

func walkIdent(v Visitor, x *Ident) error {
	if x != nil {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}

func walkIdentList(v Visitor, a []*Ident) error {
	for _, x := range a {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}

func walkExpr(v Visitor, x Expr) error {
	if x != nil {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}

func walkExprs(v Visitor, a []Expr) error {
	for _, x := range a {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}

func walkIndexedColumnList(v Visitor, a []*IndexedColumn) error {
	for _, x := range a {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}
