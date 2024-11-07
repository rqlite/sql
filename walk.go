package sql

// A Visitor's Visit method is invoked for each node encountered by Walk.
// If the result visitor w is not nil, Walk visits each of the children
// of node with the visitor w, followed by a call of w.Visit(nil).
type Visitor interface {
	Visit(n Node) (w Visitor, node Node, err error)
	VisitEnd(n Node) (Node, error)
}

// Walk traverses an AST in depth-first order: It starts by calling
// v.Visit(node); node must not be nil. If the visitor w returned by
// v.Visit(node) is not nil, Walk is invoked recursively with visitor
// w for each of the non-nil children of node, followed by a call of
// w.Visit(nil).
func Walk(v Visitor, n Node) error {
	return walk(v, n)
}

func walk(v Visitor, n Node) (err error) {
	// Visit the node itself
	if v, _, err = v.Visit(n); err != nil {
		return err
	} else if v == nil {
		return nil
	}

	// Visit node's children.
	switch nn := n.(type) {
	case *Assignment:
		if err := walkIdentList(v, nn.Columns); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Expr); err != nil {
			return err
		}

	case *ExplainStatement:
		if nn.Stmt != nil {
			if err := walk(v, nn.Stmt); err != nil {
				return err
			}
		}

	case *RollbackStatement:
		if err := walkIdent(v, nn.SavepointName); err != nil {
			return err
		}

	case *SavepointStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *ReleaseStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *CreateTableStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkColumnDefinitionList(v, nn.Columns); err != nil {
			return err
		}
		if err := walkConstraintList(v, nn.Constraints); err != nil {
			return err
		}
		if nn.Select != nil {
			if err := walk(v, nn.Select); err != nil {
				return err
			}
		}

	case *AlterTableStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdent(v, nn.NewName); err != nil {
			return err
		}
		if err := walkIdent(v, nn.ColumnName); err != nil {
			return err
		}
		if err := walkIdent(v, nn.NewColumnName); err != nil {
			return err
		}
		if nn.ColumnDef != nil {
			if err := walk(v, nn.ColumnDef); err != nil {
				return err
			}
		}

	case *AnalyzeStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *CreateViewStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdentList(v, nn.Columns); err != nil {
			return err
		}
		if nn.Select != nil {
			if err := walk(v, nn.Select); err != nil {
				return err
			}
		}

	case *DropTableStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *DropViewStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *DropIndexStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *DropTriggerStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *CreateIndexStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Table); err != nil {
			return err
		}
		if err := walkIndexedColumnList(v, nn.Columns); err != nil {
			return err
		}
		if err := walkExpr(v, nn.WhereExpr); err != nil {
			return err
		}

	case *CreateTriggerStatement:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdentList(v, nn.UpdateOfColumns); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Table); err != nil {
			return err
		}
		if err := walkExpr(v, nn.WhenExpr); err != nil {
			return err
		}
		for _, x := range nn.Body {
			if err := walk(v, x); err != nil {
				return err
			}
		}

	case *SelectStatement:
		if nn.WithClause != nil {
			if err := walk(v, nn.WithClause); err != nil {
				return err
			}
		}
		for _, x := range nn.ValueLists {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		for _, x := range nn.Columns {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if nn.Source != nil {
			if err := walk(v, nn.Source); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.WhereExpr); err != nil {
			return err
		}
		if err := walkExprList(v, nn.GroupByExprs); err != nil {
			return err
		}
		if err := walkExpr(v, nn.HavingExpr); err != nil {
			return err
		}
		for _, x := range nn.Windows {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if nn.Compound != nil {
			if err := walk(v, nn.Compound); err != nil {
				return err
			}
		}
		for _, x := range nn.OrderingTerms {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.LimitExpr); err != nil {
			return err
		}
		if err := walkExpr(v, nn.OffsetExpr); err != nil {
			return err
		}

	case *InsertStatement:
		if nn.WithClause != nil {
			if err := walk(v, nn.WithClause); err != nil {
				return err
			}
		}
		if err := walkIdent(v, nn.Table); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Alias); err != nil {
			return err
		}
		if err := walkIdentList(v, nn.Columns); err != nil {
			return err
		}
		for _, x := range nn.ValueLists {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if nn.Select != nil {
			if err := walk(v, nn.Select); err != nil {
				return err
			}
		}
		if nn.UpsertClause != nil {
			if err := walk(v, nn.UpsertClause); err != nil {
				return err
			}
		}
		if nn.ReturningClause != nil {
			if err := walk(v, nn.ReturningClause); err != nil {
				return err
			}
		}

	case *UpdateStatement:
		if nn.WithClause != nil {
			if err := walk(v, nn.WithClause); err != nil {
				return err
			}
		}
		if nn.Table != nil {
			if err := walk(v, nn.Table); err != nil {
				return err
			}
		}
		for _, x := range nn.Assignments {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.WhereExpr); err != nil {
			return err
		}
		if nn.ReturningClause != nil {
			if err := walk(v, nn.ReturningClause); err != nil {
				return err
			}
		}

	case *UpsertClause:
		if err := walkIndexedColumnList(v, nn.Columns); err != nil {
			return err
		}
		if err := walkExpr(v, nn.WhereExpr); err != nil {
			return err
		}
		for _, x := range nn.Assignments {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.UpdateWhereExpr); err != nil {
			return err
		}

	case *ReturningClause:
		for _, x := range nn.Columns {
			if err := walk(v, x); err != nil {
				return err
			}
		}

	case *DeleteStatement:
		if nn.WithClause != nil {
			if err := walk(v, nn.WithClause); err != nil {
				return err
			}
		}
		if nn.Table != nil {
			if err := walk(v, nn.Table); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.WhereExpr); err != nil {
			return err
		}
		for _, x := range nn.OrderingTerms {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.LimitExpr); err != nil {
			return err
		}
		if err := walkExpr(v, nn.OffsetExpr); err != nil {
			return err
		}
		if nn.ReturningClause != nil {
			if err := walk(v, nn.ReturningClause); err != nil {
				return err
			}
		}

	case *PrimaryKeyConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdentList(v, nn.Columns); err != nil {
			return err
		}

	case *NotNullConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}

	case *UniqueConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIndexedColumnList(v, nn.Columns); err != nil {
			return err
		}

	case *CheckConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Expr); err != nil {
			return err
		}

	case *DefaultConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Expr); err != nil {
			return err
		}

	case *GeneratedConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Expr); err != nil {
			return err
		}

	case *ForeignKeyConstraint:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdentList(v, nn.Columns); err != nil {
			return err
		}
		if err := walkIdent(v, nn.ForeignTable); err != nil {
			return err
		}
		if err := walkIdentList(v, nn.ForeignColumns); err != nil {
			return err
		}
		for _, x := range nn.Args {
			if err := walk(v, x); err != nil {
				return err
			}
		}

	case *ParenExpr:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}

	case *UnaryExpr:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}

	case *BinaryExpr:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Y); err != nil {
			return err
		}

	case *CastExpr:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}
		if nn.Type != nil {
			if err := walk(v, nn.Type); err != nil {
				return err
			}
		}

	case *CaseBlock:
		if err := walkExpr(v, nn.Condition); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Body); err != nil {
			return err
		}

	case *CaseExpr:
		if err := walkExpr(v, nn.Operand); err != nil {
			return err
		}
		for _, x := range nn.Blocks {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if err := walkExpr(v, nn.ElseExpr); err != nil {
			return err
		}

	case *ExprList:
		if err := walkExprList(v, nn.Exprs); err != nil {
			return err
		}

	case *QualifiedRef:
		if err := walkIdent(v, nn.Table); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Column); err != nil {
			return err
		}

	case *Call:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkExprList(v, nn.Args); err != nil {
			return err
		}
		if nn.Filter != nil {
			if err := walk(v, nn.Filter); err != nil {
				return err
			}
		}
		if nn.Over != nil {
			if err := walk(v, nn.Over); err != nil {
				return err
			}
		}

	case *FilterClause:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}

	case *OverClause:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if nn.Definition != nil {
			if err := walk(v, nn.Definition); err != nil {
				return err
			}
		}

	case *OrderingTerm:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}

	case *FrameSpec:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Y); err != nil {
			return err
		}

	case *Range:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}
		if err := walkExpr(v, nn.Y); err != nil {
			return err
		}

	case *Raise:
		if nn.Error != nil {
			if err := walk(v, nn.Error); err != nil {
				return err
			}
		}

	case *Exists:
		if nn.Select != nil {
			if err := walk(v, nn.Select); err != nil {
				return err
			}
		}

	case *ParenSource:
		if nn.X != nil {
			if err := walk(v, nn.X); err != nil {
				return err
			}
		}
		if err := walkIdent(v, nn.Alias); err != nil {
			return err
		}

	case *QualifiedTableName:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Alias); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Index); err != nil {
			return err
		}

	case *JoinClause:
		if nn.X != nil {
			if err := walk(v, nn.X); err != nil {
				return err
			}
		}
		if nn.Operator != nil {
			if err := walk(v, nn.Operator); err != nil {
				return err
			}
		}
		if nn.Y != nil {
			if err := walk(v, nn.Y); err != nil {
				return err
			}
		}
		if nn.Constraint != nil {
			if err := walk(v, nn.Constraint); err != nil {
				return err
			}
		}

	case *OnConstraint:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}

	case *UsingConstraint:
		if err := walkIdentList(v, nn.Columns); err != nil {
			return err
		}

	case *ColumnDefinition:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if nn.Type != nil {
			if err := walk(v, nn.Type); err != nil {
				return err
			}
		}
		if err := walkConstraintList(v, nn.Constraints); err != nil {
			return err
		}

	case *ResultColumn:
		if err := walkExpr(v, nn.Expr); err != nil {
			return err
		}
		if err := walkIdent(v, nn.Alias); err != nil {
			return err
		}

	case *IndexedColumn:
		if err := walkExpr(v, nn.X); err != nil {
			return err
		}

	case *Window:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if nn.Definition != nil {
			if err := walk(v, nn.Definition); err != nil {
				return err
			}
		}

	case *WindowDefinition:
		if err := walkIdent(v, nn.Base); err != nil {
			return err
		}
		if err := walkExprList(v, nn.Partitions); err != nil {
			return err
		}
		for _, x := range nn.OrderingTerms {
			if err := walk(v, x); err != nil {
				return err
			}
		}
		if nn.Frame != nil {
			if err := walk(v, nn.Frame); err != nil {
				return err
			}
		}

	case *Type:
		if err := walkIdent(v, nn.Name); err != nil {
			return err
		}
		if nn.Precision != nil {
			if err := walk(v, nn.Precision); err != nil {
				return err
			}
		}
		if nn.Scale != nil {
			if err := walk(v, nn.Scale); err != nil {
				return err
			}
		}
	}

	// Revisit original node after its children have been processed.
	_, err = v.VisitEnd(n)
	return err
}

// VisitFunc represents a function type that implements Visitor.
// Only executes on node entry.
type VisitFunc func(Node) (Node, error)

// Visit executes fn. Walk visits node children if fn returns true.
func (fn VisitFunc) Visit(node Node) (Visitor, Node, error) {
	if _, err := fn(node); err != nil {
		return nil, nil, err
	}
	return fn, nil, nil
}

// VisitEnd is a no-op.
func (fn VisitFunc) VisitEnd(node Node) (Node, error) { return nil, nil }

// VisitEndFunc represents a function type that implements Visitor.
// Only executes on node exit.
type VisitEndFunc func(Node) (Node, error)

// Visit is a no-op.
func (fn VisitEndFunc) Visit(node Node) (Visitor, Node, error) { return fn, nil, nil }

// VisitEnd executes fn.
func (fn VisitEndFunc) VisitEnd(node Node) (Node, error) { return fn(node) }

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

func walkExprList(v Visitor, a []Expr) error {
	for _, x := range a {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}

func walkConstraintList(v Visitor, a []Constraint) error {
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

func walkColumnDefinitionList(v Visitor, a []*ColumnDefinition) error {
	for _, x := range a {
		if err := walk(v, x); err != nil {
			return err
		}
	}
	return nil
}
