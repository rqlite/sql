package sql

// A Visitor's Visit method is invoked for each node encountered by Walk.
// If the result visitor w is not nil, Walk visits each of the children
// of node with the visitor w, followed by a call of w.VisitEnd(n).
type Visitor interface {
	Visit(n Node) (w Visitor, node Node, err error)
	VisitEnd(n Node) (Node, error)
}

// Walk traverses an AST in depth-first order: It starts by calling
// v.Visit(node); node must not be nil. If the visitor w returned by
// v.Visit(node) is not nil, Walk is invoked recursively with visitor
// w for each of the non-nil children of node, followed by a call of
// w.VisitEnd(n).
func Walk(v Visitor, n Node) (Node, error) {
	return walk(v, n)
}

func walk(v Visitor, n Node) (retNode Node, err error) {
	// Visit the node itself
	if v, retNode, err = v.Visit(n); err != nil {
		return nil, err
	} else if v == nil {
		return retNode, nil
	}

	// Node might have been modified so use the new node.
	n = retNode

	// Visit node's children.
	switch nn := n.(type) {
	case *Assignment:
		if columns, err := walkIdentList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}
		if expr, err := walkExpr(v, nn.Expr); err != nil {
			return nil, err
		} else {
			nn.Expr = expr
		}

	case *ExplainStatement:
		if nn.Stmt != nil {
			if rn, err := walk(v, nn.Stmt); err != nil {
				return nil, err
			} else {
				nn.Stmt = rn.(Statement)
			}
		}

	case *RollbackStatement:
		if ri, err := walkIdent(v, nn.SavepointName); err != nil {
			return nil, err
		} else {
			nn.SavepointName = ri
		}

	case *SavepointStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *ReleaseStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *CreateTableStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if rcd, err := walkColumnDefinitionList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = rcd
		}
		if rc, err := walkConstraintList(v, nn.Constraints); err != nil {
			return nil, err
		} else {
			nn.Constraints = rc
		}
		if nn.Select != nil {
			if rn, err := walk(v, nn.Select); err != nil {
				return nil, err
			} else {
				nn.Select = rn.(*SelectStatement)
			}
		}

	case *AlterTableStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if ri, err := walkIdent(v, nn.NewName); err != nil {
			return nil, err
		} else {
			nn.NewName = ri
		}
		if ri, err := walkIdent(v, nn.ColumnName); err != nil {
			return nil, err
		} else {
			nn.ColumnName = ri
		}
		if ri, err := walkIdent(v, nn.NewColumnName); err != nil {
			return nil, err
		} else {
			nn.NewColumnName = ri
		}
		if nn.ColumnDef != nil {
			if rn, err := walk(v, nn.ColumnDef); err != nil {
				return nil, err
			} else {
				nn.ColumnDef = rn.(*ColumnDefinition)
			}
		}

	case *AnalyzeStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *CreateViewStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if columns, err := walkIdentList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}
		if nn.Select != nil {
			if rn, err := walk(v, nn.Select); err != nil {
				return nil, err
			} else {
				nn.Select = rn.(*SelectStatement)
			}
		}

	case *DropTableStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *DropViewStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *DropIndexStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *DropTriggerStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *CreateIndexStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if ri, err := walkIdent(v, nn.Table); err != nil {
			return nil, err
		} else {
			nn.Table = ri
		}
		if columns, err := walkIndexedColumnList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}
		if expr, err := walkExpr(v, nn.WhereExpr); err != nil {
			return nil, err
		} else {
			nn.WhereExpr = expr
		}

	case *CreateTriggerStatement:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if columns, err := walkIdentList(v, nn.UpdateOfColumns); err != nil {
			return nil, err
		} else {
			nn.UpdateOfColumns = columns
		}
		if ri, err := walkIdent(v, nn.Table); err != nil {
			return nil, err
		} else {
			nn.Table = ri
		}
		if expr, err := walkExpr(v, nn.WhenExpr); err != nil {
			return nil, err
		} else {
			nn.WhenExpr = expr
		}
		for i, x := range nn.Body {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Body[i] = rn.(Statement)
			}
		}

	case *SelectStatement:
		if nn.WithClause != nil {
			if rn, err := walk(v, nn.WithClause); err != nil {
				return nil, err
			} else {
				nn.WithClause = rn.(*WithClause)
			}
		}
		for i, x := range nn.ValueLists {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.ValueLists[i] = rn.(*ExprList)
			}
		}
		for i, x := range nn.Columns {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Columns[i] = rn.(*ResultColumn)
			}
		}
		if nn.Source != nil {
			if rn, err := walk(v, nn.Source); err != nil {
				return nil, err
			} else {
				nn.Source = rn.(Source)
			}
		}
		if expr, err := walkExpr(v, nn.WhereExpr); err != nil {
			return nil, err
		} else {
			nn.WhereExpr = expr
		}
		if exprs, err := walkExprList(v, nn.GroupByExprs); err != nil {
			return nil, err
		} else {
			nn.GroupByExprs = exprs
		}
		if expr, err := walkExpr(v, nn.HavingExpr); err != nil {
			return nil, err
		} else {
			nn.HavingExpr = expr
		}
		for i, x := range nn.Windows {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Windows[i] = rn.(*Window)
			}
		}
		if nn.Compound != nil {
			if rn, err := walk(v, nn.Compound); err != nil {
				return nil, err
			} else {
				nn.Compound = rn.(*SelectStatement)
			}
		}
		for i, x := range nn.OrderingTerms {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.OrderingTerms[i] = rn.(*OrderingTerm)
			}
		}
		if expr, err := walkExpr(v, nn.LimitExpr); err != nil {
			return nil, err
		} else {
			nn.LimitExpr = expr
		}
		if expr, err := walkExpr(v, nn.OffsetExpr); err != nil {
			return nil, err
		} else {
			nn.OffsetExpr = expr
		}

	case *InsertStatement:
		if nn.WithClause != nil {
			if rn, err := walk(v, nn.WithClause); err != nil {
				return nil, err
			} else {
				nn.WithClause = rn.(*WithClause)
			}
		}
		if ri, err := walkIdent(v, nn.Table); err != nil {
			return nil, err
		} else {
			nn.Table = ri
		}
		if ri, err := walkIdent(v, nn.Alias); err != nil {
			return nil, err
		} else {
			nn.Alias = ri
		}
		if columns, err := walkIdentList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}
		for i, x := range nn.ValueLists {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.ValueLists[i] = rn.(*ExprList)
			}
		}
		if nn.Select != nil {
			if rn, err := walk(v, nn.Select); err != nil {
				return nil, err
			} else {
				nn.Select = rn.(*SelectStatement)
			}
		}
		if nn.UpsertClause != nil {
			if rn, err := walk(v, nn.UpsertClause); err != nil {
				return nil, err
			} else {
				nn.UpsertClause = rn.(*UpsertClause)
			}
		}
		if nn.ReturningClause != nil {
			if rn, err := walk(v, nn.ReturningClause); err != nil {
				return nil, err
			} else {
				nn.ReturningClause = rn.(*ReturningClause)
			}
		}

	case *UpdateStatement:
		if nn.WithClause != nil {
			if rn, err := walk(v, nn.WithClause); err != nil {
				return nil, err
			} else {
				nn.WithClause = rn.(*WithClause)
			}
		}
		if nn.Table != nil {
			if rn, err := walk(v, nn.Table); err != nil {
				return nil, err
			} else {
				nn.Table = rn.(*QualifiedTableName)
			}
		}
		for i, x := range nn.Assignments {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Assignments[i] = rn.(*Assignment)
			}
		}
		if expr, err := walkExpr(v, nn.WhereExpr); err != nil {
			return nil, err
		} else {
			nn.WhereExpr = expr
		}
		if nn.ReturningClause != nil {
			if rn, err := walk(v, nn.ReturningClause); err != nil {
				return nil, err
			} else {
				nn.ReturningClause = rn.(*ReturningClause)
			}
		}

	case *UpsertClause:
		if columns, err := walkIndexedColumnList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}
		if expr, err := walkExpr(v, nn.WhereExpr); err != nil {
			return nil, err
		} else {
			nn.WhereExpr = expr
		}
		for i, x := range nn.Assignments {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Assignments[i] = rn.(*Assignment)
			}
		}
		if expr, err := walkExpr(v, nn.UpdateWhereExpr); err != nil {
			return nil, err
		} else {
			nn.UpdateWhereExpr = expr
		}

	case *ReturningClause:
		for i, x := range nn.Columns {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Columns[i] = rn.(*ResultColumn)
			}
		}

	case *DeleteStatement:
		if nn.WithClause != nil {
			if rn, err := walk(v, nn.WithClause); err != nil {
				return nil, err
			} else {
				nn.WithClause = rn.(*WithClause)
			}
		}
		if nn.Table != nil {
			if rn, err := walk(v, nn.Table); err != nil {
				return nil, err
			} else {
				nn.Table = rn.(*QualifiedTableName)
			}
		}
		if expr, err := walkExpr(v, nn.WhereExpr); err != nil {
			return nil, err
		} else {
			nn.WhereExpr = expr
		}
		for i, x := range nn.OrderingTerms {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.OrderingTerms[i] = rn.(*OrderingTerm)
			}
		}
		if expr, err := walkExpr(v, nn.LimitExpr); err != nil {
			return nil, err
		} else {
			nn.LimitExpr = expr
		}
		if expr, err := walkExpr(v, nn.OffsetExpr); err != nil {
			return nil, err
		} else {
			nn.OffsetExpr = expr
		}
		if nn.ReturningClause != nil {
			if rn, err := walk(v, nn.ReturningClause); err != nil {
				return nil, err
			} else {
				nn.ReturningClause = rn.(*ReturningClause)
			}
		}

	case *PrimaryKeyConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if columns, err := walkIdentList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}

	case *NotNullConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}

	case *UniqueConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if columns, err := walkIndexedColumnList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}

	case *CheckConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if expr, err := walkExpr(v, nn.Expr); err != nil {
			return nil, err
		} else {
			nn.Expr = expr
		}

	case *DefaultConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if expr, err := walkExpr(v, nn.Expr); err != nil {
			return nil, err
		} else {
			nn.Expr = expr
		}

	case *GeneratedConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if expr, err := walkExpr(v, nn.Expr); err != nil {
			return nil, err
		} else {
			nn.Expr = expr
		}

	case *ForeignKeyConstraint:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if columns, err := walkIdentList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}
		if ri, err := walkIdent(v, nn.ForeignTable); err != nil {
			return nil, err
		} else {
			nn.ForeignTable = ri
		}
		if columns, err := walkIdentList(v, nn.ForeignColumns); err != nil {
			return nil, err
		} else {
			nn.ForeignColumns = columns
		}
		for i, x := range nn.Args {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Args[i] = rn.(*ForeignKeyArg)
			}
		}

	case *ParenExpr:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}

	case *UnaryExpr:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}

	case *BinaryExpr:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}
		if expr, err := walkExpr(v, nn.Y); err != nil {
			return nil, err
		} else {
			nn.Y = expr
		}

	case *CastExpr:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}
		if nn.Type != nil {
			if rn, err := walk(v, nn.Type); err != nil {
				return nil, err
			} else {
				nn.Type = rn.(*Type)
			}
		}

	case *CaseBlock:
		if expr, err := walkExpr(v, nn.Condition); err != nil {
			return nil, err
		} else {
			nn.Condition = expr
		}
		if expr, err := walkExpr(v, nn.Body); err != nil {
			return nil, err
		} else {
			nn.Body = expr
		}

	case *CaseExpr:
		if expr, err := walkExpr(v, nn.Operand); err != nil {
			return nil, err
		} else {
			nn.Operand = expr
		}
		for i, x := range nn.Blocks {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.Blocks[i] = rn.(*CaseBlock)
			}
		}
		if expr, err := walkExpr(v, nn.ElseExpr); err != nil {
			return nil, err
		} else {
			nn.ElseExpr = expr
		}

	case *ExprList:
		if exprs, err := walkExprList(v, nn.Exprs); err != nil {
			return nil, err
		} else {
			nn.Exprs = exprs
		}

	case *QualifiedRef:
		if ri, err := walkIdent(v, nn.Table); err != nil {
			return nil, err
		} else {
			nn.Table = ri
		}
		if ri, err := walkIdent(v, nn.Column); err != nil {
			return nil, err
		} else {
			nn.Column = ri
		}

	case *Call:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if exprs, err := walkExprList(v, nn.Args); err != nil {
			return nil, err
		} else {
			nn.Args = exprs
		}
		if nn.Filter != nil {
			if rn, err := walk(v, nn.Filter); err != nil {
				return nil, err
			} else {
				nn.Filter = rn.(*FilterClause)
			}
		}
		if nn.Over != nil {
			if rn, err := walk(v, nn.Over); err != nil {
				return nil, err
			} else {
				nn.Over = rn.(*OverClause)
			}
		}

	case *FilterClause:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}

	case *OverClause:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if nn.Definition != nil {
			if rn, err := walk(v, nn.Definition); err != nil {
				return nil, err
			} else {
				nn.Definition = rn.(*WindowDefinition)
			}
		}

	case *OrderingTerm:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}

	case *FrameSpec:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}
		if expr, err := walkExpr(v, nn.Y); err != nil {
			return nil, err
		} else {
			nn.Y = expr
		}

	case *Range:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}
		if expr, err := walkExpr(v, nn.Y); err != nil {
			return nil, err
		} else {
			nn.Y = expr
		}

	case *Raise:
		if nn.Error != nil {
			if rn, err := walk(v, nn.Error); err != nil {
				return nil, err
			} else {
				nn.Error = rn.(*StringLit)
			}
		}

	case *Exists:
		if nn.Select != nil {
			if rn, err := walk(v, nn.Select); err != nil {
				return nil, err
			} else {
				nn.Select = rn.(*SelectStatement)
			}
		}

	case *ParenSource:
		if nn.X != nil {
			if rn, err := walk(v, nn.X); err != nil {
				return nil, err
			} else {
				nn.X = rn.(Source)
			}
		}
		if ri, err := walkIdent(v, nn.Alias); err != nil {
			return nil, err
		} else {
			nn.Alias = ri
		}

	case *QualifiedTableName:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if ri, err := walkIdent(v, nn.Alias); err != nil {
			return nil, err
		} else {
			nn.Alias = ri
		}
		if ri, err := walkIdent(v, nn.Index); err != nil {
			return nil, err
		} else {
			nn.Index = ri
		}

	case *QualifiedTableFunctionName:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if exprs, err := walkExprList(v, nn.Args); err != nil {
			return nil, err
		} else {
			nn.Args = exprs
		}
		if ri, err := walkIdent(v, nn.Alias); err != nil {
			return nil, err
		} else {
			nn.Alias = ri
		}

	case *JoinClause:
		if nn.X != nil {
			if rn, err := walk(v, nn.X); err != nil {
				return nil, err
			} else {
				nn.X = rn.(Source)
			}
		}
		if nn.Operator != nil {
			if rn, err := walk(v, nn.Operator); err != nil {
				return nil, err
			} else {
				nn.Operator = rn.(*JoinOperator)
			}
		}
		if nn.Y != nil {
			if rn, err := walk(v, nn.Y); err != nil {
				return nil, err
			} else {
				nn.Y = rn.(Source)
			}
		}
		if nn.Constraint != nil {
			if rn, err := walk(v, nn.Constraint); err != nil {
				return nil, err
			} else {
				nn.Constraint = rn.(JoinConstraint)
			}
		}

	case *OnConstraint:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}

	case *UsingConstraint:
		if columns, err := walkIdentList(v, nn.Columns); err != nil {
			return nil, err
		} else {
			nn.Columns = columns
		}

	case *ColumnDefinition:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if nn.Type != nil {
			if rn, err := walk(v, nn.Type); err != nil {
				return nil, err
			} else {
				nn.Type = rn.(*Type)
			}
		}
		if constraints, err := walkConstraintList(v, nn.Constraints); err != nil {
			return nil, err
		} else {
			nn.Constraints = constraints
		}

	case *ResultColumn:
		if expr, err := walkExpr(v, nn.Expr); err != nil {
			return nil, err
		} else {
			nn.Expr = expr
		}
		if ri, err := walkIdent(v, nn.Alias); err != nil {
			return nil, err
		} else {
			nn.Alias = ri
		}

	case *IndexedColumn:
		if expr, err := walkExpr(v, nn.X); err != nil {
			return nil, err
		} else {
			nn.X = expr
		}

	case *Window:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if nn.Definition != nil {
			if rn, err := walk(v, nn.Definition); err != nil {
				return nil, err
			} else {
				nn.Definition = rn.(*WindowDefinition)
			}
		}

	case *WindowDefinition:
		if ri, err := walkIdent(v, nn.Base); err != nil {
			return nil, err
		} else {
			nn.Base = ri
		}
		if exprs, err := walkExprList(v, nn.Partitions); err != nil {
			return nil, err
		} else {
			nn.Partitions = exprs
		}
		for i, x := range nn.OrderingTerms {
			if rn, err := walk(v, x); err != nil {
				return nil, err
			} else {
				nn.OrderingTerms[i] = rn.(*OrderingTerm)
			}
		}
		if nn.Frame != nil {
			if rn, err := walk(v, nn.Frame); err != nil {
				return nil, err
			} else {
				nn.Frame = rn.(*FrameSpec)
			}
		}

	case *Type:
		if ri, err := walkIdent(v, nn.Name); err != nil {
			return nil, err
		} else {
			nn.Name = ri
		}
		if nn.Precision != nil {
			if rn, err := walk(v, nn.Precision); err != nil {
				return nil, err
			} else {
				nn.Precision = rn.(*NumberLit)
			}
		}
		if nn.Scale != nil {
			if rn, err := walk(v, nn.Scale); err != nil {
				return nil, err
			} else {
				nn.Scale = rn.(*NumberLit)
			}
		}
	}

	// Revisit original node after its children have been processed.
	retNode, err = v.VisitEnd(n)
	return retNode, err
}

// VisitFunc represents a function type that implements Visitor.
// Executes on node entry.
type VisitFunc func(Node) (Node, error)

// Visit executes fn. Walk visits node children if fn returns non-nil Visitor.
func (fn VisitFunc) Visit(node Node) (Visitor, Node, error) {
	if rn, err := fn(node); err != nil {
		return nil, nil, err
	} else {
		return fn, rn, nil
	}
}

// VisitEnd returns the node unmodified.
func (fn VisitFunc) VisitEnd(node Node) (Node, error) {
	return node, nil
}

// VisitEndFunc represents a function type that implements Visitor.
// Executes on node exit.
type VisitEndFunc func(Node) (Node, error)

// Visit returns the visitor itself to continue traversal.
func (fn VisitEndFunc) Visit(node Node) (Visitor, Node, error) {
	return fn, node, nil
}

// VisitEnd executes fn.
func (fn VisitEndFunc) VisitEnd(node Node) (Node, error) {
	return fn(node)
}

func walkIdent(v Visitor, x *Ident) (*Ident, error) {
	if x != nil {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			return rn.(*Ident), nil
		}
	}
	return x, nil
}

func walkIdentList(v Visitor, a []*Ident) ([]*Ident, error) {
	for i, x := range a {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			a[i] = rn.(*Ident)
		}
	}
	return a, nil
}

func walkExpr(v Visitor, x Expr) (Expr, error) {
	if x != nil {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			return rn.(Expr), nil
		}
	}
	return x, nil
}

func walkExprList(v Visitor, a []Expr) ([]Expr, error) {
	for i, x := range a {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			a[i] = rn.(Expr)
		}
	}
	return a, nil
}

func walkConstraintList(v Visitor, a []Constraint) ([]Constraint, error) {
	for i, x := range a {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			a[i] = rn.(Constraint)
		}
	}
	return a, nil
}

func walkIndexedColumnList(v Visitor, a []*IndexedColumn) ([]*IndexedColumn, error) {
	for i, x := range a {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			a[i] = rn.(*IndexedColumn)
		}
	}
	return a, nil
}

func walkColumnDefinitionList(v Visitor, a []*ColumnDefinition) ([]*ColumnDefinition, error) {
	for i, x := range a {
		if rn, err := walk(v, x); err != nil {
			return nil, err
		} else {
			a[i] = rn.(*ColumnDefinition)
		}
	}
	return a, nil
}
