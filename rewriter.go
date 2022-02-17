package sql

type Rewriter struct {
	RewriteRand bool
}

func (rw *Rewriter) Do(stmt Statement) (Statement, error) {
	err := Walk(rw, stmt)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

func (rw *Rewriter) Visit(node Node) (w Visitor, err error) {
	if !rw.RewriteRand {
		// Nothing to do.
		return nil, nil
	}

	switch n := node.(type) {
	case *OrderingTerm:
		// Don't rewrite any further down this branch
		return nil, nil
	case *Call:
		n.Eval = rw.RewriteRand
		return rw, nil
	}
	return rw, nil
}

func (rw *Rewriter) VisitEnd(node Node) error {
	return nil
}
