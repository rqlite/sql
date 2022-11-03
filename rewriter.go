package sql

import (
	"strings"
)

type Rewriter struct {
	RewriteRand bool

	randRewritten bool
}

func (rw *Rewriter) Do(stmt Statement) (Statement, bool, error) {
	err := Walk(rw, stmt)
	if err != nil {
		return nil, false, err
	}
	return stmt, rw.randRewritten, nil
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
		rw.randRewritten =
			rw.randRewritten || strings.ToUpper(n.Name.Name) == "RANDOM"
		n.Eval = rw.RewriteRand
		return rw, nil
	}
	return rw, nil
}

func (rw *Rewriter) VisitEnd(node Node) error {
	return nil
}
