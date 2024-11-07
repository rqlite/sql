package sql

import (
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Rewriter struct {
	RewriteRand bool
	RewriteTime bool

	randFn func() int64

	modified bool
}

func NewRewriter() *Rewriter {
	return &Rewriter{
		RewriteRand: true,
		RewriteTime: true,
		randFn: func() int64 {
			return rand.Int63()
		},
	}
}

func (rw *Rewriter) Do(stmt Statement) (Statement, bool, error) {
	rw.modified = false
	node, err := Walk(rw, stmt)
	if err != nil {
		return nil, false, err
	}
	return node.(Statement), rw.modified, nil
}

func (rw *Rewriter) Visit(node Node) (w Visitor, n Node, err error) {
	retNode := node

	switch n := retNode.(type) {
	case *Call:
		if strings.EqualFold(n.Name.Name, "time") && len(n.Args) > 0 {
			arg, ok := n.Args[0].(*StringLit)
			if ok && strings.EqualFold(arg.Value, "now") {
				// Replace 'now' with the current time.
				currentTime := time.Now().Format("15:04:05") // HH:MM:SS
				arg.Value = currentTime
			}
			rw.modified = true
		} else if strings.EqualFold(n.Name.Name, "random") && rw.RewriteRand {
			retNode = &NumberLit{Value: strconv.Itoa(int(rw.randFn()))}
			rw.modified = true
		}
	}
	return rw, retNode, nil
}

func (rw *Rewriter) VisitEnd(node Node) (Node, error) {
	return node, nil
}
