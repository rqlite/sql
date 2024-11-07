package sql

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Rewriter struct {
	RewriteRand bool
	RewriteTime bool

	randFn func() int64
	nowFn  func() time.Time

	modified bool
}

func NewRewriter() *Rewriter {
	return &Rewriter{
		RewriteRand: true,
		RewriteTime: true,

		randFn: func() int64 {
			return rand.Int63()
		},
		nowFn: time.Now,
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
		if rw.RewriteTime && len(n.Args) > 0 &&
			(strings.EqualFold(n.Name.Name, "date") ||
				strings.EqualFold(n.Name.Name, "time") ||
				strings.EqualFold(n.Name.Name, "datetime") ||
				strings.EqualFold(n.Name.Name, "julianday") ||
				strings.EqualFold(n.Name.Name, "unixepoch")) {
			arg, ok := n.Args[0].(*StringLit)
			if ok && strings.EqualFold(arg.Value, "now") {
				n.Args[0] = julianDayAsNumberLit(rw.nowFn())
			}
			rw.modified = true
		} else if rw.RewriteTime && len(n.Args) > 1 &&
			strings.EqualFold(n.Name.Name, "strftime") {
			arg, ok := n.Args[1].(*StringLit)
			if ok && strings.EqualFold(arg.Value, "now") {
				n.Args[1] = julianDayAsNumberLit(rw.nowFn())
			}
			rw.modified = true
		} else if rw.RewriteTime && len(n.Args) > 1 &&
			strings.EqualFold(n.Name.Name, "timediff") {
			jd := julianDayAsNumberLit(rw.nowFn())

			arg, ok := n.Args[0].(*StringLit)
			if ok && strings.EqualFold(arg.Value, "now") {
				n.Args[0] = jd
			}
			arg, ok = n.Args[1].(*StringLit)
			if ok && strings.EqualFold(arg.Value, "now") {
				n.Args[1] = jd
			}
		} else if rw.RewriteRand && strings.EqualFold(n.Name.Name, "random") {
			retNode = &NumberLit{Value: strconv.Itoa(int(rw.randFn()))}
			rw.modified = true
		}
	}
	return rw, retNode, nil
}

func (rw *Rewriter) VisitEnd(node Node) (Node, error) {
	return node, nil
}

func julianDayAsNumberLit(t time.Time) *NumberLit {
	return &NumberLit{Value: fmt.Sprintf("%f", julianDay(t))}
}

func julianDay(t time.Time) float64 {
	year := t.Year()
	month := int(t.Month())
	day := t.Day()
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	nanosecond := t.Nanosecond()

	// Adjust for months January and February
	if month <= 2 {
		year--
		month += 12
	}

	// Calculate the Julian Day Number
	A := year / 100
	B := 2 - A + A/4

	// Convert time to fractional day
	fractionalDay := (float64(hour) +
		float64(minute)/60 +
		(float64(second)+float64(nanosecond)/1e9)/3600) / 24.0

	// Use math.Floor to correctly handle the integer parts
	jd := math.Floor(365.25*float64(year+4716)) +
		math.Floor(30.6001*float64(month+1)) +
		float64(day) + float64(B) - 1524.5 + fractionalDay

	return jd
}
