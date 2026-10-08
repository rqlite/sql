package sql_test

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/rqlite/sql"
)

// Ensure Clone() returns a deep copy with the same structure.
func TestFrameSpec_Clone(t *testing.T) {
	s := &sql.FrameSpec{X: &sql.NumberLit{Value: "1"}, Y: &sql.NumberLit{Value: "2"}}
	c := s.Clone()
	if diff := deep.Equal(s, c); diff != nil {
		t.Fatal(diff)
	} else if c.X == s.X || c.Y == s.Y {
		t.Fatal("expected deep copy of X and Y")
	}
}
