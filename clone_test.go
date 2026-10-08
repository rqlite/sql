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

func TestCreateVirtualTableStatement_Clone(t *testing.T) {
	s := &sql.CreateVirtualTableStatement{
		Schema:     &sql.Ident{Name: "main"},
		Name:       &sql.Ident{Name: "tbl"},
		ModuleName: &sql.Ident{Name: "fts5"},
	}
	c := s.Clone()
	if diff := deep.Equal(s, c); diff != nil {
		t.Fatal(diff)
	} else if c.Schema == s.Schema || c.Name == s.Name {
		t.Fatal("expected deep copy of Schema and Name")
	}
}

func TestReturningClause_Clone(t *testing.T) {
	// The clause itself must be deep copied.
	rc := &sql.ReturningClause{Columns: []*sql.ResultColumn{{Expr: &sql.Ident{Name: "x"}}}}
	c := rc.Clone()
	if diff := deep.Equal(rc, c); diff != nil {
		t.Fatal(diff)
	} else if c.Columns[0] == rc.Columns[0] {
		t.Fatal("expected deep copy of Columns")
	}

	// Statements carrying a RETURNING clause must deep copy it too.
	u := &sql.UpdateStatement{
		Table:           &sql.QualifiedTableName{Name: &sql.Ident{Name: "tbl"}},
		Assignments:     []*sql.Assignment{{Columns: []*sql.Ident{{Name: "x"}}, Expr: &sql.NumberLit{Value: "1"}}},
		ReturningClause: rc,
	}
	if uc := u.Clone(); uc.ReturningClause == u.ReturningClause {
		t.Fatal("UpdateStatement.Clone() shares ReturningClause")
	}
	d := &sql.DeleteStatement{
		Table:           &sql.QualifiedTableName{Name: &sql.Ident{Name: "tbl"}},
		ReturningClause: rc,
	}
	if dc := d.Clone(); dc.ReturningClause == d.ReturningClause {
		t.Fatal("DeleteStatement.Clone() shares ReturningClause")
	}
}
