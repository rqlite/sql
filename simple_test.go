package sql

import (
	"strings"
	"testing"
)

func TestSimpleParenList(t *testing.T) {
	s := `UPDATE table1 SET col1 = 'value' WHERE (col1, col2) = ('a', 'b')`
	stmt, err := NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	_, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("failed: expected UpdateStatement")
	}
}
