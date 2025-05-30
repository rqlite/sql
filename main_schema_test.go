package sql_test

import (
	"strings"
	"testing"

	sql "github.com/rqlite/sql"
)

// TestCreateTableWithMainSchema tests the CREATE TABLE statement with main schema prefix.
func TestCreateTableWithMainSchema(t *testing.T) {
	p := sql.NewParser(strings.NewReader("CREATE TABLE main.T1 (C1 TEXT PRIMARY KEY, C2 INTEGER)"))
	_, err := p.ParseStatement()
	if err == nil {
		t.Fatal("expected error but got none")
	}
	t.Logf("error: %v", err)
}