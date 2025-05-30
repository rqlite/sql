package sql_test

import (
	"strings"
	"testing"

	sql "github.com/rqlite/sql"
)

// TestCreateTableWithMainSchema tests the CREATE TABLE statement with main schema prefix.
func TestCreateTableWithMainSchema(t *testing.T) {
	// Test with schema.table syntax
	p := sql.NewParser(strings.NewReader("CREATE TABLE main.T1 (C1 TEXT PRIMARY KEY, C2 INTEGER)"))
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	createStmt, ok := stmt.(*sql.CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.Schema == nil {
		t.Fatal("expected Schema to be non-nil")
	}

	if createStmt.Schema.Name != "main" {
		t.Fatalf("expected Schema name to be 'main', got '%s'", createStmt.Schema.Name)
	}

	if createStmt.Name.Name != "T1" {
		t.Fatalf("expected table name to be 'T1', got '%s'", createStmt.Name.Name)
	}

	// Test backward compatibility with regular table names
	p = sql.NewParser(strings.NewReader("CREATE TABLE T2 (C1 TEXT PRIMARY KEY, C2 INTEGER)"))
	stmt, err = p.ParseStatement()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	createStmt, ok = stmt.(*sql.CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.Schema != nil {
		t.Fatal("expected Schema to be nil for tables without schema")
	}

	if createStmt.Name.Name != "T2" {
		t.Fatalf("expected table name to be 'T2', got '%s'", createStmt.Name.Name)
	}
}
