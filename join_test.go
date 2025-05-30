package sql_test

import (
	"strings"
	"testing"

	"github.com/rqlite/sql"
)

func TestMultipleJoinReconstruction(t *testing.T) {
	// Test for the issue where multiple JOINs with ON clauses are not reconstructed correctly
	query := `SELECT * FROM worker w JOIN event_worker ew ON ew.worker_id = w."id" JOIN sys_event se ON se."id" = ew."event"`
	
	parser := sql.NewParser(strings.NewReader(query))
	stmt, err := parser.ParseStatement()
	if err != nil {
		t.Fatalf("failed to parse SQL: %s", err)
	}
	
	// Reconstruct the query and verify
	reconstructed := stmt.String()
	
	// With the bug, the reconstructed query looks like:
	// SELECT * FROM "worker" AS "w" JOIN "event_worker" AS "ew" JOIN "sys_event" AS "se" ON "se"."id" = "ew"."event" ON "ew"."worker_id" = "w"."id"
	
	// Expected format after fix:
	expected := `SELECT * FROM "worker" AS "w" JOIN "event_worker" AS "ew" ON "ew"."worker_id" = "w"."id" JOIN "sys_event" AS "se" ON "se"."id" = "ew"."event"`
	
	if reconstructed != expected {
		t.Errorf("Incorrect join reconstruction:\nGot:      %s\nExpected: %s", reconstructed, expected)
	}
}

func TestComplexMultipleJoinReconstruction(t *testing.T) {
	// Test with three JOINs to ensure our solution is robust
	query := `SELECT * FROM table1 t1 
                 JOIN table2 t2 ON t2.id = t1.id 
                 JOIN table3 t3 ON t3.id = t2.id 
                 JOIN table4 t4 ON t4.id = t3.id`
	
	parser := sql.NewParser(strings.NewReader(query))
	stmt, err := parser.ParseStatement()
	if err != nil {
		t.Fatalf("failed to parse SQL: %s", err)
	}
	
	// Reconstruct the query and verify
	reconstructed := stmt.String()
	
	// Expected format:
	expected := `SELECT * FROM "table1" AS "t1" JOIN "table2" AS "t2" ON "t2"."id" = "t1"."id" JOIN "table3" AS "t3" ON "t3"."id" = "t2"."id" JOIN "table4" AS "t4" ON "t4"."id" = "t3"."id"`
	
	if reconstructed != expected {
		t.Errorf("Incorrect complex join reconstruction:\nGot:      %s\nExpected: %s", reconstructed, expected)
	}
}

func TestMixedJoinTypesReconstruction(t *testing.T) {
	// Test with different JOIN types
	query := `SELECT * FROM table1 t1 
                 LEFT JOIN table2 t2 ON t2.id = t1.id 
                 INNER JOIN table3 t3 ON t3.id = t2.id 
                 CROSS JOIN table4 t4`
	
	parser := sql.NewParser(strings.NewReader(query))
	stmt, err := parser.ParseStatement()
	if err != nil {
		t.Fatalf("failed to parse SQL: %s", err)
	}
	
	// Reconstruct the query and verify
	reconstructed := stmt.String()
	
	// Expected format:
	expected := `SELECT * FROM "table1" AS "t1" LEFT JOIN "table2" AS "t2" ON "t2"."id" = "t1"."id" INNER JOIN "table3" AS "t3" ON "t3"."id" = "t2"."id" CROSS JOIN "table4" AS "t4"`
	
	if reconstructed != expected {
		t.Errorf("Incorrect mixed join types reconstruction:\nGot:      %s\nExpected: %s", reconstructed, expected)
	}
}