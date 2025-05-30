package sql

import (
	"strings"
	"testing"
)

func TestUpdate(t *testing.T) {
	s := `UPDATE asynq_tasks
SET state='active',
    pending_since=NULL,
    affinity_timeout=server_affinity,
    deadline=iif(task_deadline=0, task_timeout+1687276020, task_deadline)    
WHERE asynq_tasks.state='pending'
  AND (task_uuid,
       ndx,
       pndx,
       task_msg,
       task_timeout,
       task_deadline)=
    (SELECT task_uuid,
            ndx,
            pndx,
            task_msg,
            task_timeout,
            task_deadline
     FROM asynq_tasks)
`
	stmt, err := NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		t.Fatalf("failed: %v", err)
	}
	_, ok := stmt.(*UpdateStatement)
	if !ok {
		t.Fatalf("failed: expected UpdateStatement")
	}
}
