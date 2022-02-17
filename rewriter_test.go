package sql_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/rqlite/sql"
)

func Test_Rewriter(t *testing.T) {
	for i, tt := range []struct {
		in          string
		exp         string
		rewriteRand bool
	}{
		{
			in:          `INSERT INTO foo(col1) VALUES (random())`,
			exp:         `INSERT INTO "foo" \("col1"\) VALUES \(random\(\)\)`,
			rewriteRand: false,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (random())`,
			exp:         `INSERT INTO \"foo\" \("col1"\) VALUES \(-?[0-9]+\)`,
			rewriteRand: true,
		},
		{
			in:          `SELECT * FROM foo ORDER BY random()`,
			exp:         `SELECT \* FROM "foo" ORDER BY random\(\)`,
			rewriteRand: true,
		},
		{
			in:          `SELECT random()`,
			exp:         `SELECT -?[0-9]+`,
			rewriteRand: true,
		},
	} {
		rw := &sql.Rewriter{
			RewriteRand: tt.rewriteRand,
		}

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}

		s, err = rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}

		match := regexp.MustCompile(tt.exp)
		if !match.MatchString(s.String()) {
			t.Fatalf("test %d failed, exp: %s, got: %s", i, tt.exp, s.String())
		}
	}
}
