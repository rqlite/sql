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
		randFound   bool
	}{
		{
			in:          `INSERT INTO foo(col1) VALUES (random())`,
			exp:         `INSERT INTO "foo" \("col1"\) VALUES \(random\(\)\)`,
			rewriteRand: false,
			randFound:   false,
		},
		{
			in:          `INSERT INTO foo(col1) VALUES (random())`,
			exp:         `INSERT INTO \"foo\" \("col1"\) VALUES \(-?[0-9]+\)`,
			rewriteRand: true,
			randFound:   true,
		},
		{
			in:          `INSERT INTO "names" VALUES (RANDOM(), 'bob', '123-45-678')`,
			exp:         `INSERT INTO "names" VALUES \(-?[0-9]+, 'bob', '123-45-678'\)`,
			rewriteRand: true,
			randFound:   true,
		},
		{
			in:          `SELECT * FROM foo ORDER BY random()`,
			exp:         `SELECT \* FROM "foo" ORDER BY random\(\)`,
			rewriteRand: true,
			randFound:   false,
		},
		{
			in:          `SELECT random()`,
			exp:         `SELECT -?[0-9]+`,
			rewriteRand: true,
			randFound:   true,
		},
		{
			in:          `SELECT abs(random())`,
			exp:         `SELECT abs\(random\(\)\)`,
			rewriteRand: false,
			randFound:   false,
		},
		{
			in:          `SELECT abs(random())`,
			exp:         `SELECT abs\(-?[0-9]+\)`,
			rewriteRand: true,
			randFound:   true,
		},
		{
			in:          `SELECT random() + 1`,
			exp:         `SELECT -?[0-9]+ \+ 1`,
			rewriteRand: true,
			randFound:   true,
		},
		{
			in:          `SELECT "v" FROM "foo"`,
			exp:         `SELECT "v" FROM "foo"`,
			rewriteRand: true,
			randFound:   false,
		},
	} {
		rw := &sql.Rewriter{
			RewriteRand: tt.rewriteRand,
		}

		s, err := sql.NewParser(strings.NewReader(tt.in)).ParseStatement()
		if err != nil {
			t.Fatal(err)
		}

		s, f, err := rw.Do(s)
		if err != nil {
			t.Fatal(err)
		}

		match := regexp.MustCompile(tt.exp)
		rendered := s.String()
		if !match.MatchString(rendered) {
			t.Fatalf("test %d failed, exp: '%s', got: '%s'", i, tt.exp, rendered)
		}

		if tt.randFound != f {
			t.Fatalf("test %d randFound wrong, exp %t, got %t", i, tt.randFound, f)
		}
	}
}
