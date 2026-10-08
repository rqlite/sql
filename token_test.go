package sql_test

import (
	"testing"

	"github.com/rqlite/sql"
)

func TestPos_String(t *testing.T) {
	if got, want := (sql.Pos{}).String(), `-`; got != want {
		t.Fatalf("String()=%q, want %q", got, want)
	}
}

// Ensure only real SQLite keywords are recognised as keywords. Internal
// tokens used by the parser must not prevent their names being identifiers.
func TestLookup_InternalTokens(t *testing.T) {
	for _, name := range []string{
		"AGG_COLUMN", "AGG_FUNCTION", "ASTERISK", "COLUMNKW", "CTIME_KW",
		"FUNCTION", "IF_NULL_ROW", "ISNOT", "NOTBETWEEN", "NOTEXISTS",
		"NOTGLOB", "NOTIN", "NOTLIKE", "NOTMATCH", "NOTREGEXP", "REGISTER",
		"SELECT_COLUMN", "SPAN", "TRUTH", "VARIABLE", "VECTOR",
	} {
		if got := sql.Lookup(name); got != sql.IDENT {
			t.Errorf("Lookup(%q)=%s, want IDENT", name, got)
		}
	}
	for _, name := range []string{"SELECT", "select", "ISNULL", "NOTNULL", "FROM"} {
		if got := sql.Lookup(name); got == sql.IDENT {
			t.Errorf("Lookup(%q)=IDENT, want keyword", name)
		}
	}
}
