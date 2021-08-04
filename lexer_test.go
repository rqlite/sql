package sqlparser_test

import (
	"strings"
	"testing"

	"github.com/longbridgeapp/sqlparser"
)

func TestLexer_Lex(t *testing.T) {
	t.Run("IDENT", func(t *testing.T) {
		t.Run("Unquoted", func(t *testing.T) {
			AssertLex(t, `foo_BAR123`, sqlparser.IDENT, `foo_BAR123`)
		})
		t.Run("Quoted", func(t *testing.T) {
			AssertLex(t, `"crazy ~!#*&# column name"" foo"`, sqlparser.QIDENT, `crazy ~!#*&# column name" foo`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertLex(t, `"unfinished`, sqlparser.ILLEGAL, `"unfinished`)
		})
		t.Run("x", func(t *testing.T) {
			AssertLex(t, `x`, sqlparser.IDENT, `x`)
		})
		t.Run("StartingX", func(t *testing.T) {
			AssertLex(t, `xyz`, sqlparser.IDENT, `xyz`)
		})
	})

	t.Run("KEYWORD", func(t *testing.T) {
		AssertLex(t, `BEGIN`, sqlparser.BEGIN, `BEGIN`)
	})

	t.Run("STRING", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			AssertLex(t, `'this is ''a'' string'`, sqlparser.STRING, `this is 'a' string`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertLex(t, `'unfinished`, sqlparser.ILLEGAL, `'unfinished`)
		})
	})

	t.Run("MLCOMMENT", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			AssertLex(t, `/* this is a multiline comment */`, sqlparser.MLCOMMENT, `this is a multiline comment`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertLex(t, `/* unfinished`, sqlparser.ILLEGAL, `/* unfinished`)
		})
	})

	t.Run("BLOB", func(t *testing.T) {
		t.Run("LowerX", func(t *testing.T) {
			AssertLex(t, `x'0123456789abcdef'`, sqlparser.BLOB, `0123456789abcdef`)
		})
		t.Run("UpperX", func(t *testing.T) {
			AssertLex(t, `X'0123456789ABCDEF'`, sqlparser.BLOB, `0123456789ABCDEF`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertLex(t, `x'0123`, sqlparser.ILLEGAL, `x'0123`)
		})
		t.Run("BadHex", func(t *testing.T) {
			AssertLex(t, `x'hello`, sqlparser.ILLEGAL, `x'h`)
		})
	})

	t.Run("INTEGER", func(t *testing.T) {
		AssertLex(t, `123`, sqlparser.INTEGER, `123`)
	})

	t.Run("FLOAT", func(t *testing.T) {
		AssertLex(t, `123.456`, sqlparser.FLOAT, `123.456`)
		AssertLex(t, `.1`, sqlparser.FLOAT, `.1`)
		AssertLex(t, `123e456`, sqlparser.FLOAT, `123e456`)
		AssertLex(t, `123E456`, sqlparser.FLOAT, `123E456`)
		AssertLex(t, `123.456E78`, sqlparser.FLOAT, `123.456E78`)
		AssertLex(t, `123.E45`, sqlparser.FLOAT, `123.E45`)
		AssertLex(t, `123E+4`, sqlparser.FLOAT, `123E+4`)
		AssertLex(t, `123E-4`, sqlparser.FLOAT, `123E-4`)
		AssertLex(t, `123E`, sqlparser.ILLEGAL, `123E`)
		AssertLex(t, `123E+`, sqlparser.ILLEGAL, `123E+`)
		AssertLex(t, `123E-`, sqlparser.ILLEGAL, `123E-`)
	})
	t.Run("BIND", func(t *testing.T) {
		AssertLex(t, `?'`, sqlparser.BIND, `?`)
		AssertLex(t, `?123'`, sqlparser.BIND, `?123`)
		AssertLex(t, `:foo_bar123'`, sqlparser.BIND, `:foo_bar123`)
		AssertLex(t, `@bar'`, sqlparser.BIND, `@bar`)
		AssertLex(t, `$baz'`, sqlparser.BIND, `$baz`)
	})

	t.Run("EOF", func(t *testing.T) {
		AssertLex(t, " \n\t\r", sqlparser.EOF, ``)
	})

	t.Run("SEMI", func(t *testing.T) {
		AssertLex(t, ";", sqlparser.SEMI, ";")
	})
	t.Run("LP", func(t *testing.T) {
		AssertLex(t, "(", sqlparser.LP, "(")
	})
	t.Run("RP", func(t *testing.T) {
		AssertLex(t, ")", sqlparser.RP, ")")
	})
	t.Run("COMMA", func(t *testing.T) {
		AssertLex(t, ",", sqlparser.COMMA, ",")
	})
	t.Run("NE", func(t *testing.T) {
		AssertLex(t, "!=", sqlparser.NE, "!=")
	})
	t.Run("BITNOT", func(t *testing.T) {
		AssertLex(t, "!", sqlparser.BITNOT, "!")
	})
	t.Run("EQ", func(t *testing.T) {
		AssertLex(t, "=", sqlparser.EQ, "=")
	})
	t.Run("LE", func(t *testing.T) {
		AssertLex(t, "<=", sqlparser.LE, "<=")
	})
	t.Run("LSHIFT", func(t *testing.T) {
		AssertLex(t, "<<", sqlparser.LSHIFT, "<<")
	})
	t.Run("LG", func(t *testing.T) {
		AssertLex(t, "<>", sqlparser.LG, "<>")
	})
	t.Run("LT", func(t *testing.T) {
		AssertLex(t, "<", sqlparser.LT, "<")
	})
	t.Run("GE", func(t *testing.T) {
		AssertLex(t, ">=", sqlparser.GE, ">=")
	})
	t.Run("RSHIFT", func(t *testing.T) {
		AssertLex(t, ">>", sqlparser.RSHIFT, ">>")
	})
	t.Run("GT", func(t *testing.T) {
		AssertLex(t, ">", sqlparser.GT, ">")
	})
	t.Run("BITAND", func(t *testing.T) {
		AssertLex(t, "&", sqlparser.BITAND, "&")
	})
	t.Run("CONCAT", func(t *testing.T) {
		AssertLex(t, "||", sqlparser.CONCAT, "||")
	})
	t.Run("BITOR", func(t *testing.T) {
		AssertLex(t, "|", sqlparser.BITOR, "|")
	})
	t.Run("PLUS", func(t *testing.T) {
		AssertLex(t, "+", sqlparser.PLUS, "+")
	})
	t.Run("MINUS", func(t *testing.T) {
		AssertLex(t, "-", sqlparser.MINUS, "-")
	})
	t.Run("STAR", func(t *testing.T) {
		AssertLex(t, "*", sqlparser.STAR, "*")
	})
	t.Run("SLASH", func(t *testing.T) {
		AssertLex(t, "/", sqlparser.SLASH, "/")
	})
	t.Run("REM", func(t *testing.T) {
		AssertLex(t, "%", sqlparser.REM, "%")
	})
	t.Run("DOT", func(t *testing.T) {
		AssertLex(t, ".", sqlparser.DOT, ".")
	})
	t.Run("ILLEGAL", func(t *testing.T) {
		AssertLex(t, "^", sqlparser.ILLEGAL, "^")
	})
}

// AssertLex asserts the value of the first lex to s.
func AssertLex(tb testing.TB, s string, expectedTok sqlparser.Token, expectedLit string) {
	tb.Helper()
	_, tok, lit := sqlparser.NewLexer(strings.NewReader(s)).Lex()
	if tok != expectedTok || lit != expectedLit {
		tb.Fatalf("Lex(%q)=<%s,%s>, want <%s,%s>", s, tok, lit, expectedTok, expectedLit)
	}
}
