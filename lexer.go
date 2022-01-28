package sqlparser

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"unicode"
)

type Lexer struct {
	r   io.RuneReader
	buf bytes.Buffer

	ch   rune
	pos  Pos
	full bool
}

func NewLexer(r io.Reader) *Lexer {
	return &Lexer{
		r:   bufio.NewReader(r),
		pos: Pos{Offset: -1, Line: 1},
	}
}

func (l *Lexer) Lex() (pos Pos, token Token, lit string) {
	for {
		if ch := l.peek(); ch == -1 {
			return l.pos, EOF, ""
		} else if unicode.IsSpace(ch) {
			l.read()
			continue
		} else if isDigit(ch) || ch == '.' {
			return l.lexNumber()
		} else if ch == 'x' || ch == 'X' {
			return l.lexBlob()
		} else if isAlpha(ch) || ch == '_' {
			return l.lexUnquotedIdent(l.pos, "")
		} else if ch == '"' || ch == '`' {
			return l.lexQuotedIdent(ch)
		} else if ch == '\'' {
			return l.lexString()
		} else if ch == '?' || ch == ':' || ch == '@' || ch == '$' {
			return l.lexBind()
		}

		switch ch, pos := l.read(); ch {
		case ';':
			return pos, SEMI, ";"
		case '(':
			return pos, LP, "("
		case ')':
			return pos, RP, ")"
		case ',':
			return pos, COMMA, ","
		case '!':
			if l.peek() == '=' {
				l.read()
				return pos, NE, "!="
			}
			return pos, BITNOT, "!"
		case '=':
			return pos, EQ, "="
		case '<':
			if l.peek() == '=' {
				l.read()
				return pos, LE, "<="
			} else if l.peek() == '<' {
				l.read()
				return pos, LSHIFT, "<<"
			} else if l.peek() == '>' {
				l.read()
				return pos, LG, "<>"
			}
			return pos, LT, "<"
		case '>':
			if l.peek() == '=' {
				l.read()
				return pos, GE, ">="
			} else if l.peek() == '>' {
				l.read()
				return pos, RSHIFT, ">>"
			}
			return pos, GT, ">"
		case '&':
			return pos, BITAND, "&"
		case '|':
			if l.peek() == '|' {
				l.read()
				return pos, CONCAT, "||"
			}
			return pos, BITOR, "|"
		case '+':
			return pos, PLUS, "+"
		case '-':
			return pos, MINUS, "-"
		case '*':
			return pos, STAR, "*"
		case '/':
			if l.peek() == '*' {
				return l.lexMultilineComment()
			}
			return pos, SLASH, "/"
		case '%':
			return pos, REM, "%"
		default:
			return pos, ILLEGAL, string(ch)
		}
	}
}

func (l *Lexer) lexUnquotedIdent(pos Pos, prefix string) (Pos, Token, string) {
	assert(isUnquotedIdent(l.peek()))

	l.buf.Reset()
	l.buf.WriteString(prefix)
	for ch, _ := l.read(); isUnquotedIdent(ch); ch, _ = l.read() {
		l.buf.WriteRune(ch)
	}
	l.unread()

	lit := l.buf.String()
	tok := Lookup(lit)
	return pos, tok, lit
}

func (l *Lexer) lexQuotedIdent(char rune) (Pos, Token, string) {
	ch, pos := l.read()
	assert(ch == char)

	l.buf.Reset()
	l.buf.WriteRune(char)
	for {
		ch, _ := l.read()
		if ch == -1 {
			return pos, ILLEGAL, l.buf.String()
		} else if ch == char {
			if l.peek() == char { // escaped quote
				l.read()
				l.buf.WriteRune(char)
				continue
			}
			l.buf.WriteRune(char)
			return pos, QIDENT, l.buf.String()
		}
		l.buf.WriteRune(ch)
	}
}

func (l *Lexer) lexString() (Pos, Token, string) {
	ch, pos := l.read()
	assert(ch == '\'')

	l.buf.Reset()
	for {
		ch, _ := l.read()
		if ch == -1 {
			return pos, ILLEGAL, `'` + l.buf.String()
		} else if ch == '\'' {
			if l.peek() == '\'' { // escaped quote
				l.read()
				l.buf.WriteRune('\'')
				continue
			}
			return pos, STRING, l.buf.String()
		}
		l.buf.WriteRune(ch)
	}
}

func (l *Lexer) lexMultilineComment() (Pos, Token, string) {
	ch, pos := l.read()
	assert(ch == '*')

	l.buf.Reset()
	for {
		ch, _ := l.read()
		if ch == -1 {
			return pos, ILLEGAL, `/*` + l.buf.String()
		} else if ch == '*' {
			if l.peek() == '/' {
				l.read()
				l.read()
				return pos, MLCOMMENT, strings.Trim(l.buf.String(), " ")
			}
		}
		l.buf.WriteRune(ch)
	}
}

func (l *Lexer) lexBind() (Pos, Token, string) {
	start, pos := l.read()

	l.buf.Reset()
	l.buf.WriteRune(start)

	// Question mark starts a numeric bind.
	if start == '?' {
		for isDigit(l.peek()) {
			ch, _ := l.read()
			l.buf.WriteRune(ch)
		}
		return pos, BIND, l.buf.String()
	}

	// All other characters start an alphanumeric bind.
	assert(start == ':' || start == '@' || start == '$')
	for isUnquotedIdent(l.peek()) {
		ch, _ := l.read()
		l.buf.WriteRune(ch)
	}
	return pos, BIND, l.buf.String()
}

func (l *Lexer) lexBlob() (Pos, Token, string) {
	start, pos := l.read()
	assert(start == 'x' || start == 'X')

	// If the next character is not a quote, it's an IDENT.
	if isUnquotedIdent(l.peek()) {
		return l.lexUnquotedIdent(pos, string(start))
	} else if l.peek() != '\'' {
		return pos, IDENT, string(start)
	}
	ch, _ := l.read()
	assert(ch == '\'')

	l.buf.Reset()
	for i := 0; ; i++ {
		ch, _ := l.read()
		if ch == '\'' {
			return pos, BLOB, l.buf.String()
		} else if ch == -1 {
			return pos, ILLEGAL, string(start) + `'` + l.buf.String()
		} else if !isHex(ch) {
			return pos, ILLEGAL, string(start) + `'` + l.buf.String() + string(ch)
		}
		l.buf.WriteRune(ch)
	}
}

func (l *Lexer) lexNumber() (Pos, Token, string) {
	assert(isDigit(l.peek()) || l.peek() == '.')
	pos := l.pos
	tok := INTEGER

	l.buf.Reset()

	// Read whole number if starting with a digit.
	if isDigit(l.peek()) {
		for isDigit(l.peek()) {
			ch, _ := l.read()
			l.buf.WriteRune(ch)
		}
	}

	// Read decimal and successive digitl.
	if l.peek() == '.' {
		tok = FLOAT

		ch, _ := l.read()
		l.buf.WriteRune(ch)

		for isDigit(l.peek()) {
			ch, _ := l.read()
			l.buf.WriteRune(ch)
		}
	}

	// Read exponent with optional +/- sign.
	if ch := l.peek(); ch == 'e' || ch == 'E' {
		tok = FLOAT

		ch, _ := l.read()
		l.buf.WriteRune(ch)

		if l.peek() == '+' || l.peek() == '-' {
			ch, _ := l.read()
			l.buf.WriteRune(ch)
			if !isDigit(l.peek()) {
				return pos, ILLEGAL, l.buf.String()
			}
			for isDigit(l.peek()) {
				ch, _ := l.read()
				l.buf.WriteRune(ch)
			}
		} else if isDigit(l.peek()) {
			for isDigit(l.peek()) {
				ch, _ := l.read()
				l.buf.WriteRune(ch)
			}
		} else {
			return pos, ILLEGAL, l.buf.String()
		}
	}

	lit := l.buf.String()
	if lit == "." {
		return pos, DOT, lit
	}
	return pos, tok, lit
}

func (l *Lexer) read() (rune, Pos) {
	if l.full {
		l.full = false
		return l.ch, l.pos
	}

	var err error
	l.ch, _, err = l.r.ReadRune()
	if err != nil {
		l.ch = -1
		return l.ch, l.pos
	}

	l.pos.Offset++
	if l.ch == '\n' {
		l.pos.Line++
		l.pos.Column = 0
	} else {
		l.pos.Column++
	}
	return l.ch, l.pos
}

func (l *Lexer) peek() rune {
	if !l.full {
		l.read()
		l.unread()
	}
	return l.ch
}

func (l *Lexer) unread() {
	assert(!l.full)
	l.full = true
}

func isDigit(ch rune) bool {
	return ch >= '0' && ch <= '9'
}

func isAlpha(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
}

func isHex(ch rune) bool {
	return isDigit(ch) || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')
}

func isUnquotedIdent(ch rune) bool {
	return isAlpha(ch) || isDigit(ch) || ch == '_'
}

// IsInteger returns true if s only contains digits.
func IsInteger(s string) bool {
	for _, ch := range s {
		if !isDigit(ch) {
			return false
		}
	}
	return s != ""
}
