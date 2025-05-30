package sql

import (
	"bufio"
	"bytes"
	"io"
	"unicode"
)

type Scanner struct {
	r   io.RuneReader
	buf bytes.Buffer

	ch   rune
	pos  Pos
	full bool
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r:   bufio.NewReader(r),
		pos: Pos{Offset: -1, Line: 1},
	}
}

func (s *Scanner) Scan() (pos Pos, token Token, lit string) {
	for {
		if ch := s.peek(); ch == -1 {
			return s.pos, EOF, ""
		} else if unicode.IsSpace(ch) {
			s.read()
			continue
		} else if isDigit(ch) || ch == '.' {
			return s.scanNumber()
		} else if ch == 'x' || ch == 'X' {
			return s.scanBlob()
		} else if isAlpha(ch) || ch == '_' {
			return s.scanUnquotedIdent(s.pos, "")
		} else if ch == '"' {
			return s.scanQuotedIdent()
		} else if ch == '\'' {
			return s.scanString()
		} else if ch == '?' || ch == ':' || ch == '@' || ch == '$' {
			return s.scanBind()
		}

		switch ch, pos := s.read(); ch {
		case ';':
			return pos, SEMI, ";"
		case '(':
			return pos, LP, "("
		case ')':
			return pos, RP, ")"
		case ',':
			return pos, COMMA, ","
		case '!':
			if s.peek() == '=' {
				s.read()
				return pos, NE, "!="
			}
			return pos, BITNOT, "!"
		case '=':
			if s.peek() == '=' {
				s.read()
				return pos, EQ, "=="
			}
			return pos, EQ, "="
		case '<':
			if s.peek() == '=' {
				s.read()
				return pos, LE, "<="
			} else if s.peek() == '<' {
				s.read()
				return pos, LSHIFT, "<<"
			} else if s.peek() == '>' {
				s.read()
				return pos, NE, "<>"
			}
			return pos, LT, "<"
		case '>':
			if s.peek() == '=' {
				s.read()
				return pos, GE, ">="
			} else if s.peek() == '>' {
				s.read()
				return pos, RSHIFT, ">>"
			}
			return pos, GT, ">"
		case '&':
			return pos, BITAND, "&"
		case '|':
			if s.peek() == '|' {
				s.read()
				return pos, CONCAT, "||"
			}
			return pos, BITOR, "|"
		case '+':
			return pos, PLUS, "+"
		case '-':
			if s.peek() == '>' {
				s.read()
				if s.peek() == '>' {
					s.read()
					return pos, JSON_EXTRACT_SQL, "->>"
				}
				return pos, JSON_EXTRACT_JSON, "->"
			} else if s.peek() == '-' {
				s.read()
				return pos, COMMENT, s.scanSingleLineComment()
			}
			return pos, MINUS, "-"
		case '*':
			return pos, STAR, "*"
		case '/':
			if s.peek() == '*' {
				s.read()
				return pos, COMMENT, s.scanMultiLineComment()
			}
			return pos, SLASH, "/"
		case '%':
			return pos, REM, "%"
		default:
			return pos, ILLEGAL, string(ch)
		}
	}
}

func (s *Scanner) scanUnquotedIdent(pos Pos, prefix string) (Pos, Token, string) {
	assert(isUnquotedIdent(s.peek()))

	s.buf.Reset()
	s.buf.WriteString(prefix)
	for ch, _ := s.read(); isUnquotedIdent(ch); ch, _ = s.read() {
		s.buf.WriteRune(ch)
	}
	s.unread()

	lit := s.buf.String()
	tok := Lookup(lit)
	return pos, tok, lit
}

func (s *Scanner) scanQuotedIdent() (Pos, Token, string) {
	ch, pos := s.read()
	assert(ch == '"')

	s.buf.Reset()
	for {
		ch, _ := s.read()
		if ch == -1 {
			return pos, ILLEGAL, `"` + s.buf.String()
		} else if ch == '"' {
			if s.peek() == '"' { // escaped quote
				s.read()
				s.buf.WriteRune('"')
				continue
			}
			return pos, QIDENT, s.buf.String()
		}
		s.buf.WriteRune(ch)
	}
}

func (s *Scanner) scanString() (Pos, Token, string) {
	ch, pos := s.read()
	assert(ch == '\'')

	s.buf.Reset()
	for {
		ch, _ := s.read()
		if ch == -1 {
			return pos, ILLEGAL, `'` + s.buf.String()
		} else if ch == '\'' {
			if s.peek() == '\'' { // escaped quote
				s.read()
				s.buf.WriteRune('\'')
				continue
			}
			return pos, STRING, s.buf.String()
		}
		s.buf.WriteRune(ch)
	}
}

func (s *Scanner) scanSingleLineComment() string {
	s.buf.Reset()
	s.buf.WriteString("--")

	for {
		ch, _ := s.read()
		switch ch {
		case -1, '\n':
			return s.buf.String()
		default:
			s.buf.WriteRune(ch)
		}
	}
}

func (s *Scanner) scanMultiLineComment() string {
	s.buf.Reset()
	s.buf.WriteString("/*")
	for {
		ch, _ := s.read()
		if ch == -1 {
			return s.buf.String()
		} else if ch == '*' && s.peek() == '/' {
			s.read()
			s.buf.WriteString("*/")
			return s.buf.String()
		}
		s.buf.WriteRune(ch)
	}
}

func (s *Scanner) scanBind() (Pos, Token, string) {
	start, pos := s.read()

	s.buf.Reset()
	s.buf.WriteRune(start)

	// Question mark starts a numeric bind.
	if start == '?' {
		for isDigit(s.peek()) {
			ch, _ := s.read()
			s.buf.WriteRune(ch)
		}
		return pos, BIND, s.buf.String()
	}

	// All other characters start an alphanumeric bind.
	assert(start == ':' || start == '@' || start == '$')
	for isUnquotedIdent(s.peek()) {
		ch, _ := s.read()
		s.buf.WriteRune(ch)
	}
	return pos, BIND, s.buf.String()
}

func (s *Scanner) scanBlob() (Pos, Token, string) {
	start, pos := s.read()
	assert(start == 'x' || start == 'X')

	// If the next character is not a quote, it's an IDENT.
	if isUnquotedIdent(s.peek()) {
		return s.scanUnquotedIdent(pos, string(start))
	} else if s.peek() != '\'' {
		return pos, IDENT, string(start)
	}
	ch, _ := s.read()
	assert(ch == '\'')

	s.buf.Reset()
	for i := 0; ; i++ {
		ch, _ := s.read()
		if ch == '\'' {
			return pos, BLOB, s.buf.String()
		} else if ch == -1 {
			return pos, ILLEGAL, string(start) + `'` + s.buf.String()
		} else if !isHex(ch) {
			return pos, ILLEGAL, string(start) + `'` + s.buf.String() + string(ch)
		}
		s.buf.WriteRune(ch)
	}
}

func (s *Scanner) scanNumber() (Pos, Token, string) {
	assert(isDigit(s.peek()) || s.peek() == '.')
	pos := s.pos
	tok := INTEGER

	s.buf.Reset()

	if s.peek() == '0' {
		s.buf.WriteRune('0')
		s.read()
		if s.peek() == 'x' || s.peek() == 'X' {
			s.read()
			s.buf.WriteRune('x')
			for isHex(s.peek()) {
				ch, _ := s.read()
				s.buf.WriteRune(ch)
			}
			// TODO: error handling:
			// if len(s.buf.String()) < 2 => invalid
			// reason: means we scanned '0x'
			// if len(s.buf.String()) - 2 > 16 => invalid
			// reason: according to spec maximum of 16 significant digits)
			return pos, tok, s.buf.String()
		}
	}

	// Read whole number if starting with a digit.
	if isDigit(s.peek()) {
		for isDigit(s.peek()) {
			ch, _ := s.read()
			s.buf.WriteRune(ch)
		}
	}

	// Read decimal and successive digits.
	if s.peek() == '.' {
		tok = FLOAT

		ch, _ := s.read()
		s.buf.WriteRune(ch)

		for isDigit(s.peek()) {
			ch, _ := s.read()
			s.buf.WriteRune(ch)
		}
	}

	// If we just have a dot in the buffer with no digits by this point,
	// this can't be a number, so we can stop and return DOT
	if s.buf.String() == "." {
		return pos, DOT, "."
	}

	// Read exponent with optional +/- sign.
	if ch := s.peek(); ch == 'e' || ch == 'E' {
		tok = FLOAT

		ch, _ := s.read()
		s.buf.WriteRune(ch)

		if s.peek() == '+' || s.peek() == '-' {
			ch, _ := s.read()
			s.buf.WriteRune(ch)
			if !isDigit(s.peek()) {
				return pos, ILLEGAL, s.buf.String()
			}
			for isDigit(s.peek()) {
				ch, _ := s.read()
				s.buf.WriteRune(ch)
			}
		} else if isDigit(s.peek()) {
			for isDigit(s.peek()) {
				ch, _ := s.read()
				s.buf.WriteRune(ch)
			}
		} else {
			return pos, ILLEGAL, s.buf.String()
		}
	}

	return pos, tok, s.buf.String()
}

func (s *Scanner) read() (rune, Pos) {
	if s.full {
		s.full = false
		return s.ch, s.pos
	}

	var err error
	s.ch, _, err = s.r.ReadRune()
	if err != nil {
		s.ch = -1
		return s.ch, s.pos
	}

	s.pos.Offset++
	if s.ch == '\n' {
		s.pos.Line++
		s.pos.Column = 0
	} else {
		s.pos.Column++
	}
	return s.ch, s.pos
}

func (s *Scanner) peek() rune {
	if !s.full {
		s.read()
		s.unread()
	}
	return s.ch
}

func (s *Scanner) unread() {
	assert(!s.full)
	s.full = true
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
