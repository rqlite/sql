package sql

import (
	"fmt"
	"strconv"
	"strings"
)

var (
	keywords      = make(map[string]Token)
	bareTokensMap = make(map[Token]struct{})
)

func init() {
	for i := keyword_beg + 1; i < keyword_end; i++ {
		keywords[tokens[i]] = i
	}
	keywords[tokens[NULL]] = NULL
	keywords[tokens[TRUE]] = TRUE
	keywords[tokens[FALSE]] = FALSE

	for _, tok := range bareTokens {
		bareTokensMap[tok] = struct{}{}
	}
}

// Token is the set of lexical tokens of the Go programming language.
type Token int

// The list of tokens.
const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	COMMENT
	SPACE

	literal_beg
	IDENT   // IDENT
	QIDENT  // "IDENT"
	STRING  // 'string'
	BLOB    // ???
	FLOAT   // 123.45
	INTEGER // 123
	NULL    // NULL
	TRUE    // true
	FALSE   // false
	BIND    //? or ?NNN or :VVV or @VVV or $VVV
	literal_end

	operator_beg
	SEMI   // ;
	LP     // (
	RP     // )
	COMMA  // ,
	NE     // !=
	EQ     // =
	LE     // <=
	LT     // <
	GT     // >
	GE     // >=
	BITAND // &
	BITOR  // |
	BITNOT // !
	LSHIFT // <<
	RSHIFT // >>
	PLUS   // +
	MINUS  // -
	STAR   // *
	SLASH  // /
	REM    // %
	CONCAT // ||
	DOT    // .

	JSON_EXTRACT_JSON // ->
	JSON_EXTRACT_SQL  // ->>
	operator_end

	keyword_beg
	ABORT
	ACTION
	ADD
	AFTER
	AGG_COLUMN
	AGG_FUNCTION
	ALL
	ALTER
	ALWAYS
	ANALYZE
	AND
	AS
	ASC
	ASTERISK
	ATTACH
	AUTOINCREMENT
	BEFORE
	BEGIN
	BETWEEN
	BY
	CASCADE
	CASE
	CAST
	CHECK
	COLLATE
	COLUMN
	COLUMNKW
	COMMIT
	CONFLICT
	CONSTRAINT
	CREATE
	CROSS
	CTIME_KW
	CURRENT
	CURRENT_TIME
	CURRENT_DATE
	CURRENT_TIMESTAMP
	DATABASE
	DEFAULT
	DEFERRABLE
	DEFERRED
	DELETE
	DESC
	DETACH
	DISTINCT
	DO
	DROP
	EACH
	ELSE
	END
	ESCAPE
	EXCEPT
	EXCLUDE
	EXCLUSIVE
	EXISTS
	EXPLAIN
	FAIL
	FILTER
	FIRST
	FOLLOWING
	FOR
	FOREIGN
	FROM
	FUNCTION
	GENERATED
	GLOB
	GROUP
	GROUPS
	HAVING
	IF
	IF_NULL_ROW
	IGNORE
	IMMEDIATE
	IN
	INDEX
	INDEXED
	INITIALLY
	INNER
	INSERT
	INSTEAD
	INTERSECT
	INTO
	IS
	ISNOT
	ISNULL // TODO: REMOVE?
	JOIN
	KEY
	LAST
	LEFT
	LIKE
	LIMIT
	MATCH
	NATURAL
	NO
	NOT
	NOTBETWEEN
	NOTEXISTS
	NOTGLOB
	NOTHING
	NOTIN
	NOTLIKE
	NOTMATCH
	NOTNULL
	NOTREGEXP
	NULLS
	OF
	OFFSET
	ON
	OR
	ORDER
	OTHERS
	OUTER
	OVER
	PARTITION
	PLAN
	PRAGMA
	PRECEDING
	PRIMARY
	QUERY
	RAISE
	RANGE
	RECURSIVE
	REFERENCES
	REGEXP
	REGISTER
	REINDEX
	RELEASE
	RENAME
	REPLACE
	RESTRICT
	RETURNING
	ROLLBACK
	ROW
	ROWID
	ROWS
	SAVEPOINT
	SELECT
	SELECT_COLUMN
	SET
	SPAN
	STORED
	STRICT
	TABLE
	TEMP
	THEN
	TIES
	TO
	TRANSACTION
	TRIGGER
	TRUTH
	UNBOUNDED
	UNION
	UNIQUE
	UPDATE
	USING
	VACUUM
	VALUES
	VARIABLE
	VECTOR
	VIEW
	VIRTUAL
	WHEN
	WHERE
	WINDOW
	WITH
	WITHOUT
	keyword_end

	ANY // ???
	token_end
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	COMMENT: "COMMENT",
	SPACE:   "SPACE",

	IDENT:   "IDENT",
	QIDENT:  "QIDENT",
	STRING:  "STRING",
	BLOB:    "BLOB",
	FLOAT:   "FLOAT",
	INTEGER: "INTEGER",
	NULL:    "NULL",
	TRUE:    "TRUE",
	FALSE:   "FALSE",
	BIND:    "BIND",

	SEMI:   ";",
	LP:     "(",
	RP:     ")",
	COMMA:  ",",
	NE:     "!=",
	EQ:     "=",
	LE:     "<=",
	LT:     "<",
	GT:     ">",
	GE:     ">=",
	BITAND: "&",
	BITOR:  "|",
	BITNOT: "!",
	LSHIFT: "<<",
	RSHIFT: ">>",
	PLUS:   "+",
	MINUS:  "-",
	STAR:   "*",
	SLASH:  "/",
	REM:    "%",
	CONCAT: "||",
	DOT:    ".",

	ABORT:             "ABORT",
	ACTION:            "ACTION",
	ADD:               "ADD",
	AFTER:             "AFTER",
	AGG_COLUMN:        "AGG_COLUMN",
	AGG_FUNCTION:      "AGG_FUNCTION",
	ALL:               "ALL",
	ALTER:             "ALTER",
	ALWAYS:            "ALWAYS",
	ANALYZE:           "ANALYZE",
	AND:               "AND",
	AS:                "AS",
	ASC:               "ASC",
	ASTERISK:          "ASTERISK",
	ATTACH:            "ATTACH",
	AUTOINCREMENT:     "AUTOINCREMENT",
	BEFORE:            "BEFORE",
	BEGIN:             "BEGIN",
	BETWEEN:           "BETWEEN",
	BY:                "BY",
	CASCADE:           "CASCADE",
	CASE:              "CASE",
	CAST:              "CAST",
	CHECK:             "CHECK",
	COLLATE:           "COLLATE",
	COLUMN:            "COLUMN",
	COLUMNKW:          "COLUMNKW",
	COMMIT:            "COMMIT",
	CONFLICT:          "CONFLICT",
	CONSTRAINT:        "CONSTRAINT",
	CREATE:            "CREATE",
	CROSS:             "CROSS",
	CTIME_KW:          "CTIME_KW",
	CURRENT:           "CURRENT",
	CURRENT_TIME:      "CURRENT_TIME",
	CURRENT_DATE:      "CURRENT_DATE",
	CURRENT_TIMESTAMP: "CURRENT_TIMESTAMP",
	DATABASE:          "DATABASE",
	DEFAULT:           "DEFAULT",
	DEFERRABLE:        "DEFERRABLE",
	DEFERRED:          "DEFERRED",
	DELETE:            "DELETE",
	DESC:              "DESC",
	DETACH:            "DETACH",
	DISTINCT:          "DISTINCT",
	DO:                "DO",
	DROP:              "DROP",
	EACH:              "EACH",
	ELSE:              "ELSE",
	END:               "END",
	ESCAPE:            "ESCAPE",
	EXCEPT:            "EXCEPT",
	EXCLUDE:           "EXCLUDE",
	EXCLUSIVE:         "EXCLUSIVE",
	EXISTS:            "EXISTS",
	EXPLAIN:           "EXPLAIN",
	FAIL:              "FAIL",
	FILTER:            "FILTER",
	FIRST:             "FIRST",
	FOLLOWING:         "FOLLOWING",
	FOR:               "FOR",
	FOREIGN:           "FOREIGN",
	FROM:              "FROM",
	FUNCTION:          "FUNCTION",
	GENERATED:         "GENERATED",
	GLOB:              "GLOB",
	GROUP:             "GROUP",
	GROUPS:            "GROUPS",
	HAVING:            "HAVING",
	IF:                "IF",
	IF_NULL_ROW:       "IF_NULL_ROW",
	IGNORE:            "IGNORE",
	IMMEDIATE:         "IMMEDIATE",
	IN:                "IN",
	INDEX:             "INDEX",
	INDEXED:           "INDEXED",
	INITIALLY:         "INITIALLY",
	INNER:             "INNER",
	INSERT:            "INSERT",
	INSTEAD:           "INSTEAD",
	INTERSECT:         "INTERSECT",
	INTO:              "INTO",
	IS:                "IS",
	ISNOT:             "ISNOT",
	ISNULL:            "ISNULL",
	JOIN:              "JOIN",
	KEY:               "KEY",
	LAST:              "LAST",
	LEFT:              "LEFT",
	LIKE:              "LIKE",
	LIMIT:             "LIMIT",
	MATCH:             "MATCH",
	NO:                "NO",
	NATURAL:           "NATURAL",
	NOT:               "NOT",
	NOTBETWEEN:        "NOTBETWEEN",
	NOTEXISTS:         "NOTEXISTS",
	NOTGLOB:           "NOTGLOB",
	NOTHING:           "NOTHING",
	NOTIN:             "NOTIN",
	NOTLIKE:           "NOTLIKE",
	NOTMATCH:          "NOTMATCH",
	NOTNULL:           "NOTNULL",
	NOTREGEXP:         "NOTREGEXP",
	NULLS:             "NULLS",
	OF:                "OF",
	OFFSET:            "OFFSET",
	ON:                "ON",
	OR:                "OR",
	ORDER:             "ORDER",
	OTHERS:            "OTHERS",
	OUTER:             "OUTER",
	OVER:              "OVER",
	PARTITION:         "PARTITION",
	PLAN:              "PLAN",
	PRAGMA:            "PRAGMA",
	PRECEDING:         "PRECEDING",
	PRIMARY:           "PRIMARY",
	QUERY:             "QUERY",
	RAISE:             "RAISE",
	RANGE:             "RANGE",
	RECURSIVE:         "RECURSIVE",
	REFERENCES:        "REFERENCES",
	REGEXP:            "REGEXP",
	REGISTER:          "REGISTER",
	REINDEX:           "REINDEX",
	RELEASE:           "RELEASE",
	RENAME:            "RENAME",
	REPLACE:           "REPLACE",
	RESTRICT:          "RESTRICT",
	RETURNING:         "RETURNING",
	ROLLBACK:          "ROLLBACK",
	ROW:               "ROW",
	ROWID:             "ROWID",
	ROWS:              "ROWS",
	SAVEPOINT:         "SAVEPOINT",
	SELECT:            "SELECT",
	SELECT_COLUMN:     "SELECT_COLUMN",
	SET:               "SET",
	SPAN:              "SPAN",
	STORED:            "STORED",
	STRICT:            "STRICT",
	TABLE:             "TABLE",
	TEMP:              "TEMP",
	THEN:              "THEN",
	TIES:              "TIES",
	TO:                "TO",
	TRANSACTION:       "TRANSACTION",
	TRIGGER:           "TRIGGER",
	TRUTH:             "TRUTH",
	UNBOUNDED:         "UNBOUNDED",
	UNION:             "UNION",
	UNIQUE:            "UNIQUE",
	UPDATE:            "UPDATE",
	USING:             "USING",
	VACUUM:            "VACUUM",
	VALUES:            "VALUES",
	VARIABLE:          "VARIABLE",
	VECTOR:            "VECTOR",
	VIEW:              "VIEW",
	VIRTUAL:           "VIRTUAL",
	WHEN:              "WHEN",
	WHERE:             "WHERE",
	WINDOW:            "WINDOW",
	WITH:              "WITH",
	WITHOUT:           "WITHOUT",
}

// A list of keywords that can be used as unquoted identifiers.
var bareTokens = [...]Token{
	ABORT, ACTION, AFTER, ALWAYS, ANALYZE, ASC, ATTACH, BEFORE, BEGIN, BY,
	CASCADE, CAST, COLUMN, CONFLICT, CROSS, CURRENT, CURRENT_DATE,
	CURRENT_TIME, CURRENT_TIMESTAMP, DATABASE, DEFERRED, DESC, DETACH, DO,
	EACH, END, EXCLUDE, EXCLUSIVE, EXPLAIN, FAIL, FILTER, FIRST, FOLLOWING,
	FOR, GENERATED, GLOB, GROUPS, IF, IGNORE, IMMEDIATE, INDEXED, INITIALLY,
	INNER, INSTEAD, KEY, LAST, LEFT, LIKE, MATCH, NATURAL, NO, NULLS, OF,
	OFFSET, OTHERS, OUTER, OVER, PARTITION, PLAN, PRAGMA, PRECEDING, QUERY,
	RAISE, RANGE, RECURSIVE, REGEXP, REINDEX, RELEASE, RENAME, REPLACE,
	RESTRICT, ROLLBACK, ROW, ROWS, SAVEPOINT, TEMP, TIES, TRIGGER,
	UNBOUNDED, VACUUM, VIEW, VIRTUAL, WINDOW, WITH, WITHOUT,
}

func (tok Token) String() string {
	s := ""
	if 0 <= tok && tok < Token(len(tokens)) {
		s = tokens[tok]
	}
	if s == "" {
		s = "token(" + strconv.Itoa(int(tok)) + ")"
	}
	return s
}

func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENT
}

// isBareToken returns true if keyword token can be used as an identifier.
func isBareToken(tok Token) bool {
	_, ok := bareTokensMap[tok]
	return ok
}

func (tok Token) IsLiteral() bool {
	return tok > literal_beg && tok < literal_end
}

func (tok Token) IsBinaryOp() bool {
	switch tok {
	case PLUS, MINUS, STAR, SLASH, REM, CONCAT, NOT, BETWEEN,
		LSHIFT, RSHIFT, BITAND, BITOR, LT, LE, GT, GE, EQ, NE,
		IS, IN, LIKE, GLOB, MATCH, REGEXP, AND, OR,
		JSON_EXTRACT_JSON, JSON_EXTRACT_SQL:
		return true
	default:
		return false
	}
}

func isIdentToken(tok Token) bool {
	return tok == IDENT || tok == QIDENT
}

// isExprIdentToken returns true if tok can be used as an identifier in an expression.
// It includes IDENT, QIDENT, and certain keywords.
func isExprIdentToken(tok Token) bool {
	switch tok {
	case IDENT, QIDENT:
		return true
	// List keywords that can be used as identifiers in expressions
	case ROWID, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP:
		return true
	// Core functions
	case REPLACE, LIKE, GLOB, IF:
		return true
	// Add any other non-reserved keywords here
	default:
		return false
	}
}

const (
	LowestPrec  = 0 // non-operators
	UnaryPrec   = 13
	HighestPrec = 14
)

func (op Token) Precedence() int {
	switch op {
	case OR:
		return 1
	case AND:
		return 2
	case NOT:
		return 3
	case IS, MATCH, LIKE, GLOB, REGEXP, BETWEEN, IN, ISNULL, NOTNULL, NE, EQ:
		return 4
	case GT, LE, LT, GE:
		return 5
	case ESCAPE:
		return 6
	case BITAND, BITOR, LSHIFT, RSHIFT:
		return 7
	case PLUS, MINUS:
		return 8
	case STAR, SLASH, REM:
		return 9
	case CONCAT, JSON_EXTRACT_JSON, JSON_EXTRACT_SQL:
		return 10
	case BITNOT:
		return 11
	}
	return LowestPrec
}

type Pos struct {
	Offset int // offset, starting at 0
	Line   int // line number, starting at 1
	Column int // column number, starting at 1 (byte count)
}

// String returns a string representation of the position.
func (p Pos) String() string {
	if !p.IsValid() {
		return "-"
	}
	s := fmt.Sprintf("%d", p.Line)
	if p.Column != 0 {
		s += fmt.Sprintf(":%d", p.Column)
	}
	return s
}

// IsValid returns true if p is non-zero.
func (p Pos) IsValid() bool {
	return p != Pos{}
}

func assert(condition bool) {
	if !condition {
		panic("assert failed")
	}
}
