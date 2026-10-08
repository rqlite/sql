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
	BIDENT  // `IDENT`
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
	BITNOT // ~
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
	ALL
	ALTER
	ALWAYS
	ANALYZE
	AND
	AS
	ASC
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
	COMMIT
	CONFLICT
	CONSTRAINT
	CREATE
	CROSS
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
	FULL
	GENERATED
	GLOB
	GROUP
	GROUPS
	HAVING
	IF
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
	ISNULL // TODO: REMOVE?
	JOIN
	KEY
	LAST
	LEFT
	LIKE
	LIMIT
	MATCH
	MATERIALIZED
	NATURAL
	NO
	NOT
	NOTHING
	NOTNULL
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
	REINDEX
	RELEASE
	RENAME
	REPLACE
	RESTRICT
	RETURNING
	RIGHT
	ROLLBACK
	ROW
	ROWID
	ROWS
	SAVEPOINT
	SELECT
	SET
	STORED
	STRICT
	TABLE
	TEMP
	THEN
	TIES
	TO
	TRANSACTION
	TRIGGER
	UNBOUNDED
	UNION
	UNIQUE
	UPDATE
	USING
	VACUUM
	VALUES
	VIEW
	VIRTUAL
	WHEN
	WHERE
	WINDOW
	WITH
	WITHOUT
	keyword_end

	// Internal tokens. These are not SQLite keywords: they are produced by the
	// parser (for example the compound NOT operators) and so must not be
	// registered as keywords, otherwise their names could not be identifiers.
	internal_beg
	AGG_COLUMN
	AGG_FUNCTION
	ASTERISK
	COLUMNKW
	CTIME_KW
	FUNCTION
	IF_NULL_ROW
	ISNOT
	NOTBETWEEN
	NOTEXISTS
	NOTGLOB
	NOTIN
	NOTLIKE
	NOTMATCH
	NOTREGEXP
	REGISTER
	SELECT_COLUMN
	SPAN
	TRUTH
	VARIABLE
	VECTOR
	internal_end

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
	BIDENT:  "BIDENT",
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
	BITNOT: "~",
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
	FULL:              "FULL",
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
	MATERIALIZED:      "MATERIALIZED",
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
	RIGHT:             "RIGHT",
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
	FOR, FULL, GENERATED, GLOB, GROUPS, IF, IGNORE, IMMEDIATE, INDEXED,
	INITIALLY, INNER, INSTEAD, KEY, LAST, LEFT, LIKE, MATCH, NATURAL, NO,
	NULLS, OF, OFFSET, OTHERS, OUTER, OVER, PARTITION, PLAN, PRAGMA,
	PRECEDING, QUERY, RAISE, RANGE, RECURSIVE, REGEXP, REINDEX, RELEASE,
	RENAME, REPLACE, RESTRICT, RIGHT, ROLLBACK, ROW, ROWS, SAVEPOINT, STORED,
	STRICT, TEMP, TIES, TRIGGER,
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
	return tok == IDENT || tok == QIDENT || tok == BIDENT
}

// isExprIdentToken returns true if tok can be used as an identifier in an expression.
// It includes IDENT, QIDENT, BIDENT, bare tokens (keywords that can be used as identifiers),
// and certain other keywords like ROWID.
// Note: Some bare tokens have special expression handling (CAST, CASE, RAISE, etc.)
// and should not be treated as identifiers in parseOperand.
func isExprIdentToken(tok Token) bool {
	switch tok {
	case IDENT, QIDENT, BIDENT:
		return true
	// ROWID is a special keyword that can be used as an identifier but is not a bare token
	case ROWID:
		return true
	// Exclude tokens that have special expression handling in parseOperand.
	// These are in bareTokens but have precedence as expression keywords.
	case CAST, CASE, RAISE, EXISTS, SELECT, NOT:
		return false
	default:
		// Bare tokens are keywords that can be used as unquoted identifiers
		// (e.g., DESC, ASC, KEY, ACTION, REPLACE, LIKE, GLOB, IF, etc.)
		return isBareToken(tok)
	}
}

const (
	LowestPrec  = 0  // non-operators
	NotPrec     = 3  // unary NOT: looser than the comparison operators, tighter than AND
	UnaryPrec   = 13 // other unary operators (-, +, ~)
	HighestPrec = 14
)

func (op Token) Precedence() int {
	switch op {
	case OR:
		return 1
	case AND:
		return 2
	case NOT:
		// In binary position NOT is always the prefix of a compound operator
		// (NOT IN, NOT LIKE, NOT BETWEEN, ...), so it has that operator's
		// precedence. Unary NOT has its own level, NotPrec, and is handled
		// by the parser directly.
		return 4
	case IS, ISNOT, MATCH, NOTMATCH, LIKE, NOTLIKE, GLOB, NOTGLOB, REGEXP, NOTREGEXP,
		BETWEEN, NOTBETWEEN, IN, NOTIN, ISNULL, NOTNULL, NE, EQ:
		return 4
	case GT, LE, LT, GE:
		return 5
	case BITAND, BITOR, LSHIFT, RSHIFT:
		return 7
	case PLUS, MINUS:
		return 8
	case STAR, SLASH, REM:
		return 9
	case CONCAT, JSON_EXTRACT_JSON, JSON_EXTRACT_SQL:
		return 10
	}
	return LowestPrec
}

// isLikeOp returns true if tok is an operator that may be followed by an
// ESCAPE clause. Per https://www.sqlite.org/lang_expr.html the ESCAPE clause
// binds only to a preceding [NOT] LIKE expression.
func isLikeOp(tok Token) bool {
	return tok == LIKE || tok == NOTLIKE
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
