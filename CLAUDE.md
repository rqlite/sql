# Working in rqlite/sql

This package is the SQLite SQL parser used by rqlite. It is a syntax parser:
it accepts or rejects SQL and builds an AST, but it does not do semantic
checks (types, duplicate keys, column existence) which SQLite performs later.

## The sqlite3 binary is the authority

The parser must accept exactly what the `sqlite3` binary accepts and reject
what it rejects with a syntax error. The syntax diagrams at
https://www.sqlite.org/syntax/*.html are a useful cross-check but when they
and the binary disagree, match the binary. (Example: the table-or-subquery
diagram omits a schema prefix on table-valued functions, yet
`SELECT * FROM main.json_each('[1]')` runs fine, so the parser accepts it.)

Whenever you change what the parser accepts, run the statements concerned
through sqlite3 before committing:

    sqlite3 :memory: "CREATE TABLE t (x); <statement>;"

Check both directions: every new form the parser accepts must parse in
sqlite3, and every form the parser now rejects must be a syntax error in
sqlite3 ("near ...: syntax error" or "unrecognized token"). A statement that
parses in sqlite3 but fails later ("in prepare" errors about semantics such as
"AUTOINCREMENT is only allowed on an INTEGER PRIMARY KEY") is syntactically
valid and the parser should accept it.

## Test SQL must be real SQL

When a test treats a complete statement as valid, that statement must actually
be accepted by sqlite3, not merely by this parser. Run it before committing.
Serialized output asserted in `AssertStatementStringer` / `AssertExprStringer`
tests must also run in sqlite3. Tests that assert a rejection should reject
something sqlite3 also rejects.

Compute token positions for `pos(n)` assertions programmatically rather than
counting by hand; off-by-one offsets have cost several iterations.

## Bug-fix workflow

For each bug: add a unit test that exposes it, confirm the test fails, fix the
bug, confirm the whole suite passes, then commit that bug alone with a message
explaining the issue and the fix. Do not batch unrelated fixes into one commit.

Before every commit run, and require success from, all of:

    gofmt -l .      # must print nothing
    go vet ./...
    go test ./...

Do not pipe `go test` output into another command when gating a commit; the
pipe hides a failing exit status. Capture the output to a file or variable and
check the exit code directly.

## Invariants to preserve

- Round trip: for any statement the parser accepts, `String()` must produce
  SQL that re-parses to a tree with the same `String()`.
- `String()` must never drop a parsed clause. If you add a field to an AST
  node, update `String()`, `Clone()` (deep copy) and `walk.go` together.
- Every AST node that can contain identifiers or expressions needs a case in
  `walk.go`; rqlite relies on `Walk` to see and rewrite every sub-expression.
- Names that are not SQLite keywords must remain usable as identifiers; keep
  parser-internal tokens out of the keyword range in `token.go`, and add any
  new keyword that SQLite treats as a fallback identifier to `bareTokens`.
- Prefer a parse error over a silent misparse. Never let an unexpected token
  be consumed as something else (a phantom column, an alias, a comment).

## Branch

Fix work lands on the `claude-fixes` branch, not master, unless told otherwise.
