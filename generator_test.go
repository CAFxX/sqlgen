package sqlgen_test

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CAFxX/sqlgen"
	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

// e2eYAML is the canonical YAML specification used to generate e2e_gen_test.go.
const e2eYAML = `package: sqlgen_test
out: e2e_gen_test.go
dialect: sqlite

statements:
  - name: CreateUser
    kind: exec
    sql: |
      INSERT INTO users (id, org_id, email, role, score, bio)
      VALUES (:ID:int64, :OrgID:int64, :Email:string, :Role:string, :Score:int64, :Bio:string)

  - name: GetUserByID
    kind: one
    result_type: User
    sql: |
      SELECT id:int64, org_id:int64, email:string, role:string, score:int64, bio:string
      FROM users
      WHERE id = :ID:int64

  - name: ListUsersByIDs
    kind: many
    result_type: User
    sql: |
      SELECT id:int64, org_id:int64, email:string, role:string, score:int64, bio:string
      FROM users
      WHERE id IN (:IDs:[]int64 ASC)
      ORDER BY id ASC

  - name: SelectUserProfileSubset
    kind: many
    args: struct
    sql: |
      SELECT u.id:int64, u.email:string, u.role:string, u.score:int64
      {?WithExtra: , u.bio:string }
      FROM users u
      WHERE u.org_id = :OrgID:int64
      ORDER BY u.id ASC

  - name: SearchUsersCoupled
    kind: many
    sql: |
      SELECT u.id:int64, u.email:string
      {?WithOrg: , o.name:string }
      FROM users u
      {?WithOrg: LEFT JOIN orgs o ON o.id = u.org_id }
      WHERE 1=1
      {?WithOrg: AND o.tier IN (:OrgTiers:[]string) AND u.org_id IN (:OrgSub:sqlgen.Query) }
      {?WithRoles: AND u.role IN (:Roles:[]string) }
      ORDER BY u.id ASC

  - name: SubOrgsByMinID
    kind: subquery
    sql: |
      SELECT id FROM orgs WHERE id >= :MinID:int64 {?OnlyActive: AND active = :ActiveVal:int64 }

  - name: PageUsers
    kind: many
    args: struct
    sql: |
      SELECT id:int64, role:string, score:int64, email:string
      FROM users
      WHERE :Seek:sqlgen.Seek
      {?RoleFilter: AND role = :FilterRole:string }
      ORDER BY :Order:sqlgen.Order(role, score, id)
      LIMIT :Limit:int64

  - name: UsersFromCTEAndInSubquery
    kind: many
    sql: |
      WITH filtered AS (:CTESub:sqlgen.Query)
      SELECT f.id:int64, f.email:string
      FROM filtered f
      WHERE f.id IN (:IDSub:sqlgen.Query)
      ORDER BY f.id ASC

  - name: SubUserIDsByScore
    kind: subquery
    sql: |
      SELECT id FROM users WHERE score >= :MinScore:int64

  - name: InsertUsersBatchReturning
    kind: many
    sql: |
      INSERT INTO batch_users (id, name, points)
      VALUES :Rows:[]BatchUserInput(ID:int64 ASC, Name:string, Points:int64)
      RETURNING id:int64, name:string, points:int64

  - name: UpdateUsersBatchFromValues
    kind: exec
    sql: |
      WITH updates(id, bonus) AS (
        VALUES :Rows:[]BatchBonusInput(ID:int64 ASC, Bonus:int64)
      )
      UPDATE batch_users
      SET points = batch_users.points + (SELECT u.bonus FROM updates u WHERE u.id = batch_users.id)
      WHERE batch_users.id IN (SELECT u.id FROM updates u)

  - name: InsertQuadBatch
    kind: exec
    sql: |
      INSERT INTO quad_items (id, group_key, priority, created_ts)
      VALUES :Rows:[]QuadInput(ID:int64 ASC, GroupKey:string, Priority:int64, CreatedTS:int64)

  - name: UserIDsNearScore
    kind: many
    sql: |
      SELECT id:int64 FROM users
      WHERE (score = :Score:int64 OR score + 1 = :Score:int64 OR score + 2 = :Score:int64) AND id IN (:IDs:[]int64)

  - name: UserEmailBytes
    kind: one
    sql: |
      SELECT CAST(email AS BLOB) AS raw:[]byte FROM users WHERE id = :ID:int64

  - name: UserBioIfSet
    kind: many
    sql: |
      SELECT NULLIF(bio, '') AS bio:*string FROM users ORDER BY id

  - name: UserEmailOnly
    kind: one
    result_type: EmailOnly
    sql: |
      SELECT email:string FROM users WHERE id = :ID:int64

  - name: UserRoleCountsIn
    kind: many
    sql: |
      SELECT role:string, COUNT(*) AS n:int64 FROM users WHERE id IN (:IDs:[]int64) GROUP BY role

  - name: TopUsersIn
    kind: many
    sql: |
      SELECT id:int64 FROM users WHERE id IN (:IDs:[]int64) ORDER BY id DESC LIMIT 3

  - name: BumpUsersByIDs
    kind: exec
    sql: |
      UPDATE batch_users SET points = points + :Bonus:int64 WHERE id IN (:IDs:[]int64 ASC)

  - name: DeleteQuadsByKeys
    kind: exec
    sql: |
      DELETE FROM quad_items WHERE (group_key, id) IN (:Keys:[]QuadDelKey(GroupKey:string, ID:int64))

  - name: QuadIDsByKeys
    kind: many
    args: struct
    sql: |
      SELECT id:int64 FROM quad_items WHERE (group_key, id) IN (:Keys:[]QuadKey(GroupKey:string, ID:int64))

  - name: PageQuad
    kind: many
    args: struct
    result_type: QuadItem
    sql: |
      SELECT id:int64, group_key:string, priority:int64, created_ts:int64
      FROM quad_items
      WHERE :Seek:sqlgen.Seek
      ORDER BY :Order:sqlgen.Order(group_key, priority, created_ts, id)
      LIMIT :Limit:int64
`

func TestParserProtectedContexts(t *testing.T) {
	yamlStr := `package: store
dialect: postgres
statements:
  - name: ProtectedQuery
    kind: many
    sql: |
      -- Single line comment with FakeParam:int64 and {?FakeFlag: bad }
      /* Multi-line comment with
         AnotherFake:string and {?AnotherFlag: bad } */
      SELECT
        u.id:int64,
        'single ''quoted'' :not_a_param {?NoFlag: x}' AS lit:string,
        "double ""quoted"" :col:int64" AS dq:string,
        ` + "`backtick ``quoted`` :col:int64`" + ` AS bq:string,
        $$dollar :not_param {?NoFlag: 1}$$ AS d1:string,
        $tag_1$tagged dollar :not_param ::text$tag_1$ AS d2:string,
        arr[1:3] AS slice_expr:string,
        u.score::bigint AS casted_score:int64
      FROM users u
      WHERE u.id = :ID:int64
        AND (u.meta := 'x:y') IS NOT NULL
`
	src, err := sqlgen.GenerateFromYAML([]byte(yamlStr))
	if err != nil {
		t.Fatalf("GenerateFromYAML failed on protected contexts: %v", err)
	}
	code := string(src)
	normalizedCode := strings.Join(strings.Fields(code), " ")
	for _, forbidden := range []string{"FakeParam", "FakeFlag", "AnotherFake", "AnotherFlag", "NoFlag", "not_a_param", "not_param"} {
		if strings.Contains(code, " "+forbidden+" ") || strings.Contains(code, forbidden+" bool") {
			t.Fatalf("protected context leaked identifier %q into generated code:\n%s", forbidden, code)
		}
	}
	for _, expected := range []string{"ID int64", "Lit string", "Dq string", "Bq string", "D1 string", "D2 string", "SliceExpr string", "CastedScore int64"} {
		if !strings.Contains(normalizedCode, expected) {
			t.Fatalf("expected %q in generated code:\n%s", expected, code)
		}
	}
}

func TestParserUnclosedProtectedContexts(t *testing.T) {
	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "unclosed dollar quote",
			sql:  "SELECT $$unclosed :param:int64 FROM users",
		},
		{
			name: "unclosed tagged dollar quote",
			sql:  "SELECT $tag$unclosed FROM users",
		},
		{
			name: "unclosed block comment",
			sql:  "SELECT /* unclosed comment id:int64 FROM users",
		},
		{
			name: "unclosed single quote",
			sql:  "SELECT 'unclosed FROM users",
		},
		{
			name: "unclosed conditional block",
			sql:  "SELECT id:int64 FROM users WHERE 1=1 {?Flag: AND id = 1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{Name: "BadStmt", Kind: sqlgen.KindMany, SQL: tc.sql},
				},
			}
			if _, err := sqlgen.Generate(cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestProjectionDisambiguation(t *testing.T) {
	yamlStr := `package: store
dialect: postgres
statements:
  - name: ComplexProjection
    kind: many
    sql: |
      WITH cte AS (
        SELECT c.id, c.amount * :Rate:float64 AS scaled
        FROM charges c
        WHERE c.org_id = :OrgID:int64
      )
      SELECT
        u.id:int64,
        u.price * :Tax:float64 AS total:float64
        {?WithBonus: , u.bonus + :BonusAdd:float64 AS final_bonus:float64 }
      FROM users u
      JOIN cte ON cte.id = u.id
      WHERE u.active = :Active:bool
`
	src, err := sqlgen.GenerateFromYAML([]byte(yamlStr))
	if err != nil {
		t.Fatalf("GenerateFromYAML failed: %v", err)
	}
	code := string(src)
	normalizedCode := strings.Join(strings.Fields(code), " ")
	for _, rowField := range []string{"ID int64", "Total float64", "FinalBonus float64"} {
		if !strings.Contains(normalizedCode, rowField) {
			t.Fatalf("expected row field %q in generated code:\n%s", rowField, code)
		}
	}
	for _, paramField := range []string{"Rate float64", "OrgID int64", "Tax float64", "BonusAdd float64", "Active bool", "WithBonus bool"} {
		if !strings.Contains(normalizedCode, paramField) {
			t.Fatalf("expected param field %q in generated code:\n%s", paramField, code)
		}
	}
	// One unconditional column leaves nothing to project: no SelectCols field.
	if strings.Contains(normalizedCode, "SelectCols uint64") {
		t.Fatalf("unexpected SelectCols field for a single unconditional column:\n%s", code)
	}
}

func TestYAMLStrictValidationAndForms(t *testing.T) {
	yamlStr := `package: store
dialect: postgres
extra_imports:
  - time
statements:
  - name: GetAuditRecord
    kind: many
    sql: |
      SELECT id:int64, created_at:time.Time, payload:json.RawMessage, amount:*big.Int, ip_addr:netip.Addr, endpoint:*url.URL
      FROM audits
      WHERE id = :ID:int64 AND org_id IN (:org_ids:[]int64) AND tag IN (:tags:[]string)
`
	src, err := sqlgen.GenerateFromYAML([]byte(yamlStr))
	if err != nil {
		t.Fatalf("valid YAML failed: %v", err)
	}
	code := string(src)
	for _, imp := range []string{`"time"`, `"encoding/json"`, `"math/big"`, `"net/netip"`, `"net/url"`, `"iter"`} {
		if !strings.Contains(code, imp) {
			t.Fatalf("expected auto-detected import %s in:\n%s", imp, code)
		}
	}
	if strings.Count(code, `"time"`) != 1 {
		t.Fatalf("expected deduplicated \"time\" import, got count=%d", strings.Count(code, `"time"`))
	}

	if _, err := sqlgen.ParseYAML([]byte("unknown_top_key: true\n")); err == nil {
		t.Fatalf("expected error on unknown top-level YAML key")
	}

	// Keys that used to exist are rejected, with the line they are on.
	for key, doc := range map[string]string{
		"version":        "version: \"1\"\npackage: store\n",
		"runtime_import": "runtime_import: example.com/rt\npackage: store\n",
		"params":         "package: store\nstatements:\n  - name: S1\n    kind: exec\n    sql: DELETE FROM t\n    params:\n      - ID:int64\n",
		"columns":        "package: store\nstatements:\n  - name: S1\n    kind: one\n    sql: SELECT id FROM t\n    columns:\n      - id:int64\n",
	} {
		if _, err := sqlgen.ParseYAML([]byte(doc)); err == nil || !strings.Contains(err.Error(), "line") || !strings.Contains(err.Error(), key) {
			t.Fatalf("expected a line-numbered error naming the removed key %q, got: %v", key, err)
		}
	}
}

func TestCompileTimeValidationErrors(t *testing.T) {
	cases := []struct {
		name        string
		cfg         sqlgen.Config
		wantErrText string
	}{
		{
			name: "typoed predeclared type suggests string",
			cfg: sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "GetUser",
						Kind: sqlgen.KindOne,
						SQL:  "SELECT id:int64, name:stirng FROM users WHERE id = :ID:int64",
					},
				},
			},
			wantErrText: `did you mean "string"?`,
		},
		{
			name: "nullable pointer sort column in Order with Seek rejected",
			cfg: sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "ListUsers",
						Kind: sqlgen.KindMany,
						SQL:  "SELECT id:int64, nickname:*string FROM users WHERE :Seek:sqlgen.Seek ORDER BY :Order:sqlgen.Order(nickname, id)",
					},
				},
			},
			wantErrText: "COALESCE",
		},
		{
			name: "nullable sql.NullString sort column in Order with Seek rejected",
			cfg: sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "ListUsersNull",
						Kind: sqlgen.KindMany,
						SQL:  "SELECT id:int64, nickname:sql.NullString FROM users WHERE :Seek:sqlgen.Seek ORDER BY :Order:sqlgen.Order(nickname, id)",
					},
				},
			},
			wantErrText: "COALESCE",
		},
		{
			name: "duplicate column names rejected",
			cfg: sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "DupCols",
						Kind: sqlgen.KindOne,
						SQL:  "SELECT u.id:int64, o.id:int64 FROM users u JOIN orgs o ON o.id = u.org_id",
					},
				},
			},
			wantErrText: "duplicate output column",
		},
		{
			name: "conflicting shared result_type structs rejected",
			cfg: sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{
						Name:       "GetUserA",
						Kind:       sqlgen.KindOne,
						ResultType: "SharedUser",
						SQL:        "SELECT id:int64, email:string FROM users WHERE id = :ID:int64",
					},
					{
						Name:       "GetUserB",
						Kind:       sqlgen.KindOne,
						ResultType: "SharedUser",
						SQL:        "SELECT id:int64, email:int64 FROM users WHERE id = :ID:int64",
					},
				},
			},
			wantErrText: "conflicting shared result_type",
		},
		{
			name: "nested conditional blocks rejected",
			cfg: sqlgen.Config{
				PackageName: "store",
				Dialect:     "postgres",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "NestedOpt",
						Kind: sqlgen.KindMany,
						SQL:  "SELECT id:int64 FROM users WHERE 1=1 {?Outer: AND a = 1 {?Inner: AND b = 2 } }",
					},
				},
			},
			wantErrText: "nested conditional",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := sqlgen.Generate(tc.cfg)
			if err == nil {
				t.Fatalf("expected compile-time error containing %q, got nil", tc.wantErrText)
			}
			if !strings.Contains(err.Error(), tc.wantErrText) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErrText, err)
			}
		})
	}
}

func TestPostgresSQLGeneration(t *testing.T) {
	q1 := GetUserByIDQuery(42)
	if got := q1.BuildSQL(runtime.DialectDollar); got != "SELECT id, org_id, email, role, score, bio\nFROM users\nWHERE id = $1" {
		t.Fatalf("GetUserByID Postgres SQL mismatch:\ngot:  %q", got)
	}

	q2 := ListUsersByIDsQuery([]int64{10, 20, 30})
	if got := q2.BuildSQL(runtime.DialectDollar); got != "SELECT id, org_id, email, role, score, bio\nFROM users\nWHERE id IN ($1, $2, $3)\nORDER BY id ASC" {
		t.Fatalf("ListUsersByIDs Postgres SQL mismatch:\ngot:  %q", got)
	}

	q3 := SelectUserProfileSubsetQuery(SelectUserProfileSubsetParams{
		OrgID:      7,
		WithExtra:  true,
		SelectCols: SelectUserProfileSubsetColID | SelectUserProfileSubsetColScore,
	})
	wantQ3 := "SELECT u.id, u.score\n , u.bio \nFROM users u\nWHERE u.org_id = $1\nORDER BY u.id ASC"
	if got := q3.BuildSQL(runtime.DialectDollar); got != wantQ3 {
		t.Fatalf("SelectUserProfileSubset Postgres SQL mismatch:\ngot:  %q\nwant: %q", got, wantQ3)
	}

	cteSub := PageUsersQuery(PageUsersParams{
		RoleFilter: true,
		FilterRole: "admin",
		Order:      OrderPageUsers(PageUsersOrderByScore.Desc()),
		Seek:       runtime.NewSeek("admin", int64(50), int64(5)),
		Limit:      10,
	})
	idSub := SubUserIDsByScoreQuery(30)
	composed := UsersFromCTEAndInSubqueryQuery(cteSub, idSub)
	gotComposed := composed.BuildSQL(runtime.DialectDollar)
	if !strings.Contains(gotComposed, "(score, id) < ($1, $2)") {
		t.Fatalf("expected Row-Value Constructor keyset predicate in composed Postgres SQL, got:\n%s", gotComposed)
	}
	if !strings.Contains(gotComposed, "AND role = $3") || !strings.Contains(gotComposed, "LIMIT $4") || !strings.Contains(gotComposed, "WHERE score >= $5") {
		t.Fatalf("expected continuous $3, $4, $5 placeholders across composed subqueries, got:\n%s", gotComposed)
	}
}

func setupE2EDB(t testing.TB) (*sql.DB, *Queries) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "e2e.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=synchronous(OFF)&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() {
		_ = db.Close()
	})

	schema := `
		CREATE TABLE orgs (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			tier TEXT NOT NULL,
			active INTEGER NOT NULL
		);
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			org_id INTEGER NOT NULL,
			email TEXT NOT NULL,
			role TEXT NOT NULL,
			score INTEGER NOT NULL,
			bio TEXT NOT NULL
		);
		CREATE TABLE batch_users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			points INTEGER NOT NULL
		);
		CREATE TABLE quad_items (
			id INTEGER PRIMARY KEY,
			group_key TEXT NOT NULL,
			priority INTEGER NOT NULL,
			created_ts INTEGER NOT NULL
		);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("exec schema: %v", err)
	}

	for _, o := range []struct {
		id     int64
		name   string
		tier   string
		active int64
	}{
		{1, "Acme", "enterprise", 1},
		{2, "Beta", "pro", 1},
		{3, "Gamma", "free", 0},
	} {
		if _, err := db.Exec(`INSERT INTO orgs (id, name, tier, active) VALUES (?, ?, ?, ?)`, o.id, o.name, o.tier, o.active); err != nil {
			t.Fatalf("seed org: %v", err)
		}
	}

	q := New(db)
	t.Cleanup(func() {
		_ = q.Close()
	})
	return db, q
}

func TestE2EFixedArityAndDualConsumption(t *testing.T) {
	_, q := setupE2EDB(t)
	ctx := context.Background()

	for i := int64(1); i <= 6; i++ {
		role := "member"
		if i%2 == 0 {
			role = "admin"
		}
		orgID := (i % 3) + 1
		res, err := q.CreateUser(ctx, i, orgID, fmt.Sprintf("u%d@example.com", i), role, i*10, fmt.Sprintf("bio-%d", i))
		if err != nil {
			t.Fatalf("CreateUser(%d): %v", i, err)
		}
		aff, _ := res.RowsAffected()
		if aff != 1 {
			t.Fatalf("expected RowsAffected=1, got %d", aff)
		}
	}

	u3, err := q.GetUserByID(ctx, 3)
	if err != nil {
		t.Fatalf("GetUserByID(3): %v", err)
	}
	if u3.ID != 3 || u3.Email != "u3@example.com" || u3.Score != 30 || u3.Bio != "bio-3" {
		t.Fatalf("unexpected GetUserByID(3): %+v", u3)
	}

	if _, err := q.ListUsersByIDs(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam on empty slice, got %v", err)
	}

	users, err := q.ListUsersByIDs(ctx, []int64{1, 3, 5})
	if err != nil || len(users) != 3 {
		t.Fatalf("ListUsersByIDs: len=%d err=%v", len(users), err)
	}

	prealloc := make([]User, 0, 8)
	usersInto, err := runtime.AppendTo(prealloc, q.ListUsersByIDsSeq(ctx, []int64{2, 4}))
	if err != nil || len(usersInto) != 2 || cap(usersInto) != 8 {
		t.Fatalf("ListUsersByIDsInto failed: len=%d cap=%d err=%v", len(usersInto), cap(usersInto), err)
	}

	var seqIDs []int64
	for u, err := range q.ListUsersByIDsSeq(ctx, []int64{1, 2, 3, 4, 5, 6}) {
		if err != nil {
			t.Fatalf("ListUsersByIDsSeq err: %v", err)
		}
		seqIDs = append(seqIDs, u.ID)
		if len(seqIDs) == 2 {
			break
		}
	}
	if !slices.Equal(seqIDs, []int64{1, 2}) {
		t.Fatalf("unexpected seqIDs after early break: %v", seqIDs)
	}

	var eachIDs []int64
	if err := runtime.Each(q.ListUsersByIDsSeq(ctx, []int64{4, 6}), func(u User) error {
		eachIDs = append(eachIDs, u.ID)
		return nil
	}); err != nil {
		t.Fatalf("ListUsersByIDsEach: %v", err)
	}
	if !slices.Equal(eachIDs, []int64{4, 6}) {
		t.Fatalf("unexpected eachIDs: %v", eachIDs)
	}
}

func TestE2ESelectColsBitmaskSubsetting(t *testing.T) {
	_, q := setupE2EDB(t)
	ctx := context.Background()

	if _, err := q.CreateUser(ctx, 1, 1, "alice@example.com", "admin", 100, "CTO"); err != nil {
		t.Fatal(err)
	}
	if _, err := q.CreateUser(ctx, 2, 1, "bob@example.com", "eng", 80, "Staff"); err != nil {
		t.Fatal(err)
	}

	params := SelectUserProfileSubsetParams{
		OrgID:      1,
		WithExtra:  true,
		SelectCols: SelectUserProfileSubsetColID | SelectUserProfileSubsetColScore,
	}

	qBuilt := SelectUserProfileSubsetQuery(params).BuildSQL(runtime.DialectQuestion)
	if !strings.HasPrefix(qBuilt, "SELECT u.id, u.score\n , u.bio \nFROM users u") {
		t.Fatalf("expected exact 3-column SELECT list, got:\n%s", qBuilt)
	}

	rows, err := q.SelectUserProfileSubset(ctx, params)
	if err != nil {
		t.Fatalf("SelectUserProfileSubset failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].ID != 1 || rows[0].Score != 100 || rows[0].Bio != "CTO" || rows[0].Email != "" || rows[0].Role != "" {
		t.Fatalf("unexpected row[0] with SelectCols subsetting: %+v", rows[0])
	}
	if rows[1].ID != 2 || rows[1].Score != 80 || rows[1].Bio != "Staff" || rows[1].Email != "" || rows[1].Role != "" {
		t.Fatalf("unexpected row[1] with SelectCols subsetting: %+v", rows[1])
	}
}

func TestE2ECoupledConditionalBlocks(t *testing.T) {
	_, q := setupE2EDB(t)
	ctx := context.Background()

	_, _ = q.CreateUser(ctx, 1, 1, "u1@acme.com", "admin", 50, "b1")
	_, _ = q.CreateUser(ctx, 2, 2, "u2@beta.com", "member", 60, "b2")
	_, _ = q.CreateUser(ctx, 3, 3, "u3@gamma.com", "admin", 70, "b3")

	rowsNoOrg, err := q.SearchUsersCoupled(ctx, SearchUsersCoupledParams{
		WithOrg:   false,
		OrgTiers:  nil,
		OrgSub:    runtime.Query{},
		WithRoles: true,
		Roles:     []string{"admin"},
	})
	if err != nil {
		t.Fatalf("SearchUsersCoupled with disabled WithOrg failed: %v", err)
	}
	if len(rowsNoOrg) != 2 || rowsNoOrg[0].ID != 1 || rowsNoOrg[1].ID != 3 || rowsNoOrg[0].Name != "" {
		t.Fatalf("unexpected rows with WithOrg=false: %+v", rowsNoOrg)
	}

	orgSub := SubOrgsByMinIDQuery(SubOrgsByMinIDParams{
		MinID:      1,
		OnlyActive: true,
		ActiveVal:  1,
	})
	rowsWithOrg, err := q.SearchUsersCoupled(ctx, SearchUsersCoupledParams{
		WithOrg:   true,
		OrgTiers:  []string{"enterprise", "pro"},
		OrgSub:    orgSub,
		WithRoles: false,
		Roles:     nil,
	})
	if err != nil {
		t.Fatalf("SearchUsersCoupled with enabled WithOrg failed: %v", err)
	}
	if len(rowsWithOrg) != 2 || rowsWithOrg[0].Name != "Acme" || rowsWithOrg[1].Name != "Beta" {
		t.Fatalf("unexpected rows with WithOrg=true: %+v", rowsWithOrg)
	}
}

func TestE2EKeysetPaginationUniformAndMixedOrders(t *testing.T) {
	_, q := setupE2EDB(t)
	ctx := context.Background()

	seedData := []struct {
		id    int64
		role  string
		score int64
	}{
		{1, "admin", 10}, {2, "admin", 20}, {3, "admin", 20}, {4, "admin", 30}, {5, "admin", 30},
		{6, "eng", 10}, {7, "eng", 10}, {8, "eng", 25}, {9, "eng", 25}, {10, "eng", 40},
		{11, "ops", 5}, {12, "ops", 15}, {13, "ops", 15}, {14, "ops", 15}, {15, "ops", 50},
	}
	for _, s := range seedData {
		if _, err := q.CreateUser(ctx, s.id, 1, fmt.Sprintf("u%d@test.com", s.id), s.role, s.score, ""); err != nil {
			t.Fatal(err)
		}
	}

	orders := []struct {
		name  string
		order runtime.Order
	}{
		{
			name:  "Uniform ASC, ASC (score ASC, id ASC)",
			order: OrderPageUsers(PageUsersOrderByScore.Asc(), PageUsersOrderByID.Asc()),
		},
		{
			name:  "Uniform DESC, DESC (score DESC, auto-tiebreaker id DESC)",
			order: OrderPageUsers(PageUsersOrderByScore.Desc()),
		},
		{
			name:  "Mixed ASC, DESC, ASC (role ASC, score DESC, id ASC)",
			order: OrderPageUsers(PageUsersOrderByRole.Asc(), PageUsersOrderByScore.Desc(), PageUsersOrderByID.Asc()),
		},
	}

	for _, tc := range orders {
		t.Run(tc.name, func(t *testing.T) {
			expectedAll, err := q.PageUsers(ctx, PageUsersParams{
				Order: tc.order,
				Limit: 100,
			})
			if err != nil || len(expectedAll) != 15 {
				t.Fatalf("full fetch failed: len=%d err=%v", len(expectedAll), err)
			}

			// The sort/cursor columns (role, score, id) are always selected, so a SelectCols mask that leaves
			// them out still paginates: SeekPageUsers(lastRow) used to build the cursor from zeroed fields
			// (ASC looped forever, DESC stopped after page 1).
			for _, mask := range []uint64{0, PageUsersColEmail, PageUsersColScore} {
				var collected []PageUsersRow
				var seek runtime.Seek
				for page := 0; page < 10; page++ {
					pageRows, err := q.PageUsers(ctx, PageUsersParams{
						Seek:       seek,
						Order:      tc.order,
						Limit:      4,
						SelectCols: mask,
					})
					if err != nil {
						t.Fatalf("mask %#x page %d failed: %v", mask, page, err)
					}
					if len(pageRows) == 0 {
						break
					}
					collected = append(collected, pageRows...)
					lastRow := pageRows[len(pageRows)-1]
					seek = SeekPageUsers(lastRow)
				}

				if len(collected) != len(expectedAll) {
					t.Fatalf("mask %#x: paginated count=%d, expected=%d", mask, len(collected), len(expectedAll))
				}
				for i := range expectedAll {
					want := expectedAll[i]
					if mask == PageUsersColScore {
						want.Email = "" // not selected
					}
					if collected[i] != want {
						t.Fatalf("mask %#x: mismatch at index %d:\ngot:  %+v\nwant: %+v", mask, i, collected[i], want)
					}
				}
			}
		})
	}
}

func TestE2ECurriedQueryComposition(t *testing.T) {
	_, q := setupE2EDB(t)
	ctx := context.Background()

	for i := int64(1); i <= 8; i++ {
		role := "eng"
		if i <= 6 {
			role = "admin"
		}
		if _, err := q.CreateUser(ctx, i, 1, fmt.Sprintf("u%d@x.com", i), role, i*10, ""); err != nil {
			t.Fatal(err)
		}
	}

	cteSub := PageUsersQuery(PageUsersParams{
		RoleFilter: true,
		FilterRole: "admin",
		Order:      OrderPageUsers(PageUsersOrderByScore.Asc(), PageUsersOrderByID.Asc()),
		Seek:       runtime.NewSeek("admin", int64(20), int64(2)),
		Limit:      3,
	})
	idSub := SubUserIDsByScoreQuery(40)

	res, err := q.UsersFromCTEAndInSubquery(ctx, cteSub, idSub)
	if err != nil {
		t.Fatalf("UsersFromCTEAndInSubquery: %v", err)
	}
	if len(res) != 2 || res[0].ID != 4 || res[1].ID != 5 {
		t.Fatalf("expected composed query results [4, 5], got %+v", res)
	}
}

func TestE2EAdaptiveMultiRowBatchExecution(t *testing.T) {
	db, q := setupE2EDB(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	makeInsertBatch := func(startID int64, count int) []BatchUserInput {
		out := make([]BatchUserInput, count)
		for i := 0; i < count; i++ {
			id := startID + int64(i)
			out[i] = BatchUserInput{
				ID:     id,
				Name:   fmt.Sprintf("user_%d", id),
				Points: 10,
			}
		}
		return out
	}

	for run := 0; run < 2; run++ {
		if _, err := db.Exec(`DELETE FROM batch_users`); err != nil {
			t.Fatal(err)
		}
		inserted1000, err := q.InsertUsersBatchReturning(ctx, makeInsertBatch(1, 1000))
		if err != nil || len(inserted1000) != 1000 {
			t.Fatalf("run %d Insert 1000: len=%d err=%v", run, len(inserted1000), err)
		}
		inserted900, err := q.InsertUsersBatchReturning(ctx, makeInsertBatch(1001, 900))
		if err != nil || len(inserted900) != 900 {
			t.Fatalf("run %d Insert 900: len=%d err=%v", run, len(inserted900), err)
		}
	}

	// 2900 rows fit one statement under the limit: one chunk of exactly 2900, never a decomposition into the
	// recurring 1000/900 sizes. Above the limit: full-limit chunks, the remainder last.
	if c := q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900, 10000, false); c != 2900 {
		t.Fatalf("expected a single 2900-row chunk, got %d", c)
	}
	c1 := q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900, 1000, false)
	c2 := q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900-c1, 1000, false)
	c3 := q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900-c1-c2, 1000, false)
	if c1 != 1000 || c2 != 1000 || c3 != 900 {
		t.Fatalf("expected chunking 1000 + 1000 + 900 for 2900 rows above a 1000-row limit, got %d + %d + %d", c1, c2, c3)
	}

	if _, err := db.Exec(`DELETE FROM batch_users`); err != nil {
		t.Fatal(err)
	}
	inserted2900, err := q.InsertUsersBatchReturning(ctx, makeInsertBatch(1, 2900))
	if err != nil || len(inserted2900) != 2900 {
		t.Fatalf("Insert 2900 failed: len=%d err=%v", len(inserted2900), err)
	}

	updates := make([]BatchBonusInput, 2900)
	for i := 0; i < 2900; i++ {
		updates[i] = BatchBonusInput{ID: int64(i + 1), Bonus: 5}
	}
	// A 256-rows-per-statement budget exercises the presorted, chunked-in-a-transaction path (12 chunks) of a
	// decomposable UPDATE ... WHERE id IN (...) cheaply. The VALUES list feeding a CTE is not decomposable
	// (DOCS section 3.2): above the limit it fails before sending anything instead of being chunked.
	chunkedQ := New(db, runtime.WithMaxBatchParams(512))
	defer func() { _ = chunkedQ.Close() }()
	ids := make([]int64, len(updates))
	for i := range updates {
		ids[len(ids)-1-i] = updates[i].ID // reversed: the ASC presort reorders it
	}
	updRes, err := chunkedQ.BumpUsersByIDs(ctx, 5, ids)
	if err != nil {
		t.Fatalf("BumpUsersByIDs(2900): %v", err)
	}
	aff, _ := updRes.RowsAffected()
	if aff != 2900 {
		t.Fatalf("expected 2900 rows updated, got %d", aff)
	}
	var tooLarge *runtime.CollectionTooLargeError
	if _, err := chunkedQ.UpdateUsersBatchFromValues(ctx, updates); !errors.As(err, &tooLarge) || !errors.Is(err, runtime.ErrCollectionTooLarge) ||
		tooLarge.Statement != "UpdateUsersBatchFromValues" || tooLarge.Items != 2900 || tooLarge.Limit != 256 {
		t.Fatalf("UpdateUsersBatchFromValues(2900) over a 256-row limit: err = %v, want a CollectionTooLargeError", err)
	}
	var total int64
	if err := db.QueryRowContext(ctx, `SELECT SUM(points) FROM batch_users`).Scan(&total); err != nil || total != 2900*15 {
		t.Fatalf("points after the chunked bump = %d (%v), want %d (the rejected update must not run)", total, err, 2900*15)
	}

	if _, err := db.Exec(`DELETE FROM batch_users`); err != nil {
		t.Fatal(err)
	}
	yielded := 0
	for row, err := range q.InsertUsersBatchReturningSeq(ctx, makeInsertBatch(1, 300)) {
		if err != nil {
			t.Fatalf("Seq error: %v", err)
		}
		yielded++
		if yielded == 260 {
			var cnt int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM batch_users WHERE id = ?`, row.ID).Scan(&cnt); err != nil {
				t.Fatalf("nested query inside Seq loop failed: %v", err)
			}
			break
		}
	}
	var countAfterBreak int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM batch_users`).Scan(&countAfterBreak); err != nil {
		t.Fatal(err)
	}
	if countAfterBreak < 256 {
		t.Fatalf("expected at least 256 already-executed chunk rows to persist after early break in Seq, got %d", countAfterBreak)
	}

	errBoom := errors.New("rollback test")
	_ = q.InTx(ctx, nil, func(txQ *Queries) error {
		if _, err := txQ.InsertUsersBatchReturning(ctx, makeInsertBatch(5000, 20)); err != nil {
			return err
		}
		return errBoom
	})
	var count5000 int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM batch_users WHERE id >= 5000`).Scan(&count5000); err != nil {
		t.Fatal(err)
	}
	if count5000 != 0 {
		t.Fatalf("expected explicit InTx rollback to discard batch insert, found %d rows", count5000)
	}
}

func TestE2EGeneratedCodeUpToDate(t *testing.T) {
	src, err := sqlgen.GenerateFromYAML([]byte(e2eYAML))
	if err != nil {
		t.Fatalf("GenerateFromYAML(e2eYAML) failed: %v", err)
	}
	if len(src) == 0 {
		t.Fatalf("expected non-empty generated Go source")
	}
	if os.Getenv("SQLGEN_UPDATE_E2E") != "" {
		// Regenerates the checked-in file after a deliberate generator change:
		//   SQLGEN_UPDATE_E2E=1 go test -run TestE2EGeneratedCodeUpToDate .
		if err := os.WriteFile("e2e_gen_test.go", src, 0o644); err != nil {
			t.Fatalf("WriteFile(e2e_gen_test.go): %v", err)
		}
		return
	}
	onDisk, err := os.ReadFile("e2e_gen_test.go")
	if err != nil {
		t.Fatalf("ReadFile(e2e_gen_test.go): %v", err)
	}
	if !bytes.Equal(src, onDisk) {
		t.Fatalf("e2e_gen_test.go is out of date with e2eYAML (SQLGEN_UPDATE_E2E=1 go test -run TestE2EGeneratedCodeUpToDate . regenerates it)")
	}
}

// ============================================================================
// Section 12.2: Compilation Isolation Matrix (12 Orthogonal Schemas)
// ============================================================================

func TestCompilationIsolationMatrix(t *testing.T) {
	schemas := []struct {
		name string
		yaml string
	}{
		{
			name: "1_exec_zero_params",
			yaml: `package: iso1
dialect: postgres
statements:
  - name: VacuumItems
    kind: exec
    sql: DELETE FROM items WHERE expired = true
`,
		},
		{
			name: "2_one_scalar_params",
			yaml: `package: iso2
dialect: postgres
statements:
  - name: GetItem
    kind: one
    sql: SELECT id:int64, name:string FROM items WHERE id = :ID:int64
`,
		},
		{
			name: "3_many_scalar_params",
			yaml: `package: iso3
dialect: sqlite
statements:
  - name: ListItemsByScore
    kind: many
    sql: SELECT id:int64, score:float64 FROM items WHERE score >= :MinScore:float64
`,
		},
		{
			name: "4_many_slice_param_presorted",
			yaml: `package: iso4
dialect: postgres
statements:
  - name: ListItemsByIDs
    kind: many
    sql: SELECT id:int64 FROM items WHERE id IN (:IDs:[]int64 ASC)
`,
		},
		{
			name: "5_many_conditional_block_only",
			yaml: `package: iso5
dialect: mysql
statements:
  - name: ListItemsOpt
    kind: many
    sql: |
      SELECT id:int64 FROM items WHERE 1=1 {?OnlyActive: AND active = 1 }
`,
		},
		{
			name: "6_many_select_cols_only",
			yaml: `package: iso6
dialect: postgres
statements:
  - name: ListItemsProjection
    kind: many
    args: struct
    sql: SELECT id:int64, name:string, created_at:time.Time FROM items
`,
		},
		{
			name: "7_many_order_only",
			yaml: `package: iso7
dialect: postgres
statements:
  - name: ListItemsOrdered
    kind: many
    sql: SELECT id:int64, score:int64 FROM items ORDER BY :Order:sqlgen.Order(score, id)
`,
		},
		{
			name: "8_many_order_and_seek",
			yaml: `package: iso8
dialect: sqlite
statements:
  - name: PageItems
    kind: many
    sql: SELECT id:int64, score:int64 FROM items WHERE :Seek:sqlgen.Seek ORDER BY :Order:sqlgen.Order(score, id) LIMIT :Limit:int64
`,
		},
		{
			name: "9_subquery_and_parent_query",
			yaml: `package: iso9
dialect: postgres
statements:
  - name: SubActiveIDs
    kind: subquery
    sql: SELECT id FROM items WHERE active = :Active:bool
  - name: ListFromSub
    kind: many
    sql: SELECT id:int64 FROM items WHERE id IN (:Sub:sqlgen.Query)
`,
		},
		{
			name: "10_exec_tuple_batch_and_stream",
			yaml: `package: iso10
dialect: postgres
statements:
  - name: InsertItems
    kind: exec
    sql: INSERT INTO items (id, name) VALUES :Rows:[]ItemInput(ID:int64 ASC, Name:string)
`,
		},
		{
			name: "11_many_tuple_batch_returning",
			yaml: `package: iso11
dialect: postgres
statements:
  - name: InsertItemsReturning
    kind: many
    sql: INSERT INTO items (id, payload) VALUES :Rows:[]ItemPayload(ID:int64, Payload:[]byte ASC) RETURNING id:int64
`,
		},
		{
			name: "12_pipeline_scalar_and_row_types",
			yaml: `package: iso12
dialect: postgres
statements:
  - name: TouchItem
    kind: exec
    sql: UPDATE items SET updated_at = :UpdatedAt:time.Time WHERE id = :ID:int64
  - name: GetItemScore
    kind: one
    sql: SELECT score:int64 FROM items WHERE id = :ID:int64
`,
		},
		{
			name: "13_scalar_returns",
			yaml: `package: iso13
dialect: postgres
statements:
  - name: CreatedAt
    kind: one
    sql: SELECT created_at:time.Time FROM items WHERE id = :ID:int64
  - name: Payloads
    kind: many
    sql: SELECT payload:[]byte FROM items WHERE tenant = :Tenant:string
  - name: Notes
    kind: many
    sql: SELECT note:*string FROM items ORDER BY id
  - name: ScoreRow
    kind: one
    result_type: ScoreOnly
    sql: SELECT score:int64 FROM items WHERE id = :ID:int64
  - name: ScoreRowsAgain
    kind: many
    result_type: ScoreOnly
    sql: SELECT score:int64 FROM items WHERE tenant = :Tenant:string
  - name: ScoresByID
    kind: many
    sql: |
      INSERT INTO items (id, score) VALUES :Rows:[]ScoreIn(ID:int64 ASC, Score:int64) RETURNING score:int64
`,
		},
	}

	for _, tc := range schemas {
		t.Run(tc.name, func(t *testing.T) {
			src, err := sqlgen.GenerateFromYAML([]byte(tc.yaml))
			if err != nil {
				t.Fatalf("GenerateFromYAML(%s) failed: %v", tc.name, err)
			}
			typeCheckGenerated(t, tc.name, src)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, tc.name+".go", src, parser.AllErrors)
			if err != nil {
				t.Fatalf("ParseFile(%s) failed: %v\n%s", tc.name, err, string(src))
			}

			// Verify every imported package is referenced in the AST
			usedPkgs := make(map[string]bool)
			ast.Inspect(file, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if id, ok := sel.X.(*ast.Ident); ok {
						usedPkgs[id.Name] = true
					}
				}
				return true
			})
			for _, imp := range file.Imports {
				pathVal, _ := strconv.Unquote(imp.Path.Value)
				pkgIdent := filepath.Base(pathVal)
				if imp.Name != nil {
					pkgIdent = imp.Name.Name
				}
				if !usedPkgs[pkgIdent] {
					t.Fatalf("schema %s emitted unused import %q (ident %q)\n%s", tc.name, pathVal, pkgIdent, string(src))
				}
			}
		})
	}
}

// ============================================================================
// Section 12.5 PBT 2: Keyset Pagination Total-Order Equivalence (250 Rows, 16 Masks)
// ============================================================================

func TestPropertyKeysetPaginationEquivalence(t *testing.T) {
	db, q := setupE2EDB(t)
	ctx := context.Background()

	rng := rand.New(rand.NewPCG(20260404, 99))
	groups := []string{"alpha", "beta", "gamma"}
	priorities := []int64{1, 2, 3, 4}
	timestamps := []int64{1000, 1001, 1002, 1003}

	const totalRows = 250
	inputs := make([]QuadInput, totalRows)
	for i := 0; i < totalRows; i++ {
		inputs[i] = QuadInput{
			ID:        int64(i + 1),
			GroupKey:  groups[rng.IntN(len(groups))],
			Priority:  priorities[rng.IntN(len(priorities))],
			CreatedTs: timestamps[rng.IntN(len(timestamps))],
		}
	}
	if _, err := q.InsertQuadBatch(ctx, inputs); err != nil {
		t.Fatalf("InsertQuadBatch(250): %v", err)
	}

	pageSizes := []int64{1, 2, 7, 25, 250, 500}

	// Test all 16 sort-direction masks over the 4 columns (group_key, priority, created_ts, id)
	for mask := 0; mask < 16; mask++ {
		var terms [4]PageQuadSortTerm
		if (mask & 1) == 0 {
			terms[0] = PageQuadOrderByGroupKey.Asc()
		} else {
			terms[0] = PageQuadOrderByGroupKey.Desc()
		}
		if (mask & 2) == 0 {
			terms[1] = PageQuadOrderByPriority.Asc()
		} else {
			terms[1] = PageQuadOrderByPriority.Desc()
		}
		if (mask & 4) == 0 {
			terms[2] = PageQuadOrderByCreatedTs.Asc()
		} else {
			terms[2] = PageQuadOrderByCreatedTs.Desc()
		}
		if (mask & 8) == 0 {
			terms[3] = PageQuadOrderByID.Asc()
		} else {
			terms[3] = PageQuadOrderByID.Desc()
		}
		orderSpec := OrderPageQuad(terms[0], terms[1], terms[2], terms[3])

		// Ground truth: fetch all 250 rows in a single unpaginated query
		expected, err := q.PageQuad(ctx, PageQuadParams{
			Order: orderSpec,
			Limit: 1000,
		})
		if err != nil || len(expected) != totalRows {
			t.Fatalf("mask=%04b ground truth failed: len=%d err=%v", mask, len(expected), err)
		}

		// Verify Postgres ($1) vs SQLite (?) SQL & parameter count consistency on active Seek
		sampleSeek := SeekPageQuad(expected[0])
		qCurried := PageQuadQuery(PageQuadParams{
			Seek:  sampleSeek,
			Order: orderSpec,
			Limit: 10,
		})
		pgSQL := qCurried.BuildSQL(runtime.DialectDollar)
		pgArgs := runtime.AcquireArgsCap(16)
		if err := qCurried.AppendArgs(pgArgs, runtime.DialectDollar); err != nil {
			t.Fatalf("AppendArgs(DialectPostgres): %v", err)
		}
		// In Postgres, 4-column seek uses 4 args ($1..$4) + 1 Limit arg ($5) = 5 args total
		if len(*pgArgs) != 5 || !strings.Contains(pgSQL, "$5") {
			t.Fatalf("mask=%04b unexpected Postgres args=%d sql=%s", mask, len(*pgArgs), pgSQL)
		}
		runtime.ReleaseArgs(pgArgs)

		for _, pageSize := range pageSizes {
			var collected []QuadItem
			var seek runtime.Seek
			for page := 0; page <= totalRows+2; page++ {
				rows, err := q.PageQuad(ctx, PageQuadParams{
					Seek:  seek,
					Order: orderSpec,
					Limit: pageSize,
				})
				if err != nil {
					t.Fatalf("mask=%04b pageSize=%d page=%d err=%v", mask, pageSize, page, err)
				}
				if len(rows) == 0 {
					break
				}
				collected = append(collected, rows...)
				seek = SeekPageQuad(rows[len(rows)-1])
			}
			if !slices.Equal(collected, expected) {
				t.Fatalf("mask=%04b pageSize=%d pagination mismatch (got %d rows, want %d)", mask, pageSize, len(collected), len(expected))
			}
		}
	}
	_ = db
}

// ============================================================================
// Section 12.5 PBT 3: Presort Immutability & Monotonicity (500 Inputs)
// ============================================================================

func TestPropertyPresortImmutabilityAndMonotonicity(t *testing.T) {
	rng := rand.New(rand.NewPCG(777, 888))

	for iterIdx := 0; iterIdx < 500; iterIdx++ {
		n := 1 + rng.IntN(32)

		// 1. 1D Slice Presort (IDs:[]int64 ASC in ListUsersByIDs)
		origIDs := make([]int64, n)
		for i := range origIDs {
			switch iterIdx % 4 {
			case 0: // random with duplicates
				origIDs[i] = int64(rng.IntN(10))
			case 1: // already sorted
				origIDs[i] = int64(i)
			case 2: // reverse sorted
				origIDs[i] = int64(n - i)
			default:
				origIDs[i] = rng.Int64N(1000) - 500
			}
		}
		snapshotIDs := slices.Clone(origIDs)

		// Auto-presort via curried Query argument expansion
		qVal := ListUsersByIDsQuery(origIDs)
		argBuf := runtime.AcquireArgsCap(n)
		if err := qVal.AppendArgs(argBuf, runtime.DialectQuestion); err != nil {
			t.Fatalf("iter %d AppendArgs: %v", iterIdx, err)
		}
		// Caller's input slice MUST NOT be mutated!
		if !slices.Equal(origIDs, snapshotIDs) {
			t.Fatalf("iter %d: auto-presort mutated caller's 1D slice!", iterIdx)
		}
		// Bound arguments must be monotonically non-decreasing
		for i := 1; i < len(*argBuf); i++ {
			prev := (*argBuf)[i-1].(int64)
			curr := (*argBuf)[i].(int64)
			if prev > curr {
				t.Fatalf("iter %d: 1D bind args not sorted at %d: %d > %d", iterIdx, i, prev, curr)
			}
		}
		runtime.ReleaseArgs(argBuf)

		// 2. 2D Tuple Slice Presort (Rows:[]BatchBonusInput(ID:int64 ASC, Bonus:int64))
		origTuples := make([]BatchBonusInput, n)
		for i := range origTuples {
			origTuples[i] = BatchBonusInput{
				ID:    int64(rng.IntN(16)),
				Bonus: int64(i * 10),
			}
		}
		snapshotTuples := slices.Clone(origTuples)

		argBuf2 := runtime.AcquireArgsCap(n * 2)
		if err := expandUpdateUsersBatchFromValuesParams(argBuf2, runtime.DialectQuestion, paramsUpdateUsersBatchFromValues{Rows: origTuples}); err != nil {
			t.Fatalf("iter %d expandUpdateUsersBatchFromValuesParams: %v", iterIdx, err)
		}
		if !slices.Equal(origTuples, snapshotTuples) {
			t.Fatalf("iter %d: auto-presort mutated caller's tuple slice!", iterIdx)
		}
		for i := 1; i < n; i++ {
			prevID := (*argBuf2)[(i-1)*2].(int64)
			currID := (*argBuf2)[i*2].(int64)
			if prevID > currID {
				t.Fatalf("iter %d: tuple bind args not sorted at row %d: %d > %d", iterIdx, i, prevID, currID)
			}
		}
		runtime.ReleaseArgs(argBuf2)
	}
}

// ============================================================================
// Section 12.5 PBT 5: Pipeline Wire vs. Sequential Parity
// ============================================================================

type parityWireConn struct {
	sqliteDB *sql.DB
}

func (c *parityWireConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *parityWireConn) Close() error                        { return nil }
func (c *parityWireConn) Begin() (driver.Tx, error)           { return &parityWireTx{}, nil }

func (c *parityWireConn) ExecPipeline(ctx context.Context, ops []runtime.PipelineOp) error {
	// Execute the pipeline batch against the mirror SQLite connection using the exact wire contract
	condVals := make(map[int]bool)
	for i := range ops {
		op := &ops[i]
		taken := true
		for _, g := range op.Input.BranchGates {
			if condVals[g.CondID] != g.WantTrue {
				taken = false
				break
			}
		}
		if !taken {
			op.Skipped = true
			if op.OnSkipped != nil {
				op.OnSkipped()
			}
			continue
		}
		if op.Decorators.IsAbort {
			return runtime.TranslateWireError(op, errors.New("SQLSTATE 22003: integer out of range"))
		}
		if op.HasResultCols {
			// Render clean core statement for mirror SQLite execution
			coreSQL := runtime.BuildComposedSQL(op.Desc, &runtime.RenderInput{
				ColMask:   op.Input.ColMask,
				FlagMask:  op.Input.FlagMask,
				OrderSpec: op.Input.OrderSpec,
				HasSeek:   op.Input.HasSeek,
				SliceLens: op.Input.SliceLens,
				TupleLens: op.Input.TupleLens,
				Subs:      op.Input.Subs,
			}, runtime.DialectQuestion)
			rows, err := c.sqliteDB.QueryContext(ctx, coreSQL, op.Args...)
			if err != nil {
				return err
			}
			n, scanErr := op.ScanWireRows(rows)
			_ = rows.Close()
			if scanErr != nil {
				return scanErr
			}
			op.RowCount = n
		} else {
			coreSQL := runtime.BuildComposedSQL(op.Desc, &runtime.RenderInput{
				ColMask:   op.Input.ColMask,
				FlagMask:  op.Input.FlagMask,
				OrderSpec: op.Input.OrderSpec,
				HasSeek:   op.Input.HasSeek,
				SliceLens: op.Input.SliceLens,
				TupleLens: op.Input.TupleLens,
				Subs:      op.Input.Subs,
			}, runtime.DialectQuestion)
			res, err := c.sqliteDB.ExecContext(ctx, coreSQL, op.Args...)
			if err != nil {
				return err
			}
			aff, _ := res.RowsAffected()
			op.RowCount = aff
		}
		if op.Decorators.AssertRows >= 0 && op.RowCount != op.Decorators.AssertRows {
			return runtime.TranslateWireError(op, errors.New("SQLSTATE 22003: numeric value out of range"))
		}
		if op.OnDone != nil {
			op.OnDone(op.RowCount, nil)
		}
		if op.Decorators.ProduceCond != nil {
			v := op.Decorators.ProduceCond.EvalGoFn()
			op.CondVal = v
			op.CondSet = true
			condVals[op.Decorators.CondID] = v
		}
		for _, ec := range op.ExtraConds {
			if ec.Cond != nil {
				condVals[ec.CondID] = ec.Cond.EvalGoFn()
			}
		}
	}
	return nil
}

type parityWireTx struct{}

func (t *parityWireTx) Commit() error   { return nil }
func (t *parityWireTx) Rollback() error { return nil }

type parityWireConnector struct {
	sqliteDB *sql.DB
}

func (c *parityWireConnector) Connect(context.Context) (driver.Conn, error) {
	return &parityWireConn{sqliteDB: c.sqliteDB}, nil
}

func (c *parityWireConnector) Driver() driver.Driver { return nil }

func TestPropertyPipelineWireVsSequentialParity(t *testing.T) {
	seqDB, seqQ := setupE2EDB(t)
	wireMirrorDB, _ := setupE2EDB(t)
	wireDB := sql.OpenDB(runtime.WrapConnector(&parityWireConnector{sqliteDB: wireMirrorDB}))
	defer func() { _ = wireDB.Close() }()
	wireQ := New(wireDB)
	defer func() { _ = wireQ.Close() }()

	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		if _, err := seqQ.CreateUser(ctx, i, 1, fmt.Sprintf("u%d@p.com", i), "member", i*10, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := wireMirrorDB.Exec(`INSERT INTO users (id, org_id, email, role, score, bio) VALUES (?, 1, ?, 'member', ?, '')`, i, fmt.Sprintf("u%d@p.com", i), i*10); err != nil {
			t.Fatal(err)
		}
	}

	rng := rand.New(rand.NewPCG(4242, 9999))
	errCustomAbort := errors.New("custom pipeline abort")

	for trial := 0; trial < 40; trial++ {
		targetID := int64(1 + rng.IntN(7)) // IDs 1..5 exist, 6..7 do not exist
		minScore := int64(rng.IntN(60))
		triggerAbort := rng.IntN(5) == 0

		runDAG := func(q *Queries) (User, error, bool, bool, error) {
			var getFut *runtime.RowFuture[User]
			var ifRes *runtime.IfResult
			err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
				getFut = p.GetUserByID(targetID)
				cond := getFut.WhereCol("score >= ?", func(u User) bool {
					return u.Score >= minScore
				}, minScore).And(getFut.Found())

				ifRes = p.If(cond, func(p *Pipeline) {
					p.CreateUser(100+int64(trial), 1, "then@p.com", "admin", 99, "ok").AssertRows(1)
				}, func(p *Pipeline) {
					if triggerAbort {
						p.Abort(errCustomAbort)
					} else {
						p.CreateUser(200+int64(trial), 1, "else@p.com", "guest", 1, "else").AssertFound()
					}
				})
				return nil
			})
			u, getErr := getFut.Get()
			return u, getErr, ifRes.Taken(), ifRes.Skipped(), err
		}

		uSeq, getErrSeq, takenSeq, skippedSeq, errSeq := runDAG(seqQ)
		uWire, getErrWire, takenWire, skippedWire, errWire := runDAG(wireQ)

		if (errSeq != nil) != (errWire != nil) || !errors.Is(errSeq, errCustomAbort) == errors.Is(errWire, errCustomAbort) {
			t.Fatalf("trial %d: pipeline error mismatch: seq=%v wire=%v", trial, errSeq, errWire)
		}
		if errSeq == nil {
			if uSeq != uWire || (getErrSeq != nil) != (getErrWire != nil) || takenSeq != takenWire || skippedSeq != skippedWire {
				t.Fatalf("trial %d parity mismatch: seq=(%+v, %v, taken=%v) wire=(%+v, %v, taken=%v)",
					trial, uSeq, getErrSeq, takenSeq, uWire, getErrWire, takenWire)
			}
		}
	}
	_ = seqDB
}

// ============================================================================
// Section 12.2: MaxOpenConns(1) Live SQLite Deadlock Gate
// ============================================================================

func TestMaxOpenConns1DeadlockGate(t *testing.T) {
	db, q := setupE2EDB(t)
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Warm statements into Tier 2 while MaxOpenConns(1) is active
	for i := int64(1); i <= 4; i++ {
		if _, err := q.CreateUser(ctx, i, 1, fmt.Sprintf("m%d@x.com", i), "admin", i*10, ""); err != nil {
			t.Fatalf("CreateUser(%d): %v", i, err)
		}
	}

	// 2. Seq iterator with early break followed immediately by a write on the single connection
	for u, err := range q.ListUsersByIDsSeq(ctx, []int64{1, 2, 3, 4}) {
		if err != nil {
			t.Fatalf("ListUsersByIDsSeq: %v", err)
		}
		if u.ID == 2 {
			break
		}
	}
	if _, err := q.GetUserByID(ctx, 1); err != nil {
		t.Fatalf("GetUserByID after Seq early break: %v", err)
	}

	// 3. InTx executing Tier-2 cached statements, unseen variations, Batch, Stream, and nested InTx
	if err := q.InTx(ctx, nil, func(txQ *Queries) error {
		if _, err := txQ.GetUserByID(ctx, 1); err != nil {
			return err
		}
		// Unseen slice length inside transaction when MaxOpenConns == 1 must not deadlock trying to db.PrepareContext
		if _, err := txQ.ListUsersByIDs(ctx, []int64{1, 2, 3}); err != nil {
			return err
		}
		if _, err := txQ.InsertUsersBatchReturning(ctx, []BatchUserInput{
			{ID: 10, Name: "b10", Points: 1},
			{ID: 11, Name: "b11", Points: 2},
		}); err != nil {
			return err
		}
		if _, err := txQ.UpdateUsersBatchFromValues(ctx, []BatchBonusInput{
			{ID: 10, Bonus: 5},
			{ID: 11, Bonus: 5},
		}); err != nil {
			return err
		}
		return txQ.InTx(ctx, nil, func(innerQ *Queries) error {
			_, err := innerQ.GetUserByID(ctx, 2)
			return err
		})
	}); err != nil {
		t.Fatalf("InTx with MaxOpenConns(1) deadlocked or failed: %v", err)
	}

	// 4. Pipeline and PipelineTx on MaxOpenConns(1)
	p := q.Pipeline(ctx)
	rf := p.GetUserByID(1)
	if err := p.Flush(); err != nil {
		t.Fatalf("Pipeline.Flush on MaxOpenConns(1): %v", err)
	}
	if u, err := rf.Get(); err != nil || u.ID != 1 {
		t.Fatalf("Pipeline GetUserByID(1): %+v, %v", u, err)
	}

	if err := q.PipelineTx(ctx, nil, func(ptx *Pipeline) error {
		uFut := ptx.GetUserByID(2)
		ptx.If(uFut.Found(), func(ptx *Pipeline) {
			ptx.CreateUser(99, 1, "p99@x.com", "member", 50, "bio")
		}, nil)
		return nil
	}); err != nil {
		t.Fatalf("PipelineTx on MaxOpenConns(1): %v", err)
	}

	// 5. Manual WithTx + Close
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txQ := q.WithTx(tx)
	if _, err := txQ.GetUserByID(ctx, 99); err != nil {
		_ = tx.Rollback()
		t.Fatalf("WithTx GetUserByID(99): %v", err)
	}
	_ = txQ.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// ============================================================================
// Additional Generator & E2E Coverage Tests
// ============================================================================

func TestGeneratorAndE2EAdditionalCoverage(t *testing.T) {
	_, q := setupE2EDB(t)
	ctx := context.Background()

	// Exercise Stream methods, Pipeline methods, and helper methods on e2e_gen_test.go
	_ = New(nil)
	_ = q.CacheStats()
	_ = q.WithPrepared(false)

	batchIn := []BatchUserInput{
		{ID: 2, Name: "b", Points: 20},
		{ID: 1, Name: "a", Points: 10},
	}
	if _, err := q.InsertUsersBatchReturning(ctx, batchIn); err != nil {
		t.Fatalf("InsertUsersBatchReturning: %v", err)
	}
	if _, err := runtime.AppendTo(nil, q.InsertUsersBatchReturningSeq(ctx, []BatchUserInput{{ID: 3, Name: "c", Points: 30}})); err != nil {
		t.Fatalf("InsertUsersBatchReturningInto: %v", err)
	}
	for _, err := range q.InsertUsersBatchReturningSeq(ctx, []BatchUserInput{{ID: 4, Name: "d", Points: 40}}) {
		if err != nil {
			t.Fatalf("InsertUsersBatchReturningSeq: %v", err)
		}
	}
	_ = runtime.Each(q.InsertUsersBatchReturningSeq(ctx, []BatchUserInput{{ID: 5, Name: "e", Points: 50}}), func(InsertUsersBatchReturningRow) error { return nil })
	_ = runtime.Each(q.InsertUsersBatchReturningSeq(ctx, []BatchUserInput{{ID: 6, Name: "f", Points: 60}}), func(InsertUsersBatchReturningRow) error { return nil })
	if _, err := q.InsertUsersBatchReturningStream(ctx, runtime.FromSlice([]BatchUserInput{{ID: 7, Name: "g", Points: 70}})); err != nil {
		t.Fatalf("InsertUsersBatchReturningStream: %v", err)
	}

	bonusIn := []BatchBonusInput{{ID: 2, Bonus: 20}, {ID: 1, Bonus: 10}}
	if _, err := q.UpdateUsersBatchFromValues(ctx, bonusIn); err != nil {
		t.Fatalf("UpdateUsersBatchFromValues: %v", err)
	}

	quadIn := []QuadInput{{ID: 100, GroupKey: "g", Priority: 1, CreatedTs: 1}}
	if _, err := q.InsertQuadBatch(ctx, quadIn); err != nil {
		t.Fatalf("InsertQuadBatch: %v", err)
	}
	if _, err := q.InsertQuadBatchStream(ctx, runtime.FromSlice([]QuadInput{{ID: 101, GroupKey: "g", Priority: 1, CreatedTs: 2}})); err != nil {
		t.Fatalf("InsertQuadBatchStream: %v", err)
	}
	if _, err := q.InsertQuadBatchStream(ctx, runtime.FromSlice([]QuadInput{{ID: 102, GroupKey: "g", Priority: 1, CreatedTs: 3}})); err != nil {
		t.Fatalf("InsertQuadBatchStream: %v", err)
	}

	_, _ = q.CreateUser(ctx, 1, 1, "u1@c.com", "admin", 10, "bio")

	for _, err := range q.SelectUserProfileSubsetSeq(ctx, SelectUserProfileSubsetParams{OrgID: 1}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = runtime.Each(q.SelectUserProfileSubsetSeq(ctx, SelectUserProfileSubsetParams{OrgID: 1}), func(SelectUserProfileSubsetRow) error { return nil })

	for _, err := range q.SearchUsersCoupledSeq(ctx, SearchUsersCoupledParams{}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = runtime.Each(q.SearchUsersCoupledSeq(ctx, SearchUsersCoupledParams{}), func(SearchUsersCoupledRow) error { return nil })
	_ = SearchUsersCoupledQuery(SearchUsersCoupledParams{})

	for _, err := range q.PageUsersSeq(ctx, PageUsersParams{Limit: 5}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = runtime.Each(q.PageUsersSeq(ctx, PageUsersParams{Limit: 5}), func(PageUsersRow) error { return nil })
	_, _ = runtime.AppendTo(nil, q.PageUsersSeq(ctx, PageUsersParams{Limit: 5}))

	for _, err := range q.UsersFromCTEAndInSubquerySeq(ctx, PageUsersQuery(PageUsersParams{Limit: 5}), SubUserIDsByScoreQuery(0)) {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = runtime.Each(q.UsersFromCTEAndInSubquerySeq(ctx, PageUsersQuery(PageUsersParams{Limit: 5}), SubUserIDsByScoreQuery(0)), func(UsersFromCTEAndInSubqueryRow) error { return nil })
	_, _ = runtime.AppendTo(nil, q.UsersFromCTEAndInSubquerySeq(ctx, PageUsersQuery(PageUsersParams{Limit: 5}), SubUserIDsByScoreQuery(0)))
	_ = UsersFromCTEAndInSubqueryQuery(PageUsersQuery(PageUsersParams{Limit: 5}), SubUserIDsByScoreQuery(0))

	for _, err := range q.PageQuadSeq(ctx, PageQuadParams{Limit: 5}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	_ = runtime.Each(q.PageQuadSeq(ctx, PageQuadParams{Limit: 5}), func(QuadItem) error { return nil })
	_, _ = runtime.AppendTo(nil, q.PageQuadSeq(ctx, PageQuadParams{Limit: 5}))

	// Pipeline queueing methods for all statements
	pl := q.Pipeline(ctx)
	_ = pl.ListUsersByIDs([]int64{1})
	_ = pl.SelectUserProfileSubset(SelectUserProfileSubsetParams{OrgID: 1})
	_ = pl.SearchUsersCoupled(SearchUsersCoupledParams{})
	_ = pl.PageUsers(PageUsersParams{Limit: 5})
	_ = pl.UsersFromCTEAndInSubquery(PageUsersQuery(PageUsersParams{Limit: 5}), SubUserIDsByScoreQuery(0))
	_ = pl.InsertUsersBatchReturning([]BatchUserInput{{ID: 20, Name: "p", Points: 1}})
	_ = pl.UpdateUsersBatchFromValues([]BatchBonusInput{{ID: 20, Bonus: 1}})
	_ = pl.InsertQuadBatch([]QuadInput{{ID: 200, GroupKey: "g", Priority: 1, CreatedTs: 1}})
	_ = pl.PageQuad(PageQuadParams{Limit: 5})
	if err := pl.Flush(); err != nil {
		t.Fatalf("pl.Flush(): %v", err)
	}
}

func TestGeneratorValidatorAndParserBranches(t *testing.T) {
	// Exercise remaining validation and parser branches in generator.go
	errCases := []sqlgen.Config{
		{PackageName: "p", Statements: []sqlgen.StatementSpec{{Name: "S", Kind: sqlgen.KindExec, SQL: "DELETE FROM t"}}},
		{PackageName: "123bad", Dialect: "postgres", Statements: []sqlgen.StatementSpec{{Name: "S", Kind: sqlgen.KindExec, SQL: "DELETE FROM t"}}},
		{PackageName: "p", Dialect: "oracle", Statements: []sqlgen.StatementSpec{{Name: "S", Kind: sqlgen.KindExec, SQL: "DELETE FROM t"}}},
		{PackageName: "p", Dialect: "postgres", Statements: nil},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "Dup", Kind: sqlgen.KindExec, SQL: "DELETE FROM t"},
			{Name: "Dup", Kind: sqlgen.KindExec, SQL: "DELETE FROM t"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "123bad", Kind: sqlgen.KindExec, SQL: "DELETE FROM t"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "EmptySQL", Kind: sqlgen.KindExec, SQL: "   "},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "BadKind", Kind: "invalid_kind", SQL: "SELECT 1"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "ExecWithCols", Kind: sqlgen.KindExec, SQL: "SELECT id:int64 FROM t"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "OneNoCols", Kind: sqlgen.KindOne, SQL: "UPDATE t SET a = 1"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "SeekWithoutOrder", Kind: sqlgen.KindMany, SQL: "SELECT id:int64 FROM t WHERE :Seek:sqlgen.Seek"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "OrderUnknownCol", Kind: sqlgen.KindMany, SQL: "SELECT id:int64 FROM t ORDER BY :Order:sqlgen.Order(missing_col)"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "NonOrderablePresort", Kind: sqlgen.KindMany, SQL: "SELECT id:int64 FROM t WHERE meta IN (:Metas:[]json.RawMessage ASC)"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "ConflictingParamTypes", Kind: sqlgen.KindMany, SQL: "SELECT id:int64 FROM t WHERE id = :ID:int64 OR id = :ID:string"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "ConflictingSortDirs", Kind: sqlgen.KindMany, SQL: "SELECT id:int64 FROM t WHERE id IN (:IDs:[]int64 ASC) OR id IN (:IDs:[]int64 DESC)"},
		}},
		{PackageName: "p", Dialect: "postgres", Statements: []sqlgen.StatementSpec{
			{Name: "T1", Kind: sqlgen.KindExec, SQL: "INSERT INTO t (a, b) VALUES :Rows:[]SharedTup(A:int64, B:string)"},
			{Name: "T2", Kind: sqlgen.KindExec, SQL: "INSERT INTO t (a, b) VALUES :Rows:[]SharedTup(A:int64, B:int64)"},
		}},
	}
	for i, cfg := range errCases {
		if _, err := sqlgen.Generate(cfg); err == nil {
			t.Fatalf("errCase %d (%+v) expected error, got nil", i, cfg)
		}
	}

	// Test DESC slice presort, time.Time / []byte tuple presort, and scalar_result on kind: many
	advYAML := `package: adv
dialect: mariadb
statements:
  - name: ListScoresDesc
    kind: many
    sql: |
      SELECT score:int64
      FROM items
      WHERE tag IN (:Tags:[]string DESC)
        AND created_at IN (:Times:[]time.Time ASC)
        AND hash IN (:Hashes:[][]byte DESC)
  - name: UpsertEvents
    kind: exec
    sql: |
      INSERT INTO events (id, ts, raw_hash)
      VALUES :Rows:[]EventTuple('const ''lit''', "dq""col", ` + "`bt``col`" + `, ID:int64::bigint DESC, TS:time.Time::timestamptz(6) ASC, RawHash:[]byte::bytea[] DESC)
      ON DUPLICATE KEY UPDATE ts = VALUES(ts)
  - name: DeleteBatchWithFlags
    kind: exec
    args: struct
    sql: |
      DELETE FROM events
      WHERE org_id = :OrgID:int64::bigint
        AND id IN (:IDs:[]int64 ASC)
        {?WithCutoff: AND 'a''b' = 'a''b' AND "c""d" = "c""d" AND ` + "`e``f` = `e``f`" + ` AND $$dlr$$ = $$dlr$$ -- line cmt
        /* block cmt */ AND ts < :Cutoff:int64 }
  - name: ComplexParenTuple
    kind: many
    sql: |
      INSERT INTO events (id, meta)
      VALUES :Rows:[]ParenTuple((1 + 2), 'x''y', "a""b", ` + "`c``d`" + `, $$tag$$, -- cmt
      /* blk */ ID:int64, Meta:string)
      RETURNING id:int64 -- trailing comment
      /* block comment before terminator */ ;
  - name: ReuseInlineSliceAndNonValuesTuple
    kind: many
    sql: |
      SELECT u.id:int64, u.name AS HTTPServerVal:string
      FROM users u
      WHERE u.id IN (:IDs:[]int64)
        AND u.url = :http_server_url:string
        AND u.id IN (:IDs ASC)
        AND u.id = :_type:int64
        AND u.id = :_type
        AND u.hash IN (:Hashes:[][]byte)
        AND u.code IN (:Codes:[]int64 DESC)
        AND u.code IN (:Codes DESC)
        AND u.tag IN (:Tags:[]string)
        AND u.tag IN (:Tags)
        AND (u.id, u.name) IN (:Pairs:[]PairItem(UserIDVal:int64, Name:string))
  - name: StreamTupleWithExtraScalar
    kind: exec
    sql: |
      INSERT INTO events (id, meta)
      VALUES :Rows:[]ParenTuple(ID:int64, Meta:string)
      ON CONFLICT (id) DO UPDATE SET meta = :Fallback:string
`
	if _, err := sqlgen.GenerateFromYAML([]byte(advYAML)); err != nil {
		t.Fatalf("GenerateFromYAML(advYAML) failed: %v", err)
	}

	// Test WriteFileAtomic and GenerateFromYAMLFile
	tmpDir := t.TempDir()
	cfgFile := filepath.Join(tmpDir, "sqlgen.yaml")
	outFile := filepath.Join(tmpDir, "custom_out.go")
	validFileYAML := `package: filepkg
dialect: sqlite
statements:
  - name: Ping
    kind: one
    sql: SELECT 1 AS id:int64
`
	if err := sqlgen.WriteFileAtomic(cfgFile, []byte(validFileYAML)); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	if err := sqlgen.GenerateFromYAMLFile(cfgFile, ""); err != nil {
		t.Fatalf("GenerateFromYAMLFile(default out): %v", err)
	}
	if err := sqlgen.GenerateFromYAMLFile(cfgFile, outFile); err != nil {
		t.Fatalf("GenerateFromYAMLFile: %v", err)
	}
	if err := sqlgen.GenerateFromYAMLFile(filepath.Join(tmpDir, "missing.yaml"), ""); err == nil {
		t.Fatalf("expected error on missing YAML file")
	}
	badCfgFile := filepath.Join(tmpDir, "bad_yaml.yaml")
	_ = sqlgen.WriteFileAtomic(badCfgFile, []byte("unknown_key: 1\n"))
	if err := sqlgen.GenerateFromYAMLFile(badCfgFile, ""); err == nil {
		t.Fatalf("expected error on invalid YAML file")
	}
	badSQLCfgFile := filepath.Join(tmpDir, "bad_sql.yaml")
	_ = sqlgen.WriteFileAtomic(badSQLCfgFile, []byte("package: p\ndialect: sqlite\nstatements:\n  - name: S\n    kind: one\n    sql: SELECT 1\n"))
	if err := sqlgen.GenerateFromYAMLFile(badSQLCfgFile, ""); err == nil {
		t.Fatalf("expected error on invalid SQL in YAML file")
	}
	if err := sqlgen.WriteFileAtomic(tmpDir, []byte("x")); err == nil {
		t.Fatalf("expected WriteFileAtomic error when target is a directory")
	}
	// A missing directory is created; a parent that is a regular file cannot be.
	if err := sqlgen.WriteFileAtomic(filepath.Join(tmpDir, "no_such_dir", "x.go"), []byte("x")); err != nil {
		t.Fatalf("WriteFileAtomic must create a missing directory: %v", err)
	}
	if err := sqlgen.WriteFileAtomic(filepath.Join(tmpDir, "no_such_dir", "x.go", "y.go"), []byte("x")); err == nil {
		t.Fatalf("expected WriteFileAtomic error when a parent is a regular file")
	}

	// Test YAML error paths in ParseYAML and Generate
	badYAMLs := []string{
		"",
		"package: p\n---\nextra: doc\n",
		"package: p\nstatements:\n  - name: S\n    kind: exec\n    sql: DELETE FROM t\n",                    // dialect is required
		"package: p\ndialect: pg\nstatements:\n  - name: S\n    kind: exec\n    sql: DELETE FROM t\n",       // aliases are gone
		"package: p\ndialect: dollar\nstatements:\n  - name: S\n    kind: exec\n    sql: DELETE FROM t\n",   // aliases are gone
		"package: p\ndialect: Postgres\nstatements:\n  - name: S\n    kind: exec\n    sql: DELETE FROM t\n", // exact names only
		"package: p\ndialect: sqlite\nstatements:\n  - name: S\n    kind: query\n    sql: SELECT 1\n",       // kind: query is gone (use subquery)
	}
	for i, by := range badYAMLs {
		if _, err := sqlgen.GenerateFromYAML([]byte(by)); err == nil {
			t.Fatalf("badYAML[%d] expected error, got nil:\n%s", i, by)
		}
	}
}

// ============================================================================
// Section 12.6: Native Go Fuzzing Targets (FuzzParseYAML here, FuzzParseSQL in parse_internal_test.go)
// ============================================================================

func FuzzParseYAML(f *testing.F) {
	f.Add([]byte(e2eYAML))
	f.Add([]byte("package: p\ndialect: sqlite\nstatements:\n  - name: S1\n    kind: one\n    sql: SELECT id:int64 FROM t WHERE id = :ID:int64\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := sqlgen.ParseYAML(data)
		if err != nil {
			return
		}
		src, err := sqlgen.Generate(cfg)
		if err == nil && len(src) > 0 {
			fset := token.NewFileSet()
			if _, parseErr := parser.ParseFile(fset, "fuzz_yaml.go", src, parser.AllErrors); parseErr != nil {
				t.Fatalf("GenerateFromYAML succeeded but emitted invalid Go syntax: %v\nYAML:\n%s\nCode:\n%s", parseErr, string(data), string(src))
			}
		}
	})
}

// Branch-gated INSERTs are rewritten to INSERT ... SELECT, where PostgreSQL does not infer placeholder
// types from the target columns, so the dollar dialect must carry per-placeholder casts. Other
// dialects and statement kinds must not.
func TestGeneratedDescriptorsCarryPlaceholderCastsOnlyForDollarInserts(t *testing.T) {
	const stmts = `
statements:
  - name: InsertOne
    kind: exec
    sql: INSERT INTO t (id, name, ok, at, n) VALUES (:id:int64, :name:string, :ok:bool, :at:time.Time, :n:uint64)
  - name: InsertMany
    kind: exec
    sql: INSERT INTO t (id, name, blob) VALUES :rows:[]Row(ID:int64, Name:string, Blob:[]byte, Odd:uint64)
  - name: UpdateOne
    kind: exec
    sql: UPDATE t SET name = :name:string WHERE id = :id:int64
`
	gen := func(dialect string) string {
		out, err := sqlgen.GenerateFromYAML([]byte("package: p\nout: p_gen.go\ndialect: " + dialect + "\n" + stmts))
		if err != nil {
			t.Fatalf("%s: %v", dialect, err)
		}
		return string(out)
	}
	pg := gen("postgres")
	for _, want := range []string{
		`{Kind: runtime.SlotScalar, Cast: "bigint"}`,
		`{Kind: runtime.SlotScalar, Cast: "text"}`,
		`{Kind: runtime.SlotScalar, Cast: "boolean"}`,
		`{Kind: runtime.SlotScalar, Cast: "timestamptz"}`,
		`Casts: []string{"bigint", "text", "bytea", ""}`,
	} {
		if !strings.Contains(pg, want) {
			t.Errorf("postgres output missing %s", want)
		}
	}
	// uint64 has no safe mapping: its slot stays untyped; and UPDATE statements never get casts.
	if strings.Count(pg, "Cast:") != 4 || strings.Count(pg, "Casts:") != 1 {
		t.Errorf("unexpected number of casts in postgres output:\n%s", pg)
	}
	if my := gen("mysql"); strings.Contains(my, "Cast") {
		t.Error("mysql (question) output must not contain PostgreSQL casts")
	}
}

// ----------------------------------------------------------------------------------------------
// Inputs found by independent review that used to generate uncompilable or silently wrong code.
// ----------------------------------------------------------------------------------------------

func genYAML(dialect, statements string) ([]byte, error) {
	return sqlgen.GenerateFromYAML([]byte("package: p\nout: p_gen.go\ndialect: " + dialect + "\nstatements:\n" + statements))
}

func TestGeneratorRejectsInputsThatCannotGenerateCorrectCode(t *testing.T) {
	var sixtyFive []string
	for i := 0; i < 65; i++ {
		sixtyFive = append(sixtyFive, fmt.Sprintf("c%d:int64", i))
	}
	for _, tc := range []struct{ name, stmts, want string }{
		{"flag and param share a Go field", "  - name: Q\n    kind: many\n    sql: |\n      SELECT id:int64 FROM t WHERE 1=1 {?Name: AND name = :name:string}\n", `collides with parameter "name"`},
		{"flag named SelectCols", "  - name: Q\n    kind: many\n    sql: |\n      SELECT id:int64 FROM t WHERE 1=1 {?SelectCols: AND x = 1}\n", "SelectCols projection mask"},
		{"param named SelectCols in params struct", "  - name: Q\n    kind: many\n    args: struct\n    sql: |\n      SELECT id:int64 FROM t WHERE x = :select_cols:int64\n", "SelectCols projection mask"},
		{"unconditional column after a conditional one", "  - name: Q\n    kind: many\n    sql: |\n      SELECT id:int64 {?WithName: , name:string}, created:time.Time FROM t\n", "follows a conditional"},
		{"more than 64 columns", "  - name: Q\n    kind: many\n    sql: |\n      SELECT " + strings.Join(sixtyFive, ", ") + " FROM t\n", "more than 64 unconditional output columns"},
		{"result_type is a predeclared type", "  - name: Q\n    kind: many\n    result_type: int64\n    sql: |\n      SELECT id:int64 FROM t\n", `result_type "int64" is a predeclared Go identifier`},
		{"result_type is a predeclared type, several columns", "  - name: Q\n    kind: many\n    result_type: string\n    sql: |\n      SELECT id:int64, name:string FROM t\n", `result_type "string" is a predeclared Go identifier`},
		{"result_type is a predeclared non-type", "  - name: Q\n    kind: one\n    result_type: nil\n    sql: |\n      SELECT id:int64 FROM t\n", `result_type "nil" is a predeclared Go identifier`},
		{"result_type is any", "  - name: Q\n    kind: one\n    result_type: any\n    sql: |\n      SELECT id:int64 FROM t\n", `result_type "any" is a predeclared Go identifier`},
		{"result_type is not an identifier", "  - name: Q\n    kind: one\n    result_type: time.Time\n    sql: |\n      SELECT id:int64 FROM t\n", `invalid result_type identifier`},
		{"result_type collides with generated type", "  - name: Q\n    kind: many\n    result_type: Queries\n    sql: |\n      SELECT id:int64 FROM t\n", `result_type "Queries" collides`},
		{"tuple struct collides with generated type", "  - name: Q\n    kind: exec\n    sql: |\n      INSERT INTO t(a) VALUES :rows:[]Pipeline(A:int64)\n", `tuple struct name "Pipeline" collides`},
		{"statement named like a generated method", "  - name: Close\n    kind: exec\n    sql: DELETE FROM t\n", `statement name "Close" collides`},
		// Unexported type names could collide with the file's own unexported declarations (sqlDialect,
		// descQ, paramsQ, ...) and generate code that does not compile.
		{"unexported result_type", "  - name: Q\n    kind: one\n    result_type: paramsQ\n    sql: |\n      SELECT id:int64, n:int64 FROM t\n", `result_type "paramsQ" must be an exported Go identifier`},
		{"unexported tuple struct", "  - name: Q\n    kind: exec\n    sql: |\n      INSERT INTO t(a) VALUES :rows:[]sqlDialect(A:int64)\n", `tuple struct name "sqlDialect" must be an exported Go identifier`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := genYAML("postgres", tc.stmts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestGeneratorHandlesTrickyButValidInputs(t *testing.T) {
	for _, tc := range []struct {
		name, stmts string
		check       func(t *testing.T, src string)
	}{
		{"parameter named like a generated local", "  - name: Q\n    kind: exec\n    sql: |\n      UPDATE t SET a = :in:string, b = :item:int64, c = :err:int64, d = :len:int64 WHERE id = :zero:int64\n",
			func(t *testing.T, src string) {
				for _, want := range []string{"inParam string", "itemParam int64", "errParam int64", "lenParam int64", "zeroParam int64"} {
					if !strings.Contains(src, want) {
						t.Errorf("missing %q", want)
					}
				}
			}},
		{"shared result_type with different Order lists", "  - name: A\n    kind: many\n    result_type: M\n    sql: |\n      SELECT name:string, age:int64 FROM t WHERE :After:sqlgen.Seek ORDER BY :Sort:sqlgen.Order(name, age)\n  - name: B\n    kind: many\n    result_type: M\n    sql: |\n      SELECT name:string, age:int64 FROM u WHERE :After:sqlgen.Seek ORDER BY :Sort:sqlgen.Order(age, name)\n",
			func(t *testing.T, src string) {
				// One helper set per statement; no row-type-keyed duplicate and no ambiguous row.Seek().
				for _, want := range []string{"func SeekA(row M) runtime.Seek", "runtime.NewSeek(row.Name, row.Age)", "func SeekB(row M) runtime.Seek", "runtime.NewSeek(row.Age, row.Name)", "AOrderByName", "BOrderByAge"} {
					if !strings.Contains(src, want) {
						t.Errorf("missing %q", want)
					}
				}
				for _, bad := range []string{"SeekM(", ") Seek()", "OrderM(", "MOrderBy"} {
					if strings.Contains(src, bad) {
						t.Errorf("unexpected row-type-keyed helper %q", bad)
					}
				}
			}},
		{"line comment inside a tuple body", "  - name: Q\n    kind: exec\n    sql: |\n      INSERT INTO t(a,b) VALUES :Rows:[]R(A:int64, -- note: x\n        B:string)\n",
			func(t *testing.T, src string) {
				if !strings.Contains(src, "-- note: x\\n") {
					t.Error("comment not preserved verbatim (with its newline)")
				}
				if strings.Contains(src, "type R struct") && strings.Contains(src, "\tNote ") {
					t.Error("comment text was parsed as a tuple field")
				}
			}},
		{"trailing line comment and block comment in a tuple body", "  - name: Q\n    kind: exec\n    sql: |\n      INSERT INTO t(a,b) VALUES :Rows:[]R(A:int64 /* a:int64 */, B:string -- trailing)\n      )\n", nil},
		{"dollar quoted text in a tuple body", "  - name: Q\n    kind: exec\n    sql: |\n      INSERT INTO t(a,b) VALUES :Rows:[]R(A:int64, $q$B:string)$q$, B:string)\n", nil},
		{"DISTINCT ON stays in the projection prefix", "  - name: Q\n    kind: many\n    sql: |\n      SELECT DISTINCT ON (a) a:int64, b:string FROM t\n",
			func(t *testing.T, src string) {
				if !strings.Contains(src, `"SELECT DISTINCT ON (a) "`) || !strings.Contains(src, `SelectColExprs: []string{"a", "b"}`) {
					t.Errorf("DISTINCT ON leaked into the first column expression:\n%s", src)
				}
			}},
		{"IS NOT DISTINCT FROM and qualified keywords in a projection", "  - name: Q\n    kind: many\n    sql: |\n      SELECT t.id:int64, t.limit:int64, a IS NOT DISTINCT FROM b AS same:bool FROM t WHERE t.offset > :o:int64\n",
			func(t *testing.T, src string) {
				if !regexp.MustCompile(`\bSame\s+bool\b`).MatchString(src) || !regexp.MustCompile(`\bLimit\s+int64\b`).MatchString(src) {
					t.Error("projection ended early: output columns were lost")
				}
			}},
		{"shared result_type, only one statement ordered", "  - name: A\n    kind: many\n    result_type: M\n    sql: |\n      SELECT name:string, age:int64 FROM t ORDER BY :Sort:sqlgen.Order(name, age)\n  - name: B\n    kind: many\n    result_type: M\n    sql: |\n      SELECT name:string, age:int64 FROM u\n", nil},
		{"64 unconditional columns is allowed", "  - name: Q\n    kind: many\n    sql: |\n      SELECT " + func() string {
			var c []string
			for i := 0; i < 64; i++ {
				c = append(c, fmt.Sprintf("c%d:int64", i))
			}
			return strings.Join(c, ", ")
		}() + " FROM t\n", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src, err := genYAML("postgres", tc.stmts)
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			typeCheckGenerated(t, "tricky", src)
			if tc.check != nil {
				tc.check(t, string(src))
			}
		})
	}
}

// Descriptors carry the statement name so runtime diagnostics (failed assertions) can name it.
func TestGeneratedDescriptorsCarryStatementName(t *testing.T) {
	src, err := genYAML("postgres", "  - name: InsertAudit\n    kind: exec\n    sql: INSERT INTO t(a) VALUES (:a:int64)\n  - name: Pick\n    kind: many\n    sql: SELECT id:int64 FROM t WHERE x = :x:int64\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"InsertAudit", "Pick"} {
		if !regexp.MustCompile(`\bName:\s+"` + name + `",`).Match(src) {
			t.Errorf("generated descriptor for %s carries no Name", name)
		}
	}
}

// Diagnostics found by a clean-room user: each used to produce a misleading error or uncompilable output.
func TestGeneratorDiagnosticsAreActionable(t *testing.T) {
	for _, tc := range []struct{ name, stmts, want string }{
		{"expression annotation without alias", "  - name: Q\n    kind: one\n    sql: SELECT COUNT(*):int64 FROM t\n",
			`output column expression "COUNT(*)" needs an alias: write COUNT(*) AS name:int64`},
		{"expression annotation without alias, later column", "  - name: Q\n    kind: one\n    sql: SELECT id:int64, SUM(a + b):float64 FROM t\n",
			`output column expression "SUM(a + b)" needs an alias: write SUM(a + b) AS name:float64`},
		{"untyped parameter hint", "  - name: Q\n    kind: many\n    sql: SELECT id:int64 FROM t WHERE x = :x\n",
			`parameter :x is used before it is declared; declare its type at its first use (:x:int64)`},
		{"package not imported", "  - name: Q\n    kind: many\n    sql: SELECT id:uuid.UUID FROM t\n",
			`type "uuid.UUID" uses package "uuid" which is not imported; add its import path to extra_imports`},
		{"removed ids... spelling without a colon", "  - name: Q\n    kind: many\n    sql: SELECT id:int64 FROM t WHERE id IN (ids...)\n",
			`"ids...": the "..." slice spelling is not supported; declare a slice as :ids:[]Type`},
		{"removed params_struct key", "  - name: Q\n    kind: one\n    params_struct: true\n    sql: SELECT id:int64 FROM t WHERE id = :id:int64\n",
			"params_struct was removed: use args: struct"},
		{"shared result_type column count", "  - name: A\n    kind: many\n    result_type: R\n    sql: SELECT id:int64, name:string FROM t\n" +
			"  - name: B\n    kind: many\n    result_type: R\n    sql: SELECT id:int64 FROM t\n",
			`result_type "R": statement A declares 2 columns (ID int64, Name string), statement B declares 1 (ID int64)`},
		{"shared result_type column type", "  - name: A\n    kind: many\n    result_type: R\n    sql: SELECT id:int64, name:string FROM t\n" +
			"  - name: B\n    kind: many\n    result_type: R\n    sql: SELECT id:int64, name:*string FROM t\n",
			`result_type "R": column 2 is Name string in statement A but Name *string in statement B`},
		{"package not imported in a param", "  - name: Q\n    kind: many\n    sql: SELECT id:int64 FROM t WHERE x IN (:x:[]decimal.Decimal)\n",
			`uses package "decimal" which is not imported`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := genYAML("postgres", tc.stmts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
	// With the import declared (any version/major-suffix spelling) the same statement generates and type-checks.
	for _, imp := range []string{"github.com/google/uuid", "github.com/gofrs/uuid/v5", "gopkg.in/x/uuid.v1", "github.com/x/go-uuid"} {
		src, err := sqlgen.GenerateFromYAML([]byte("package: p\nout: p_gen.go\ndialect: postgres\nextra_imports:\n  - " + imp +
			"\nstatements:\n  - name: Q\n    kind: many\n    sql: SELECT id:uuid.UUID FROM t\n"))
		if err != nil {
			t.Errorf("%s: %v", imp, err)
			continue
		}
		if !strings.Contains(string(src), `"`+imp+`"`) {
			t.Errorf("%s: import not emitted", imp)
		}
	}
}

// The widest sqlgen.Order the generator accepts is exactly what the runtime can encode.
func TestOrderDirectiveColumnLimitMatchesRuntime(t *testing.T) {
	stmt := func(n int) string {
		var sel, ord []string
		for i := 1; i <= n; i++ {
			sel = append(sel, fmt.Sprintf("c%d:int64", i))
			ord = append(ord, fmt.Sprintf("c%d", i))
		}
		return "  - name: Q\n    kind: many\n    sql: |\n      SELECT " + strings.Join(sel, ", ") + " FROM t WHERE :a:sqlgen.Seek ORDER BY :s:sqlgen.Order(" + strings.Join(ord, ", ") + ")\n"
	}
	if _, err := genYAML("postgres", stmt(runtime.MaxOrderCols)); err != nil {
		t.Fatalf("%d columns must be accepted: %v", runtime.MaxOrderCols, err)
	}
	_, err := genYAML("postgres", stmt(runtime.MaxOrderCols+1))
	want := fmt.Sprintf("supports at most %d columns (got %d)", runtime.MaxOrderCols, runtime.MaxOrderCols+1)
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("want error containing %q, got %v", want, err)
	}
}

// Diagnostics found by the second clean-room round: each of these generated misleading, uncompilable or
// silently wrong output instead of an error at generate time.
func TestGeneratorRejectsMisuseAtGenerateTime(t *testing.T) {
	one := func(sql string) string {
		return "  - name: Q\n    kind: many\n    sql: |\n      " + sql + "\n"
	}
	for _, tc := range []struct{ name, dialect, stmts, want string }{
		{"lowercase statement name", "postgres", "  - name: lower\n    kind: exec\n    sql: DELETE FROM t\n",
			`statement name "lower" must be an exported Go identifier (start with an upper-case letter), e.g. "Lower"`},
		{"tuple slice compared with =", "postgres", one("SELECT id:int64 FROM t WHERE x = :a:[]T(A:int64)"),
			"tuple slice parameter :a:[]T"},
		{"tuple slice outside VALUES or IN", "mysql", one("SELECT id:int64 FROM t WHERE x > :a:[]T(A:int64)"),
			"only valid after VALUES or inside IN (...)"},
		{"1D slice compared with =", "postgres", one("SELECT id:int64 FROM t WHERE x = :x:[]int64"),
			"slice parameter :x:[]int64 is only valid as the whole list of IN (...) / NOT IN (...)"},
		{"1D slice repeated outside IN", "sqlite", one("SELECT id:int64 FROM t WHERE x IN (:x:[]int64) OR y = :x"),
			"slice parameter :x:[]int64 is only valid as the whole list of IN (...)"},
		{"1D slice as a function argument", "mysql", one("SELECT id:int64 FROM t WHERE x = GREATEST(:x:[]int64)"),
			"only valid as the whole list of IN (...)"},
		{"1D slice sharing an IN list", "postgres", one("SELECT id:int64 FROM t WHERE x IN (:x:[]int64, 7)"),
			"only valid as the whole list of IN (...)"},
		{"literal $1 placeholder (postgres)", "postgres", one("SELECT id:int64 FROM t WHERE x = $1"),
			`literal placeholder "$1"`},
		{"literal $2 placeholder (cockroachdb)", "cockroachdb", one("SELECT id:int64 FROM t WHERE x = :x:int64 AND y = $2"),
			`literal placeholder "$2"`},
		{"literal ? placeholder (mysql)", "mysql", one("SELECT id:int64 FROM t WHERE x = ?"),
			`literal placeholder "?"`},
		{"literal ? placeholder (sqlite)", "sqlite", one("SELECT id:int64 FROM t WHERE x = :x:int64 AND y = ?"),
			`literal placeholder "?"`},
		{"RETURNING on mysql", "mysql", "  - name: Q\n    kind: many\n    sql: |\n      INSERT INTO t (a) VALUES (:a:int64) RETURNING id:int64\n",
			"MySQL has no RETURNING"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := genYAML(tc.dialect, tc.stmts); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}

	// The same shapes that are legitimate must keep generating.
	for _, tc := range []struct{ name, dialect, stmts string }{
		{"placeholder-looking text in literals and comments", "postgres", one("SELECT id:int64 FROM t WHERE a = '$1' AND b = \"$2\" -- $3\n      AND c = $$ $4 $$ /* $5 */")},
		{"? in literals and comments", "mysql", one("SELECT id:int64 FROM t WHERE a = '?' AND b = `?` -- ?\n      AND c = :c:int64 /* ? */")},
		{"jsonb ? operator on postgres", "postgres", one("SELECT id:int64 FROM t WHERE data ? 'k' AND data ?| array['a']")},
		{"$1 text on mysql is not a placeholder", "mysql", one("SELECT id:int64 FROM t WHERE cost = '$1'")},
		{"conditional block marker", "mysql", one("SELECT id:int64 FROM t WHERE 1=1 {?F: AND x = :x:int64}")},
		{"tuple slice after VALUES", "mysql", "  - name: Q\n    kind: exec\n    sql: INSERT INTO t (a) VALUES :r:[]T(A:int64)\n"},
		{"tuple slice inside IN", "postgres", one("SELECT id:int64 FROM t WHERE (a, b) IN (:r:[]T(A:int64, B:int64))")},
		{"tuple slice inside NOT IN", "postgres", one("SELECT id:int64 FROM t WHERE (a, b) NOT IN (:r:[]T(A:int64, B:int64))")},
		{"tuple slice in FROM (VALUES", "postgres", one("SELECT v.a:int64 FROM (VALUES :r:[]T(A:int64)) AS v(a)")},
		{"RETURNING on mariadb", "mariadb", "  - name: Q\n    kind: many\n    sql: |\n      INSERT INTO t (a) VALUES (:a:int64) RETURNING id:int64\n"},
		{"RETURNING on sqlite", "sqlite", "  - name: Q\n    kind: many\n    sql: |\n      INSERT INTO t (a) VALUES (:a:int64) RETURNING id:int64\n"},
		{"RETURNING on cockroachdb", "cockroachdb", "  - name: Q\n    kind: many\n    sql: |\n      DELETE FROM t WHERE a = :a:int64 RETURNING id:int64\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := genYAML(tc.dialect, tc.stmts); err != nil {
				t.Fatalf("must generate: %v", err)
			}
		})
	}
}

// An out: path whose directory does not exist yet is created instead of failing on a temp file.
func TestGenerateFromYAMLFileCreatesOutputDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "sqlgen.yaml")
	body := "package: p\nout: gen/deep/p_gen.go\ndialect: postgres\nstatements:\n  - name: Q\n    kind: exec\n    sql: DELETE FROM t\n"
	if err := os.WriteFile(cfg, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := sqlgen.GenerateFromYAMLFile(cfg, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "gen", "deep", "p_gen.go")); err != nil {
		t.Fatal(err)
	}
}

// SQLite accepts RETURNING (DOCS.md "Engine support"): the generated shape runs on the sqlite driver.
func TestSQLiteAcceptsReturning(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"INSERT INTO t (id, v) VALUES (1, 10) RETURNING id",
		"UPDATE t SET v = v + 1 WHERE id = 1 RETURNING v",
		"DELETE FROM t WHERE id = 1 RETURNING id",
	} {
		var n int64
		if err := db.QueryRow(q).Scan(&n); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// SelectCols constants exist only where SelectCols does: a single-column statement has no projection, so it
// gets no <Stmt>Col<X> / <Stmt>ColsAll constants nobody could use.
func TestSelectColsConstantsOnlyWithProjection(t *testing.T) {
	src, err := genYAML("postgres", "  - name: One\n    kind: many\n    sql: SELECT id:int64 FROM t\n"+
		"  - name: Two\n    kind: many\n    sql: SELECT id:int64, name:string FROM t\n")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		want bool
	}{{"OneColID", false}, {"OneColsAll", false}, {"TwoColID", true}, {"TwoColName", true}, {"TwoColsAll", true}} {
		if got := bytes.Contains(src, []byte("\t"+c.name+" ")); got != c.want {
			t.Errorf("constant %s emitted = %v, want %v", c.name, got, c.want)
		}
	}
}

// A <Name>Query constructor exists only for statements a composition slot can embed: a SELECT (also
// WITH ... SELECT) or a kind: subquery. PostgreSQL rejects INSERT/UPDATE/DELETE ... RETURNING inside
// IN (...) ("syntax error at or near INTO"), so a data-modifying statement gets none.
func TestQueryConstructorOnlyForSelect(t *testing.T) {
	src, err := genYAML("postgres", ""+
		"  - name: Sel\n    kind: many\n    sql: SELECT id:int64 FROM t WHERE x = :x:int64\n"+
		"  - name: WithSel\n    kind: one\n    sql: WITH c AS (SELECT 1 AS id) SELECT id:int64 FROM c\n"+
		"  - name: Sub\n    kind: subquery\n    sql: SELECT id FROM t\n"+
		"  - name: Ins\n    kind: one\n    sql: INSERT INTO t (x) VALUES (:x:int64) RETURNING id:int64\n"+
		"  - name: Upd\n    kind: many\n    sql: UPDATE t SET x = :x:int64 RETURNING id:int64\n"+
		"  - name: Del\n    kind: many\n    sql: DELETE FROM t WHERE x = :x:int64 RETURNING id:int64\n")
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{"Sel": true, "WithSel": true, "Sub": true, "Ins": false, "Upd": false, "Del": false} {
		if got := bytes.Contains(src, []byte("func "+name+"Query(")); got != want {
			t.Errorf("%sQuery generated = %v, want %v", name, got, want)
		}
	}
}
