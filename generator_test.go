package sqlgen_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/CAFxX/sqlgen"
	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

// e2eYAML is the canonical YAML specification used to generate the E2E test suite methods below.
const e2eYAML = `version: "1"
package: sqlgen_test
out: e2e_gen_test.go
runtime_import: github.com/CAFxX/sqlgen/runtime
dialect: sqlite

statements:
  - name: CreateUser
    kind: exec
    sql: |
      INSERT INTO users (id, org_id, email, role, score, bio)
      VALUES (ID:int64, OrgID:int64, Email:string, Role:string, Score:int64, Bio:string)

  - name: GetUserByID
    kind: one
    result_type: User
    sql: |
      SELECT id:int64, org_id:int64, email:string, role:string, score:int64, bio:string
      FROM users
      WHERE id = ID:int64

  - name: ListUsersByIDs
    kind: many
    result_type: User
    sql: |
      SELECT id:int64, org_id:int64, email:string, role:string, score:int64, bio:string
      FROM users
      WHERE id IN (IDs:[]int64)
      ORDER BY id ASC

  - name: SelectUserProfileSubset
    kind: many
    params_struct: true
    sql: |
      SELECT u.id:int64, u.email:string, u.role:string, u.score:int64
      {?WithExtra: , u.bio:string }
      FROM users u
      WHERE u.org_id = OrgID:int64
      ORDER BY u.id ASC

  - name: SearchUsersCoupled
    kind: many
    sql: |
      SELECT u.id:int64, u.email:string
      {?WithOrg: , o.name:string }
      FROM users u
      {?WithOrg: LEFT JOIN orgs o ON o.id = u.org_id }
      WHERE 1=1
      {?WithOrg: AND o.tier IN (OrgTiers:[]string) AND u.org_id IN (OrgSub:sqlgen.Query) }
      {?WithRoles: AND u.role IN (Roles:[]string) }
      ORDER BY u.id ASC

  - name: SubOrgsByMinID
    kind: subquery
    sql: |
      SELECT id FROM orgs WHERE id >= MinID:int64 {?OnlyActive: AND active = ActiveVal:int64 }

  - name: PageUsers
    kind: many
    params_struct: true
    sql: |
      SELECT id:int64, role:string, score:int64, email:string
      FROM users
      WHERE Seek:sqlgen.Seek
      {?RoleFilter: AND role = FilterRole:string }
      ORDER BY Order:sqlgen.Order(role, score, id)
      LIMIT Limit:int64

  - name: UsersFromCTEAndInSubquery
    kind: many
    sql: |
      WITH filtered AS (CTESub:sqlgen.Query)
      SELECT f.id:int64, f.email:string
      FROM filtered f
      WHERE f.id IN (IDSub:sqlgen.Query)
      ORDER BY f.id ASC

  - name: SubUserIDsByScore
    kind: subquery
    sql: |
      SELECT id FROM users WHERE score >= MinScore:int64

  - name: InsertUsersBatchReturning
    kind: many
    sql: |
      INSERT INTO batch_users (id, name, points)
      VALUES Rows:[]BatchUserInput(ID:int64, Name:string, Points:int64)
      RETURNING id:int64, name:string, points:int64

  - name: UpdateUsersBatchFromValues
    kind: exec
    sql: |
      WITH updates(id, bonus) AS (
        VALUES Rows:[]BatchBonusInput(ID:int64, Bonus:int64)
      )
      UPDATE batch_users
      SET points = batch_users.points + (SELECT u.bonus FROM updates u WHERE u.id = batch_users.id)
      WHERE batch_users.id IN (SELECT u.id FROM updates u)
`

func TestParserProtectedContexts(t *testing.T) {
	yamlStr := `version: "1"
package: store
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
      WHERE u.id = ID:int64
        AND (u.meta := 'x:y') IS NOT NULL
`
	src, err := sqlgen.GenerateFromYAML([]byte(yamlStr))
	if err != nil {
		t.Fatalf("GenerateFromYAML failed on protected contexts: %v", err)
	}
	code := string(src)
	normalizedCode := strings.Join(strings.Fields(code), " ")
	// Ensure fake parameters/flags inside comments, quotes, dollar-quotes, brackets, and :: / := were NOT extracted
	for _, forbidden := range []string{"FakeParam", "FakeFlag", "AnotherFake", "AnotherFlag", "NoFlag", "not_a_param", "not_param"} {
		if strings.Contains(code, " "+forbidden+" ") || strings.Contains(code, forbidden+" bool") {
			t.Fatalf("protected context leaked identifier %q into generated code:\n%s", forbidden, code)
		}
	}
	// Ensure real columns and parameter ID were extracted
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
			cfg := &sqlgen.Config{
				Version:     "1",
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
	yamlStr := `version: "1"
package: store
dialect: postgres
statements:
  - name: ComplexProjection
    kind: many
    sql: |
      WITH cte AS (
        SELECT c.id, c.amount * Rate:float64 AS scaled
        FROM charges c
        WHERE c.org_id = OrgID:int64
      )
      SELECT
        u.id:int64,
        u.price * Tax:float64 AS total:float64
        {?WithBonus: , u.bonus + BonusAdd:float64 AS final_bonus:float64 }
      FROM users u
      JOIN cte ON cte.id = u.id
      WHERE u.active = Active:bool
`
	src, err := sqlgen.GenerateFromYAML([]byte(yamlStr))
	if err != nil {
		t.Fatalf("GenerateFromYAML failed: %v", err)
	}
	code := string(src)
	normalizedCode := strings.Join(strings.Fields(code), " ")
	// Row struct should have ID, Total, FinalBonus (NOT Rate, OrgID, Tax, BonusAdd, Active)
	for _, rowField := range []string{"ID int64", "Total float64", "FinalBonus float64"} {
		if !strings.Contains(normalizedCode, rowField) {
			t.Fatalf("expected row field %q in generated code:\n%s", rowField, code)
		}
	}
	// Params struct should have Rate, OrgID, Tax, BonusAdd, Active, WithBonus, SelectCols
	for _, paramField := range []string{"Rate float64", "OrgID int64", "Tax float64", "BonusAdd float64", "Active bool", "WithBonus bool", "SelectCols uint64"} {
		if !strings.Contains(normalizedCode, paramField) {
			t.Fatalf("expected param field %q in generated code:\n%s", paramField, code)
		}
	}
}

func TestYAMLStrictValidationAndForms(t *testing.T) {
	// Test all 3 YAML forms for params and columns + auto-imports
	yamlStr := `version: "1"
package: store
dialect: postgres
extra_imports:
  - time
statements:
  - name: GetAuditRecord
    kind: many
    sql: |
      SELECT id, created_at, payload, amount, ip_addr, endpoint
      FROM audits
      WHERE id = :ID AND org_id IN (:org_ids...) AND tag IN (:tags)
    params:
      - { name: ID, type: int64 }
      - org_ids...: int64
      - "tags:string..."
    columns:
      - { name: id, type: int64 }
      - created_at: time.Time
      - "payload:json.RawMessage"
      - "amount:*big.Int"
      - "ip_addr:netip.Addr"
      - "endpoint:*url.URL"
`
	src, err := sqlgen.GenerateFromYAML([]byte(yamlStr))
	if err != nil {
		t.Fatalf("valid 3-form YAML failed: %v", err)
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

	// Unknown top-level key must fail
	if _, err := sqlgen.ParseYAML([]byte("version: \"1\"\nunknown_top_key: true\n")); err == nil {
		t.Fatalf("expected error on unknown top-level YAML key")
	}

	// Unknown key in ParamSpec mapping must fail with line number
	badParamYAML := `version: "1"
package: store
statements:
  - name: S1
    kind: exec
    sql: DELETE FROM t WHERE id = :ID
    params:
      - name: ID
        type: int64
        bogus_key: 123
`
	if _, err := sqlgen.ParseYAML([]byte(badParamYAML)); err == nil || !strings.Contains(err.Error(), "line") {
		t.Fatalf("expected error with line number on unknown ParamSpec key, got: %v", err)
	}

	// Unknown key in ColumnSpec mapping must fail with line number
	badColYAML := `version: "1"
package: store
statements:
  - name: S1
    kind: one
    sql: SELECT id FROM t
    columns:
      - name: id
        type: int64
        extra_col_key: abc
`
	if _, err := sqlgen.ParseYAML([]byte(badColYAML)); err == nil || !strings.Contains(err.Error(), "line") {
		t.Fatalf("expected error with line number on unknown ColumnSpec key, got: %v", err)
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
				Version:     "1",
				PackageName: "store",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "GetUser",
						Kind: sqlgen.KindOne,
						SQL:  "SELECT id:int64, name:stirng FROM users WHERE id = ID:int64",
					},
				},
			},
			wantErrText: `did you mean "string"?`,
		},
		{
			name: "nullable pointer sort column in Order with Seek rejected",
			cfg: sqlgen.Config{
				Version:     "1",
				PackageName: "store",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "ListUsers",
						Kind: sqlgen.KindMany,
						SQL:  "SELECT id:int64, nickname:*string FROM users WHERE Seek:sqlgen.Seek ORDER BY Order:sqlgen.Order(nickname, id)",
					},
				},
			},
			wantErrText: "COALESCE",
		},
		{
			name: "nullable sql.NullString sort column in Order with Seek rejected",
			cfg: sqlgen.Config{
				Version:     "1",
				PackageName: "store",
				Statements: []sqlgen.StatementSpec{
					{
						Name: "ListUsersNull",
						Kind: sqlgen.KindMany,
						SQL:  "SELECT id:int64, nickname:sql.NullString FROM users WHERE Seek:sqlgen.Seek ORDER BY Order:sqlgen.Order(nickname, id)",
					},
				},
			},
			wantErrText: "COALESCE",
		},
		{
			name: "duplicate column names rejected",
			cfg: sqlgen.Config{
				Version:     "1",
				PackageName: "store",
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
				Version:     "1",
				PackageName: "store",
				Statements: []sqlgen.StatementSpec{
					{
						Name:       "GetUserA",
						Kind:       sqlgen.KindOne,
						ResultType: "SharedUser",
						SQL:        "SELECT id:int64, email:string FROM users WHERE id = ID:int64",
					},
					{
						Name:       "GetUserB",
						Kind:       sqlgen.KindOne,
						ResultType: "SharedUser",
						SQL:        "SELECT id:int64, email:int64 FROM users WHERE id = ID:int64",
					},
				},
			},
			wantErrText: "conflicting shared result_type",
		},
		{
			name: "nested conditional blocks rejected",
			cfg: sqlgen.Config{
				Version:     "1",
				PackageName: "store",
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
			_, err := sqlgen.Generate(&tc.cfg)
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
	// Verify PostgreSQL ($1, $2, ...) SQL rendering across all constructs
	q1 := GetUserByIDQuery(42)
	if got := q1.BuildSQL(runtime.DialectPostgres); got != "SELECT id, org_id, email, role, score, bio\nFROM users\nWHERE id = $1" {
		t.Fatalf("GetUserByID Postgres SQL mismatch:\ngot:  %q", got)
	}

	q2 := ListUsersByIDsQuery([]int64{10, 20, 30})
	if got := q2.BuildSQL(runtime.DialectPostgres); got != "SELECT id, org_id, email, role, score, bio\nFROM users\nWHERE id IN ($1, $2, $3)\nORDER BY id ASC" {
		t.Fatalf("ListUsersByIDs Postgres SQL mismatch:\ngot:  %q", got)
	}

	// SelectCols subsetting (2 of 4 unconditional columns + 1 conditional column)
	q3 := SelectUserProfileSubsetQuery(SelectUserProfileSubsetParams{
		OrgID:      7,
		WithExtra:  true,
		SelectCols: SelectUserProfileSubsetColID | SelectUserProfileSubsetColScore,
	})
	wantQ3 := "SELECT u.id, u.score\n , u.bio \nFROM users u\nWHERE u.org_id = $1\nORDER BY u.id ASC"
	if got := q3.BuildSQL(runtime.DialectPostgres); got != wantQ3 {
		t.Fatalf("SelectUserProfileSubset Postgres SQL mismatch:\ngot:  %q\nwant: %q", got, wantQ3)
	}

	// Composed CTE + IN subquery with continuous $1..$N numbering
	cteSub := PageUsersQuery(PageUsersParams{
		RoleFilter: true,
		FilterRole: "admin",
		Order:      OrderPageUsersRow(OrderPageUsersRowScoreDesc),
		Seek:       runtime.NewSeek("admin", int64(50), int64(5)),
		Limit:      10,
	})
	idSub := SubUserIDsByScoreQuery(30)
	composed := UsersFromCTEAndInSubqueryQuery(cteSub, idSub)
	gotComposed := composed.BuildSQL(runtime.DialectPostgres)
	// Seek on (score DESC, id DESC) has 3 params ($1, $2, $3), FilterRole is $4, Limit is $5, and MinScore is $6!
	if !strings.Contains(gotComposed, "((score < $1) OR (score = $2 AND id < $3))") {
		t.Fatalf("expected $1..$3 keyset predicate in composed Postgres SQL, got:\n%s", gotComposed)
	}
	if !strings.Contains(gotComposed, "AND role = $4") || !strings.Contains(gotComposed, "LIMIT $5") || !strings.Contains(gotComposed, "WHERE score >= $6") {
		t.Fatalf("expected continuous $4, $5, $6 placeholders across composed subqueries, got:\n%s", gotComposed)
	}
}

func setupE2EDB(t *testing.T) (*sql.DB, *Queries) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "e2e.db")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
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

	q := New(db, runtime.WithPromoteThreshold(2))
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

	// Test kind: one (GetUserByID)
	u3, err := q.GetUserByID(ctx, 3)
	if err != nil {
		t.Fatalf("GetUserByID(3): %v", err)
	}
	if u3.ID != 3 || u3.Email != "u3@example.com" || u3.Score != 30 || u3.Bio != "bio-3" {
		t.Fatalf("unexpected GetUserByID(3): %+v", u3)
	}

	// Test empty slice rejection
	if _, err := q.ListUsersByIDs(ctx, nil); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam on empty slice, got %v", err)
	}

	// Test kind: many Slice
	users, err := q.ListUsersByIDs(ctx, []int64{1, 3, 5})
	if err != nil || len(users) != 3 {
		t.Fatalf("ListUsersByIDs: len=%d err=%v", len(users), err)
	}

	// Test kind: many Into (reusing preallocated slice)
	prealloc := make([]User, 0, 8)
	usersInto, err := q.ListUsersByIDsInto(ctx, prealloc, []int64{2, 4})
	if err != nil || len(usersInto) != 2 || cap(usersInto) != 8 {
		t.Fatalf("ListUsersByIDsInto failed: len=%d cap=%d err=%v", len(usersInto), cap(usersInto), err)
	}

	// Test kind: many Seq with early break
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

	// Test kind: many Each
	var eachIDs []int64
	if err := q.ListUsersByIDsEach(ctx, []int64{4, 6}, func(u User) error {
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

	// Select 2 of 4 unconditional columns (ID + Score) plus 1 conditional column (WithExtra -> Bio)
	params := SelectUserProfileSubsetParams{
		OrgID:      1,
		WithExtra:  true,
		SelectCols: SelectUserProfileSubsetColID | SelectUserProfileSubsetColScore,
	}

	// Verify exact 3-column SQL emission
	qBuilt := q.SelectUserProfileSubsetQuery(params).BuildSQL(runtime.DialectQuestion)
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
	// Selected fields (ID, Score, Bio) must be populated; unselected fields (Email, Role) must remain zero
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

	// 1. WithOrg = false (inactive OrgTiers empty slice and inactive uninitialized OrgSub Query MUST NOT error!)
	rowsNoOrg, err := q.SearchUsersCoupled(ctx, SearchUsersCoupledParams{
		WithOrg:   false,
		OrgTiers:  nil,             // empty slice inside disabled block
		OrgSub:    runtime.Query{}, // uninitialized Query inside disabled block
		WithRoles: true,
		Roles:     []string{"admin"},
	})
	if err != nil {
		t.Fatalf("SearchUsersCoupled with disabled WithOrg failed: %v", err)
	}
	if len(rowsNoOrg) != 2 || rowsNoOrg[0].ID != 1 || rowsNoOrg[1].ID != 3 || rowsNoOrg[0].Name != "" {
		t.Fatalf("unexpected rows with WithOrg=false: %+v", rowsNoOrg)
	}

	// 2. WithOrg = true (coupled across SELECT, LEFT JOIN, and WHERE)
	orgSub := q.SubOrgsByMinIDQuery(SubOrgsByMinIDParams{
		MinID:      1,
		OnlyActive: true,
		ActiveVal:  1,
	})
	rowsWithOrg, err := q.SearchUsersCoupled(ctx, SearchUsersCoupledParams{
		WithOrg:   true,
		OrgTiers:  []string{"enterprise", "pro"},
		OrgSub:    orgSub,
		WithRoles: false,
		Roles:     nil, // disabled block empty slice
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

	// Seed 15 users with duplicate roles and duplicate scores to exercise multi-column keyset tie-breaking
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
			order: OrderPageUsersRow(OrderPageUsersRowScoreAsc, OrderPageUsersRowIDAsc),
		},
		{
			name:  "Uniform DESC, DESC (score DESC, auto-tiebreaker id DESC)",
			order: OrderPageUsersRow(OrderPageUsersRowScoreDesc),
		},
		{
			name:  "Mixed ASC, DESC, ASC (role ASC, score DESC, id ASC)",
			order: OrderPageUsersRow(OrderPageUsersRowRoleAsc, OrderPageUsersRowScoreDesc, OrderPageUsersRowIDAsc),
		},
	}

	for _, tc := range orders {
		t.Run(tc.name, func(t *testing.T) {
			// Fetch all 15 rows in one page as ground truth
			expectedAll, err := q.PageUsers(ctx, PageUsersParams{
				Order: tc.order,
				Limit: 100,
			})
			if err != nil || len(expectedAll) != 15 {
				t.Fatalf("full fetch failed: len=%d err=%v", len(expectedAll), err)
			}

			// Now paginate 4 rows at a time using SeekPageUsersRow
			var collected []PageUsersRow
			var seek runtime.Seek
			for page := 0; page < 10; page++ {
				pageRows, err := q.PageUsers(ctx, PageUsersParams{
					Seek:  seek,
					Order: tc.order,
					Limit: 4,
				})
				if err != nil {
					t.Fatalf("page %d failed: %v", page, err)
				}
				if len(pageRows) == 0 {
					break
				}
				collected = append(collected, pageRows...)
				lastRow := pageRows[len(pageRows)-1]
				seek = SeekPageUsersRow(lastRow)
			}

			if len(collected) != len(expectedAll) {
				t.Fatalf("paginated count=%d, expected=%d", len(collected), len(expectedAll))
			}
			for i := range expectedAll {
				if collected[i] != expectedAll[i] {
					t.Fatalf("mismatch at index %d:\ngot:  %+v\nwant: %+v", i, collected[i], expectedAll[i])
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

	// Curried CTE subquery using Order, Seek, and {?RoleFilter: ...}
	cteSub := q.PageUsersQuery(PageUsersParams{
		RoleFilter: true,
		FilterRole: "admin",
		Order:      OrderPageUsersRow(OrderPageUsersRowScoreAsc, OrderPageUsersRowIDAsc),
		Seek:       runtime.NewSeek("admin", int64(20), int64(2)), // after id=2 (score=20) -> ids 3, 4, 5, 6
		Limit:      3,                                             // ids 3, 4, 5
	})
	// Curried IN subquery filtering score >= 40 -> ids 4, 5, 6, 7, 8
	idSub := q.SubUserIDsByScoreQuery(40)

	// Intersection of {3, 4, 5} and {4, 5, 6, 7, 8} -> {4, 5}
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
	// Allow multiple connections so Tier 2 prepares on db even inside transactions
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

	// 1. Recurring 1000-row and 900-row batches -> promoted as exact-size heavy hitters in Tier 2
	for run := 0; run < 2; run++ {
		if _, err := db.Exec(`DELETE FROM batch_users`); err != nil {
			t.Fatal(err)
		}
		inserted1000, err := q.InsertUsersBatchReturningBatch(ctx, makeInsertBatch(1, 1000))
		if err != nil || len(inserted1000) != 1000 {
			t.Fatalf("run %d Insert 1000: len=%d err=%v", run, len(inserted1000), err)
		}
		inserted900, err := q.InsertUsersBatchReturningBatch(ctx, makeInsertBatch(1001, 900))
		if err != nil || len(inserted900) != 900 {
			t.Fatalf("run %d Insert 900: len=%d err=%v", run, len(inserted900), err)
		}
	}

	// Verify that 1000 and 900 are now promoted heavy-hitter sizes and 2900 rows greedily chunks as 1000 + 1000 + 900!
	c1 := q.Cache().PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900, 10000, false)
	c2 := q.Cache().PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900-c1, 10000, false)
	c3 := q.Cache().PlanBatchChunk(stmtIDInsertUsersBatchReturning, 0, 0, 2900-c1-c2, 10000, false)
	if c1 != 1000 || c2 != 1000 || c3 != 900 {
		t.Fatalf("expected greedy chunking 1000 + 1000 + 900 for 2900 rows, got %d + %d + %d", c1, c2, c3)
	}

	// Execute 2900-row batch INSERT ... RETURNING
	if _, err := db.Exec(`DELETE FROM batch_users`); err != nil {
		t.Fatal(err)
	}
	inserted2900, err := q.InsertUsersBatchReturningBatch(ctx, makeInsertBatch(1, 2900))
	if err != nil || len(inserted2900) != 2900 {
		t.Fatalf("Insert 2900 failed: len=%d err=%v", len(inserted2900), err)
	}

	// Execute multi-row batch UPDATE ... FROM (VALUES ...) across all 2900 rows
	updates := make([]BatchBonusInput, 2900)
	for i := 0; i < 2900; i++ {
		updates[i] = BatchBonusInput{ID: int64(i + 1), Bonus: 5}
	}
	updRes, err := q.UpdateUsersBatchFromValuesBatch(ctx, updates)
	if err != nil {
		t.Fatalf("UpdateUsersBatchFromValuesBatch(2900): %v", err)
	}
	aff, _ := updRes.RowsAffected()
	if aff != 2900 {
		t.Fatalf("expected 2900 rows updated, got %d", aff)
	}

	// Verify BatchSeq with early break does NOT open a hidden transaction or roll back completed chunks
	if _, err := db.Exec(`DELETE FROM batch_users`); err != nil {
		t.Fatal(err)
	}
	yielded := 0
	for row, err := range q.InsertUsersBatchReturningBatchSeq(ctx, makeInsertBatch(1, 300)) {
		if err != nil {
			t.Fatalf("BatchSeq error: %v", err)
		}
		yielded++
		if yielded == 260 {
			// Chunk 1 (256 rows) has completed and closed its *sql.Rows without a hidden transaction;
			// verify a nested query inside the loop body can read committed rows from chunk 1.
			var cnt int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM batch_users WHERE id = ?`, row.ID).Scan(&cnt); err != nil {
				t.Fatalf("nested query inside BatchSeq loop failed: %v", err)
			}
			break
		}
	}
	// First chunk of 300 (256 rows) was committed because BatchSeq does not open a hidden rollback transaction
	var countAfterBreak int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM batch_users`).Scan(&countAfterBreak); err != nil {
		t.Fatal(err)
	}
	if countAfterBreak < 256 {
		t.Fatalf("expected at least 256 already-executed chunk rows to persist after early break in BatchSeq, got %d", countAfterBreak)
	}

	// Verify explicit InTx rollback on error
	errBoom := errors.New("rollback test")
	_ = q.InTx(ctx, func(txQ *Queries) error {
		if _, err := txQ.InsertUsersBatchReturningBatch(ctx, makeInsertBatch(5000, 20)); err != nil {
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
	// Ensure GenerateFromYAML([]byte(e2eYAML)) compiles cleanly without error
	src, err := sqlgen.GenerateFromYAML([]byte(e2eYAML))
	if err != nil {
		t.Fatalf("GenerateFromYAML(e2eYAML) failed: %v", err)
	}
	if len(src) == 0 {
		t.Fatalf("expected non-empty generated Go source")
	}
}

// ============================================================================
// Generated E2E code under test (compiled directly into sqlgen_test)
// ============================================================================

type (
	Query = runtime.Query
	Order = runtime.Order
	Seek  = runtime.Seek
)

const sqlDialect = runtime.DialectQuestion

type Queries struct {
	db          *sql.DB
	cache       *runtime.StmtCache
	txCache     *runtime.TxStmtCache
	usePrepared bool
	fastSlots   *[3]atomic.Pointer[runtime.ManagedStmt]
}

func New(db *sql.DB, opts ...runtime.CacheOption) *Queries {
	return NewWithCache(db, runtime.NewStmtCache(opts...))
}

func NewWithCache(db *sql.DB, cache *runtime.StmtCache) *Queries {
	if cache == nil {
		cache = runtime.NewStmtCache()
	}
	return &Queries{
		db:          db,
		cache:       cache,
		usePrepared: true,
		fastSlots:   new([3]atomic.Pointer[runtime.ManagedStmt]),
	}
}

func (q *Queries) Cache() *runtime.StmtCache { return q.cache }

func (q *Queries) Close() error {
	if q.txCache != nil {
		return q.txCache.Close()
	}
	return q.cache.Close()
}

func (q *Queries) WithPrepared(enabled bool) *Queries {
	cp := *q
	cp.usePrepared = enabled
	return &cp
}

func (q *Queries) WithTx(tx *sql.Tx) *Queries {
	cp := *q
	cp.txCache = runtime.NewTxStmtCache(tx)
	return &cp
}

func (q *Queries) InTx(ctx context.Context, fn func(*Queries) error) error {
	return q.InTxOpts(ctx, nil, fn)
}

func (q *Queries) InTxOpts(ctx context.Context, opts *sql.TxOptions, fn func(*Queries) error) error {
	if q.txCache != nil {
		return fn(q)
	}
	tx, err := q.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	tc := runtime.NewTxStmtCache(tx)
	defer func() { _ = tc.Close() }()
	txQ := *q
	txQ.txCache = tc
	if err := fn(&txQ); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

type BatchUserInput struct {
	ID     int64
	Name   string
	Points int64
}

type BatchBonusInput struct {
	ID    int64
	Bonus int64
}

type User struct {
	ID    int64
	OrgID int64
	Email string
	Role  string
	Score int64
	Bio   string
}

type SelectUserProfileSubsetRow struct {
	ID    int64
	Email string
	Role  string
	Score int64
	Bio   string
}

type SearchUsersCoupledRow struct {
	ID    int64
	Email string
	Name  string
}

type PageUsersRow struct {
	ID    int64
	Role  string
	Score int64
	Email string
}

type UsersFromCTEAndInSubqueryRow struct {
	ID    int64
	Email string
}

type InsertUsersBatchReturningRow struct {
	ID     int64
	Name   string
	Points int64
}

const stmtIDCreateUser uint32 = 1

type paramsCreateUser struct {
	ID    int64
	OrgID int64
	Email string
	Role  string
	Score int64
	Bio   string
}

var descCreateUser = &runtime.QueryDescriptor{
	Segments: []string{"INSERT INTO users (id, org_id, email, role, score, bio)\nVALUES (", ", ", ", ", ", ", ", ", ", ", ")"},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotScalar},
		{Kind: runtime.SlotScalar},
		{Kind: runtime.SlotScalar},
		{Kind: runtime.SlotScalar},
		{Kind: runtime.SlotScalar},
		{Kind: runtime.SlotScalar},
	},
}

func expandCreateUserParams(dst *[]any, p paramsCreateUser) error {
	_ = p
	*dst = append(*dst, p.ID)
	*dst = append(*dst, p.OrgID)
	*dst = append(*dst, p.Email)
	*dst = append(*dst, p.Role)
	*dst = append(*dst, p.Score)
	*dst = append(*dst, p.Bio)
	return nil
}

func expandCreateUserArgs(dst *[]any, q *runtime.Query) error {
	return expandCreateUserParams(dst, q.Params.(paramsCreateUser))
}

func computeCreateUserKey(p paramsCreateUser) (runtime.VariantKey, uint64, uint64, int) {
	_ = p
	var colMask uint64
	var flagMask uint64
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	approxArgs := 10
	vk := runtime.MakeVariantKey(stmtIDCreateUser, colMask, flagMask, orderKey)
	return vk, colMask, flagMask, approxArgs
}

func prepareCreateUser(id int64, orgID int64, email string, role string, score int64, bio string) (runtime.Query, runtime.VariantKey, int) {
	p := paramsCreateUser{
		ID:    id,
		OrgID: orgID,
		Email: email,
		Role:  role,
		Score: score,
		Bio:   bio,
	}
	vk, colMask, flagMask, approxArgs := computeCreateUserKey(p)
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	var sliceLens []int
	var tupleLens []int
	var subs []runtime.Query
	shapeTag := runtime.ComputeSubqueryShape(stmtIDCreateUser, colMask, flagMask, orderKey, false)
	qVal := runtime.Query{
		StmtID: stmtIDCreateUser,
		Desc:   descCreateUser,
		Input: runtime.RenderInput{
			ColMask:   colMask,
			FlagMask:  flagMask,
			OrderSpec: orderSpec,
			HasSeek:   hasSeek,
			SliceLens: sliceLens,
			TupleLens: tupleLens,
			Subs:      subs,
		},
		Params:   p,
		Expand:   expandCreateUserArgs,
		ShapeTag: shapeTag,
	}
	return qVal, vk, approxArgs
}

func (q *Queries) CreateUser(ctx context.Context, id int64, orgID int64, email string, role string, score int64, bio string) (sql.Result, error) {
	p := paramsCreateUser{
		ID:    id,
		OrgID: orgID,
		Email: email,
		Role:  role,
		Score: score,
		Bio:   bio,
	}
	vk, colMask, flagMask, approxArgs := computeCreateUserKey(p)
	argBuf := runtime.AcquireArgs(approxArgs)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandCreateUserParams(argBuf, p); err != nil {
		return nil, err
	}
	buildSQL := func() string {
		qVal, _, _ := prepareCreateUser(id, orgID, email, role, score, bio)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	return q.cache.Exec(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDCreateUser, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, &q.fastSlots[0], buildSQL, (*argBuf)...)
}

const stmtIDGetUserByID uint32 = 2

type paramsGetUserByID struct {
	ID int64
}

var descGetUserByID = &runtime.QueryDescriptor{
	Segments: []string{"SELECT id, org_id, email, role, score, bio\nFROM users\nWHERE id = ", ""},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotScalar},
	},
}

func expandGetUserByIDParams(dst *[]any, p paramsGetUserByID) error {
	_ = p
	*dst = append(*dst, p.ID)
	return nil
}

func expandGetUserByIDArgs(dst *[]any, q *runtime.Query) error {
	return expandGetUserByIDParams(dst, q.Params.(paramsGetUserByID))
}

func computeGetUserByIDKey(p paramsGetUserByID) (runtime.VariantKey, uint64, uint64, int) {
	_ = p
	var colMask uint64
	var flagMask uint64
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	approxArgs := 5
	vk := runtime.MakeVariantKey(stmtIDGetUserByID, colMask, flagMask, orderKey)
	return vk, colMask, flagMask, approxArgs
}

func prepareGetUserByID(id int64) (runtime.Query, runtime.VariantKey, int) {
	p := paramsGetUserByID{
		ID: id,
	}
	vk, colMask, flagMask, approxArgs := computeGetUserByIDKey(p)
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	var sliceLens []int
	var tupleLens []int
	var subs []runtime.Query
	shapeTag := runtime.ComputeSubqueryShape(stmtIDGetUserByID, colMask, flagMask, orderKey, false)
	qVal := runtime.Query{
		StmtID: stmtIDGetUserByID,
		Desc:   descGetUserByID,
		Input: runtime.RenderInput{
			ColMask:   colMask,
			FlagMask:  flagMask,
			OrderSpec: orderSpec,
			HasSeek:   hasSeek,
			SliceLens: sliceLens,
			TupleLens: tupleLens,
			Subs:      subs,
		},
		Params:   p,
		Expand:   expandGetUserByIDArgs,
		ShapeTag: shapeTag,
	}
	return qVal, vk, approxArgs
}

func GetUserByIDQuery(id int64) runtime.Query {
	qVal, _, _ := prepareGetUserByID(id)
	return qVal
}

func (q *Queries) GetUserByIDQuery(id int64) runtime.Query {
	return GetUserByIDQuery(id)
}

func (q *Queries) GetUserByID(ctx context.Context, id int64) (User, error) {
	var zero User
	p := paramsGetUserByID{
		ID: id,
	}
	vk, colMask, flagMask, approxArgs := computeGetUserByIDKey(p)
	_ = colMask
	argBuf := runtime.AcquireArgs(approxArgs)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandGetUserByIDParams(argBuf, p); err != nil {
		return zero, err
	}
	buildSQL := func() string {
		qVal, _, _ := prepareGetUserByID(id)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	row, ms, err := q.cache.QueryRow(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDGetUserByID, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, &q.fastSlots[1], buildSQL, (*argBuf)...)
	if err != nil {
		return zero, err
	}
	var item User
	var scanBuf [6]any
	scanBuf[0] = &item.ID
	scanBuf[1] = &item.OrgID
	scanBuf[2] = &item.Email
	scanBuf[3] = &item.Role
	scanBuf[4] = &item.Score
	scanBuf[5] = &item.Bio
	scanSlice := scanBuf[:6]
	err = row.Scan(scanSlice...)
	if ms != nil {
		_ = ms.Release()
	}
	if err != nil {
		return zero, err
	}
	return item, nil
}

const stmtIDListUsersByIDs uint32 = 3

type paramsListUsersByIDs struct {
	IDs []int64
}

var descListUsersByIDs = &runtime.QueryDescriptor{
	Segments: []string{"SELECT id, org_id, email, role, score, bio\nFROM users\nWHERE id IN (", ")\nORDER BY id ASC"},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotSlice},
	},
	NumSlices: 1,
}

func expandListUsersByIDsParams(dst *[]any, p paramsListUsersByIDs) error {
	_ = p
	if len(p.IDs) == 0 {
		return runtime.ErrEmptySliceParam
	}
	for _, v := range p.IDs {
		*dst = append(*dst, v)
	}
	return nil
}

func expandListUsersByIDsArgs(dst *[]any, q *runtime.Query) error {
	return expandListUsersByIDsParams(dst, q.Params.(paramsListUsersByIDs))
}

func computeListUsersByIDsKey(p paramsListUsersByIDs) (runtime.VariantKey, uint64, uint64, int) {
	_ = p
	var colMask uint64
	var flagMask uint64
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	approxArgs := 5
	var d0 uint64
	d0 = uint64(len(p.IDs))
	approxArgs += len(p.IDs)
	vk := runtime.MakeVariantKey(stmtIDListUsersByIDs, colMask, flagMask, orderKey, d0)
	return vk, colMask, flagMask, approxArgs
}

func prepareListUsersByIDs(ids []int64) (runtime.Query, runtime.VariantKey, int) {
	p := paramsListUsersByIDs{
		IDs: ids,
	}
	vk, colMask, flagMask, approxArgs := computeListUsersByIDsKey(p)
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	sliceLens := make([]int, 1)
	var tupleLens []int
	var subs []runtime.Query
	sliceLens[0] = len(p.IDs)
	shapeLens := make([]int, 0, 1)
	shapeLens = append(shapeLens, sliceLens...)
	shapeTag := runtime.ComputeSubqueryShape(stmtIDListUsersByIDs, colMask, flagMask, orderKey, false, shapeLens...)
	qVal := runtime.Query{
		StmtID: stmtIDListUsersByIDs,
		Desc:   descListUsersByIDs,
		Input: runtime.RenderInput{
			ColMask:   colMask,
			FlagMask:  flagMask,
			OrderSpec: orderSpec,
			HasSeek:   hasSeek,
			SliceLens: sliceLens,
			TupleLens: tupleLens,
			Subs:      subs,
		},
		Params:   p,
		Expand:   expandListUsersByIDsArgs,
		ShapeTag: shapeTag,
	}
	return qVal, vk, approxArgs
}

func ListUsersByIDsQuery(ids []int64) runtime.Query {
	qVal, _, _ := prepareListUsersByIDs(ids)
	return qVal
}

func (q *Queries) ListUsersByIDsQuery(ids []int64) runtime.Query {
	return ListUsersByIDsQuery(ids)
}

func (q *Queries) ListUsersByIDs(ctx context.Context, ids []int64) ([]User, error) {
	return q.ListUsersByIDsInto(ctx, nil, ids)
}

func (q *Queries) ListUsersByIDsInto(ctx context.Context, dst []User, ids []int64) ([]User, error) {
	p := paramsListUsersByIDs{
		IDs: ids,
	}
	vk, colMask, flagMask, approxArgs := computeListUsersByIDsKey(p)
	_ = colMask
	argBuf := runtime.AcquireArgs(approxArgs)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandListUsersByIDsParams(argBuf, p); err != nil {
		return nil, err
	}
	buildSQL := func() string {
		qVal, _, _ := prepareListUsersByIDs(ids)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	rows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDListUsersByIDs, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, nil, buildSQL, (*argBuf)...)
	if err != nil {
		return nil, err
	}
	var item User
	var scanBuf [6]any
	scanBuf[0] = &item.ID
	scanBuf[1] = &item.OrgID
	scanBuf[2] = &item.Email
	scanBuf[3] = &item.Role
	scanBuf[4] = &item.Score
	scanBuf[5] = &item.Bio
	scanSlice := scanBuf[:6]
	for rows.Next() {
		item = User{}
		if err := rows.Scan(scanSlice...); err != nil {
			_ = rows.Close()
			if ms != nil {
				_ = ms.Release()
			}
			return nil, err
		}
		dst = append(dst, item)
	}
	err = rows.Err()
	_ = rows.Close()
	if ms != nil {
		_ = ms.Release()
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}

func (q *Queries) ListUsersByIDsSeq(ctx context.Context, ids []int64) iter.Seq2[User, error] {
	return func(yield func(User, error) bool) {
		var zero User
		p := paramsListUsersByIDs{
			IDs: ids,
		}
		vk, colMask, flagMask, approxArgs := computeListUsersByIDsKey(p)
		_ = colMask
		argBuf := runtime.AcquireArgs(approxArgs)
		defer runtime.ReleaseArgs(argBuf)
		if err := expandListUsersByIDsParams(argBuf, p); err != nil {
			yield(zero, err)
			return
		}
		buildSQL := func() string {
			qVal, _, _ := prepareListUsersByIDs(ids)
			return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
		}
		rows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDListUsersByIDs, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, nil, buildSQL, (*argBuf)...)
		if err != nil {
			yield(zero, err)
			return
		}
		var item User
		var scanBuf [6]any
		scanBuf[0] = &item.ID
		scanBuf[1] = &item.OrgID
		scanBuf[2] = &item.Email
		scanBuf[3] = &item.Role
		scanBuf[4] = &item.Score
		scanBuf[5] = &item.Bio
		scanSlice := scanBuf[:6]
		for rows.Next() {
			item = User{}
			if err := rows.Scan(scanSlice...); err != nil {
				_ = rows.Close()
				if ms != nil {
					_ = ms.Release()
				}
				yield(zero, err)
				return
			}
			if !yield(item, nil) {
				_ = rows.Close()
				if ms != nil {
					_ = ms.Release()
				}
				return
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if ms != nil {
			_ = ms.Release()
		}
		if err != nil {
			yield(zero, err)
		}
	}
}

func (q *Queries) ListUsersByIDsEach(ctx context.Context, ids []int64, fn func(User) error) error {
	for row, err := range q.ListUsersByIDsSeq(ctx, ids) {
		if err != nil {
			return err
		}
		if err := fn(row); err != nil {
			return err
		}
	}
	return nil
}

const stmtIDSelectUserProfileSubset uint32 = 4

const (
	SelectUserProfileSubsetColID    uint64 = 1 << 0
	SelectUserProfileSubsetColEmail uint64 = 1 << 1
	SelectUserProfileSubsetColRole  uint64 = 1 << 2
	SelectUserProfileSubsetColScore uint64 = 1 << 3
	SelectUserProfileSubsetColsAll  uint64 = (1 << 4) - 1
)

type SelectUserProfileSubsetParams struct {
	OrgID      int64
	WithExtra  bool
	SelectCols uint64
}

var descSelectUserProfileSubset = &runtime.QueryDescriptor{
	Segments: []string{"SELECT ", "\n", "\nFROM users u\nWHERE u.org_id = ", "\nORDER BY u.id ASC"},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotSelectCols},
		{Kind: runtime.SlotOptBlock, FlagBit: 0, Inner: &runtime.QueryDescriptor{
			Segments: []string{" , u.bio "},
		}},
		{Kind: runtime.SlotScalar},
	},
	SelectColExprs: []string{"u.id", "u.email", "u.role", "u.score"},
}

func expandSelectUserProfileSubsetParams(dst *[]any, p SelectUserProfileSubsetParams) error {
	_ = p
	if p.WithExtra {
	}
	*dst = append(*dst, p.OrgID)
	return nil
}

func expandSelectUserProfileSubsetArgs(dst *[]any, q *runtime.Query) error {
	return expandSelectUserProfileSubsetParams(dst, q.Params.(SelectUserProfileSubsetParams))
}

func computeSelectUserProfileSubsetKey(p SelectUserProfileSubsetParams) (runtime.VariantKey, uint64, uint64, int) {
	_ = p
	var colMask uint64
	colMask = p.SelectCols & SelectUserProfileSubsetColsAll
	if colMask == 0 {
		colMask = SelectUserProfileSubsetColsAll
	}
	var flagMask uint64
	if p.WithExtra {
		flagMask |= 1 << 0
	}
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	approxArgs := 5
	vk := runtime.MakeVariantKey(stmtIDSelectUserProfileSubset, colMask, flagMask, orderKey)
	return vk, colMask, flagMask, approxArgs
}

func prepareSelectUserProfileSubset(p SelectUserProfileSubsetParams) (runtime.Query, runtime.VariantKey, int) {
	vk, colMask, flagMask, approxArgs := computeSelectUserProfileSubsetKey(p)
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	var sliceLens []int
	var tupleLens []int
	var subs []runtime.Query
	shapeTag := runtime.ComputeSubqueryShape(stmtIDSelectUserProfileSubset, colMask, flagMask, orderKey, false)
	qVal := runtime.Query{
		StmtID: stmtIDSelectUserProfileSubset,
		Desc:   descSelectUserProfileSubset,
		Input: runtime.RenderInput{
			ColMask:   colMask,
			FlagMask:  flagMask,
			OrderSpec: orderSpec,
			HasSeek:   hasSeek,
			SliceLens: sliceLens,
			TupleLens: tupleLens,
			Subs:      subs,
		},
		Params:   p,
		Expand:   expandSelectUserProfileSubsetArgs,
		ShapeTag: shapeTag,
	}
	return qVal, vk, approxArgs
}

func SelectUserProfileSubsetQuery(p SelectUserProfileSubsetParams) runtime.Query {
	qVal, _, _ := prepareSelectUserProfileSubset(p)
	return qVal
}

func (q *Queries) SelectUserProfileSubsetQuery(p SelectUserProfileSubsetParams) runtime.Query {
	return SelectUserProfileSubsetQuery(p)
}

func (q *Queries) SelectUserProfileSubset(ctx context.Context, p SelectUserProfileSubsetParams) ([]SelectUserProfileSubsetRow, error) {
	return q.SelectUserProfileSubsetInto(ctx, nil, p)
}

func (q *Queries) SelectUserProfileSubsetInto(ctx context.Context, dst []SelectUserProfileSubsetRow, p SelectUserProfileSubsetParams) ([]SelectUserProfileSubsetRow, error) {
	vk, colMask, flagMask, approxArgs := computeSelectUserProfileSubsetKey(p)
	_ = colMask
	argBuf := runtime.AcquireArgs(approxArgs)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandSelectUserProfileSubsetParams(argBuf, p); err != nil {
		return nil, err
	}
	buildSQL := func() string {
		qVal, _, _ := prepareSelectUserProfileSubset(p)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	rows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDSelectUserProfileSubset, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, nil, buildSQL, (*argBuf)...)
	if err != nil {
		return nil, err
	}
	var item SelectUserProfileSubsetRow
	var scanBuf [5]any
	scanCount := 0
	effMask := colMask
	if (effMask & (1 << 0)) != 0 {
		scanBuf[scanCount] = &item.ID
		scanCount++
	}
	if (effMask & (1 << 1)) != 0 {
		scanBuf[scanCount] = &item.Email
		scanCount++
	}
	if (effMask & (1 << 2)) != 0 {
		scanBuf[scanCount] = &item.Role
		scanCount++
	}
	if (effMask & (1 << 3)) != 0 {
		scanBuf[scanCount] = &item.Score
		scanCount++
	}
	if p.WithExtra {
		scanBuf[scanCount] = &item.Bio
		scanCount++
	}
	scanSlice := scanBuf[:scanCount]
	for rows.Next() {
		item = SelectUserProfileSubsetRow{}
		if err := rows.Scan(scanSlice...); err != nil {
			_ = rows.Close()
			if ms != nil {
				_ = ms.Release()
			}
			return nil, err
		}
		dst = append(dst, item)
	}
	err = rows.Err()
	_ = rows.Close()
	if ms != nil {
		_ = ms.Release()
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}

const stmtIDSearchUsersCoupled uint32 = 5

const (
	SearchUsersCoupledColID    uint64 = 1 << 0
	SearchUsersCoupledColEmail uint64 = 1 << 1
	SearchUsersCoupledColsAll  uint64 = (1 << 2) - 1
)

type SearchUsersCoupledParams struct {
	OrgTiers   []string
	OrgSub     runtime.Query
	Roles      []string
	WithOrg    bool
	WithRoles  bool
	SelectCols uint64
}

var descSearchUsersCoupled = &runtime.QueryDescriptor{
	Segments: []string{"SELECT ", "\n", "\nFROM users u\n", "\nWHERE 1=1\n", "\n", "\nORDER BY u.id ASC"},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotSelectCols},
		{Kind: runtime.SlotOptBlock, FlagBit: 0, Inner: &runtime.QueryDescriptor{
			Segments: []string{" , o.name "},
		}},
		{Kind: runtime.SlotOptBlock, FlagBit: 0, Inner: &runtime.QueryDescriptor{
			Segments: []string{" LEFT JOIN orgs o ON o.id = u.org_id "},
		}},
		{Kind: runtime.SlotOptBlock, FlagBit: 0, Inner: &runtime.QueryDescriptor{
			Segments: []string{" AND o.tier IN (", ") AND u.org_id IN (", ") "},
			Slots: []runtime.Slot{
				{Kind: runtime.SlotSlice},
				{Kind: runtime.SlotQuery},
			},
			NumSlices: 1,
			NumSubs:   1,
		}},
		{Kind: runtime.SlotOptBlock, FlagBit: 1, Inner: &runtime.QueryDescriptor{
			Segments: []string{" AND u.role IN (", ") "},
			Slots: []runtime.Slot{
				{Kind: runtime.SlotSlice},
			},
			NumSlices: 1,
		}},
	},
	SelectColExprs: []string{"u.id", "u.email"},
	NumSlices:      2,
	NumSubs:        1,
}

func expandSearchUsersCoupledParams(dst *[]any, p SearchUsersCoupledParams) error {
	_ = p
	if p.WithOrg {
		if len(p.OrgTiers) == 0 {
			return runtime.ErrEmptySliceParam
		}
		for _, v := range p.OrgTiers {
			*dst = append(*dst, v)
		}
		if err := p.OrgSub.AppendArgs(dst); err != nil {
			return err
		}
	}
	if p.WithRoles {
		if len(p.Roles) == 0 {
			return runtime.ErrEmptySliceParam
		}
		for _, v := range p.Roles {
			*dst = append(*dst, v)
		}
	}
	return nil
}

func expandSearchUsersCoupledArgs(dst *[]any, q *runtime.Query) error {
	return expandSearchUsersCoupledParams(dst, q.Params.(SearchUsersCoupledParams))
}

func computeSearchUsersCoupledKey(p SearchUsersCoupledParams) (runtime.VariantKey, uint64, uint64, int) {
	_ = p
	var colMask uint64
	colMask = p.SelectCols & SearchUsersCoupledColsAll
	if colMask == 0 {
		colMask = SearchUsersCoupledColsAll
	}
	var flagMask uint64
	if p.WithOrg {
		flagMask |= 1 << 0
	}
	if p.WithRoles {
		flagMask |= 1 << 1
	}
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	approxArgs := 7
	var d0 uint64
	if p.WithOrg {
		d0 = uint64(len(p.OrgTiers))
		approxArgs += len(p.OrgTiers)
	}
	var d1 uint64
	if p.WithOrg {
		d1 = p.OrgSub.ShapeTag
	}
	var d2 uint64
	if p.WithRoles {
		d2 = uint64(len(p.Roles))
		approxArgs += len(p.Roles)
	}
	vk := runtime.MakeVariantKey(stmtIDSearchUsersCoupled, colMask, flagMask, orderKey, d0, d1, d2)
	return vk, colMask, flagMask, approxArgs
}

func prepareSearchUsersCoupled(p SearchUsersCoupledParams) (runtime.Query, runtime.VariantKey, int) {
	vk, colMask, flagMask, approxArgs := computeSearchUsersCoupledKey(p)
	var orderSpec runtime.Order
	var hasSeek bool
	orderKey := runtime.EncodeOrderKey(orderSpec, hasSeek)
	sliceLens := make([]int, 2)
	var tupleLens []int
	subs := make([]runtime.Query, 1)
	if p.WithOrg {
		sliceLens[0] = len(p.OrgTiers)
		subs[0] = p.OrgSub
	}
	if p.WithRoles {
		sliceLens[1] = len(p.Roles)
	}
	shapeLens := make([]int, 0, 2)
	shapeLens = append(shapeLens, sliceLens...)
	shapeTag := runtime.ComputeSubqueryShape(stmtIDSearchUsersCoupled, colMask, flagMask, orderKey, true, shapeLens...)
	qVal := runtime.Query{
		StmtID: stmtIDSearchUsersCoupled,
		Desc:   descSearchUsersCoupled,
		Input: runtime.RenderInput{
			ColMask:   colMask,
			FlagMask:  flagMask,
			OrderSpec: orderSpec,
			HasSeek:   hasSeek,
			SliceLens: sliceLens,
			TupleLens: tupleLens,
			Subs:      subs,
		},
		Params:   p,
		Expand:   expandSearchUsersCoupledArgs,
		ShapeTag: shapeTag,
	}
	return qVal, vk, approxArgs
}

func (q *Queries) SearchUsersCoupled(ctx context.Context, p SearchUsersCoupledParams) ([]SearchUsersCoupledRow, error) {
	vk, colMask, flagMask, approxArgs := computeSearchUsersCoupledKey(p)
	_ = colMask
	argBuf := runtime.AcquireArgs(approxArgs)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandSearchUsersCoupledParams(argBuf, p); err != nil {
		return nil, err
	}
	buildSQL := func() string {
		qVal, _, _ := prepareSearchUsersCoupled(p)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	rows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDSearchUsersCoupled, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, nil, buildSQL, (*argBuf)...)
	if err != nil {
		return nil, err
	}
	var dst []SearchUsersCoupledRow
	var item SearchUsersCoupledRow
	var scanBuf [3]any
	scanCount := 0
	effMask := colMask
	if (effMask & (1 << 0)) != 0 {
		scanBuf[scanCount] = &item.ID
		scanCount++
	}
	if (effMask & (1 << 1)) != 0 {
		scanBuf[scanCount] = &item.Email
		scanCount++
	}
	if p.WithOrg {
		scanBuf[scanCount] = &item.Name
		scanCount++
	}
	scanSlice := scanBuf[:scanCount]
	for rows.Next() {
		item = SearchUsersCoupledRow{}
		if err := rows.Scan(scanSlice...); err != nil {
			_ = rows.Close()
			if ms != nil {
				_ = ms.Release()
			}
			return nil, err
		}
		dst = append(dst, item)
	}
	err = rows.Err()
	_ = rows.Close()
	if ms != nil {
		_ = ms.Release()
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}

const stmtIDSubOrgsByMinID uint32 = 6

type SubOrgsByMinIDParams struct {
	MinID      int64
	ActiveVal  int64
	OnlyActive bool
}

var descSubOrgsByMinID = &runtime.QueryDescriptor{
	Segments: []string{"SELECT id FROM orgs WHERE id >= ", " ", ""},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotScalar},
		{Kind: runtime.SlotOptBlock, FlagBit: 0, Inner: &runtime.QueryDescriptor{
			Segments: []string{" AND active = ", " "},
			Slots: []runtime.Slot{
				{Kind: runtime.SlotScalar},
			},
		}},
	},
}

func expandSubOrgsByMinIDParams(dst *[]any, p SubOrgsByMinIDParams) error {
	*dst = append(*dst, p.MinID)
	if p.OnlyActive {
		*dst = append(*dst, p.ActiveVal)
	}
	return nil
}

func expandSubOrgsByMinIDArgs(dst *[]any, q *runtime.Query) error {
	return expandSubOrgsByMinIDParams(dst, q.Params.(SubOrgsByMinIDParams))
}

func SubOrgsByMinIDQuery(p SubOrgsByMinIDParams) runtime.Query {
	var flagMask uint64
	if p.OnlyActive {
		flagMask |= 1
	}
	shapeTag := runtime.ComputeSubqueryShape(stmtIDSubOrgsByMinID, 0, flagMask, 0, false)
	return runtime.Query{
		StmtID: stmtIDSubOrgsByMinID,
		Desc:   descSubOrgsByMinID,
		Input: runtime.RenderInput{
			FlagMask: flagMask,
		},
		Params:   p,
		Expand:   expandSubOrgsByMinIDArgs,
		ShapeTag: shapeTag,
	}
}

func (q *Queries) SubOrgsByMinIDQuery(p SubOrgsByMinIDParams) runtime.Query {
	return SubOrgsByMinIDQuery(p)
}

const stmtIDPageUsers uint32 = 7

const (
	PageUsersColID    uint64 = 1 << 0
	PageUsersColRole  uint64 = 1 << 1
	PageUsersColScore uint64 = 1 << 2
	PageUsersColEmail uint64 = 1 << 3
	PageUsersColsAll  uint64 = (1 << 4) - 1
)

const (
	OrderPageUsersRowRoleAsc   uint8 = 2
	OrderPageUsersRowRoleDesc  uint8 = 3
	OrderPageUsersRowScoreAsc  uint8 = 4
	OrderPageUsersRowScoreDesc uint8 = 5
	OrderPageUsersRowIDAsc     uint8 = 6
	OrderPageUsersRowIDDesc    uint8 = 7
)

func OrderPageUsersRow(terms ...uint8) runtime.Order {
	return runtime.PackOrder(terms...)
}

func SeekPageUsersRow(row PageUsersRow) runtime.Seek {
	return runtime.NewSeek(row.Role, row.Score, row.ID)
}

type PageUsersParams struct {
	Seek       runtime.Seek
	FilterRole string
	Order      runtime.Order
	Limit      int64
	RoleFilter bool
	SelectCols uint64
}

var descPageUsers = &runtime.QueryDescriptor{
	Segments: []string{"SELECT ", "\nFROM users\nWHERE ", "\n", "\nORDER BY ", "\nLIMIT ", ""},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotSelectCols},
		{Kind: runtime.SlotSeek},
		{Kind: runtime.SlotOptBlock, FlagBit: 0, Inner: &runtime.QueryDescriptor{
			Segments: []string{" AND role = ", " "},
			Slots: []runtime.Slot{
				{Kind: runtime.SlotScalar},
			},
		}},
		{Kind: runtime.SlotOrder},
		{Kind: runtime.SlotScalar},
	},
	SelectColExprs: []string{"id", "role", "score", "email"},
	OrderCols:      []string{"role", "score", "id"},
}

func expandPageUsersParams(dst *[]any, p PageUsersParams) error {
	if err := runtime.AppendSeekArgs(dst, p.Seek, p.Order, 3); err != nil {
		return err
	}
	if p.RoleFilter {
		*dst = append(*dst, p.FilterRole)
	}
	if _, _, err := runtime.ExpandOrder(p.Order, 3); err != nil {
		return err
	}
	*dst = append(*dst, p.Limit)
	return nil
}

func expandPageUsersArgs(dst *[]any, q *runtime.Query) error {
	return expandPageUsersParams(dst, q.Params.(PageUsersParams))
}

func computePageUsersKey(p PageUsersParams) (runtime.VariantKey, uint64, uint64, int) {
	colMask := p.SelectCols & PageUsersColsAll
	if colMask == 0 {
		colMask = PageUsersColsAll
	}
	var flagMask uint64
	if p.RoleFilter {
		flagMask |= 1
	}
	orderKey := runtime.EncodeOrderKey(p.Order, p.Seek.IsActive())
	vk := runtime.MakeVariantKey(stmtIDPageUsers, colMask, flagMask, orderKey)
	return vk, colMask, flagMask, 8
}

func preparePageUsers(p PageUsersParams) (runtime.Query, runtime.VariantKey, int) {
	vk, colMask, flagMask, approxArgs := computePageUsersKey(p)
	orderKey := runtime.EncodeOrderKey(p.Order, p.Seek.IsActive())
	shapeTag := runtime.ComputeSubqueryShape(stmtIDPageUsers, colMask, flagMask, orderKey, false)
	qVal := runtime.Query{
		StmtID: stmtIDPageUsers,
		Desc:   descPageUsers,
		Input: runtime.RenderInput{
			ColMask:   colMask,
			FlagMask:  flagMask,
			OrderSpec: p.Order,
			HasSeek:   p.Seek.IsActive(),
		},
		Params:   p,
		Expand:   expandPageUsersArgs,
		ShapeTag: shapeTag,
	}
	return qVal, vk, approxArgs
}

func PageUsersQuery(p PageUsersParams) runtime.Query {
	qVal, _, _ := preparePageUsers(p)
	return qVal
}

func (q *Queries) PageUsersQuery(p PageUsersParams) runtime.Query {
	return PageUsersQuery(p)
}

func (q *Queries) PageUsers(ctx context.Context, p PageUsersParams) ([]PageUsersRow, error) {
	vk, colMask, flagMask, approxArgs := computePageUsersKey(p)
	argBuf := runtime.AcquireArgs(approxArgs)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandPageUsersParams(argBuf, p); err != nil {
		return nil, err
	}
	buildSQL := func() string {
		qVal, _, _ := preparePageUsers(p)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	rows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDPageUsers, runtime.ComputeBatchShapeTag(colMask, flagMask), 0, nil, buildSQL, (*argBuf)...)
	if err != nil {
		return nil, err
	}
	var dst []PageUsersRow
	var item PageUsersRow
	var scanBuf [4]any
	scanCount := 0
	if (colMask & (1 << 0)) != 0 {
		scanBuf[scanCount] = &item.ID
		scanCount++
	}
	if (colMask & (1 << 1)) != 0 {
		scanBuf[scanCount] = &item.Role
		scanCount++
	}
	if (colMask & (1 << 2)) != 0 {
		scanBuf[scanCount] = &item.Score
		scanCount++
	}
	if (colMask & (1 << 3)) != 0 {
		scanBuf[scanCount] = &item.Email
		scanCount++
	}
	scanSlice := scanBuf[:scanCount]
	for rows.Next() {
		item = PageUsersRow{}
		if err := rows.Scan(scanSlice...); err != nil {
			_ = rows.Close()
			if ms != nil {
				_ = ms.Release()
			}
			return nil, err
		}
		dst = append(dst, item)
	}
	err = rows.Err()
	_ = rows.Close()
	if ms != nil {
		_ = ms.Release()
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}

const stmtIDUsersFromCTEAndInSubquery uint32 = 8

type paramsUsersFromCTEAndInSubquery struct {
	CTESub runtime.Query
	IDSub  runtime.Query
}

var descUsersFromCTEAndInSubquery = &runtime.QueryDescriptor{
	Segments: []string{"WITH filtered AS (", ")\nSELECT f.id, f.email\nFROM filtered f\nWHERE f.id IN (", ")\nORDER BY f.id ASC"},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotQuery},
		{Kind: runtime.SlotQuery},
	},
	NumSubs: 2,
}

func expandUsersFromCTEAndInSubqueryParams(dst *[]any, p paramsUsersFromCTEAndInSubquery) error {
	if err := p.CTESub.AppendArgs(dst); err != nil {
		return err
	}
	if err := p.IDSub.AppendArgs(dst); err != nil {
		return err
	}
	return nil
}

func expandUsersFromCTEAndInSubqueryArgs(dst *[]any, q *runtime.Query) error {
	return expandUsersFromCTEAndInSubqueryParams(dst, q.Params.(paramsUsersFromCTEAndInSubquery))
}

func UsersFromCTEAndInSubqueryQuery(cteSub runtime.Query, idSub runtime.Query) runtime.Query {
	p := paramsUsersFromCTEAndInSubquery{CTESub: cteSub, IDSub: idSub}
	shapeTag := runtime.ComputeSubqueryShape(stmtIDUsersFromCTEAndInSubquery, 0, 0, 0, true)
	return runtime.Query{
		StmtID: stmtIDUsersFromCTEAndInSubquery,
		Desc:   descUsersFromCTEAndInSubquery,
		Input: runtime.RenderInput{
			Subs: []runtime.Query{cteSub, idSub},
		},
		Params:   p,
		Expand:   expandUsersFromCTEAndInSubqueryArgs,
		ShapeTag: shapeTag,
	}
}

func (q *Queries) UsersFromCTEAndInSubquery(ctx context.Context, cteSub runtime.Query, idSub runtime.Query) ([]UsersFromCTEAndInSubqueryRow, error) {
	p := paramsUsersFromCTEAndInSubquery{CTESub: cteSub, IDSub: idSub}
	vk := runtime.MakeVariantKey(stmtIDUsersFromCTEAndInSubquery, 0, 0, 0, cteSub.ShapeTag, idSub.ShapeTag)
	argBuf := runtime.AcquireArgs(8)
	defer runtime.ReleaseArgs(argBuf)
	if err := expandUsersFromCTEAndInSubqueryParams(argBuf, p); err != nil {
		return nil, err
	}
	buildSQL := func() string {
		qVal := UsersFromCTEAndInSubqueryQuery(cteSub, idSub)
		return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
	}
	rows, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDUsersFromCTEAndInSubquery, 0, 0, nil, buildSQL, (*argBuf)...)
	if err != nil {
		return nil, err
	}
	var dst []UsersFromCTEAndInSubqueryRow
	var item UsersFromCTEAndInSubqueryRow
	scanBuf := [2]any{&item.ID, &item.Email}
	for rows.Next() {
		item = UsersFromCTEAndInSubqueryRow{}
		if err := rows.Scan(scanBuf[:]...); err != nil {
			_ = rows.Close()
			if ms != nil {
				_ = ms.Release()
			}
			return nil, err
		}
		dst = append(dst, item)
	}
	err = rows.Err()
	_ = rows.Close()
	if ms != nil {
		_ = ms.Release()
	}
	if err != nil {
		return nil, err
	}
	return dst, nil
}

const stmtIDSubUserIDsByScore uint32 = 9

type paramsSubUserIDsByScore struct {
	MinScore int64
}

var descSubUserIDsByScore = &runtime.QueryDescriptor{
	Segments: []string{"SELECT id FROM users WHERE score >= ", ""},
	Slots:    []runtime.Slot{{Kind: runtime.SlotScalar}},
}

func expandSubUserIDsByScoreArgs(dst *[]any, q *runtime.Query) error {
	p := q.Params.(paramsSubUserIDsByScore)
	*dst = append(*dst, p.MinScore)
	return nil
}

func SubUserIDsByScoreQuery(minScore int64) runtime.Query {
	return runtime.Query{
		StmtID:   stmtIDSubUserIDsByScore,
		Desc:     descSubUserIDsByScore,
		Params:   paramsSubUserIDsByScore{MinScore: minScore},
		Expand:   expandSubUserIDsByScoreArgs,
		ShapeTag: runtime.ComputeSubqueryShape(stmtIDSubUserIDsByScore, 0, 0, 0, false),
	}
}

func (q *Queries) SubUserIDsByScoreQuery(minScore int64) runtime.Query {
	return SubUserIDsByScoreQuery(minScore)
}

const stmtIDInsertUsersBatchReturning uint32 = 10

type paramsInsertUsersBatchReturning struct {
	Rows []BatchUserInput
}

var descInsertUsersBatchReturning = &runtime.QueryDescriptor{
	Segments: []string{"INSERT INTO batch_users (id, name, points)\nVALUES ", "\nRETURNING id, name, points"},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotTupleSlice, Tuple: &runtime.TupleDescriptor{Segments: []string{"", ", ", ", ", ""}, NumFields: 3}},
	},
	NumTuples: 1,
}

func expandInsertUsersBatchReturningParams(dst *[]any, p paramsInsertUsersBatchReturning) error {
	if len(p.Rows) == 0 {
		return runtime.ErrEmptySliceParam
	}
	for i := range p.Rows {
		*dst = append(*dst, p.Rows[i].ID, p.Rows[i].Name, p.Rows[i].Points)
	}
	return nil
}

func computeInsertUsersBatchReturningKey(p paramsInsertUsersBatchReturning) (runtime.VariantKey, uint64, uint64, int) {
	vk := runtime.MakeVariantKey(stmtIDInsertUsersBatchReturning, 0, 0, 0, uint64(len(p.Rows)))
	return vk, 0, 0, len(p.Rows)*3 + 4
}

func prepareInsertUsersBatchReturning(rows []BatchUserInput) (runtime.Query, runtime.VariantKey, int) {
	p := paramsInsertUsersBatchReturning{Rows: rows}
	vk, _, _, approxArgs := computeInsertUsersBatchReturningKey(p)
	qVal := runtime.Query{
		StmtID: stmtIDInsertUsersBatchReturning,
		Desc:   descInsertUsersBatchReturning,
		Input: runtime.RenderInput{
			TupleLens: []int{len(rows)},
		},
		Params: p,
	}
	return qVal, vk, approxArgs
}

func (q *Queries) InsertUsersBatchReturningBatch(ctx context.Context, rows []BatchUserInput) ([]InsertUsersBatchReturningRow, error) {
	return q.InsertUsersBatchReturningBatchInto(ctx, nil, rows)
}

func (q *Queries) InsertUsersBatchReturningBatchInto(ctx context.Context, dst []InsertUsersBatchReturningRow, rows []BatchUserInput) ([]InsertUsersBatchReturningRow, error) {
	totalRows := len(rows)
	if totalRows == 0 {
		return nil, runtime.ErrEmptySliceParam
	}
	p := paramsInsertUsersBatchReturning{Rows: rows}
	_, colMask, flagMask, _ := computeInsertUsersBatchReturningKey(p)
	shapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)
	firstChunk := q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, colMask, flagMask, totalRows, 10922, true)
	txCache := q.txCache
	var autoTx *sql.Tx
	if txCache == nil && firstChunk < totalRows {
		var err error
		autoTx, err = q.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		txCache = runtime.NewTxStmtCache(autoTx)
		defer func() { _ = txCache.Close() }()
	}
	for offset := 0; offset < totalRows; {
		chunkSize := firstChunk
		if offset > 0 {
			chunkSize = q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, colMask, flagMask, totalRows-offset, 10922, false)
		}
		chunkRows := rows[offset : offset+chunkSize]
		chunkP := p
		chunkP.Rows = chunkRows
		vk, _, _, approxArgs := computeInsertUsersBatchReturningKey(chunkP)
		argBuf := runtime.AcquireArgs(approxArgs)
		if err := expandInsertUsersBatchReturningParams(argBuf, chunkP); err != nil {
			runtime.ReleaseArgs(argBuf)
			if autoTx != nil {
				_ = autoTx.Rollback()
			}
			return nil, err
		}
		buildSQL := func() string {
			qVal, _, _ := prepareInsertUsersBatchReturning(chunkRows)
			return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
		}
		r, ms, err := q.cache.Query(ctx, q.db, txCache, q.usePrepared, vk, stmtIDInsertUsersBatchReturning, shapeTag, chunkSize, nil, buildSQL, (*argBuf)...)
		runtime.ReleaseArgs(argBuf)
		if err != nil {
			if autoTx != nil {
				_ = autoTx.Rollback()
			}
			return nil, err
		}
		var item InsertUsersBatchReturningRow
		scanBuf := [3]any{&item.ID, &item.Name, &item.Points}
		for r.Next() {
			item = InsertUsersBatchReturningRow{}
			if err := r.Scan(scanBuf[:]...); err != nil {
				_ = r.Close()
				if ms != nil {
					_ = ms.Release()
				}
				if autoTx != nil {
					_ = autoTx.Rollback()
				}
				return nil, err
			}
			dst = append(dst, item)
		}
		err = r.Err()
		_ = r.Close()
		if ms != nil {
			_ = ms.Release()
		}
		if err != nil {
			if autoTx != nil {
				_ = autoTx.Rollback()
			}
			return nil, err
		}
		offset += chunkSize
	}
	if autoTx != nil {
		if err := autoTx.Commit(); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (q *Queries) InsertUsersBatchReturningBatchSeq(ctx context.Context, rows []BatchUserInput) iter.Seq2[InsertUsersBatchReturningRow, error] {
	return func(yield func(InsertUsersBatchReturningRow, error) bool) {
		var zero InsertUsersBatchReturningRow
		totalRows := len(rows)
		if totalRows == 0 {
			yield(zero, runtime.ErrEmptySliceParam)
			return
		}
		p := paramsInsertUsersBatchReturning{Rows: rows}
		_, colMask, flagMask, _ := computeInsertUsersBatchReturningKey(p)
		shapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)
		firstChunk := q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, colMask, flagMask, totalRows, 10922, true)
		for offset := 0; offset < totalRows; {
			chunkSize := firstChunk
			if offset > 0 {
				chunkSize = q.cache.PlanBatchChunk(stmtIDInsertUsersBatchReturning, colMask, flagMask, totalRows-offset, 10922, false)
			}
			chunkRows := rows[offset : offset+chunkSize]
			chunkP := p
			chunkP.Rows = chunkRows
			vk, _, _, approxArgs := computeInsertUsersBatchReturningKey(chunkP)
			argBuf := runtime.AcquireArgs(approxArgs)
			if err := expandInsertUsersBatchReturningParams(argBuf, chunkP); err != nil {
				runtime.ReleaseArgs(argBuf)
				yield(zero, err)
				return
			}
			buildSQL := func() string {
				qVal, _, _ := prepareInsertUsersBatchReturning(chunkRows)
				return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
			}
			r, ms, err := q.cache.Query(ctx, q.db, q.txCache, q.usePrepared, vk, stmtIDInsertUsersBatchReturning, shapeTag, chunkSize, nil, buildSQL, (*argBuf)...)
			runtime.ReleaseArgs(argBuf)
			if err != nil {
				yield(zero, err)
				return
			}
			var item InsertUsersBatchReturningRow
			scanBuf := [3]any{&item.ID, &item.Name, &item.Points}
			for r.Next() {
				item = InsertUsersBatchReturningRow{}
				if err := r.Scan(scanBuf[:]...); err != nil {
					_ = r.Close()
					if ms != nil {
						_ = ms.Release()
					}
					yield(zero, err)
					return
				}
				if !yield(item, nil) {
					_ = r.Close()
					if ms != nil {
						_ = ms.Release()
					}
					return
				}
			}
			err = r.Err()
			_ = r.Close()
			if ms != nil {
				_ = ms.Release()
			}
			if err != nil {
				yield(zero, err)
				return
			}
			offset += chunkSize
		}
	}
}

const stmtIDUpdateUsersBatchFromValues uint32 = 11

type paramsUpdateUsersBatchFromValues struct {
	Rows []BatchBonusInput
}

var descUpdateUsersBatchFromValues = &runtime.QueryDescriptor{
	Segments: []string{
		"WITH updates(id, bonus) AS (\n  VALUES ",
		"\n)\nUPDATE batch_users\nSET points = batch_users.points + (SELECT u.bonus FROM updates u WHERE u.id = batch_users.id)\nWHERE batch_users.id IN (SELECT u.id FROM updates u)",
	},
	Slots: []runtime.Slot{
		{Kind: runtime.SlotTupleSlice, Tuple: &runtime.TupleDescriptor{Segments: []string{"", ", ", ""}, NumFields: 2}},
	},
	NumTuples: 1,
}

func expandUpdateUsersBatchFromValuesParams(dst *[]any, p paramsUpdateUsersBatchFromValues) error {
	if len(p.Rows) == 0 {
		return runtime.ErrEmptySliceParam
	}
	for i := range p.Rows {
		*dst = append(*dst, p.Rows[i].ID, p.Rows[i].Bonus)
	}
	return nil
}

func computeUpdateUsersBatchFromValuesKey(p paramsUpdateUsersBatchFromValues) (runtime.VariantKey, uint64, uint64, int) {
	vk := runtime.MakeVariantKey(stmtIDUpdateUsersBatchFromValues, 0, 0, 0, uint64(len(p.Rows)))
	return vk, 0, 0, len(p.Rows)*2 + 4
}

func prepareUpdateUsersBatchFromValues(rows []BatchBonusInput) (runtime.Query, runtime.VariantKey, int) {
	p := paramsUpdateUsersBatchFromValues{Rows: rows}
	vk, _, _, approxArgs := computeUpdateUsersBatchFromValuesKey(p)
	qVal := runtime.Query{
		StmtID: stmtIDUpdateUsersBatchFromValues,
		Desc:   descUpdateUsersBatchFromValues,
		Input: runtime.RenderInput{
			TupleLens: []int{len(rows)},
		},
		Params: p,
	}
	return qVal, vk, approxArgs
}

func (q *Queries) UpdateUsersBatchFromValuesBatch(ctx context.Context, rows []BatchBonusInput) (sql.Result, error) {
	totalRows := len(rows)
	if totalRows == 0 {
		return nil, runtime.ErrEmptySliceParam
	}
	p := paramsUpdateUsersBatchFromValues{Rows: rows}
	_, colMask, flagMask, _ := computeUpdateUsersBatchFromValuesKey(p)
	shapeTag := runtime.ComputeBatchShapeTag(colMask, flagMask)
	firstChunk := q.cache.PlanBatchChunk(stmtIDUpdateUsersBatchFromValues, colMask, flagMask, totalRows, 16383, true)
	txCache := q.txCache
	var autoTx *sql.Tx
	if txCache == nil && firstChunk < totalRows {
		var err error
		autoTx, err = q.db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		txCache = runtime.NewTxStmtCache(autoTx)
		defer func() { _ = txCache.Close() }()
	}
	var batchRes runtime.BatchExecResult
	for offset := 0; offset < totalRows; {
		chunkSize := firstChunk
		if offset > 0 {
			chunkSize = q.cache.PlanBatchChunk(stmtIDUpdateUsersBatchFromValues, colMask, flagMask, totalRows-offset, 16383, false)
		}
		chunkRows := rows[offset : offset+chunkSize]
		chunkP := p
		chunkP.Rows = chunkRows
		vk, _, _, approxArgs := computeUpdateUsersBatchFromValuesKey(chunkP)
		argBuf := runtime.AcquireArgs(approxArgs)
		if err := expandUpdateUsersBatchFromValuesParams(argBuf, chunkP); err != nil {
			runtime.ReleaseArgs(argBuf)
			if autoTx != nil {
				_ = autoTx.Rollback()
			}
			return nil, err
		}
		buildSQL := func() string {
			qVal, _, _ := prepareUpdateUsersBatchFromValues(chunkRows)
			return runtime.BuildComposedSQL(qVal.Desc, &qVal.Input, sqlDialect)
		}
		res, err := q.cache.Exec(ctx, q.db, txCache, q.usePrepared, vk, stmtIDUpdateUsersBatchFromValues, shapeTag, chunkSize, nil, buildSQL, (*argBuf)...)
		runtime.ReleaseArgs(argBuf)
		if err != nil {
			if autoTx != nil {
				_ = autoTx.Rollback()
			}
			return nil, err
		}
		batchRes.Add(res)
		offset += chunkSize
	}
	if autoTx != nil {
		if err := autoTx.Commit(); err != nil {
			return nil, err
		}
	}
	return batchRes, nil
}
