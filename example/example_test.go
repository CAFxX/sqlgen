package example_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/CAFxX/sqlgen"
	"github.com/CAFxX/sqlgen/example"
	"github.com/CAFxX/sqlgen/runtime"
)

func TestExampleGeneratedCodeMatchesYAML(t *testing.T) {
	yamlBytes, err := os.ReadFile("sqlgen.yaml")
	if err != nil {
		t.Fatalf("read sqlgen.yaml: %v", err)
	}
	wantGen, err := sqlgen.GenerateFromYAML(yamlBytes)
	if err != nil {
		t.Fatalf("GenerateFromYAML: %v", err)
	}
	gotGen, err := os.ReadFile("example_gen.go")
	if err != nil {
		t.Fatalf("read example_gen.go: %v", err)
	}
	if !bytes.Equal(gotGen, wantGen) {
		t.Fatalf("example_gen.go is out of sync with sqlgen.yaml")
	}
}

func TestExampleQueriesBuildSQL(t *testing.T) {
	// 1. Slice query with 3 slice notations
	qList := example.ListUsersByFilterQuery(
		[]int64{1, 2},
		[]string{"admin", "owner"},
		[]string{"active"},
	)
	sqlList := qList.BuildSQL(runtime.DialectPostgres)
	if !strings.Contains(sqlList, "WHERE org_id IN ($1, $2)") ||
		!strings.Contains(sqlList, "AND role IN ($3, $4)") ||
		!strings.Contains(sqlList, "AND status IN ($5)") {
		t.Fatalf("unexpected ListUsersByFilter SQL:\n%s", sqlList)
	}

	// 2. SelectCols subsetting + conditional column
	qSum := example.ListUserSummariesQuery(example.ListUserSummariesParams{
		OrgID:      10,
		IncludeBio: true,
		SelectCols: example.ListUserSummariesColID | example.ListUserSummariesColDisplayLabel,
	})
	sqlSum := qSum.BuildSQL(runtime.DialectPostgres)
	if !strings.Contains(sqlSum, "u.id, COALESCE(u.display_name, u.email) AS display_label") || !strings.Contains(sqlSum, "COALESCE(u.bio, '') AS bio") {
		t.Fatalf("unexpected ListUserSummaries SQL:\n%s", sqlSum)
	}

	// 2b. Arithmetic expression with input parameters inside SELECT projection
	qWeighted := example.CalculateUserWeightedScoresQuery(10, 5, 42)
	sqlWeighted := qWeighted.BuildSQL(runtime.DialectPostgres)
	if !strings.Contains(sqlWeighted, "u.score * $1 + $2 AS weighted_score") {
		t.Fatalf("unexpected CalculateUserWeightedScores SQL:\n%s", sqlWeighted)
	}

	// 3. Coupled conditional blocks + composed subquery
	orgSub := example.ActiveOrgIDsSubqueryQuery(example.ActiveOrgIDsSubqueryParams{
		Active:       true,
		FilterRegion: true,
		Regions:      []string{"eu", "us"},
	})
	qSearch := example.SearchUsersAdvancedQuery(example.SearchUsersAdvancedParams{
		WithOrg:        true,
		OrgTiers:       []string{"enterprise"},
		AllowedOrgs:    orgSub,
		FilterRoles:    false,
		MinScoreFilter: true,
		MinScore:       100,
	})
	sqlSearch := qSearch.BuildSQL(runtime.DialectPostgres)
	if !strings.Contains(sqlSearch, "LEFT JOIN organizations o ON o.id = u.org_id") ||
		!strings.Contains(sqlSearch, "AND o.region IN ($3, $4)") ||
		!strings.Contains(sqlSearch, "AND u.score >= $5") {
		t.Fatalf("unexpected SearchUsersAdvanced SQL:\n%s", sqlSearch)
	}

	// 4. Keyset pagination + custom multi-column ordering
	qPage := example.PaginateUsersQuery(example.PaginateUsersParams{
		OrgID:    42,
		Seek:     example.SeekPaginateUsersRow(example.PaginateUsersRow{Role: "admin", Score: 90, Nickname: "ace", ID: 7}),
		OnlyRole: true,
		RoleFilter: "admin",
		Order:    example.OrderPaginateUsersRow(example.OrderPaginateUsersRowRoleAsc, example.OrderPaginateUsersRowScoreDesc),
		Limit:    25,
	})
	sqlPage := qPage.BuildSQL(runtime.DialectPostgres)
	if !strings.Contains(sqlPage, "ORDER BY u.role ASC, u.score DESC, u.id DESC") {
		t.Fatalf("unexpected PaginateUsers ORDER BY SQL:\n%s", sqlPage)
	}
}
