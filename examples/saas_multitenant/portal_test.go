package saas_multitenant

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/CAFxX/sqlgen/runtime"
	_ "modernc.org/sqlite"
)

type seedOrg struct {
	ID     int64
	Name   string
	Plan   string
	Status string
}

type seedMember struct {
	ID        int64
	OrgID     int64
	Email     string
	Role      string
	Status    string
	Score     int64
	CreatedAt int64
}

type seedInvoice struct {
	ID          int64
	MemberID    int64
	AmountCents int64
	Status      string
}

func TestSaaSMultitenantPortalEndToEnd(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, SchemaDDL); err != nil {
		t.Fatalf("schema DDL: %v", err)
	}

	svc := NewPortalService(db)
	defer func() { _ = svc.Close() }()
	q := svc.Queries()

	// 1. Seed 5 organizations, 300 members, and 1,000 invoices
	orgs := []seedOrg{
		{ID: 1, Name: "Acme Corp", Plan: "enterprise", Status: "active"},
		{ID: 2, Name: "Beta Labs", Plan: "pro", Status: "active"},
		{ID: 3, Name: "Gamma Inc", Plan: "free", Status: "active"},
		{ID: 4, Name: "Delta LLC", Plan: "enterprise", Status: "suspended"},
		{ID: 5, Name: "Epsilon IO", Plan: "pro", Status: "active"},
	}
	orgByID := make(map[int64]seedOrg, len(orgs))
	for _, o := range orgs {
		orgByID[o.ID] = o
		if _, err := db.ExecContext(ctx, `INSERT INTO orgs (id, name, plan, status) VALUES (?, ?, ?, ?)`, o.ID, o.Name, o.Plan, o.Status); err != nil {
			t.Fatalf("insert org %d: %v", o.ID, err)
		}
	}

	roles := []string{"admin", "editor", "viewer"}
	statuses := []string{"active", "invited", "disabled"}
	scores := []int64{10, 25, 50, 75, 100}
	timestamps := []int64{1700000100, 1700000200, 1700000300, 1700000400}

	members := make([]seedMember, 0, 300)
	for i := 1; i <= 300; i++ {
		m := seedMember{
			ID:        int64(i),
			OrgID:     int64(((i - 1) % 5) + 1),
			Email:     fmt.Sprintf("user%03d@org%d.example.com", i, ((i-1)%5)+1),
			Role:      roles[(i*3)%len(roles)],
			Status:    statuses[(i*5)%len(statuses)],
			Score:     scores[(i*7)%len(scores)],
			CreatedAt: timestamps[(i*11)%len(timestamps)],
		}
		members = append(members, m)
		if _, err := db.ExecContext(ctx,
			`INSERT INTO members (id, org_id, email, role, status, score, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			m.ID, m.OrgID, m.Email, m.Role, m.Status, m.Score, m.CreatedAt,
		); err != nil {
			t.Fatalf("insert member %d: %v", m.ID, err)
		}
	}

	invStatuses := []string{"paid", "open", "void"}
	invoices := make([]seedInvoice, 0, 1000)
	for i := 1; i <= 1000; i++ {
		inv := seedInvoice{
			ID:          int64(i),
			MemberID:    int64(((i * 13) % 300) + 1),
			AmountCents: int64(((i % 20) + 1) * 500),
			Status:      invStatuses[i%len(invStatuses)],
		}
		invoices = append(invoices, inv)
		if _, err := db.ExecContext(ctx,
			`INSERT INTO invoices (id, member_id, amount_cents, status) VALUES (?, ?, ?, ?)`,
			inv.ID, inv.MemberID, inv.AmountCents, inv.Status,
		); err != nil {
			t.Fatalf("insert invoice %d: %v", inv.ID, err)
		}
	}

	// 2. Test all 16 combinations of the 4 optional filter flags on SearchMembers + SelectCols projection
	const targetOrg int64 = 1
	for mask := 0; mask < 16; mask++ {
		withOrg := (mask & (1 << 0)) != 0
		filterRole := (mask & (1 << 1)) != 0
		filterStatuses := (mask & (1 << 2)) != 0
		filterMinScore := (mask & (1 << 3)) != 0

		params := SearchMembersParams{
			OrgID:          targetOrg,
			Role:           "admin",
			Statuses:       []string{"active", "invited"},
			MinScore:       50,
			WithOrgDetails: withOrg,
			FilterRole:     filterRole,
			FilterStatuses: filterStatuses,
			FilterMinScore: filterMinScore,
		}

		gotFull, err := svc.SearchDirectory(ctx, params)
		if err != nil {
			t.Fatalf("SearchDirectory(mask=%04b): %v", mask, err)
		}
		gotCompact, err := svc.SearchDirectoryCompact(ctx, params)
		if err != nil {
			t.Fatalf("SearchDirectoryCompact(mask=%04b): %v", mask, err)
		}

		// Compute reference result set
		var wantFull []MemberRow
		for _, m := range members {
			if m.OrgID != targetOrg {
				continue
			}
			if filterRole && m.Role != params.Role {
				continue
			}
			if filterStatuses && !slices.Contains(params.Statuses, m.Status) {
				continue
			}
			if filterMinScore && m.Score < params.MinScore {
				continue
			}
			row := MemberRow{
				ID:        m.ID,
				OrgID:     m.OrgID,
				Email:     m.Email,
				Role:      m.Role,
				Status:    m.Status,
				Score:     m.Score,
				CreatedAt: m.CreatedAt,
			}
			if withOrg {
				o := orgByID[m.OrgID]
				row.OrgName = o.Name
				row.OrgPlan = o.Plan
			}
			wantFull = append(wantFull, row)
		}

		if !slices.Equal(gotFull, wantFull) {
			t.Fatalf("SearchDirectory(mask=%04b) mismatch: got %d rows, want %d rows", mask, len(gotFull), len(wantFull))
		}
		if len(gotCompact) != len(wantFull) {
			t.Fatalf("SearchDirectoryCompact(mask=%04b) len=%d, want %d", mask, len(gotCompact), len(wantFull))
		}
		for i, cRow := range gotCompact {
			w := wantFull[i]
			if cRow.ID != w.ID || cRow.Email != w.Email || cRow.Role != w.Role || cRow.Status != w.Status {
				t.Fatalf("compact row[%d] core fields mismatch: got %+v, want %+v", i, cRow, w)
			}
			// Unrequested unconditional columns (OrgID, Score, CreatedAt) must remain zero-valued
			if cRow.OrgID != 0 || cRow.Score != 0 || cRow.CreatedAt != 0 {
				t.Fatalf("compact row[%d] unrequested fields were populated: %+v", i, cRow)
			}
			if withOrg && (cRow.OrgName != w.OrgName || cRow.OrgPlan != w.OrgPlan) {
				t.Fatalf("compact row[%d] org details mismatch: got %+v, want %+v", i, cRow, w)
			}
		}
	}

	// 3. Test composite keyset pagination across mixed ASC/DESC sort orders and page sizes
	var org2Items []MemberItem
	for _, m := range members {
		if m.OrgID == 2 {
			org2Items = append(org2Items, MemberItem{
				ID:        m.ID,
				OrgID:     m.OrgID,
				Email:     m.Email,
				Role:      m.Role,
				Score:     m.Score,
				CreatedAt: m.CreatedAt,
			})
		}
	}

	sortConfigs := []struct {
		name  string
		order runtime.Order
		cmpFn func(a, b MemberItem) int
	}{
		{
			name: "RoleAsc_ScoreDesc_CreatedAtDesc_IDAsc",
			order: OrderMemberItem(
				MemberItemOrderByRole.Asc(),
				MemberItemOrderByScore.Desc(),
				MemberItemOrderByCreatedAt.Desc(),
				MemberItemOrderByID.Asc(),
			),
			cmpFn: func(a, b MemberItem) int {
				if c := cmp.Compare(a.Role, b.Role); c != 0 {
					return c
				}
				if c := cmp.Compare(b.Score, a.Score); c != 0 {
					return c
				}
				if c := cmp.Compare(b.CreatedAt, a.CreatedAt); c != 0 {
					return c
				}
				return cmp.Compare(a.ID, b.ID)
			},
		},
		{
			name: "RoleDesc_ScoreAsc_CreatedAtAsc_IDDesc",
			order: OrderListMembersPaginated(
				ListMembersPaginatedOrderByRole.Desc(),
				ListMembersPaginatedOrderByScore.Asc(),
				ListMembersPaginatedOrderByCreatedAt.Asc(),
				ListMembersPaginatedOrderByID.Desc(),
			),
			cmpFn: func(a, b MemberItem) int {
				if c := cmp.Compare(b.Role, a.Role); c != 0 {
					return c
				}
				if c := cmp.Compare(a.Score, b.Score); c != 0 {
					return c
				}
				if c := cmp.Compare(a.CreatedAt, b.CreatedAt); c != 0 {
					return c
				}
				return cmp.Compare(b.ID, a.ID)
			},
		},
	}

	for _, sc := range sortConfigs {
		wantSorted := slices.Clone(org2Items)
		slices.SortFunc(wantSorted, sc.cmpFn)

		for _, pageSize := range []int64{1, 7, 15, 60, 100} {
			gotPaged, err := svc.FetchAllMembersPaginated(ctx, 2, sc.order, pageSize)
			if err != nil {
				t.Fatalf("FetchAllMembersPaginated(%s, pageSize=%d): %v", sc.name, pageSize, err)
			}
			if !slices.Equal(gotPaged, wantSorted) {
				t.Fatalf("FetchAllMembersPaginated(%s, pageSize=%d) mismatch: got %d items, want %d", sc.name, pageSize, len(gotPaged), len(wantSorted))
			}
		}
	}

	// 4. Test 3-level composable curried Query CTE report (ActiveOrgs -> CohortMemberIDs -> AggregateCohortInvoices)
	plans := []string{"enterprise", "pro"}
	const minScore int64 = 50
	const invStatus = "paid"

	gotSummary, err := svc.CohortBillingReport(ctx, plans, minScore, invStatus)
	if err != nil {
		t.Fatalf("CohortBillingReport: %v", err)
	}

	// Reference calculation in Go
	type groupKey struct {
		orgID int64
		role  string
	}
	wantGroups := make(map[groupKey]*CohortInvoiceSummary)
	memberByID := make(map[int64]seedMember, len(members))
	for _, m := range members {
		o := orgByID[m.OrgID]
		if o.Status == "active" && slices.Contains(plans, o.Plan) && m.Score >= minScore {
			memberByID[m.ID] = m
			gk := groupKey{orgID: m.OrgID, role: m.Role}
			if _, ok := wantGroups[gk]; !ok {
				wantGroups[gk] = &CohortInvoiceSummary{OrgID: m.OrgID, Role: m.Role}
			}
		}
	}
	for _, inv := range invoices {
		if inv.Status != invStatus {
			continue
		}
		if m, ok := memberByID[inv.MemberID]; ok {
			gk := groupKey{orgID: m.OrgID, role: m.Role}
			g := wantGroups[gk]
			g.InvoiceCount++
			g.TotalCents += inv.AmountCents
		}
	}
	var wantSummary []CohortInvoiceSummary
	for _, g := range wantGroups {
		wantSummary = append(wantSummary, *g)
	}
	slices.SortFunc(wantSummary, func(a, b CohortInvoiceSummary) int {
		if c := cmp.Compare(a.OrgID, b.OrgID); c != 0 {
			return c
		}
		return cmp.Compare(a.Role, b.Role)
	})

	if !slices.Equal(gotSummary, wantSummary) {
		t.Fatalf("CohortBillingReport mismatch:\ngot:  %+v\nwant: %+v", gotSummary, wantSummary)
	}

	// 5. Exercise Seq, Each, Query currying, Pipeline, and error paths
	var eachMembers int
	if err := runtime.Each(q.SearchMembersSeq(ctx, SearchMembersParams{OrgID: 1}), func(r MemberRow) error {
		eachMembers++
		return nil
	}); err != nil || eachMembers != 60 {
		t.Fatalf("SearchMembersEach: count=%d, err=%v", eachMembers, err)
	}
	// Early break in Seq and error paths in Seq/Each
	for range q.SearchMembersSeq(ctx, SearchMembersParams{OrgID: 1}) {
		break
	}
	errStop := errors.New("stop iteration")
	if err := runtime.Each(q.SearchMembersSeq(ctx, SearchMembersParams{OrgID: 1}), func(r MemberRow) error {
		return errStop
	}); !errors.Is(err, errStop) {
		t.Fatalf("expected errStop from SearchMembersEach, got %v", err)
	}
	if err := runtime.Each(q.SearchMembersSeq(ctx, SearchMembersParams{OrgID: 1, FilterStatuses: true, Statuses: nil}), func(r MemberRow) error {
		return nil
	}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam from SearchMembersEach, got %v", err)
	}

	var eachPaged int
	if err := runtime.Each(q.ListMembersPaginatedSeq(ctx, ListMembersPaginatedParams{
		OrgID:      1,
		Sort:       sortConfigs[0].order,
		Limit:      10,
		SelectCols: MemberItemColID | MemberItemColEmail | MemberItemColRole,
	}), func(r MemberItem) error {
		eachPaged++
		_ = SeekMemberItem(r)
		_ = SeekListMembersPaginated(r)
		return nil
	}); err != nil || eachPaged != 10 {
		t.Fatalf("ListMembersPaginatedEach: count=%d, err=%v", eachPaged, err)
	}
	for range q.ListMembersPaginatedSeq(ctx, ListMembersPaginatedParams{OrgID: 1, Sort: sortConfigs[0].order, Limit: 5}) {
		break
	}
	if err := runtime.Each(q.ListMembersPaginatedSeq(ctx, ListMembersPaginatedParams{OrgID: 1, Sort: sortConfigs[0].order, Limit: 5}), func(r MemberItem) error {
		return errStop
	}); !errors.Is(err, errStop) {
		t.Fatalf("expected errStop from ListMembersPaginatedEach, got %v", err)
	}
	if err := runtime.Each(q.ListMembersPaginatedSeq(ctx, ListMembersPaginatedParams{OrgID: 1, After: runtime.NewSeek("bad"), Sort: sortConfigs[0].order, Limit: 5}), func(r MemberItem) error {
		return nil
	}); !errors.Is(err, runtime.ErrInvalidSeek) {
		t.Fatalf("expected ErrInvalidSeek from ListMembersPaginatedEach, got %v", err)
	}

	activeQ := ActiveOrgsQuery(plans)
	cohortQ := CohortMemberIDsQuery(activeQ, minScore)
	var eachSummary int
	if err := runtime.Each(q.AggregateCohortInvoicesSeq(ctx, cohortQ, invStatus), func(r CohortInvoiceSummary) error {
		eachSummary++
		return nil
	}); err != nil || eachSummary != len(wantSummary) {
		t.Fatalf("AggregateCohortInvoicesEach: count=%d, err=%v", eachSummary, err)
	}
	for range q.AggregateCohortInvoicesSeq(ctx, cohortQ, invStatus) {
		break
	}
	if err := runtime.Each(q.AggregateCohortInvoicesSeq(ctx, cohortQ, invStatus), func(r CohortInvoiceSummary) error {
		return errStop
	}); !errors.Is(err, errStop) {
		t.Fatalf("expected errStop from AggregateCohortInvoicesEach, got %v", err)
	}
	if err := runtime.Each(q.AggregateCohortInvoicesSeq(ctx, CohortMemberIDsQuery(ActiveOrgsQuery(nil), 0), invStatus), func(r CohortInvoiceSummary) error {
		return nil
	}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam from AggregateCohortInvoicesEach, got %v", err)
	}

	// Curried Query constructors & Expand helpers
	var dummyArgs []any
	smQ := q.SearchMembersQuery(SearchMembersParams{OrgID: 1, FilterStatuses: true, Statuses: []string{"active"}})
	_ = smQ.Expand(&dummyArgs, runtime.DialectQuestion, &smQ)
	lmpQ := q.ListMembersPaginatedQuery(ListMembersPaginatedParams{OrgID: 1, Sort: sortConfigs[0].order, Limit: 5})
	_ = lmpQ.Expand(&dummyArgs, runtime.DialectQuestion, &lmpQ)
	aciQ := q.AggregateCohortInvoicesQuery(cohortQ, invStatus)
	_ = aciQ.Expand(&dummyArgs, runtime.DialectQuestion, &aciQ)

	// Error paths: empty slice & invalid seek arity
	if _, err := q.SearchMembers(ctx, SearchMembersParams{OrgID: 1, FilterStatuses: true, Statuses: nil}); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam, got %v", err)
	}
	if _, err := q.ListMembersPaginated(ctx, ListMembersPaginatedParams{
		OrgID: 1,
		After: runtime.NewSeek("admin"), // wrong arity (1 instead of 4)
		Sort:  sortConfigs[0].order,
		Limit: 5,
	}); !errors.Is(err, runtime.ErrInvalidSeek) {
		t.Fatalf("expected ErrInvalidSeek, got %v", err)
	}
	emptyActiveQ := ActiveOrgsQuery(nil)
	if _, err := q.AggregateCohortInvoices(ctx, CohortMemberIDsQuery(emptyActiveQ, 0), invStatus); !errors.Is(err, runtime.ErrEmptySliceParam) {
		t.Fatalf("expected ErrEmptySliceParam from nested subquery, got %v", err)
	}

	// Pipeline, PipelineTx, InTxOpts, WithPrepared, WithTx, Cache, CacheStats
	_ = New(db, runtime.WithCache(nil))
	_ = q.Cache()
	_ = q.CacheStats()
	_ = (&runtime.RowFuture[int]{}).WhereCol("1=1", nil)
	unprep := q.WithPrepared(false)
	if _, err := unprep.SearchMembers(ctx, SearchMembersParams{OrgID: 1}); err != nil {
		t.Fatalf("unprep.SearchMembers: %v", err)
	}
	if err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
		f1 := p.SearchMembers(SearchMembersParams{OrgID: 1, FilterRole: true, Role: "admin"})
		f2 := p.ListMembersPaginated(ListMembersPaginatedParams{OrgID: 1, Sort: sortConfigs[0].order, Limit: 5})
		f3 := p.AggregateCohortInvoices(cohortQ, invStatus)
		p.If(f1.Found(), func(thenP *Pipeline) {
			_ = thenP.ListMembersPaginated(ListMembersPaginatedParams{OrgID: 1, Sort: sortConfigs[0].order, Limit: 2})
		}, func(elseP *Pipeline) {
			_ = elseP.SearchMembers(SearchMembersParams{OrgID: 1})
		})
		p.If(f1.NotFound(), func(thenP *Pipeline) {
			_ = thenP.SearchMembers(SearchMembersParams{OrgID: 1})
		}, func(elseP *Pipeline) {
			_ = elseP.ListMembersPaginated(ListMembersPaginatedParams{OrgID: 1, Sort: sortConfigs[0].order, Limit: 1})
		})
		_ = f2
		_ = f3
		return nil
	}, runtime.WithTxRetry(runtime.DefaultRetryPolicy())); err != nil {
		t.Fatalf("PipelineTx: %v", err)
	}
	if err := q.PipelineTx(ctx, nil, func(p *Pipeline) error {
		return errStop
	}); !errors.Is(err, errStop) {
		t.Fatalf("expected errStop from PipelineTx, got %v", err)
	}
	if err := q.InTx(ctx, nil, func(txQ *Queries) error {
		_, err := txQ.SearchMembers(ctx, SearchMembersParams{OrgID: 1})
		return err
	}); err != nil {
		t.Fatalf("InTx: %v", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	txView := q.WithTx(tx)
	_ = txView.Close()
	_ = tx.Rollback()

	// Exercise DB error paths on a closed connection
	closedDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	_ = closedDB.Close()
	closedSvc := NewPortalService(closedDB)
	_, _ = closedSvc.SearchDirectory(ctx, SearchMembersParams{OrgID: 1})
	_, _ = closedSvc.FetchAllMembersPaginated(ctx, 1, sortConfigs[0].order, 10)
	_, _ = closedSvc.CohortBillingReport(ctx, plans, minScore, invStatus)
}
