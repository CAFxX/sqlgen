//go:generate go run ../../cmd/sqlgen -config sqlgen.yaml

package saas_multitenant

import (
	"context"
	"database/sql"

	"github.com/CAFxX/sqlgen/runtime"
)

const SchemaDDL = `
CREATE TABLE IF NOT EXISTS orgs (
	id     INTEGER PRIMARY KEY,
	name   TEXT    NOT NULL,
	plan   TEXT    NOT NULL,
	status TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS members (
	id         INTEGER PRIMARY KEY,
	org_id     INTEGER NOT NULL,
	email      TEXT    NOT NULL,
	role       TEXT    NOT NULL,
	status     TEXT    NOT NULL,
	score      INTEGER NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS invoices (
	id           INTEGER PRIMARY KEY,
	member_id    INTEGER NOT NULL,
	amount_cents INTEGER NOT NULL,
	status       TEXT    NOT NULL
);
`

const CompactMemberCols = MemberRowColID | MemberRowColEmail | MemberRowColRole | MemberRowColStatus

type PortalService struct {
	q *Queries
}

func NewPortalService(db *sql.DB, opts ...runtime.CacheOption) *PortalService {
	return &PortalService{
		q: New(db, opts...),
	}
}

func (s *PortalService) Queries() *Queries {
	return s.q
}

func (s *PortalService) Close() error {
	return s.q.Close()
}

// SearchDirectory executes a dynamic multi-facet member search with all unconditional columns
// (plus optional joined organization details if WithOrgDetails is true).
func (s *PortalService) SearchDirectory(ctx context.Context, p SearchMembersParams) ([]MemberRow, error) {
	return s.q.SearchMembers(ctx, p)
}

// SearchDirectoryCompact executes a dynamic multi-facet member search projecting only
// (id, email, role, status) via the SelectCols bitmask, leaving unrequested fields zero-valued.
func (s *PortalService) SearchDirectoryCompact(ctx context.Context, p SearchMembersParams) ([]MemberRow, error) {
	p.SelectCols = CompactMemberCols
	return s.q.SearchMembers(ctx, p)
}

// FetchAllMembersPaginated walks all pages of an organization's members using composite keyset
// pagination (Seek + Order) without OFFSET.
func (s *PortalService) FetchAllMembersPaginated(
	ctx context.Context,
	orgID int64,
	sortOrder Order,
	pageSize int64,
) ([]MemberItem, error) {
	var all []MemberItem
	var cursor Seek
	for {
		page, err := s.q.ListMembersPaginated(ctx, ListMembersPaginatedParams{
			OrgID: orgID,
			After: cursor,
			Sort:  sortOrder,
			Limit: pageSize,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if int64(len(page)) < pageSize || len(page) == 0 {
			break
		}
		cursor = page[len(page)-1].Seek()
	}
	return all, nil
}

// CohortBillingReport composes two reusable subquery fragments (ActiveOrgsQuery and CohortMemberIDsQuery)
// into a CTE aggregation query (AggregateCohortInvoices) with automatic parameter renumbering.
func (s *PortalService) CohortBillingReport(
	ctx context.Context,
	plans []string,
	minScore int64,
	invoiceStatus string,
) ([]CohortInvoiceSummary, error) {
	activeOrgs := s.q.ActiveOrgsQuery(plans)
	cohortMembers := s.q.CohortMemberIDsQuery(activeOrgs, minScore)
	return s.q.AggregateCohortInvoices(ctx, cohortMembers, invoiceStatus)
}
