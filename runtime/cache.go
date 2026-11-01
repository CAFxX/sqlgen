package runtime

import (
	"context"
	"database/sql"
	"errors"
	stdruntime "runtime"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	numCacheShards        = 16
	defaultMaxSQLVariants = 4096
	defaultMaxPartialBytes = 16 * 1024 * 1024 // 16 MB total (1 MB per shard)
	defaultMaxPrepared    = 256               // 256 total (16 per shard)
	defaultPromoteThresh  = 2
	maxSinglePartialBytes = 256 * 1024 // 256 KB
)

// EntryState describes the cache residency tier of a statement variation.
type EntryState uint8

const (
	// StateAbsent indicates the variation is not present in either cache tier.
	StateAbsent EntryState = iota
	// StatePartial indicates the variation's rendered SQL string is cached in Tier 1.
	StatePartial
	// StatePrepared indicates the variation has an open *sql.Stmt cached in Tier 2.
	StatePrepared
)

type entryTier uint8

const (
	tierNone entryTier = iota
	tierPartialProb
	tierPartialProt
	tierPreparedProb
	tierPreparedProt
)

// ManagedStmt wraps a *sql.Stmt with atomic reference counting so concurrent
// callers and open transactions never observe a closed statement on demotion.
type ManagedStmt struct {
	stmt *sql.Stmt
	refs atomic.Int32
}

func newManagedStmt(stmt *sql.Stmt, initialRefs int32) *ManagedStmt {
	m := &ManagedStmt{stmt: stmt}
	m.refs.Store(initialRefs)
	return m
}

// Acquire increments the reference count if the statement is still alive (refs > 0).
func (m *ManagedStmt) Acquire() bool {
	for {
		cur := m.refs.Load()
		if cur <= 0 {
			return false
		}
		if m.refs.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// Release decrements the reference count and closes the underlying *sql.Stmt when it hits 0.
func (m *ManagedStmt) Release() error {
	next := m.refs.Add(-1)
	if next == 0 && m.stmt != nil {
		return m.stmt.Close()
	}
	return nil
}

// Stmt returns the underlying *sql.Stmt.
func (m *ManagedStmt) Stmt() *sql.Stmt {
	return m.stmt
}

// Refs returns the current reference count (for testing/diagnostics).
func (m *ManagedStmt) Refs() int32 {
	return m.refs.Load()
}

type txState struct {
	mu     sync.Mutex
	heldMS []*ManagedStmt
	closed bool
}

func (st *txState) releaseAll() {
	st.mu.Lock()
	if st.closed {
		st.mu.Unlock()
		return
	}
	st.closed = true
	held := st.heldMS
	st.heldMS = nil
	st.mu.Unlock()

	for _, ms := range held {
		_ = ms.Release()
	}
}

// TxStmtCache caches transaction-scoped *sql.Stmt handles bound via tx.StmtContext
// and holds a reference to each underlying ManagedStmt until the transaction ends.
type TxStmtCache struct {
	tx     *sql.Tx
	mu     sync.Mutex
	byBase map[*ManagedStmt]*sql.Stmt
	state  *txState
}

// NewTxStmtCache creates a transaction statement cache for tx and registers a GC
// cleanup fallback in case the caller used WithTx manually without InTx.
func NewTxStmtCache(tx *sql.Tx) *TxStmtCache {
	state := &txState{}
	tc := &TxStmtCache{
		tx:     tx,
		byBase: make(map[*ManagedStmt]*sql.Stmt, 8),
		state:  state,
	}
	stdruntime.AddCleanup(tc, func(st *txState) {
		st.releaseAll()
	}, state)
	return tc
}

// Tx returns the underlying *sql.Tx.
func (tc *TxStmtCache) Tx() *sql.Tx {
	return tc.tx
}

// StmtFromManaged returns a transaction-bound *sql.Stmt for ms, acquiring a reference
// on ms for the lifetime of tc on first use within the transaction.
func (tc *TxStmtCache) StmtFromManaged(ctx context.Context, ms *ManagedStmt) (*sql.Stmt, error) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.state.mu.Lock()
	if tc.state.closed {
		tc.state.mu.Unlock()
		return nil, errors.New("sqlgen: transaction statement cache is closed")
	}
	if txStmt, ok := tc.byBase[ms]; ok {
		tc.state.mu.Unlock()
		return txStmt, nil
	}
	if !ms.Acquire() {
		tc.state.mu.Unlock()
		return nil, errors.New("sqlgen: managed statement already released")
	}
	tc.state.heldMS = append(tc.state.heldMS, ms)
	tc.state.mu.Unlock()

	txStmt := tc.tx.StmtContext(ctx, ms.stmt)
	tc.byBase[ms] = txStmt
	return txStmt, nil
}

// Close deterministically releases all held ManagedStmt references for this transaction.
func (tc *TxStmtCache) Close() error {
	if tc == nil || tc.state == nil {
		return nil
	}
	tc.state.releaseAll()
	tc.mu.Lock()
	clear(tc.byBase)
	tc.mu.Unlock()
	return nil
}

// tinyLFUSketch is a 4-bit Count-Min Sketch with 4,096 counters (256 uint64 words = 2 KB).
type tinyLFUSketch struct {
	table        [256]uint64
	sampleCount  uint32
	sampleWindow uint32
}

func (s *tinyLFUSketch) init(window uint32) {
	if window < 512 {
		window = 512
	}
	s.sampleWindow = window
}

func sketchCoords(h uint64, row int) (wordIdx uint8, shift uint) {
	// Derive 4 independent 12-bit indices (0..4095) from the 64-bit hash.
	sub := uint16((h >> uint(row*16)) & 0xFFF)
	wordIdx = uint8(sub >> 4)
	shift = uint(sub&0xF) * 4
	return wordIdx, shift
}

func (s *tinyLFUSketch) estimate(h uint64) uint8 {
	minVal := uint8(15)
	for r := 0; r < 4; r++ {
		wIdx, shift := sketchCoords(h, r)
		val := uint8((s.table[wIdx] >> shift) & 0xF)
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}

func (s *tinyLFUSketch) increment(h uint64) uint8 {
	s.sampleCount++
	if s.sampleCount >= s.sampleWindow {
		for i := range s.table {
			s.table[i] = (s.table[i] >> 1) & 0x7777777777777777
		}
		s.sampleCount = 0
	}

	var vals [4]uint8
	var wIdxs [4]uint8
	var shifts [4]uint
	minVal := uint8(15)
	for r := 0; r < 4; r++ {
		wIdx, shift := sketchCoords(h, r)
		wIdxs[r] = wIdx
		shifts[r] = shift
		v := uint8((s.table[wIdx] >> shift) & 0xF)
		vals[r] = v
		if v < minVal {
			minVal = v
		}
	}
	if minVal < 15 {
		for r := 0; r < 4; r++ {
			if vals[r] == minVal {
				s.table[wIdxs[r]] += uint64(1) << shifts[r]
			}
		}
		return minVal + 1
	}
	return 15
}

type cacheEntry struct {
	key       VariantKey
	sqlStr    string
	hash      uint64
	stmtID    uint32
	shapeTag  uint64
	batchSize int
	exact     bool
	inMap     bool
	preparing bool
	tier      entryTier
	prev      *cacheEntry
	next      *cacheEntry
	ms        atomic.Pointer[ManagedStmt]
	fastSlot  *atomic.Pointer[ManagedStmt]
}

type entryList struct {
	head *cacheEntry
	tail *cacheEntry
	len  int
}

func (l *entryList) pushFront(e *cacheEntry, tier entryTier) {
	e.tier = tier
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	} else {
		l.tail = e
	}
	l.head = e
	l.len++
}

func (l *entryList) remove(e *cacheEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev = nil
	e.next = nil
	e.tier = tierNone
	l.len--
}

type cacheShard struct {
	mu               sync.Mutex
	sketch           tinyLFUSketch
	byKey            map[VariantKey]*cacheEntry
	bySQL            map[string]*cacheEntry
	partialProb      entryList
	partialProt      entryList
	preparedProb     entryList
	preparedProt     entryList
	maxPartial       int
	maxPartialProt   int
	maxPartialBytes  int
	partialBytes     int
	maxPrepared      int
	maxPreparedProt  int
	promoteThreshold uint8

	preparedHits   uint64
	partialHits    uint64
	misses         uint64
	promotions     uint64
	demotions      uint64
	evictions      uint64
	rejectedAdmits uint64
}

func (s *cacheShard) lookupLocked(exact bool, key VariantKey, sqlStr string) *cacheEntry {
	if exact {
		return s.byKey[key]
	}
	return s.bySQL[sqlStr]
}

func (s *cacheShard) insertMapLocked(e *cacheEntry) {
	if e.exact {
		s.byKey[e.key] = e
	} else {
		s.bySQL[e.sqlStr] = e
	}
	e.inMap = true
}

func (s *cacheShard) removeMapLocked(e *cacheEntry) {
	if !e.inMap {
		return
	}
	if e.exact {
		delete(s.byKey, e.key)
	} else {
		delete(s.bySQL, e.sqlStr)
	}
	e.inMap = false
}

func (s *cacheShard) partialVictim() *cacheEntry {
	if s.partialProb.tail != nil {
		return s.partialProb.tail
	}
	return s.partialProt.tail
}

func (s *cacheShard) preparedVictim() *cacheEntry {
	if s.preparedProb.tail != nil {
		return s.preparedProb.tail
	}
	return s.preparedProt.tail
}

func (s *cacheShard) touchPartialLocked(e *cacheEntry) {
	switch e.tier {
	case tierPartialProt:
		if s.partialProt.head != e {
			s.partialProt.remove(e)
			s.partialProt.pushFront(e, tierPartialProt)
		}
	case tierPartialProb:
		s.partialProb.remove(e)
		if s.maxPartialProt > 0 {
			s.partialProt.pushFront(e, tierPartialProt)
			if s.partialProt.len > s.maxPartialProt {
				stepDown := s.partialProt.tail
				s.partialProt.remove(stepDown)
				s.partialProb.pushFront(stepDown, tierPartialProb)
			}
		} else {
			s.partialProb.pushFront(e, tierPartialProb)
		}
	}
}

func (s *cacheShard) touchPreparedLocked(e *cacheEntry) {
	switch e.tier {
	case tierPreparedProt:
		if s.preparedProt.head != e {
			s.preparedProt.remove(e)
			s.preparedProt.pushFront(e, tierPreparedProt)
		}
	case tierPreparedProb:
		s.preparedProb.remove(e)
		if s.maxPreparedProt > 0 {
			s.preparedProt.pushFront(e, tierPreparedProt)
			if s.preparedProt.len > s.maxPreparedProt {
				stepDown := s.preparedProt.tail
				s.preparedProt.remove(stepDown)
				s.preparedProb.pushFront(stepDown, tierPreparedProb)
			}
		} else {
			s.preparedProb.pushFront(e, tierPreparedProb)
		}
	}
}

func (s *cacheShard) removeFromPartialListLocked(e *cacheEntry) {
	switch e.tier {
	case tierPartialProb:
		s.partialProb.remove(e)
		s.partialBytes -= len(e.sqlStr)
	case tierPartialProt:
		s.partialProt.remove(e)
		s.partialBytes -= len(e.sqlStr)
	}
}

func (s *cacheShard) removeFromPreparedListLocked(e *cacheEntry) {
	switch e.tier {
	case tierPreparedProb:
		s.preparedProb.remove(e)
	case tierPreparedProt:
		s.preparedProt.remove(e)
	}
}

func (s *cacheShard) evictPartialEntryLocked(victim *cacheEntry) *cacheEntry {
	s.removeFromPartialListLocked(victim)
	s.removeMapLocked(victim)
	s.evictions++
	return victim
}

func (s *cacheShard) demoteToPartialLocked(victim *cacheEntry, c *StmtCache) *ManagedStmt {
	s.removeFromPreparedListLocked(victim)
	demotedMS := victim.ms.Swap(nil)
	if victim.fastSlot != nil && demotedMS != nil {
		victim.fastSlot.CompareAndSwap(demotedMS, nil)
	}
	if victim.batchSize > 0 {
		c.removePreparedBatchSize(victim.stmtID, victim.shapeTag, victim.batchSize)
	}
	s.demotions++
	c.totalPrepared.Add(-1)

	sqlLen := len(victim.sqlStr)
	if s.maxPartial > 0 && sqlLen <= maxSinglePartialBytes && sqlLen <= s.maxPartialBytes {
		for (s.partialProb.len+s.partialProt.len >= s.maxPartial || s.partialBytes+sqlLen > s.maxPartialBytes) && s.partialVictim() != nil {
			pv := s.partialVictim()
			s.evictPartialEntryLocked(pv)
		}
		if s.maxPartialProt > 0 {
			s.partialProt.pushFront(victim, tierPartialProt)
			if s.partialProt.len > s.maxPartialProt {
				stepDown := s.partialProt.tail
				s.partialProt.remove(stepDown)
				s.partialProb.pushFront(stepDown, tierPartialProb)
			}
		} else {
			s.partialProb.pushFront(victim, tierPartialProb)
		}
		s.partialBytes += sqlLen
	} else {
		s.removeMapLocked(victim)
	}
	return demotedMS
}

func (s *cacheShard) initEntry(e *cacheEntry, exact bool, key VariantKey, sqlStr string, h uint64, stmtID uint32, shapeTag uint64, batchSize int, fastSlot *atomic.Pointer[ManagedStmt]) {
	e.key = key
	e.sqlStr = sqlStr
	e.hash = h
	e.stmtID = stmtID
	e.shapeTag = shapeTag
	e.batchSize = batchSize
	e.exact = exact
	e.inMap = false
	e.preparing = false
	e.tier = tierNone
	e.prev = nil
	e.next = nil
	e.ms.Store(nil)
	e.fastSlot = fastSlot
}

// CacheConfig configures StmtCache capacities and promotion behavior.
type CacheConfig struct {
	MaxSQLVariants   int
	MaxPartialBytes  int
	MaxPreparedStmts int
	PromoteThreshold uint8
}

// CacheOption configures a StmtCache.
type CacheOption func(*CacheConfig)

// WithMaxSQLVariants sets the maximum number of Tier-1 SQL strings cached across all shards.
func WithMaxSQLVariants(n int) CacheOption {
	return func(cfg *CacheConfig) {
		cfg.MaxSQLVariants = n
	}
}

// WithMaxPartialBytes sets the maximum total byte budget for Tier-1 SQL strings across all shards.
func WithMaxPartialBytes(bytes int) CacheOption {
	return func(cfg *CacheConfig) {
		cfg.MaxPartialBytes = bytes
	}
}

// WithMaxPreparedStmts sets the maximum number of Tier-2 prepared *sql.Stmt handles across all shards.
func WithMaxPreparedStmts(n int) CacheOption {
	return func(cfg *CacheConfig) {
		cfg.MaxPreparedStmts = n
	}
}

// WithPromoteThreshold sets the W-TinyLFU frequency threshold (1..15) required to promote a statement to Tier 2.
func WithPromoteThreshold(threshold uint8) CacheOption {
	return func(cfg *CacheConfig) {
		if threshold < 1 {
			threshold = 1
		}
		if threshold > 15 {
			threshold = 15
		}
		cfg.PromoteThreshold = threshold
	}
}

// CacheStats reports cumulative and current telemetry across all StmtCache shards.
type CacheStats struct {
	PreparedHits   uint64
	PartialHits    uint64
	Misses         uint64
	Promotions     uint64
	Demotions      uint64
	Evictions      uint64
	RejectedAdmits uint64
	ActivePrepared int
	ActivePartial  int
	PartialBytes   int
}

type batchShapeKey struct {
	stmtID   uint32
	shapeTag uint64
}

type batchShapeTracker struct {
	sketch        tinyLFUSketch
	preparedSizes []int // sorted ascending
	promotedSizes map[int]struct{}
}

// StmtCache is a 16-way sharded, two-tier adaptive statement cache (W-TinyLFU + SLRU).
type StmtCache struct {
	shards          [numCacheShards]cacheShard
	fastHits        atomic.Uint64
	totalPrepared   atomic.Int64
	globalMaxPrep   int
	promoteThresh   uint8
	closed          atomic.Bool

	batchMu       sync.RWMutex
	batchTrackers map[batchShapeKey]*batchShapeTracker
}

// NewStmtCache creates a new 16-shard two-tier adaptive statement cache.
func NewStmtCache(opts ...CacheOption) *StmtCache {
	cfg := CacheConfig{
		MaxSQLVariants:   defaultMaxSQLVariants,
		MaxPartialBytes:  defaultMaxPartialBytes,
		MaxPreparedStmts: defaultMaxPrepared,
		PromoteThreshold: defaultPromoteThresh,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	c := &StmtCache{
		globalMaxPrep: cfg.MaxPreparedStmts,
		promoteThresh: cfg.PromoteThreshold,
		batchTrackers: make(map[batchShapeKey]*batchShapeTracker),
	}

	perShardPartial := 0
	if cfg.MaxSQLVariants > 0 {
		perShardPartial = (cfg.MaxSQLVariants + numCacheShards - 1) / numCacheShards
		if cfg.MaxSQLVariants < numCacheShards {
			perShardPartial = cfg.MaxSQLVariants
		}
	}
	perShardBytes := 0
	if cfg.MaxPartialBytes > 0 {
		perShardBytes = (cfg.MaxPartialBytes + numCacheShards - 1) / numCacheShards
		if cfg.MaxPartialBytes < numCacheShards {
			perShardBytes = cfg.MaxPartialBytes
		}
	}
	perShardPrep := 0
	if cfg.MaxPreparedStmts > 0 {
		perShardPrep = cfg.MaxPreparedStmts / numCacheShards
		if perShardPrep < 1 {
			perShardPrep = 1
		}
	}

	for i := range c.shards {
		sh := &c.shards[i]
		sh.maxPartial = perShardPartial
		sh.maxPartialProt = (perShardPartial * 4) / 5
		if sh.maxPartialProt == 0 && perShardPartial > 1 {
			sh.maxPartialProt = 1
		}
		sh.maxPartialBytes = perShardBytes
		sh.maxPrepared = perShardPrep
		sh.maxPreparedProt = (perShardPrep * 4) / 5
		if sh.maxPreparedProt == 0 && perShardPrep > 1 {
			sh.maxPreparedProt = 1
		}
		sh.promoteThreshold = cfg.PromoteThreshold
		sh.byKey = make(map[VariantKey]*cacheEntry, 32)
		sh.bySQL = make(map[string]*cacheEntry, 8)
		sh.sketch.init(uint32(max(perShardPartial+perShardPrep, 64) * 8))
	}

	return c
}

// ComputeBatchShapeTag computes a compact shape tag for batch size tracking.
func ComputeBatchShapeTag(colMask, flagMask uint64) uint64 {
	return (colMask << 32) ^ flagMask
}

func (c *StmtCache) isPromotedBatchSize(stmtID uint32, shapeTag uint64, batchSize int) bool {
	c.batchMu.RLock()
	tr := c.batchTrackers[batchShapeKey{stmtID: stmtID, shapeTag: shapeTag}]
	if tr == nil {
		c.batchMu.RUnlock()
		return false
	}
	_, ok := tr.promotedSizes[batchSize]
	c.batchMu.RUnlock()
	return ok
}

func (c *StmtCache) recordPreparedBatchSize(stmtID uint32, shapeTag uint64, batchSize int) {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()
	key := batchShapeKey{stmtID: stmtID, shapeTag: shapeTag}
	tr := c.batchTrackers[key]
	if tr == nil {
		tr = &batchShapeTracker{
			promotedSizes: make(map[int]struct{}),
		}
		tr.sketch.init(512)
		c.batchTrackers[key] = tr
	}
	tr.promotedSizes[batchSize] = struct{}{}
	idx, found := slices.BinarySearch(tr.preparedSizes, batchSize)
	if !found {
		tr.preparedSizes = slices.Insert(tr.preparedSizes, idx, batchSize)
	}
}

func (c *StmtCache) removePreparedBatchSize(stmtID uint32, shapeTag uint64, batchSize int) {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()
	key := batchShapeKey{stmtID: stmtID, shapeTag: shapeTag}
	tr := c.batchTrackers[key]
	if tr == nil {
		return
	}
	if idx, found := slices.BinarySearch(tr.preparedSizes, batchSize); found {
		tr.preparedSizes = slices.Delete(tr.preparedSizes, idx, idx+1)
	}
}

var fallbackPowerOf2 = [...]int{256, 128, 64, 32, 16, 8, 4, 2, 1}

// PlanBatchChunk selects the next batch chunk size for a tuple-slice statement.
// It combines exact-size heavy-hitter promotion, greedy reuse of Tier-2 prepared
// batch sizes, and power-of-2 fallback chunking (256..1).
func (c *StmtCache) PlanBatchChunk(stmtID uint32, colMask, flagMask uint64, remainingRows, maxRowsPerStmt int, isFirstChunk bool) int {
	if remainingRows <= 0 {
		return 0
	}
	capRows := remainingRows
	if maxRowsPerStmt > 0 && capRows > maxRowsPerStmt {
		capRows = maxRowsPerStmt
	}
	if c == nil {
		for _, p2 := range fallbackPowerOf2 {
			if p2 <= capRows {
				return p2
			}
		}
		return 1
	}

	shapeTag := ComputeBatchShapeTag(colMask, flagMask)
	key := batchShapeKey{stmtID: stmtID, shapeTag: shapeTag}

	c.batchMu.Lock()
	tr := c.batchTrackers[key]
	if tr == nil {
		tr = &batchShapeTracker{
			promotedSizes: make(map[int]struct{}),
		}
		tr.sketch.init(512)
		c.batchTrackers[key] = tr
	}

	// 1. If capRows is already a prepared Tier-2 size or promoted heavy-hitter size, use it directly.
	if _, ok := tr.promotedSizes[capRows]; ok {
		c.batchMu.Unlock()
		return capRows
	}
	if _, found := slices.BinarySearch(tr.preparedSizes, capRows); found {
		c.batchMu.Unlock()
		return capRows
	}

	// 2. If this is the initial chunk check for a recurring batch call, update the heavy-hitter sketch.
	if isFirstChunk {
		h := mix64(uint64(stmtID)<<32 ^ shapeTag ^ uint64(capRows)*0x9e3779b97f4a7c15)
		freq := tr.sketch.increment(h)
		if freq >= c.promoteThresh {
			tr.promotedSizes[capRows] = struct{}{}
			c.batchMu.Unlock()
			return capRows
		}
	}

	// 3. Find the largest prepared Tier-2 batch size <= capRows.
	bestPrepared := 0
	for i := len(tr.preparedSizes) - 1; i >= 0; i-- {
		if tr.preparedSizes[i] <= capRows {
			bestPrepared = tr.preparedSizes[i]
			break
		}
	}
	c.batchMu.Unlock()

	// 4. Find the largest power-of-2 fallback chunk <= capRows.
	bestPow2 := 1
	for _, p2 := range fallbackPowerOf2 {
		if p2 <= capRows {
			bestPow2 = p2
			break
		}
	}

	if bestPrepared >= bestPow2 {
		return bestPrepared
	}
	return bestPow2
}

// evictGlobalPreparedVictim demotes one prepared statement from another shard when globalMaxPrep < numCacheShards.
func (c *StmtCache) evictGlobalPreparedVictim(skipShardIdx int) *ManagedStmt {
	for i := 0; i < numCacheShards; i++ {
		idx := (skipShardIdx + 1 + i) & (numCacheShards - 1)
		if idx == skipShardIdx {
			continue
		}
		sh := &c.shards[idx]
		sh.mu.Lock()
		if v := sh.preparedVictim(); v != nil {
			dms := sh.demoteToPartialLocked(v, c)
			sh.mu.Unlock()
			return dms
		}
		sh.mu.Unlock()
	}
	return nil
}

func (c *StmtCache) resolve(
	ctx context.Context,
	db *sql.DB,
	txCache *TxStmtCache,
	usePrepared bool,
	key VariantKey,
	stmtID uint32,
	shapeTag uint64,
	batchSize int,
	fastSlot *atomic.Pointer[ManagedStmt],
	buildSQL func() string,
) (*ManagedStmt, string, error) {
	if c == nil {
		return nil, buildSQL(), nil
	}
	if c.closed.Load() {
		return nil, "", errors.New("sqlgen: statement cache is closed")
	}

	if usePrepared && fastSlot != nil {
		if ms := fastSlot.Load(); ms != nil && ms.Acquire() {
			c.fastHits.Add(1)
			return ms, "", nil
		}
	}

	exact := key.IsExact()
	var sqlStr string
	var h uint64
	if exact {
		h = key.Hash()
	} else {
		sqlStr = buildSQL()
		h = HashString(sqlStr)
	}

	shardIdx := int(h & (numCacheShards - 1))
	sh := &c.shards[shardIdx]

	sh.mu.Lock()
	entry := sh.lookupLocked(exact, key, sqlStr)

	// 1. Hit in Tier 2 (Prepared)
	if entry != nil {
		if ms := entry.ms.Load(); ms != nil {
			if usePrepared && ms.Acquire() {
				sh.sketch.increment(entry.hash)
				sh.touchPreparedLocked(entry)
				sh.preparedHits++
				if fastSlot != nil {
					entry.fastSlot = fastSlot
					fastSlot.Store(ms)
				}
				sh.mu.Unlock()
				return ms, "", nil
			}
			if !usePrepared {
				sh.sketch.increment(entry.hash)
				sh.touchPreparedLocked(entry)
				sh.partialHits++
				s := entry.sqlStr
				sh.mu.Unlock()
				return nil, s, nil
			}
		}
	}

	// 2. Hit in Tier 1 (Partial)
	if entry != nil {
		freq := sh.sketch.increment(entry.hash)
		sh.touchPartialLocked(entry)
		canPrepareOnDB := usePrepared && sh.maxPrepared > 0 && c.globalMaxPrep > 0 && (txCache == nil || db.Stats().MaxOpenConnections != 1)
		effThresh := sh.promoteThreshold
		if batchSize > 0 && effThresh > 1 && c.isPromotedBatchSize(stmtID, shapeTag, batchSize) {
			effThresh = 1
		}

		if canPrepareOnDB && !entry.preparing && freq >= effThresh {
			eligible := true
			if sh.preparedProb.len+sh.preparedProt.len >= sh.maxPrepared {
				if victim := sh.preparedVictim(); victim != nil {
					if freq <= sh.sketch.estimate(victim.hash) {
						eligible = false
					}
				}
			}
			if eligible {
				entry.preparing = true
				s := entry.sqlStr
				sh.mu.Unlock()

				prepared, err := db.PrepareContext(ctx, s)

				sh.mu.Lock()
				entry.preparing = false
				if err != nil {
					sh.mu.Unlock()
					return nil, "", err
				}
				if c.closed.Load() {
					sh.mu.Unlock()
					_ = prepared.Close()
					return nil, "", errors.New("sqlgen: statement cache is closed")
				}
				if existingMS := entry.ms.Load(); existingMS != nil && existingMS.Acquire() {
					sh.mu.Unlock()
					_ = prepared.Close()
					return existingMS, "", nil
				}
				if !entry.inMap {
					newMS := newManagedStmt(prepared, 1)
					sh.mu.Unlock()
					return newMS, "", nil
				}

				var demoted []*ManagedStmt
				for sh.preparedProb.len+sh.preparedProt.len >= sh.maxPrepared {
					victim := sh.preparedVictim()
					if victim == nil {
						break
					}
					if dms := sh.demoteToPartialLocked(victim, c); dms != nil {
						demoted = append(demoted, dms)
					}
				}

				sh.removeFromPartialListLocked(entry)
				newMS := newManagedStmt(prepared, 2)
				entry.ms.Store(newMS)
				if fastSlot != nil {
					entry.fastSlot = fastSlot
					fastSlot.Store(newMS)
				}
				sh.preparedProb.pushFront(entry, tierPreparedProb)
				sh.promotions++
				totalPrep := c.totalPrepared.Add(1)
				if batchSize > 0 {
					c.recordPreparedBatchSize(stmtID, shapeTag, batchSize)
				}
				sh.mu.Unlock()

				if c.globalMaxPrep < numCacheShards && int(totalPrep) > c.globalMaxPrep {
					if gdms := c.evictGlobalPreparedVictim(shardIdx); gdms != nil {
						demoted = append(demoted, gdms)
					}
				}
				for _, dms := range demoted {
					_ = dms.Release()
				}
				return newMS, "", nil
			}
		}

		s := entry.sqlStr
		sh.partialHits++
		sh.mu.Unlock()
		return nil, s, nil
	}

	// 3. Cache Miss (Unseen Variation)
	freq := sh.sketch.increment(h)
	sh.misses++
	effThresh := sh.promoteThreshold
	sh.mu.Unlock()

	if exact {
		sqlStr = buildSQL()
	}

	if batchSize > 0 && effThresh > 1 && c.isPromotedBatchSize(stmtID, shapeTag, batchSize) {
		effThresh = 1
	}

	canPrepareOnDB := usePrepared && sh.maxPrepared > 0 && c.globalMaxPrep > 0 && (txCache == nil || db.Stats().MaxOpenConnections != 1)
	if effThresh == 1 && canPrepareOnDB {
		prepared, err := db.PrepareContext(ctx, sqlStr)
		if err != nil {
			return nil, "", err
		}

		sh.mu.Lock()
		if c.closed.Load() {
			sh.mu.Unlock()
			_ = prepared.Close()
			return nil, "", errors.New("sqlgen: statement cache is closed")
		}
		if existing := sh.lookupLocked(exact, key, sqlStr); existing != nil {
			if ems := existing.ms.Load(); ems != nil && ems.Acquire() {
				sh.mu.Unlock()
				_ = prepared.Close()
				return ems, "", nil
			}
			sh.removeFromPartialListLocked(existing)
			sh.removeMapLocked(existing)
		}

		var demoted []*ManagedStmt
		for sh.preparedProb.len+sh.preparedProt.len >= sh.maxPrepared {
			victim := sh.preparedVictim()
			if victim == nil {
				break
			}
			if dms := sh.demoteToPartialLocked(victim, c); dms != nil {
				demoted = append(demoted, dms)
			}
		}

		e := &cacheEntry{}
		sh.initEntry(e, exact, key, sqlStr, h, stmtID, shapeTag, batchSize, fastSlot)
		newMS := newManagedStmt(prepared, 2)
		e.ms.Store(newMS)
		if fastSlot != nil {
			fastSlot.Store(newMS)
		}
		sh.insertMapLocked(e)
		sh.preparedProb.pushFront(e, tierPreparedProb)
		sh.promotions++
		totalPrep := c.totalPrepared.Add(1)
		if batchSize > 0 {
			c.recordPreparedBatchSize(stmtID, shapeTag, batchSize)
		}
		sh.mu.Unlock()

		if c.globalMaxPrep < numCacheShards && int(totalPrep) > c.globalMaxPrep {
			if gdms := c.evictGlobalPreparedVictim(shardIdx); gdms != nil {
				demoted = append(demoted, gdms)
			}
		}
		for _, dms := range demoted {
			_ = dms.Release()
		}
		return newMS, "", nil
	}

	// Tier-1 W-TinyLFU Admission
	sqlLen := len(sqlStr)
	if sh.maxPartial <= 0 || sqlLen > maxSinglePartialBytes || sqlLen > sh.maxPartialBytes {
		return nil, sqlStr, nil
	}

	sh.mu.Lock()
	if c.closed.Load() {
		sh.mu.Unlock()
		return nil, sqlStr, nil
	}
	if existing := sh.lookupLocked(exact, key, sqlStr); existing != nil {
		sh.mu.Unlock()
		return nil, sqlStr, nil
	}

	atCap := sh.partialProb.len+sh.partialProt.len >= sh.maxPartial || sh.partialBytes+sqlLen > sh.maxPartialBytes
	if atCap {
		victim := sh.partialVictim()
		if victim != nil {
			victimFreq := sh.sketch.estimate(victim.hash)
			if freq < victimFreq {
				sh.rejectedAdmits++
				sh.mu.Unlock()
				return nil, sqlStr, nil
			}
			// Recycle victim's *cacheEntry struct in-place!
			recycled := sh.evictPartialEntryLocked(victim)
			for (sh.partialProb.len+sh.partialProt.len >= sh.maxPartial || sh.partialBytes+sqlLen > sh.maxPartialBytes) && sh.partialVictim() != nil {
				sh.evictPartialEntryLocked(sh.partialVictim())
			}
			sh.initEntry(recycled, exact, key, sqlStr, h, stmtID, shapeTag, batchSize, fastSlot)
			sh.insertMapLocked(recycled)
			sh.partialProb.pushFront(recycled, tierPartialProb)
			sh.partialBytes += sqlLen
			sh.mu.Unlock()
			return nil, sqlStr, nil
		}
	}

	e := &cacheEntry{}
	sh.initEntry(e, exact, key, sqlStr, h, stmtID, shapeTag, batchSize, fastSlot)
	sh.insertMapLocked(e)
	sh.partialProb.pushFront(e, tierPartialProb)
	sh.partialBytes += sqlLen
	sh.mu.Unlock()
	return nil, sqlStr, nil
}

// Query resolves the statement variation in the two-tier cache and executes QueryContext.
// When a Tier-2 ManagedStmt is used, it is returned with an active reference that the caller
// MUST release after closing *sql.Rows.
func (c *StmtCache) Query(
	ctx context.Context,
	db *sql.DB,
	txCache *TxStmtCache,
	usePrepared bool,
	key VariantKey,
	stmtID uint32,
	shapeTag uint64,
	batchSize int,
	fastSlot *atomic.Pointer[ManagedStmt],
	buildSQL func() string,
	args ...any,
) (*sql.Rows, *ManagedStmt, error) {
	ms, sqlStr, err := c.resolve(ctx, db, txCache, usePrepared, key, stmtID, shapeTag, batchSize, fastSlot, buildSQL)
	if err != nil {
		return nil, nil, err
	}
	if ms != nil {
		if txCache != nil {
			txStmt, err := txCache.StmtFromManaged(ctx, ms)
			if err != nil {
				_ = ms.Release()
				return nil, nil, err
			}
			rows, err := txStmt.QueryContext(ctx, args...)
			if err != nil {
				_ = ms.Release()
				return nil, nil, err
			}
			return rows, ms, nil
		}
		rows, err := ms.stmt.QueryContext(ctx, args...)
		if err != nil {
			_ = ms.Release()
			return nil, nil, err
		}
		return rows, ms, nil
	}

	if txCache != nil {
		rows, err := txCache.tx.QueryContext(ctx, sqlStr, args...)
		return rows, nil, err
	}
	rows, err := db.QueryContext(ctx, sqlStr, args...)
	return rows, nil, err
}

// QueryRow resolves the statement variation in the two-tier cache and returns (*sql.Row, *ManagedStmt, error).
// The caller MUST call ms.Release() after calling row.Scan(...).
func (c *StmtCache) QueryRow(
	ctx context.Context,
	db *sql.DB,
	txCache *TxStmtCache,
	usePrepared bool,
	key VariantKey,
	stmtID uint32,
	shapeTag uint64,
	batchSize int,
	fastSlot *atomic.Pointer[ManagedStmt],
	buildSQL func() string,
	args ...any,
) (*sql.Row, *ManagedStmt, error) {
	ms, sqlStr, err := c.resolve(ctx, db, txCache, usePrepared, key, stmtID, shapeTag, batchSize, fastSlot, buildSQL)
	if err != nil {
		return nil, nil, err
	}
	if ms != nil {
		if txCache != nil {
			txStmt, err := txCache.StmtFromManaged(ctx, ms)
			if err != nil {
				_ = ms.Release()
				return nil, nil, err
			}
			return txStmt.QueryRowContext(ctx, args...), ms, nil
		}
		return ms.stmt.QueryRowContext(ctx, args...), ms, nil
	}

	if txCache != nil {
		return txCache.tx.QueryRowContext(ctx, sqlStr, args...), nil, nil
	}
	return db.QueryRowContext(ctx, sqlStr, args...), nil, nil
}

// Exec resolves the statement variation in the two-tier cache and executes ExecContext.
func (c *StmtCache) Exec(
	ctx context.Context,
	db *sql.DB,
	txCache *TxStmtCache,
	usePrepared bool,
	key VariantKey,
	stmtID uint32,
	shapeTag uint64,
	batchSize int,
	fastSlot *atomic.Pointer[ManagedStmt],
	buildSQL func() string,
	args ...any,
) (sql.Result, error) {
	ms, sqlStr, err := c.resolve(ctx, db, txCache, usePrepared, key, stmtID, shapeTag, batchSize, fastSlot, buildSQL)
	if err != nil {
		return nil, err
	}
	if ms != nil {
		defer func() { _ = ms.Release() }()
		if txCache != nil {
			txStmt, err := txCache.StmtFromManaged(ctx, ms)
			if err != nil {
				return nil, err
			}
			return txStmt.ExecContext(ctx, args...)
		}
		return ms.stmt.ExecContext(ctx, args...)
	}

	if txCache != nil {
		return txCache.tx.ExecContext(ctx, sqlStr, args...)
	}
	return db.ExecContext(ctx, sqlStr, args...)
}

// LookupState inspects the current tier of a statement variation in the cache.
func (c *StmtCache) LookupState(key VariantKey, sqlStr string) EntryState {
	if c == nil {
		return StateAbsent
	}
	exact := key.IsExact()
	var h uint64
	if exact {
		h = key.Hash()
	} else {
		h = HashString(sqlStr)
	}
	sh := &c.shards[h&(numCacheShards-1)]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := sh.lookupLocked(exact, key, sqlStr)
	if e == nil {
		return StateAbsent
	}
	if e.ms.Load() != nil {
		return StatePrepared
	}
	return StatePartial
}

// Stats returns aggregated telemetry across all 16 shards.
func (c *StmtCache) Stats() CacheStats {
	if c == nil {
		return CacheStats{}
	}
	st := CacheStats{
		PreparedHits: c.fastHits.Load(),
	}
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		st.PreparedHits += sh.preparedHits
		st.PartialHits += sh.partialHits
		st.Misses += sh.misses
		st.Promotions += sh.promotions
		st.Demotions += sh.demotions
		st.Evictions += sh.evictions
		st.RejectedAdmits += sh.rejectedAdmits
		st.ActivePrepared += sh.preparedProb.len + sh.preparedProt.len
		st.ActivePartial += sh.partialProb.len + sh.partialProt.len
		st.PartialBytes += sh.partialBytes
		sh.mu.Unlock()
	}
	return st
}

// Close closes all Tier-2 prepared statements and clears all shards.
func (c *StmtCache) Close() error {
	if c == nil || !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	var firstErr error
	for i := range c.shards {
		sh := &c.shards[i]
		sh.mu.Lock()
		var toRelease []*ManagedStmt
		for _, e := range sh.byKey {
			if ms := e.ms.Swap(nil); ms != nil {
				if e.fastSlot != nil {
					e.fastSlot.CompareAndSwap(ms, nil)
				}
				toRelease = append(toRelease, ms)
			}
		}
		for _, e := range sh.bySQL {
			if ms := e.ms.Swap(nil); ms != nil {
				if e.fastSlot != nil {
					e.fastSlot.CompareAndSwap(ms, nil)
				}
				toRelease = append(toRelease, ms)
			}
		}
		clear(sh.byKey)
		clear(sh.bySQL)
		sh.partialProb = entryList{}
		sh.partialProt = entryList{}
		sh.preparedProb = entryList{}
		sh.preparedProt = entryList{}
		sh.partialBytes = 0
		sh.mu.Unlock()

		for _, ms := range toRelease {
			if err := ms.Release(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
