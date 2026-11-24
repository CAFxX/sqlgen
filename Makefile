# Developer entry points (CI runs the same targets). The real-engine suite lives in the nested module
# ./integration so the root module keeps only yaml + sqlite as dependencies.

# `docker compose` where a daemon is reachable, otherwise the Lima wrapper used on this workstation.
COMPOSE ?= $(shell docker info >/dev/null 2>&1 && echo "docker compose" || echo "docker.lima compose")

# Engines are named once, everywhere (docker-compose.yml service, SQLGEN_ENGINES, SQLGEN_<NAME>_DSN / _PORT,
# CI matrix): postgres15 postgres18 cockroach yugabyte mysql84 mysql97 mariadb1011 mariadb123 tidb (plus the
# in-process sqlite for the integration suite). ENGINES is a comma separated subset; empty means all of them.
ENGINES ?=
# Per-run knobs of test-integration: GOTESTFLAGS=-v, INTEG_TIMEOUT=15m, RUN=<go test -run regex>, SMOKE=1.
GOTESTFLAGS ?=
RUN ?=
INTEG_TIMEOUT ?= 45m
# SMOKE=1 runs only the smoke subset of the integration suite (SMOKE_RUN): a quick local check on the slow engines
# (YugabyteDB); CI runs the full suite on every engine. The subset covers the generated API (batch,
# lookup, keyset, update/delete, retries), the single-round-trip pipelines (guard matrix, differential fuzz,
# failure paths, round-trip counts) and the seeded differential fuzz.
SMOKE ?=
SMOKE_RUN ?= ^(TestRealEnginePipelineGuardMatrix|TestRealEnginePipelineDifferentialFuzz|TestRealEnginePipelineWireMatchesSequential|TestRealEngineWireFailurePathsLeaveTheConnectionClean|TestRealEngineWirePipelineTxIsOneRoundTrip|TestRealEngineBatchInsertAndLookup|TestRealEngineKeysetPagination|TestRealEngineUpdateAndDelete|TestRealEngineReturningSupport|TestRealEngineDeadlockIsRetryable|TestRealEngineSerializationFailureIsRetryable|TestFuzzConditionalFlags|TestFuzzComposedQueries)$$
run_regex = $(if $(SMOKE),$(SMOKE_RUN),$(RUN))

comma := ,
ENV := . scripts/engines-env.sh &&

.PHONY: all test check gen engines-up engines-down test-engines test-integration test-pgxwire test-mysqlwire test-wire-hermetic test-fuzz-sqlite bench-engines

all: check test

# Hermetic tests (SQLite + mock driver); no containers needed.
test:
	go test -race -count=1 ./...

# gofmt, vet of every module, the committed fixtures and generated code are up to date.
check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi
	@for m in . integration pgxwire mysqlwire; do (cd $$m && go vet ./...) || exit 1; done
	cd integration && go run ./fixtures -check
	scripts/sqlgen-all.sh -check

# Regenerate the engine fixtures from their templates (integration/fixtures), then every generated package.
gen:
	cd integration && go run ./fixtures
	scripts/sqlgen-all.sh

# Real engines: PostgreSQL 15/18, CockroachDB, YugabyteDB, MySQL 8.4/9.7, MariaDB 10.11/12.3, TiDB. The integration module
# also exercises pgxwire and mysqlwire (single-round-trip pipelines); `test-pgxwire` / `test-mysqlwire`
# run the drivers' own tests. ENGINES narrows engines-up and every test target.
engines-up:
	$(COMPOSE) up -d --wait $(subst $(comma), ,$(ENGINES))

engines-down:
	$(COMPOSE) down --volumes

# make test-engines                      -> every engine
# make test-engines ENGINES=postgres18   -> only the named engines
test-engines: test-integration test-pgxwire test-mysqlwire

# The integration module (every engine in ENGINES, or all; a missing DSN fails instead of skipping).
test-integration:
	$(ENV) cd integration && SQLGEN_REQUIRE_ENGINES=1 SQLGEN_ENGINES=$(ENGINES) go test -race -count=1 -timeout $(INTEG_TIMEOUT) $(if $(run_regex),-run '$(run_regex)') $(GOTESTFLAGS) ./...

# The pgxwire driver's own tests (PostgreSQL / CockroachDB / YugabyteDB engines only; skipped when no DSN is set). With
# ENGINES set the driver's coverage must stay >= 90%.
test-pgxwire:
	$(ENV) cd pgxwire && SQLGEN_ENGINES=$(ENGINES) go test -race -count=1 -coverprofile=cover.out ./...
	@if [ -n "$(ENGINES)" ]; then cd pgxwire && ../scripts/covgate.sh cover.out 90 pgxwire; fi

# The mysqlwire driver's own tests (MySQL / MariaDB / TiDB engines only; skipped when no DSN is set).
test-mysqlwire:
	$(ENV) cd mysqlwire && SQLGEN_ENGINES=$(ENGINES) go test -race -count=1 -coverprofile=cover.out ./...
	@if [ -n "$(ENGINES)" ]; then cd mysqlwire && ../scripts/covgate.sh cover.out 90 mysqlwire; fi

# The wire drivers' hermetic tests (no engines; the engine tests are skipped without DSNs).
test-wire-hermetic:
	cd pgxwire && go test -race -count=1 ./...
	cd mysqlwire && go test -race -count=1 ./...

# The seeded differential suite against in-process SQLite (the engines run it as part of test-integration).
test-fuzz-sqlite:
	cd integration && SQLGEN_ENGINES=sqlite go test -race -count=1 -run 'TestFuzz' ./

# Overhead of a generated call vs hand-written database/sql on real servers (ENGINES is required).
bench-engines:
	$(ENV) cd integration && SQLGEN_ENGINES=$(ENGINES) go test -run '^$$' -bench . -benchtime=2s ./...
