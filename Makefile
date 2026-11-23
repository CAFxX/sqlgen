# Developer entry points. The real-engine suite lives in the nested module ./integration so the root
# module keeps only yaml + sqlite as dependencies.

# `docker compose` where a daemon is reachable, otherwise the Lima wrapper used on this workstation.
COMPOSE ?= $(shell docker info >/dev/null 2>&1 && echo "docker compose" || echo "docker.lima compose")
ENGINES ?=

.PHONY: all test check gen engines-up engines-down test-engines test-pgxwire test-mysqlwire bench-engines sync-adapters

all: check test

# Hermetic tests (SQLite + mock driver); no containers needed.
test:
	go test -race -count=1 ./...

check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi
	go vet ./...
	cd integration && go vet ./...
	cd pgxwire && go vet ./...
	cd mysqlwire && go vet ./...
	@for c in examples/*/sqlgen.yaml integration/*/sqlgen.yaml; do go run ./cmd/sqlgen -config $$c -check || exit 1; done

gen:
	@for c in examples/*/sqlgen.yaml integration/*/sqlgen.yaml; do go run ./cmd/sqlgen -config $$c || exit 1; done

# Real engines: PostgreSQL 15/18, CockroachDB, MySQL 8.4/9.7, MariaDB 10.11/12.3. The integration module
# also exercises pgxwire and mysqlwire (single-round-trip pipelines); `test-pgxwire` / `test-mysqlwire`
# run the drivers' own tests.
engines-up:
	$(COMPOSE) up -d --wait

engines-down:
	$(COMPOSE) down --volumes

# make test-engines                      -> every engine
# make test-engines ENGINES=postgres18   -> only the named engines (comma separated)
test-engines:
	. scripts/engines-env.sh && cd integration && SQLGEN_REQUIRE_ENGINES=1 SQLGEN_ENGINES=$(ENGINES) go test -count=1 -timeout 45m ./...
	$(MAKE) test-pgxwire ENGINES=$(ENGINES)
	$(MAKE) test-mysqlwire ENGINES=$(ENGINES)

# The pgxwire driver's own tests (PostgreSQL / CockroachDB engines only; skipped when no DSN is set).
test-pgxwire:
	. scripts/engines-env.sh && cd pgxwire && SQLGEN_ENGINES=$(ENGINES) go test -race -count=1 -cover ./...

# The mysqlwire driver's own tests (MySQL / MariaDB engines only; skipped when no DSN is set).
test-mysqlwire:
	. scripts/engines-env.sh && cd mysqlwire && SQLGEN_ENGINES=$(ENGINES) go test -race -count=1 -cover ./...

# Overhead of a generated call vs hand-written database/sql on real servers (ENGINES is required).
bench-engines:
	. scripts/engines-env.sh && cd integration && SQLGEN_ENGINES=$(ENGINES) go test -run '^$$' -bench . -benchtime=2s ./...

# adapter_myq_test.go is adapter_pgq_test.go with the package name swapped (likewise the fuzz adapter).
sync-adapters:
	sed 's/pgq/myq/g; s/Pgq/Myq/g' integration/adapter_pgq_test.go > integration/adapter_myq_test.go
	sed 's/fzpg/fzmy/g; s/Fzpg/Fzmy/g' integration/fuzz_adapter_fzpg_test.go > integration/fuzz_adapter_fzmy_test.go
	sed 's/fzpg/fzsl/g; s/Fzpg/Fzsl/g' integration/fuzz_adapter_fzpg_test.go > integration/fuzz_adapter_fzsl_test.go
	sed 's/fzpg/fzsl/g; s/Fzpg/Fzsl/g' integration/fuzz_bump_fzpg_test.go > integration/fuzz_bump_fzsl_test.go
