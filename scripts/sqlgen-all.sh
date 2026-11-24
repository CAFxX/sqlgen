#!/bin/sh
# Run sqlgen over every example and integration fixture config; extra arguments (for example -check) are
# passed through. Used by `make gen`, `make check` and CI.
set -e
cd "$(dirname "$0")/.."
for c in examples/*/sqlgen.yaml integration/*/sqlgen.yaml; do
	go run ./cmd/sqlgen -config "$c" "$@"
done
