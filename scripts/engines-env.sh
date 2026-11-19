# Source me: exports DSNs for the docker-compose.yml engines (ports overridable via SQLGEN_*_PORT).
export SQLGEN_PG15_DSN="postgres://postgres:password@127.0.0.1:${SQLGEN_PG15_PORT:-54315}/sqlgen_test?sslmode=disable"
export SQLGEN_PG18_DSN="postgres://postgres:password@127.0.0.1:${SQLGEN_PG18_PORT:-54318}/sqlgen_test?sslmode=disable"
export SQLGEN_CRDB_DSN="postgres://root@127.0.0.1:${SQLGEN_CRDB_PORT:-54326}/defaultdb?sslmode=disable"
export SQLGEN_MYSQL84_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MYSQL84_PORT:-33184})/sqlgen_test"
export SQLGEN_MYSQL97_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MYSQL97_PORT:-33197})/sqlgen_test"
export SQLGEN_MARIADB1011_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MARIADB1011_PORT:-33111})/sqlgen_test"
export SQLGEN_MARIADB123_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MARIADB123_PORT:-33123})/sqlgen_test"
