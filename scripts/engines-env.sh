# Source me: exports DSNs for the docker-compose.yml engines. Every name derives from the engine id (the compose
# service, SQLGEN_ENGINES, the CI matrix entry): SQLGEN_<ID>_DSN, ports overridable via SQLGEN_<ID>_PORT.
export SQLGEN_POSTGRES15_DSN="postgres://postgres:password@127.0.0.1:${SQLGEN_POSTGRES15_PORT:-54315}/sqlgen_test?sslmode=disable"
export SQLGEN_POSTGRES18_DSN="postgres://postgres:password@127.0.0.1:${SQLGEN_POSTGRES18_PORT:-54318}/sqlgen_test?sslmode=disable"
export SQLGEN_COCKROACH_DSN="postgres://root@127.0.0.1:${SQLGEN_COCKROACH_PORT:-54326}/defaultdb?sslmode=disable"
export SQLGEN_MYSQL84_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MYSQL84_PORT:-33184})/sqlgen_test"
export SQLGEN_MYSQL97_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MYSQL97_PORT:-33197})/sqlgen_test"
export SQLGEN_MARIADB1011_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MARIADB1011_PORT:-33111})/sqlgen_test"
export SQLGEN_MARIADB123_DSN="root:password@tcp(127.0.0.1:${SQLGEN_MARIADB123_PORT:-33123})/sqlgen_test"
