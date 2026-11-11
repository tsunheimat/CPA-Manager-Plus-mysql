#!/usr/bin/env bash
set -euo pipefail

: "${CPA_TEST_MYSQL_DSN:?Set a disposable MySQL 8.4 test DSN with CREATE/DROP DATABASE privileges}"
if [[ "${CPA_MYSQL_BENCHMARK_DISPOSABLE:-}" != "1" ]]; then
  printf '%s\n' 'Refusing to benchmark without CPA_MYSQL_BENCHMARK_DISPOSABLE=1.' >&2
  printf '%s\n' 'Use a dedicated test server, never production. Fixtures create and drop cpamp_test_* databases.' >&2
  exit 2
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd -- "${repo_root}/apps/manager-server"

# Do not print the DSN, enable shell tracing, or run EXPLAIN ANALYZE on production.
# Keep -count samples separate; benchmark means are not request-latency percentiles.
go test -p 1 -run '^$' \
  -bench 'BenchmarkUsageAnalytics' \
  -benchmem \
  -benchtime "${CPA_MYSQL_BENCHMARK_TIME:-3x}" \
  -count "${CPA_MYSQL_BENCHMARK_COUNT:-3}" \
  ./internal/service/monitoring ./internal/analyticsbench "$@"
