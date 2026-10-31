#!/usr/bin/env bash
set -euo pipefail

: "${CPA_TEST_MYSQL_DSN:?Set a MySQL 8.4 test DSN with CREATE/DROP DATABASE privileges}"
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd -- "${repo_root}/apps/manager-server"

# All application, API, worker, service, and repository packages participate.
# Explicit SQLite schema/physical-plan tests still exercise their own engine.
# Shared fixtures create and remove isolated cpamp_test_* MySQL databases.
go test -p "${CPA_TEST_MYSQL_PACKAGES:-4}" -count=1 ./... "$@"
