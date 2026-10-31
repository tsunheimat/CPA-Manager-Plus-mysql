#!/usr/bin/env bash
# Disposable end-to-end crash recovery check; never connects to an existing DB.
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/cpamp-mysql-recovery.XXXXXX")"
container_name="cpamp-mysql-recovery-$$"
manager_pid=""
docker_cmd=(docker)
if [[ "${CPAMP_TEST_DOCKER_SUDO:-0}" == 1 ]]; then docker_cmd=(sudo -n docker); fi
cleanup() {
  local status=$?
  if [[ -n "$manager_pid" ]]; then kill "$manager_pid" 2>/dev/null || true; wait "$manager_pid" 2>/dev/null || true; fi
  if [[ "$status" != 0 ]]; then
    tail -80 "$work_dir/manager.log" 2>/dev/null || true
    "${docker_cmd[@]}" logs --tail 30 "$container_name" 2>/dev/null || true
  fi
  "${docker_cmd[@]}" rm -fv "$container_name" >/dev/null 2>&1 || true
  rm -rf -- "$work_dir"
}
trap cleanup EXIT
binary="${1:-$work_dir/cpa-manager-plus}"
if [[ $# == 0 ]]; then (cd "$repo_root/apps/manager-server" && go build -o "$binary" ./cmd/cpa-manager-plus); fi
binary="$(cd -- "$(dirname -- "$binary")" && pwd)/$(basename -- "$binary")"
password="cpamp-disposable-$RANDOM-$RANDOM"
"${docker_cmd[@]}" run -d --name "$container_name" --label cpamp.disposable-recovery=true \
  -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_DATABASE=cpamp_recovery \
  -p 127.0.0.1::3306 mysql:8.4 >/dev/null
mysql_exec() { "${docker_cmd[@]}" exec -e MYSQL_PWD="$password" "$container_name" mysql -uroot -N -B cpamp_recovery -e "$1"; }
wait_mysql() {
  for ((attempt=0; attempt<120; attempt++)); do
    if mysql_exec 'select 1' >/dev/null 2>&1; then return 0; fi
    sleep 0.5
  done
  echo 'Disposable MySQL did not start' >&2; return 1
}
wait_mysql
mysql_port="$("${docker_cmd[@]}" port "$container_name" 3306 | awk -F: '{print $NF}')"
http_port="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')"
export USAGE_DB_DRIVER=mysql USAGE_DB_DSN="root:$password@tcp(127.0.0.1:$mysql_port)/cpamp_recovery"
export USAGE_DATA_DIR="$work_dir/data" CPA_MANAGER_DATA_KEY_PATH="$work_dir/data/data.key"
export USAGE_DB_PATH="$work_dir/must-not-exist.sqlite" HTTP_ADDR="127.0.0.1:$http_port"
export CPA_MANAGER_CONFIG="$work_dir/config.json" CPA_MANAGER_ADMIN_KEY="cpamp_disposable_recovery_admin_key"
unset USAGE_DB_DSN_FILE CPA_UPSTREAM_URL CPA_MANAGEMENT_KEY CPA_MANAGEMENT_KEY_FILE CPA_MANAGER_DATA_KEY
start_manager() {
  "$binary" >>"$work_dir/manager.log" 2>&1 & manager_pid=$!
  for ((attempt=0; attempt<100; attempt++)); do
    kill -0 "$manager_pid" 2>/dev/null || return 1
    if curl -fsS "http://$HTTP_ADDR/health" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo 'Manager did not start with MySQL' >&2; return 1
}
start_manager
mysql_exec "insert into settings(\`key\`,value,updated_at_ms) values('recovery-proof','committed-before-crash',0); insert into usage_events(event_hash,timestamp_ms,timestamp,model,created_at_ms) values(repeat('a',64),1800000000000,'2027-01-15T08:00:00Z','crash-test',1800000000000)" >/dev/null
"${docker_cmd[@]}" kill --signal=KILL "$container_name" >/dev/null
for ((attempt=0; attempt<100; attempt++)); do
  if ! kill -0 "$manager_pid" 2>/dev/null; then break; fi
  sleep 0.5
 done
if kill -0 "$manager_pid" 2>/dev/null; then echo 'Manager kept running after MySQL was killed' >&2; exit 1; fi
if wait "$manager_pid"; then echo 'Expected a failed process after ownership loss' >&2; exit 1; fi
manager_pid=""
"${docker_cmd[@]}" start "$container_name" >/dev/null
wait_mysql
mysql_port="$("${docker_cmd[@]}" port "$container_name" 3306 | awk -F: '{print $NF}')"
export USAGE_DB_DSN="root:$password@tcp(127.0.0.1:$mysql_port)/cpamp_recovery"
start_manager
[[ "$(mysql_exec "select value from settings where \`key\`='recovery-proof'")" == committed-before-crash ]]
[[ "$(mysql_exec "select count(*) from usage_events where event_hash=repeat('a',64)")" == 1 ]]
[[ ! -e "$USAGE_DB_PATH" ]]
curl -fsS -H "Authorization: Bearer $CPA_MANAGER_ADMIN_KEY" "http://$HTTP_ADDR/status" | grep -q '"dbDriver":"mysql"'
kill "$manager_pid"; wait "$manager_pid"; manager_pid=""
"$binary" manager-data-snapshot create --snapshot-dir "$work_dir/backup" >>"$work_dir/manager.log" 2>&1
mysql_exec "update settings set value='changed' where \`key\`='recovery-proof'" >/dev/null
"$binary" manager-data-snapshot restore --snapshot-dir "$work_dir/backup" >>"$work_dir/manager.log" 2>&1
[[ "$(mysql_exec "select value from settings where \`key\`='recovery-proof'")" == committed-before-crash ]]
"$binary" cleanup-derived >>"$work_dir/manager.log" 2>&1
"$binary" compact-usage >>"$work_dir/manager.log" 2>&1
printf '%s\n' 'disposable-new-admin-key' > "$work_dir/admin-key"
"$binary" reset-admin-key --admin-key-file "$work_dir/admin-key" >>"$work_dir/manager.log" 2>&1
export CPA_MANAGER_ADMIN_KEY=disposable-new-admin-key
start_manager
curl -fsS -H "Authorization: Bearer $CPA_MANAGER_ADMIN_KEY" "http://$HTTP_ADDR/status" | grep -q '"dbDriver":"mysql"'
[[ "$(mysql_exec "select count(*) from usage_events where event_hash=repeat('a',64)")" == 1 ]]
[[ ! -e "$USAGE_DB_PATH" ]]
printf '%s\n' '{"mysql_crash_recovery":true,"manager_stops_on_database_loss":true,"committed_data_preserved":true,"snapshot_restore":true,"offline_cleanup_and_optimize":true,"admin_recovery":true,"sqlite_file_created":false}'
