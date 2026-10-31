#!/usr/bin/env bash
# Run the built image with the same user, writable paths, and MySQL mode as k3s.
set -euo pipefail
image="${1:?Usage: smoke-manager-image.sh IMAGE}"
prefix="cpamp-image-smoke-$$"
docker_cmd=(docker)
if [[ "${CPAMP_TEST_DOCKER_SUDO:-0}" == 1 ]]; then docker_cmd=(sudo -n docker); fi
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/cpamp-image-smoke.XXXXXX")"
cleanup() {
  local status=$?
  if [[ "$status" != 0 ]]; then
    "${docker_cmd[@]}" logs --tail 60 "$prefix-manager" >&2 || true
    "${docker_cmd[@]}" logs --tail 30 "$prefix-mysql" >&2 || true
  fi
  "${docker_cmd[@]}" rm -fv "$prefix-manager" "$prefix-mysql" >/dev/null 2>&1 || true
  "${docker_cmd[@]}" volume rm "$prefix-data" >/dev/null 2>&1 || true
  "${docker_cmd[@]}" network rm "$prefix" >/dev/null 2>&1 || true
  rm -rf -- "$work_dir"
}
trap cleanup EXIT
password="disposable-$RANDOM-$RANDOM"
admin="cpamp-smoke-admin-$RANDOM-$RANDOM"
printf '%s\n' "$admin" > "$work_dir/admin-key"
chmod 0444 "$work_dir/admin-key"
if [[ -n "${CPAMP_EXPECTED_REVISION:-}" ]]; then
  actual="$("${docker_cmd[@]}" image inspect "$image" --format '{{ index .Config.Labels "org.opencontainers.image.revision" }}')"
  [[ "$actual" == "$CPAMP_EXPECTED_REVISION" ]] || { echo 'Image revision label mismatch' >&2; exit 1; }
fi
"${docker_cmd[@]}" network create "$prefix" >/dev/null
"${docker_cmd[@]}" volume create "$prefix-data" >/dev/null
"${docker_cmd[@]}" run --rm -v "$prefix-data:/data" --entrypoint sh "$image" -ec 'chown 10001:10001 /data'
"${docker_cmd[@]}" run -d --name "$prefix-mysql" --network "$prefix" --network-alias mysql \
  --label cpamp.disposable-image-smoke=true \
  -e MYSQL_ROOT_PASSWORD="$password" -e MYSQL_DATABASE=cpamp -e MYSQL_USER=cpamp \
  -e MYSQL_PASSWORD="$password" mysql:8.4 >/dev/null
mysql_exec() {
  "${docker_cmd[@]}" exec -e MYSQL_PWD="$password" "$prefix-mysql" \
    mysql --protocol=TCP -h127.0.0.1 -ucpamp -N -B cpamp -e "$1"
}
ready=0
for ((attempt=0; attempt<120; attempt++)); do
  if mysql_exec 'select 1' >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[[ "$ready" == 1 ]] || { echo 'MySQL did not become ready' >&2; exit 1; }
"${docker_cmd[@]}" run -d --name "$prefix-manager" --network "$prefix" \
  --label cpamp.disposable-image-smoke=true \
  --user 10001:10001 --read-only --cap-drop ALL --security-opt no-new-privileges \
  --tmpfs /tmp:rw,nosuid,nodev,mode=1777 -v "$prefix-data:/data" \
  -v "$work_dir/admin-key:/run/secrets/admin-key:ro" \
  -p 127.0.0.1::18317 \
  -e USAGE_DB_DRIVER=mysql -e USAGE_DB_DSN="cpamp:$password@tcp(mysql:3306)/cpamp" \
  -e USAGE_DATA_DIR=/data -e CPA_MANAGER_CONFIG=/data/config.json \
  -e CPA_MANAGER_DATA_KEY_PATH=/data/data.key -e CPA_MANAGER_ADMIN_KEY_FILE=/run/secrets/admin-key \
  "$image" >/dev/null
wait_manager() {
  local port
  for ((attempt=0; attempt<90; attempt++)); do
    [[ "$("${docker_cmd[@]}" inspect "$prefix-manager" --format '{{.State.Running}}')" == true ]] || break
    port="$("${docker_cmd[@]}" port "$prefix-manager" 18317 2>/dev/null | awk -F: '{print $NF}')" || port=""
    if [[ -z "$port" ]]; then sleep 1; continue; fi
    base="http://127.0.0.1:$port"
    if curl -fsS "$base/health" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo 'Manager image did not become healthy' >&2
  return 1
}
wait_manager
[[ "$(curl -sS -o /dev/null -w '%{http_code}' "$base/status")" == 401 ]]
curl -fsS -H "Authorization: Bearer $admin" "$base/status" > "$work_dir/status.json"
grep -q '"dbDriver":"mysql"' "$work_dir/status.json"
if grep -Fq "$password" "$work_dir/status.json"; then echo 'Status leaked credentials' >&2; exit 1; fi
curl -fsS "$base/management.html" > "$work_dir/management.html"
grep -qi '<!doctype html' "$work_dir/management.html"
[[ "$(wc -c < "$work_dir/management.html")" -gt 100000 ]]
"${docker_cmd[@]}" exec "$prefix-manager" sh -ec 'test "$(id -u)" = 10001; test -s /data/data.key; test ! -e /data/usage.sqlite; test -s /etc/ssl/certs/ca-certificates.crt; nc -z mysql 3306'
mysql_exec "insert into settings(\`key\`,value,updated_at_ms) values('image-smoke','persisted',0)" >/dev/null
"${docker_cmd[@]}" restart --time 30 "$prefix-manager" >/dev/null
wait_manager
[[ "$(mysql_exec "select value from settings where \`key\`='image-smoke'")" == persisted ]]
"${docker_cmd[@]}" exec "$prefix-manager" test ! -e /data/usage.sqlite
printf '%s\n' '{"image_smoke":true,"mysql":true,"non_root":true,"read_only_root":true,"embedded_ui":true,"restart_persistence":true,"sqlite_file_created":false}'
