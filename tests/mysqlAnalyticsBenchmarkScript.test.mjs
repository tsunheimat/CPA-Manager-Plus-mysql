import { spawnSync } from 'node:child_process';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { classifyChangedFiles } from '../bin/ci/classify-pr-checks.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const script = path.join(root, 'bin/ci/bench-mysql-analytics.sh');

function run(overrides = {}, args = []) {
  const env = { ...process.env, ...overrides };
  for (const key of ['CPA_TEST_MYSQL_DSN', 'CPA_MYSQL_BENCHMARK_DISPOSABLE']) {
    if (!(key in overrides)) delete env[key];
  }
  return spawnSync('bash', [script, ...args], { cwd: root, env, encoding: 'utf8' });
}

describe('MySQL analytics benchmark guardrails', () => {
  it('checks the wrapper and server when only the benchmark script changes', () => {
    expect(classifyChangedFiles(['bin/ci/bench-mysql-analytics.sh'])).toMatchObject({
      frontend: true,
      manager_server: true,
    });
  });

  it('requires a test DSN before executing a benchmark', () => {
    const result = run();
    expect(result.status).not.toBe(0);
    expect(result.stderr).toContain('disposable MySQL 8.4 test DSN');
  });

  it('requires explicit disposable-server acknowledgement without logging the DSN', () => {
    const secret = 'test-user:do-not-log-this@tcp(example.invalid:3306)/test';
    const result = run({ CPA_TEST_MYSQL_DSN: secret });
    expect(result.status).toBe(2);
    expect(result.stderr).toContain('CPA_MYSQL_BENCHMARK_DISPOSABLE=1');
    expect(result.stdout + result.stderr).not.toContain(secret);
  });

  it('runs only benchmarks with bounded package concurrency and forwards arguments', () => {
    const directory = mkdtempSync(path.join(tmpdir(), 'cpamp-benchmark-script-'));
    try {
      const output = path.join(directory, 'arguments');
      const fakeGo = path.join(directory, 'go');
      writeFileSync(
        fakeGo,
        '#!/usr/bin/env bash\nprintf "%s\\n" "$PWD" "$@" > "$BENCHMARK_TEST_OUTPUT"\n'
      );
      chmodSync(fakeGo, 0o700);
      const result = run(
        {
          PATH: `${directory}:${process.env.PATH}`,
          CPA_TEST_MYSQL_DSN: 'fake-only-never-connected',
          CPA_MYSQL_BENCHMARK_DISPOSABLE: '1',
          CPA_MYSQL_BENCHMARK_TIME: '2x',
          CPA_MYSQL_BENCHMARK_COUNT: '4',
          BENCHMARK_TEST_OUTPUT: output,
        },
        ['-timeout=10m']
      );
      expect(result.status).toBe(0);
      const lines = readFileSync(output, 'utf8').trim().split('\n');
      expect(lines[0]).toBe(path.join(root, 'apps/manager-server'));
      expect(lines.slice(1)).toEqual([
        'test',
        '-p',
        '1',
        '-run',
        '^$',
        '-bench',
        'BenchmarkUsageAnalytics',
        '-benchmem',
        '-benchtime',
        '2x',
        '-count',
        '4',
        './internal/service/monitoring',
        './internal/analyticsbench',
        '-timeout=10m',
      ]);
      expect(result.stdout + result.stderr).not.toContain('fake-only-never-connected');
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
