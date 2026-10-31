import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { parse, parseAllDocuments } from 'yaml';
import { classifyChangedFiles } from '../bin/ci/classify-pr-checks.mjs';

const read = (name) => readFileSync(new URL(`../${name}`, import.meta.url), 'utf8');
const resources = parseAllDocuments(read('deploy/k3s/cpa-manager-plus.yaml')).map((doc) => {
  if (doc.errors.length) throw doc.errors[0];
  return doc.toJSON();
});
const resource = (kind, name) =>
  resources.find((item) => item.kind === kind && item.metadata.name === name);
const deployment = resource('Deployment', 'cpa-manager-plus');
const mysql = resource('StatefulSet', 'cpamp-mysql');
const config = resource('ConfigMap', 'cpamp-mysql-config');
const pod = deployment.spec.template.spec;
const manager = pod.containers[0];
const workflow = parse(read('.github/workflows/pr-check.yml'));
const imageJob = workflow.jobs['docker-build'];

describe('k3s MySQL deployment', () => {
  it('uses one Manager with persistent keys and archives across replacements', () => {
    expect(deployment.spec.replicas).toBe(1);
    expect(deployment.spec.strategy.type).toBe('Recreate');
    expect(pod.securityContext.fsGroup).toBe(pod.securityContext.runAsGroup);
    expect(pod.securityContext.runAsNonRoot).toBe(true);
    expect(manager.securityContext.readOnlyRootFilesystem).toBe(true);
    expect(manager.volumeMounts.find((v) => v.mountPath === '/data').name).toBe('data');
    const claim = pod.volumes.find((v) => v.name === 'data').persistentVolumeClaim.claimName;
    expect(resource('PersistentVolumeClaim', claim)).toBeDefined();
    expect(manager.env.find((e) => e.name === 'CPA_MANAGER_CONFIG').value).toBe(
      '/data/config.json'
    );
    expect(manager.env.find((e) => e.name === 'CPA_MANAGER_DATA_KEY_PATH').value).toBe(
      '/data/data.key'
    );
  });

  it('shares the same database identity and application password with MySQL initialization', () => {
    expect(mysql.spec.template.spec.containers[0].envFrom).toEqual(manager.envFrom);
    expect(config.data.MYSQL_HOST).toBe(mysql.spec.serviceName);
    const secret = parseAllDocuments(read('deploy/k3s/secrets.example.yaml'))[1].toJSON();
    const values = { ...config.data };
    for (const env of manager.env) {
      if (env.valueFrom) {
        const ref = env.valueFrom.secretKeyRef;
        expect(ref.name).toBe(secret.metadata.name);
        expect(secret.stringData[ref.key]).toBeDefined();
        values[env.name] = 'example-password';
      } else {
        values[env.name] = env.value.replace(/\$\(([^)]+)\)/g, (_, key) => {
          expect(values[key], `undefined environment reference ${key}`).toBeDefined();
          return values[key];
        });
      }
    }
    expect(values.USAGE_DB_DRIVER).toBe('mysql');
    expect(values.USAGE_DB_DSN).toBe('cpamp:example-password@tcp(cpamp-mysql:3306)/cpamp');
    expect(mysql.spec.template.spec.volumes[0].secret.secretName).toBe(secret.metadata.name);
    expect(mysql.spec.template.spec.volumes[0].secret.items.map((i) => i.key)).toContain(
      'mysql-password'
    );
  });

  it('uses the fork image for both startup waiting and the application', () => {
    expect(manager.image).toBe('ghcr.io/tsunheimat/cpa-manager-plus-mysql:main');
    expect(pod.initContainers[0].image).toBe(manager.image);
    expect(manager.startupProbe.httpGet.path).toBe('/health');
    expect(mysql.spec.template.spec.containers[0].startupProbe.exec.command.join(' ')).toContain(
      'SELECT 1'
    );
    expect(resource('Service', 'cpa-manager-plus').spec.selector).toEqual(
      deployment.spec.template.metadata.labels
    );
  });

  it.each(['deploy/k3s/cpa-manager-plus.yaml', 'bin/ci/smoke-manager-image.sh'])(
    'validates and rebuilds the image after changing %s',
    (file) => {
      expect(classifyChangedFiles([file]).docker).toBe(true);
    }
  );
});

describe('image publication policy', () => {
  const resolver = imageJob.steps.find((step) => step.id === 'image').run;
  it.each([
    ['push', 'refs/heads/main', '', true, 0],
    ['push', 'refs/heads/dev', '', false, 0],
    ['pull_request', 'refs/pull/1/merge', 'true', false, 0],
    ['workflow_dispatch', 'refs/heads/main', 'true', true, 0],
    ['workflow_dispatch', 'refs/heads/main', 'false', false, 0],
    ['workflow_dispatch', 'refs/heads/dev', 'true', false, 1],
  ])('%s on %s with publish=%s', (event, ref, requested, publishes, status) => {
    const dir = mkdtempSync(path.join(tmpdir(), 'cpamp-image-policy-'));
    try {
      const output = path.join(dir, 'output');
      const result = spawnSync('bash', ['-c', resolver], {
        encoding: 'utf8',
        env: {
          ...process.env,
          GITHUB_EVENT_NAME: event,
          GITHUB_REF: ref,
          GITHUB_REPOSITORY: 'Example/Manager',
          GITHUB_SHA: 'a'.repeat(40),
          GITHUB_OUTPUT: output,
          PUBLISH_REQUESTED: requested,
        },
      });
      expect(result.status, result.stderr).toBe(status);
      if (status === 0) {
        const values = readFileSync(output, 'utf8');
        expect(values).toContain(`publish=${publishes}`);
        expect(values).toContain('name=ghcr.io/example/manager');
      }
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('requires successful security checks and an image smoke test before publication', () => {
    expect(imageJob.needs).toEqual(
      expect.arrayContaining(['security', 'frontend', 'manager-server'])
    );
    expect(imageJob.if).toContain("needs.security.result == 'success'");
    const smoke = imageJob.steps.findIndex((step) => step.name === 'Smoke test image with MySQL');
    const publish = imageJob.steps.findIndex((step) => step.id === 'build');
    expect(smoke).toBeGreaterThan(-1);
    expect(publish).toBeGreaterThan(smoke);
    expect(imageJob.steps[publish].with.platforms).toBe('linux/amd64,linux/arm64');
    expect(imageJob.steps[publish].with.push).toBe("${{ steps.image.outputs.publish == 'true' }}");
    expect(imageJob.steps[publish].with['build-args']).toContain('SOURCE_COMMIT=${{ github.sha }}');
    expect(workflow.on.workflow_dispatch.inputs.publish_image.default).toBe(false);
  });
});
