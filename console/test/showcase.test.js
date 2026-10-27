import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import { validateRun, complete, number, observedAt } from '../showcase/run-model.js';

const recording = JSON.parse(await fs.readFile(new URL('../dist/recorded-run.json', import.meta.url)));
test('captured Compose and Kubernetes records retain their separate identities', () => {
  const run = validateRun(structuredClone(recording));
  assert.equal(run.environments.compose.requests.length, 20);
  assert.equal(run.environments.kind.requests.length, 20);
  assert.ok(complete(run.environments.compose)); assert.ok(complete(run.environments.kind));
  assert.notEqual(run.environments.compose.requests[0].event_id, run.environments.kind.requests[0].event_id);
});
test('duplicate event identities are rejected', () => {
  const run = structuredClone(recording);
  run.environments.compose.requests[1].event_id = run.environments.compose.requests[0].event_id;
  assert.throws(() => validateRun(run), /duplicate event/);
});
test('a missing observation cannot be relabelled as successful reconciliation', () => {
  const run = structuredClone(recording);
  delete run.environments.compose.requests[0].observed_visibility_upper_bound_s;
  assert.throws(() => validateRun(run), /Reconciliation/);
});
test('mixed execution revisions and nonfinite timings are rejected', () => {
  const revision = structuredClone(recording);
  revision.environments.kind.summary.revision = 'b'.repeat(40);
  assert.throws(() => validateRun(revision), /execution revisions/);
  const timings = structuredClone(recording);
  timings.environments.kind.requests[0].http_elapsed_s = Infinity;
  assert.throws(() => validateRun(timings), /client observation/);
});
test('incorrect reconciliation totals and unrelated report links are rejected', () => {
  const totals = structuredClone(recording);
  totals.environments.compose.summary.accepted_reconciliation.accepted = 21;
  assert.throws(() => validateRun(totals), /Reconciliation/);
  const link = structuredClone(recording);
  link.provenance.workflowUrl = 'https://example.com/run';
  assert.throws(() => validateRun(link), /provenance/);
});
test('unknown values remain unknown', () => {
  assert.equal(number(null), '—'); assert.equal(number(undefined), '—');
  assert.equal(number(NaN), '—'); assert.equal(number(0), '0.0');
});
test('visibility bounds use the offered schedule even when request dispatch is delayed', () => {
  assert.equal(observedAt({ scheduled_s: 2, started_s: 5, observed_visibility_upper_bound_s: 7 }), 9);
  assert.equal(observedAt({ scheduled_s: 2, started_s: 5 }), null);
});
