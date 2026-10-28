import http from 'k6/http';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { SharedArray } from 'k6/data';
import { Counter, Rate, Trend } from 'k6/metrics';

function readJson(file) {
  try { return JSON.parse(open(file)); }
  catch (_) { throw new Error('Invalid workload input file'); }
}
const config = readJson(__ENV.SLO_CONFIG);
const credentials = new SharedArray('workload-credentials', () => readJson(__ENV.SLO_TOKENS));
const reconciling = config.mode === 'reconcile';
const manifest = reconciling
  ? new SharedArray('reconciliation-identities', () => readJson(__ENV.SLO_MANIFEST)) : [];
if (!Number.isInteger(config.virtual_users) || config.virtual_users < 1 || config.virtual_users > 256 ||
    credentials.length !== config.tenants || credentials.length < 1 || credentials.length > 32 ||
    !/^[-a-f0-9]{24}$/.test(config.run_prefix) || !Number.isFinite(Date.parse(config.event_time)))
  throw new Error('Invalid bounded workload configuration');
for (let slot = 0; slot < credentials.length; slot++) {
  const value = credentials[slot];
  if (typeof value.token !== 'string' || !value.token || typeof value.tenant !== 'string')
    throw new Error('Invalid workload credential configuration');
}

const started = new Counter('ingest_started');
const completed = new Counter('ingest_completed');
const http202 = new Counter('http_202');
const validReceipts = new Counter('valid_receipts');
const contracts = new Counter('contract_failures');
const records = new Counter('audit_records');
const validRate = new Rate('valid_receipt_rate');
const visibleRate = new Rate('live_visibility_good');
const wallLatency = new Trend('acceptance_http_ms', true);
const scheduledLatency = new Trend('acceptance_scheduled_ms', true);
const visibilityLatency = new Trend('live_visibility_ms', true);
const generatorDelay = new Trend('generator_delay_ms', true);
const reconciled = new Counter('reconciled_202');
const missing = new Counter('missing_202');
const wrongContent = new Counter('wrong_stored_content');
const unknownVisible = new Counter('non202_later_visible');
const queryRows = new Counter('reconciliation_records');
const isolationChecks = new Counter('isolation_checks');
const isolationDenied = new Counter('isolation_denied');
const isolationLeaks = new Counter('isolation_leaks');
const isolationGood = new Rate('isolation_good');

const thresholds = reconciling ? {
  missing_202: ['count==0'], wrong_stored_content: ['count==0']
} : {
  contract_failures: ['count==0'], isolation_leaks: ['count==0'],
};
if (!reconciling && !config.stages && config.kind !== 'warmup') {
  thresholds.dropped_iterations = ['count==0'];
  thresholds.valid_receipt_rate = [`rate>=${config.acceptance_fraction}`];
  thresholds.acceptance_scheduled_ms = [`p(99)<=${config.acceptance_p99_ms}`, `p(99.9)<=${config.acceptance_p999_ms}`];
  thresholds.live_visibility_ms = [`p(99)<=${config.visibility_p99_ms}`];
  thresholds.live_visibility_good = [`rate>=${config.visibility_fraction}`];
  thresholds.generator_delay_ms = [`p(99)<=${config.generator_delay_p99_ms}`];
}
if (!reconciling && config.tenants > 1) thresholds.isolation_good = ['rate==1'];
if (!reconciling && config.kind === 'warmup') {
  thresholds.dropped_iterations = ['count==0'];
  thresholds.valid_receipt_rate = ['rate==1'];
}
if (!reconciling && config.stages)
  thresholds.acceptance_http_ms = [`p(99)<=${config.response_p99_ms}`];

const scenario = reconciling ? {
  executor: 'shared-iterations', iterations: manifest.length, vus: Math.min(64, config.virtual_users),
  maxDuration: `${config.settle_seconds + Math.ceil(config.request_timeout_ms / 1000) + 2}s`, gracefulStop: '10s'
} : (config.stages ? {
  executor: 'ramping-arrival-rate', startRate: config.rate_per_second, timeUnit: '1s',
  stages: config.stages.map(s => ({duration: `${s.duration_seconds}s`, target: s.target})),
  preAllocatedVUs: config.virtual_users, maxVUs: config.virtual_users, gracefulStop: '15s'
} : {
  executor: 'constant-arrival-rate', rate: config.rate_per_second, timeUnit: '1s',
  duration: `${config.duration_seconds}s`, preAllocatedVUs: config.virtual_users,
  maxVUs: config.virtual_users, gracefulStop: '15s'
});
export const options = {
  scenarios: {workload: scenario}, thresholds,
  systemTags: ['status', 'method', 'name', 'scenario', 'error_code', 'expected_response'],
  summaryTrendStats: ['min', 'avg', 'med', 'p(95)', 'p(99)', 'p(99.9)', 'max', 'count'],
  noConnectionReuse: false, noVUConnectionReuse: false,
  userAgent: 'telemetry-slo-lab/1',
};

function envelope(id) {
  const event = {eventId: id, eventType: 'performance.sample', schemaVersion: 1,
    occurredAt: config.event_time,
    attributes: {platform: 'synthetic-linux', region: 'lab-eu', sdkVersion: '1.0.0',
      statusCode: '200', durationMs: '123', metric: 'slo.sample', value: '16.7'}};
  let remaining = config.payload_bytes - JSON.stringify(event).length;
  for (const key of ['metric', 'value', 'sdkVersion', 'region', 'platform']) {
    const n = Math.min(remaining, 128 - event.attributes[key].length);
    if (n > 0) { event.attributes[key] += 'x'.repeat(n); remaining -= n; }
  }
  if (remaining !== 0) throw new Error('Payload size cannot fit the bounded event schema');
  return event;
}
function identity(index) { return config.run_prefix + index.toString(16).padStart(12, '0'); }
function params(slot, name) {
  return {headers: {'Authorization': `Bearer ${credentials[slot].token}`, 'Content-Type': 'application/json'},
    timeout: `${config.request_timeout_ms}ms`, responseType: 'text', tags: {name}};
}
function body(response) {
  if (typeof response.body !== 'string' || response.body.length > 65536) return null;
  try { return JSON.parse(response.body); } catch (_) { return null; }
}
function correctReceipt(response, id, slot) {
  const value = body(response), p = value && value.position;
  return response.status === 202 && value && value.eventId === id && value.tenantId === credentials[slot].tenant &&
    p && p.stream === 'telemetry.events.v1' && Number.isInteger(p.partition) && p.partition >= 0 &&
    Number.isInteger(p.offset) && p.offset >= 0;
}
function matches(response, id, slot) {
  const value = body(response), expected = envelope(id);
  return value && value.eventId === id && (value.tenantId === undefined || value.tenantId === credentials[slot].tenant) &&
    value.eventType === expected.eventType && value.schemaVersion === expected.schemaVersion &&
    Date.parse(value.occurredAt) === Date.parse(expected.occurredAt) &&
    value.attributes && Object.keys(value.attributes).length === Object.keys(expected.attributes).length &&
    Object.keys(expected.attributes).every(k => value.attributes[k] === expected.attributes[k]);
}
function lookup(id, slot, deadline, retry = true) {
  let observed = false, mismatch = false;
  do {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    const p = params(slot, 'retrieve');
    p.timeout = `${Math.max(1, Math.min(config.request_timeout_ms, remaining))}ms`;
    const response = http.get(`${config.worker_url}/v1/events/${id}`, p);
    if (response.status === 200) {
      observed = matches(response, id, slot);
      mismatch = !observed;
      break;
    }
    if (!retry || Date.now() >= deadline) break;
    sleep(Math.min(0.02, Math.max(0, (deadline - Date.now()) / 1000)));
  } while (true);
  return {observed, mismatch};
}

function ingest() {
  const index = exec.scenario.iterationInTest, id = identity(index), slot = index % config.tenants;
  const began = Date.now();
  const nominal = config.stages ? began : exec.scenario.startTime + index * 1000 / config.rate_per_second;
  let status = 0, receipt = false, visible = false, observed = false, invalid = false, elapsed = null, scheduled = null, visibility = null;
  started.add(1); contracts.add(0); isolationChecks.add(0); isolationDenied.add(0); isolationLeaks.add(0);
  generatorDelay.add(Math.max(0, began - nominal));
  try {
    const response = http.post(`${config.collector_url}/v1/events`, JSON.stringify(envelope(id)), params(slot, 'ingest'));
    elapsed = Date.now() - began; scheduled = Date.now() - nominal;
    status = response.status;
    wallLatency.add(elapsed); scheduledLatency.add(scheduled);
    if (status === 202) {
      http202.add(1); receipt = correctReceipt(response, id, slot);
      if (receipt) validReceipts.add(1); else { contracts.add(1); invalid = true; }
      const value = lookup(id, slot, began + config.visibility_p99_ms);
      observed = value.observed;
      visibility = observed ? Date.now() - nominal : null;
      visible = observed && visibility <= config.visibility_p99_ms;
      if (value.mismatch) { contracts.add(1); invalid = true; }
      visibleRate.add(visible);
      if (observed) visibilityLatency.add(visibility);
      if (observed && config.tenants > 1 && index % 100 === 0) {
        isolationChecks.add(1);
        const probe = http.get(`${config.worker_url}/v1/events/${id}`, params((slot + 1) % config.tenants, 'isolation'));
        isolationGood.add(probe.status === 404);
        if (probe.status === 404) isolationDenied.add(1);
        if (probe.status === 200) { isolationLeaks.add(1); contracts.add(1); invalid = true; }
      }
    } else if (status !== 0 && ![429, 503].includes(status)) {
      contracts.add(1); invalid = true;
    }
    validRate.add(receipt);
  } catch (_) {
    if (elapsed === null) { elapsed = Date.now() - began; scheduled = Date.now() - nominal; wallLatency.add(elapsed); scheduledLatency.add(scheduled); }
    validRate.add(false); contracts.add(1); invalid = true;
  } finally {
    completed.add(1); records.add(1);
    console.log('SLO_RECORD ' + JSON.stringify({index, id, tenant_slot: slot, http_status: status,
      receipt_valid: receipt, live_visible: visible, live_observed: observed, live_visibility_ms: visibility, contract_failure: invalid,
      scenario_start_ms: exec.scenario.startTime, started_ms: began, http_elapsed_ms: elapsed, scheduled_to_response_ms: scheduled,
      payload_bytes: config.payload_bytes}));
  }
}
function reconcile() {
  const index = exec.scenario.iterationInTest, item = manifest[index];
  const deadline = item.http_status === 202
    ? exec.scenario.startTime + config.settle_seconds * 1000 : Date.now() + config.request_timeout_ms;
  const value = lookup(item.id, item.tenant_slot, deadline, item.http_status === 202);
  reconciled.add(0); missing.add(0); wrongContent.add(0); unknownVisible.add(0);
  if (value.mismatch) wrongContent.add(1);
  if (item.http_status === 202) {
    if (value.observed) reconciled.add(1); else missing.add(1);
  } else if (value.observed) unknownVisible.add(1);
  queryRows.add(1);
  console.log('SLO_RECONCILE ' + JSON.stringify({index, id: item.id, tenant_slot: item.tenant_slot,
    http_status: item.http_status, visible: value.observed, mismatch: value.mismatch}));
}
export default function () { if (reconciling) reconcile(); else ingest(); }
export function handleSummary(data) {
  return {stdout: 'SLO_SUMMARY ' + JSON.stringify({schema_version: 1, mode: config.mode,
    metrics: data.metrics, state: data.state}) + '\n'};
}
