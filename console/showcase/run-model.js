export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const finite = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const invariant = (condition, message) => { if (!condition) throw new Error(message); };

export function validateRun(run) {
  invariant(run?.schemaVersion === 1, 'Unsupported recording format.');
  const source = run.provenance;
  invariant(/^[a-f0-9]{40}$/.test(source?.sourceRevision || '') &&
    /^[a-f0-9]{40}$/.test(source?.executionRevision || ''), 'Missing source revisions.');
  invariant(source.workflowUrl === 'https://github.com/GeekNandy/real-time-telemetry-data-platform/actions/runs/' +
    source.workflowRun && Number.isSafeInteger(source.workflowRun), 'Invalid workflow provenance.');
  for (const name of ['compose', 'kind']) {
    const environment = run.environments?.[name], summary = environment?.summary, rows = environment?.requests;
    invariant(summary?.revision === source.executionRevision, 'Reports refer to different execution revisions.');
    invariant(Number.isSafeInteger(summary.scheduled) && summary.scheduled > 0 && summary.scheduled <= 200 &&
      Array.isArray(rows) && rows.length === summary.scheduled, 'Incomplete request recording.');
    const ids = new Set();
    for (const row of rows) {
      invariant(UUID.test(row.event_id || '') && !ids.has(row.event_id), 'Invalid or duplicate event identity.');
      ids.add(row.event_id);
      invariant(Number.isInteger(row.status) && row.status >= 100 && row.status <= 599 &&
        ['scheduled_s', 'started_s', 'completed_s', 'http_elapsed_s'].every(key => finite(row[key])) &&
        row.completed_s >= row.started_s, 'Invalid client observation.');
      invariant(row.observed_visibility_upper_bound_s === undefined ||
        finite(row.observed_visibility_upper_bound_s), 'Invalid visibility observation.');
    }
    const accepted = rows.filter(row => row.status === 202);
    const visible = accepted.filter(row => finite(row.observed_visibility_upper_bound_s));
    const reconciliation = summary.accepted_reconciliation;
    invariant(reconciliation && reconciliation.accepted === accepted.length &&
      reconciliation.visible === visible.length && Array.isArray(reconciliation.unresolved_ids) &&
      reconciliation.unresolved_ids.length === accepted.length - visible.length &&
      reconciliation.unresolved_ids.every(id => ids.has(id)), 'Reconciliation does not match recorded requests.');
    invariant(reconciliation.passed === (visible.length === accepted.length &&
      reconciliation.unresolved_ids.length === 0), 'Unconfirmed outcomes cannot be reported as passed.');
    invariant(finite(summary.successful_http_elapsed?.p95_ms), 'Missing recorded latency percentile.');
  }
  return run;
}

export function complete(environment) {
  const result = environment.summary.accepted_reconciliation;
  return result.passed && result.accepted === environment.summary.scheduled && result.visible === result.accepted;
}

export function number(value, digits = 1) {
  return finite(value) ? value.toFixed(digits) : '—';
}

export function observedAt(row) {
  return finite(row.scheduled_s) && finite(row.observed_visibility_upper_bound_s) ?
    row.scheduled_s + row.observed_visibility_upper_bound_s : null;
}
