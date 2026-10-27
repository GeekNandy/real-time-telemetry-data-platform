import { complete, number, observedAt, validateRun } from './run-model.js';

const $ = id => document.getElementById(id);
const views = ['overview', 'events', 'evidence', 'local'];
let recording, environmentName = 'compose', selected = 0;
const environment = () => recording.environments[environmentName];
const element = (tag, text) => { const node = document.createElement(tag); if (text !== undefined) node.textContent = text; return node; };

function switchView() {
  const name = views.includes(location.hash.slice(1)) ? location.hash.slice(1) : 'overview';
  document.querySelectorAll('.view').forEach(view => { view.hidden = view.id !== name; });
  document.querySelectorAll('[data-view]').forEach(link => {
    if (link.dataset.view === name) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  });
  window.scrollTo(0, 0);
  if (recording && name === 'overview') drawChart();
}
window.addEventListener('hashchange', switchView);
document.querySelector('.skip').addEventListener('click', event => {
  event.preventDefault(); $('main').focus({ preventScroll: true }); window.scrollTo(0, 0);
});

function drawChart() {
  const host = $('chart'); host.replaceChildren();
  const rows = environment().requests, measure = $('measure').value;
  const visible = measure === 'observed_visibility_upper_bound_s';
  $('chart-description').textContent = visible ? 'Observed upper bounds include client polling and sweep delay' : 'Elapsed time for each recorded HTTP request';
  const width = Math.max(240, host.clientWidth), height = host.clientHeight || 190;
  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg'); svg.setAttribute('viewBox', '0 0 ' + width + ' ' + height);
  const values = rows.map(row => row[measure]).filter(value => typeof value === 'number');
  const maximum = Math.max(0.001, ...values);
  const last = Math.max(0.001, ...rows.map(row => row.scheduled_s));
  const points = [];
  for (const row of rows) {
    if (typeof row[measure] !== 'number') continue;
    const x = 8 + row.scheduled_s / last * (width - 22), y = height - 20 - row[measure] / maximum * (height - 45);
    points.push(x.toFixed(2) + ',' + y.toFixed(2));
    const circle = document.createElementNS(ns, 'circle');
    circle.setAttribute('cx', x); circle.setAttribute('cy', y); circle.setAttribute('r', '3');
    const title = document.createElementNS(ns, 'title');
    title.textContent = row.event_id + ': ' + number(row[measure] * (visible ? 1 : 1000)) + (visible ? ' s upper bound' : ' ms HTTP time');
    circle.append(title); svg.append(circle);
  }
  const line = document.createElementNS(ns, 'polyline');
  line.setAttribute('points', points.join(' ')); line.setAttribute('fill', 'none');
  line.setAttribute('stroke', '#b8f56b'); line.setAttribute('stroke-width', '1.5');
  svg.prepend(line);
  const label = document.createElementNS(ns, 'text'); label.setAttribute('x', '8'); label.setAttribute('y', '12');
  label.textContent = number(maximum * (visible ? 1 : 1000)) + (visible ? ' s' : ' ms'); svg.append(label);
  host.append(svg); host.setAttribute('aria-label', visible ? 'Recorded visibility upper bounds' : 'Recorded HTTP request times');
  $('chart-scale').textContent = 'Offered over ' + number(environment().summary.generator.scheduled_duration_s) + ' s · ' + rows.length + ' captured requests';
}

function showEvent(index) {
  selected = index;
  const row = environment().requests[index];
  const observation = observedAt(row);
  $('selected-id').textContent = row.event_id;
  const steps = [
    ['request-step', 'At +' + number(row.started_s, 3) + ' s', true],
    ['receipt-step', 'HTTP ' + row.status + ' at +' + number(row.completed_s, 3) + ' s', row.status === 202],
    ['query-step', observation !== null ?
      'Observed by +' + number(observation, 3) + ' s; bound includes polling delay' :
      'Visibility was not confirmed in this recording', observation !== null]
  ];
  for (const [id, text, confirmed] of steps) {
    $(id).className = confirmed ? 'complete' : 'active'; $(id).querySelector('p').textContent = text;
  }
  $('event-record').textContent = JSON.stringify(row, null, 2);
  document.querySelectorAll('#event-rows tr').forEach((tr, i) => {
    tr.classList.toggle('selected', i === index);
    tr.querySelector('button').setAttribute('aria-pressed', String(i === index));
  });
}

function render() {
  const data = environment(), result = data.summary.accepted_reconciliation;
  const okay = complete(data);
  $('run-status').textContent = okay ? 'All ' + result.accepted + ' accepted IDs reconciled' : 'Some outcomes remain unconfirmed';
  $('recorded-at').textContent = 'Captured ' + new Date(data.summary.started_at_utc).toLocaleString('en-GB', { timeZone: 'UTC' }) + ' UTC';
  $('accepted').textContent = result.accepted; $('visible').textContent = result.visible;
  $('unresolved').textContent = result.unresolved_ids.length;
  $('latency').textContent = number(data.summary.successful_http_elapsed.p95_ms) + ' ms';
  $('identities').textContent = data.requests.length + ' unique IDs';
  $('receipts').textContent = result.accepted + ' confirmed';
  $('queries').textContent = result.visible + ' confirmed';
  $('event-count').textContent = data.requests.length + ' recorded events';
  $('event-rows').replaceChildren();
  data.requests.forEach((row, index) => {
    const tr = element('tr');
    tr.append(element('td', String(index + 1).padStart(2, '0') + ' · ' + row.event_id.slice(0, 8)),
      element('td', 'HTTP ' + row.status), element('td', number(row.http_elapsed_s * 1000) + ' ms'));
    const td = element('td'), button = element('button', 'Inspect ↗');
    button.className = 'button subtle'; button.type = 'button';
    button.setAttribute('aria-label', 'Inspect event ' + (index + 1));
    button.addEventListener('click', () => showEvent(index)); td.append(button); tr.append(td);
    $('event-rows').append(tr);
  });
  showEvent(Math.min(selected, data.requests.length - 1)); drawChart();
}
$('environment').addEventListener('change', () => { environmentName = $('environment').value; selected = 0; render(); });
$('measure').addEventListener('change', () => { if (recording) drawChart(); });
new ResizeObserver(() => { if (recording && !$('overview').hidden) drawChart(); }).observe($('chart'));

async function load() {
  const response = await fetch('./recorded-run.json', { signal: AbortSignal.timeout(5000), credentials: 'same-origin', redirect: 'error' });
  if (!response.ok) throw new Error('The captured run is unavailable.');
  const reader = response.body?.getReader();
  if (!reader) throw new Error('The captured run is empty.');
  const parts = []; let size = 0;
  try {
    while (true) {
      const item = await reader.read(); if (item.done) break;
      size += item.value.byteLength;
      if (size > 65536) { await reader.cancel(); throw new Error('The captured run exceeds the viewer limit.'); }
      parts.push(item.value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const part of parts) { bytes.set(part, offset); offset += part.byteLength; }
  recording = validateRun(JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)));
  const provenance = recording.provenance;
  $('workflow-link').href = provenance.workflowUrl;
  $('source-link').href = 'https://github.com/GeekNandy/real-time-telemetry-data-platform/commit/' + provenance.sourceRevision;
  $('source-link').textContent = provenance.sourceRevision;
  $('execution-revision').textContent = provenance.executionRevision;
  $('execution-tree').textContent = provenance.executionTree;
  const browser = recording.browser, observe = recording.observability;
  $('browser-evidence').textContent = browser.acceptedReceipt && browser.matchingQueryResult ?
    'A real browser submitted an event, verified its receipt and retrieved the matching event from PostgreSQL.' :
    'The recorded browser result does not confirm the complete event journey.';
  $('observability-evidence').textContent = observe.grafanaDashboardProvisioned && observe.grafanaDatasourceProvisioned ?
    'The dashboard and datasource were provisioned; authenticated collector and worker scrapes were verified.' :
    'Some provisioning outcomes remain unconfirmed.';
  $('environment').disabled = false; render();
}
switchView();
load().catch(() => {
  recording = null; $('load-error').hidden = false;
  $('load-error').textContent = 'The captured run could not be verified. Open the original CI reports from the source repository.';
  $('run-status').textContent = 'Recording unavailable'; $('environment').disabled = true;
});
