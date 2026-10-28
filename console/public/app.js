import { ApiError, UUID, acceptedRate, format, isNumber, matchingEvent, request, syntheticEvent, validateEvent } from './model.js';

const $ = id => document.getElementById(id);
document.querySelector('.skip').addEventListener('click', event => {
  event.preventDefault();
  $('main').focus({ preventScroll: true });
  window.scrollTo(0, 0);
});
let accessToken = '', observeToken = '', session = new AbortController(), generation = 0;
let refreshTimer, refreshing = false, previous = null, samples = [], currentEvent = null, currentTrace = null;
let submitting = false, checking = false, lookupGeneration = 0;
let currentAccepted = false;
const pause = (milliseconds, signal) => new Promise((resolve, reject) => {
  if (signal.aborted) { reject(signal.reason); return; }
  const timer = setTimeout(done, milliseconds);
  function done() { signal.removeEventListener('abort', aborted); resolve(); }
  function aborted() { clearTimeout(timer); reject(signal.reason); }
  signal.addEventListener('abort', aborted, { once: true });
});

function toolUrl(port, path = '/') {
  const url = new URL(location.href); url.port = port; url.pathname = path; url.search = ''; url.hash = '';
  return url.href;
}
$('grafana-link').href = toolUrl(3000, '/d/telemetry-platform/telemetry-platform');
$('jaeger-link').href = toolUrl(16686);
$('prometheus-link').href = toolUrl(9090);

function switchView() {
  const selected = ['overview', 'playground', 'inspector', 'diagnostics'].includes(location.hash.slice(1)) ? location.hash.slice(1) : 'overview';
  for (const element of document.querySelectorAll('.view')) element.hidden = element.id !== selected;
  for (const link of document.querySelectorAll('[data-view]')) {
    if (link.dataset.view === selected) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  }
  clearTimeout(refreshTimer);
  window.scrollTo(0, 0);
  requestAnimationFrame(() => window.scrollTo(0, 0));
  if (selected === 'overview') { drawChart(); refresh(); }
}
window.addEventListener('hashchange', switchView);
window.addEventListener('resize', () => { if (!$('overview').hidden) drawChart(); });
document.addEventListener('visibilitychange', () => {
  clearTimeout(refreshTimer);
  if (!document.hidden && !$('overview').hidden) refresh();
});

function notice(message, tone = '') {
  $('event-notice').textContent = message;
  $('event-notice').className = `notice ${tone}`;
}

function step(id, text, state = '') {
  $(id).className = state;
  $(id).querySelector('p').textContent = text;
}

function newEvent() {
  if (submitting || checking) return;
  currentEvent = null; currentTrace = null; currentAccepted = false;
  $('event-json').value = JSON.stringify(syntheticEvent(), null, 2);
  $('event-json').readOnly = false;
  $('send-event').textContent = 'Submit event ↗';
  $('event-actions').hidden = true;
  step('journey-submitted', 'Ready to submit your event');
  step('journey-accepted', 'Awaiting an HTTP 202 receipt with a matching ID');
  step('journey-visible', 'Awaiting a matching result from the worker');
  notice(accessToken ? 'Synthetic data only. The API will verify your token and event schema.' : 'Load a session token with write and read scopes to begin');
}
$('new-event').addEventListener('click', newEvent);

$('session-open').addEventListener('click', () => $('session-dialog').showModal());
$('session-close').addEventListener('click', () => { $('session-dialog').close(); clearTokenInputs(); });
$('session-dialog').addEventListener('close', clearTokenInputs);
function clearTokenInputs() { $('access-token').value = ''; $('observe-token').value = ''; $('session-error').textContent = ''; }
function endSession() {
  session.abort(); session = new AbortController(); generation++; lookupGeneration++;
  accessToken = ''; observeToken = ''; previous = null; samples = [];
  submitting = false; checking = false;
  $('send-event').disabled = false; $('new-event').disabled = false; $('check-current').disabled = false;
  $('lookup-button').disabled = false;
  $('lookup-id').value = ''; $('lookup-result').textContent = ''; $('lookup-result').hidden = true;
  $('lookup-status').textContent = 'No query yet'; $('lookup-status').className = 'badge';
  $('lookup-notice').textContent = 'Enter an event ID to inspect its stored schema and attributes';
  $('session-status').textContent = 'No credentials'; $('session-status').className = 'badge';
  $('session-open').textContent = 'Connect session ↗'; $('session-clear').hidden = true;
  clearMetrics('Load an observer token to see live intake activity'); newEvent(); clearTokenInputs();
}
$('session-clear').addEventListener('click', () => { endSession(); refresh(); });
$('session-form').addEventListener('submit', event => {
  event.preventDefault();
  const token = $('access-token').value.trim(), observer = $('observe-token').value.trim();
  if ((!token && !observer) || [token, observer].some(value => value.length > 16384 || /\s/.test(value))) {
    $('session-error').textContent = 'Provide a token without whitespace, up to 16 KiB.'; return;
  }
  endSession(); accessToken = token; observeToken = observer || token;
  $('session-status').textContent = 'Credentials loaded'; $('session-status').className = 'badge';
  $('session-open').textContent = 'Replace credentials ↗'; $('session-clear').hidden = false;
  $('session-dialog').close(); notice('Credentials loaded. Identity and scopes are verified by the API.');
  refresh();
});

function clearMetrics(message) {
  for (const id of ['accepted-rate', 'buffer-records', 'buffer-bytes', 'buffer-age',
    'ingress-count', 'ingress-failures', 'dispatch-count', 'dispatch-failures', 'persistence-count', 'persistence-failures']) $(id).textContent = '—';
  $('accepted-note').textContent = 'Waiting for two live samples';
  $('metrics-status').textContent = message;
  $('freshness').textContent = message;
  previous = null; samples = []; drawChart();
}

function drawChart() {
  const host = $('chart'); host.replaceChildren();
  const available = samples.filter(item => isNumber(item.value));
  if (available.length < 2) {
    const hint = document.createElement('p'); hint.className = 'empty-chart';
    hint.textContent = observeToken ? 'Waiting for two measured rate samples' : 'Load an observer token to see live intake activity';
    host.append(hint); return;
  }
  const ns = 'http://www.w3.org/2000/svg';
  const width = host.clientWidth || 600, height = host.clientHeight || 190;
  const svg = document.createElementNS(ns, 'svg'); svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', 'Accepted event rate across measured session samples');
  const max = Math.max(1, ...available.map(item => item.value));
  const start = samples[0].at, duration = Math.max(1, samples.at(-1).at - start);
  let path = '', connected = false;
  for (const item of samples) {
    if (!isNumber(item.value)) { connected = false; continue; }
    const x = 4 + (item.at - start) / duration * (width - 10), y = height - 20 - item.value / max * (height - 45);
    path += `${connected ? ' L' : ' M'}${x.toFixed(2)},${y.toFixed(2)}`; connected = true;
  }
  const line = document.createElementNS(ns, 'path');
  for (const [key, value] of Object.entries({ d: path, fill: 'none', stroke: '#b8f56b', 'stroke-width': '2', 'stroke-linejoin': 'round' })) line.setAttribute(key, value);
  svg.append(line);
  const label = document.createElementNS(ns, 'text'); label.setAttribute('x', '4'); label.setAttribute('y', '13');
  label.setAttribute('fill', '#9ba5b3'); label.setAttribute('font-size', '10'); label.textContent = `${max.toFixed(1)} events/s`;
  svg.append(label); host.append(svg);
}

function showStage(name, snapshot) {
  const value = snapshot?.stages?.[name];
  $(`${name}-count`).textContent = format(value?.completed);
  $(`${name}-failures`).textContent = format(value?.failures);
  $(`${name}-failures`).classList.toggle('error-text', isNumber(value?.failures) && value.failures > 0);
}

async function refresh() {
  if (refreshing || document.hidden || $('overview').hidden) return;
  refreshing = true; clearTimeout(refreshTimer);
  const version = generation, signal = session.signal, credential = observeToken;
  $('refresh').disabled = true;
  try {
    const health = await Promise.allSettled(['collector', 'worker'].map(role => request(`/api/${role}/readiness`, '', { signal, timeout: 3000 })));
    if (version !== generation) return;
    let ready = 0;
    for (let index = 0; index < health.length; index++) {
      const good = health[index].status === 'fulfilled' && health[index].value.data.status === 'UP';
      if (good) ready++;
      const element = $(`${index === 0 ? 'collector' : 'worker'}-health`);
      element.textContent = good ? 'Ready' : 'Unavailable'; element.className = `health ${good ? 'good' : 'bad'}`;
    }
    $('platform-status').replaceChildren();
    const light = document.createElement('span'); light.className = `status-light ${ready === 2 ? '' : 'warn'}`;
    $('platform-status').append(light, document.createTextNode(ready === 2 ? 'Both applications ready' : `${ready} of 2 applications ready`));
    if (!credential) { clearMetrics('Platform-wide aggregates require telemetry.observe'); return; }
    const data = await Promise.allSettled(['collector', 'worker'].map(role => request(`/api/${role}/metrics`, credential, { signal, timeout: 3000 })));
    if (version !== generation) return;
    const collector = data[0].status === 'fulfilled' ? data[0].value.data : null;
    const worker = data[1].status === 'fulfilled' ? data[1].value.data : null;
    if (!collector && !worker) {
      clearMetrics(data[0].reason instanceof ApiError ? data[0].reason.message : 'Metrics are unavailable'); return;
    }
    const sampledAt = performance.now(), sample = collector ? { ...collector, sampledAt } : null;
    const rate = acceptedRate(previous, sample); previous = sample;
    samples.push({ at: sampledAt, value: rate }); samples = samples.filter(item => item.at >= sampledAt - 300000).slice(-61);
    $('accepted-rate').textContent = format(rate, 'rate');
    $('accepted-note').textContent = rate === null ? 'Waiting for two live samples' : 'Observed acknowledged event rate';
    $('buffer-records').textContent = format(collector?.bufferRecords);
    $('buffer-bytes').textContent = format(collector?.bufferBytes, 'bytes');
    $('buffer-age').textContent = format(collector?.oldestBufferedAgeSeconds, 'seconds');
    showStage('ingress', collector); showStage('dispatch', collector); showStage('persistence', worker);
    $('freshness').textContent = `Updated ${new Date().toLocaleTimeString()} · ${collector && worker ? 'both roles' : 'partial data'} · every 5 s`;
    $('metrics-status').textContent = collector && worker ? 'Platform-wide totals · reset when each process restarts' : 'One role is unavailable or unauthorized; its values remain unknown';
    drawChart();
  } catch (error) {
    if (version === generation) clearMetrics(error instanceof ApiError ? error.message : 'Metrics are unavailable');
  } finally {
    refreshing = false; $('refresh').disabled = false;
    if (!document.hidden && !$('overview').hidden) refreshTimer = setTimeout(refresh, version === generation ? 5000 : 0);
  }
}
$('refresh').addEventListener('click', refresh);

function eventContext() {
  const traceBytes = new Uint8Array(16); crypto.getRandomValues(traceBytes);
  return Array.from(traceBytes, item => item.toString(16).padStart(2, '0')).join('');
}

async function checkVisibility(eventValue, version, signal, wait = true) {
  if (checking) return;
  checking = true; $('check-current').disabled = true;
  const deadline = performance.now() + (wait ? 30000 : 1);
  step('journey-visible', 'Checking the tenant-scoped worker endpoint…', 'active');
  let interval = 500;
  try {
    do {
      try {
        const response = await request(`/api/worker/events/${eventValue.eventId}`, accessToken, {
          signal, timeout: wait ? Math.max(1, Math.min(6000, deadline - performance.now())) : 6000
        });
        if (version !== generation) return;
        if (response.status !== 200 || !matchingEvent(response.data, eventValue.eventId)) throw new ApiError(0, 'The query response did not confirm the event identity and visibility.');
        step('journey-visible', 'Matching event returned from the PostgreSQL-backed worker', 'complete');
        notice('The event is persisted and queryable in this tenant.', 'good');
        return;
      } catch (error) {
        if (version !== generation) return;
        if (!(error instanceof ApiError) || error.status !== 404) throw error;
      }
      if (!wait || performance.now() >= deadline) break;
      notice(currentAccepted ? 'Locally accepted. Waiting for the event to become queryable…' : 'No acceptance receipt. Checking whether this event ID became queryable…', 'warn');
      await pause(Math.min(interval, Math.max(0, deadline - performance.now())), signal);
      interval = Math.min(2000, interval * 2);
    } while (performance.now() < deadline);
    if (version === generation) {
      step('journey-visible', 'No matching query result within the observation window', 'active');
      notice('Visibility is still unconfirmed. The event may remain buffered or be processing; check again with the same ID.', 'warn');
    }
  } catch (error) {
    if (version === generation) {
      step('journey-visible', 'Visibility could not be confirmed', 'active');
      notice(error instanceof ApiError ? error.message : 'Visibility check interrupted', 'warn');
    }
  } finally {
    if (version === generation) { checking = false; $('check-current').disabled = false; }
  }
}

$('event-form').addEventListener('submit', async event => {
  event.preventDefault();
  if (submitting || checking) return;
  if (!accessToken) { notice('Load an event access token before submitting.', 'warn'); $('session-dialog').showModal(); return; }
  let value;
  try { value = currentEvent || validateEvent(JSON.parse($('event-json').value)); }
  catch (error) { notice(error instanceof SyntaxError ? 'The event JSON is invalid.' : error.message, 'bad'); return; }
  currentEvent = value; currentTrace ||= eventContext();
  $('event-json').value = JSON.stringify(value, null, 2);
  $('event-json').readOnly = true; $('event-actions').hidden = false;
  $('trace-current').href = toolUrl(16686, `/trace/${currentTrace}`);
  submitting = true; $('send-event').disabled = true; $('new-event').disabled = true;
  const version = generation, signal = session.signal;
  step('journey-submitted', `Submitting ${value.eventId}`, 'active');
  notice('Awaiting the durable acceptance receipt…');
  try {
    const response = await request('/api/collector/events', accessToken, {
      body: value, signal, traceparent: `00-${currentTrace}-${eventContext().slice(0, 16)}-01`
    });
    if (version !== generation) return;
    if (response.status !== 202 || !matchingEvent(response.data, value.eventId)) throw new ApiError(0, 'The acceptance receipt did not match. Delivery remains unconfirmed; retry the same event.');
    step('journey-submitted', `HTTP 202 · ${value.eventId}`, 'complete');
    currentAccepted = true;
    step('journey-accepted', 'Receipt confirmed: Kafka acknowledgement and durable checkpoint', 'complete');
    notice('Locally accepted. Query visibility has not yet been confirmed.', 'warn');
    $('send-event').textContent = 'Retry same event ↗';
    await checkVisibility(value, version, signal);
  } catch (error) {
    if (version !== generation) return;
    step('journey-submitted', error instanceof ApiError && error.status ? `HTTP ${error.status} · no acceptance receipt` : 'Request outcome unconfirmed', 'active');
    notice(error instanceof ApiError ? error.message : 'Request interrupted. Its delivery outcome is unconfirmed.', 'warn');
    $('send-event').textContent = 'Retry same event ↗';
  } finally {
    if (version === generation) { submitting = false; $('send-event').disabled = false; $('new-event').disabled = false; refresh(); }
  }
});
$('check-current').addEventListener('click', () => { if (currentEvent && !submitting) checkVisibility(currentEvent, generation, session.signal); });
$('inspect-current').addEventListener('click', () => { if (currentEvent) { $('lookup-id').value = currentEvent.eventId; location.hash = 'inspector'; $('lookup-form').requestSubmit(); } });

$('lookup-form').addEventListener('submit', async event => {
  event.preventDefault();
  const id = $('lookup-id').value.trim().toLowerCase();
  if (!UUID.test(id)) { $('lookup-notice').textContent = 'Enter a valid UUID event ID.'; return; }
  if (!accessToken) { $('lookup-notice').textContent = 'Load a token with telemetry.read first.'; $('session-dialog').showModal(); return; }
  const query = ++lookupGeneration, version = generation, signal = session.signal;
  $('lookup-result').hidden = true; $('lookup-result').textContent = '';
  $('lookup-status').textContent = 'Querying'; $('lookup-status').className = 'badge';
  $('lookup-notice').textContent = 'Looking up this event in the authenticated tenant…'; $('lookup-button').disabled = true;
  try {
    const response = await request(`/api/worker/events/${id}`, accessToken, { signal });
    if (query !== lookupGeneration || version !== generation) return;
    if (response.status !== 200 || !matchingEvent(response.data, id)) throw new ApiError(0, 'The response identity did not match or visibility was not confirmed.');
    $('lookup-result').textContent = JSON.stringify(response.data, null, 2); $('lookup-result').hidden = false;
    $('lookup-status').textContent = 'Queryable'; $('lookup-status').className = 'badge good';
    $('lookup-notice').textContent = 'Matching event returned from the tenant-scoped worker query';
  } catch (error) {
    if (query !== lookupGeneration || version !== generation) return;
    $('lookup-status').textContent = error.status === 404 ? 'Not found' : 'Unavailable'; $('lookup-status').className = 'badge warn';
    $('lookup-notice').textContent = error instanceof ApiError ? error.message : 'The query could not be confirmed.';
  } finally { if (query === lookupGeneration && version === generation) $('lookup-button').disabled = false; }
});
for (const link of document.querySelectorAll('a[href^="#"]')) {
  if (!['#overview', '#playground', '#inspector', '#diagnostics'].includes(link.hash)) continue;
  link.addEventListener('click', event => {
    if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    event.preventDefault();
    if (location.hash !== link.hash) history.pushState(null, '', link.hash);
    switchView();
  });
}
newEvent(); switchView();
