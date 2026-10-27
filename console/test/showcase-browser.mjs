import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import http from 'node:http';
import { launch, reports, root } from './support.mjs';

const data = JSON.parse(await fs.readFile(path.join(root, 'dist/recorded-run.json')));
const assets = new Set(['index.html', 'app.js', 'run-model.js', 'showcase.css', 'base.css', 'mark.svg', 'recorded-run.json']);
let mode = 'normal';
const server = http.createServer(async (request, response) => {
  const name = new URL(request.url, 'http://localhost').pathname.slice(1) || 'index.html';
  if (request.method !== 'GET' || !assets.has(name)) { response.writeHead(404).end(); return; }
  if (name === 'recorded-run.json' && mode === 'unavailable') { response.writeHead(503).end(); return; }
  const type = name.endsWith('.js') ? 'text/javascript' : name.endsWith('.css') ? 'text/css' :
    name.endsWith('.json') ? 'application/json' : name.endsWith('.svg') ? 'image/svg+xml' : 'text/html';
  response.writeHead(200, { 'content-type': type, 'x-content-type-options': 'nosniff' });
  if (name === 'recorded-run.json' && mode === 'payload') {
    const value = structuredClone(data);
    value.environments.compose.requests[0].note = '<img src=x onerror="window.snapshotInjected=true">';
    response.end(JSON.stringify(value));
  } else response.end(await fs.readFile(path.join(root, 'dist', name)));
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const url = 'http://127.0.0.1:' + server.address().port;
const browser = await launch(), cases = [];
async function run(name, body) {
  mode = 'normal';
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage(), errors = [];
  page.on('pageerror', error => errors.push(error.message));
  try {
    await body(page);
    assert.deepEqual(errors, []);
    assert.equal(await page.evaluate(() => localStorage.length + sessionStorage.length), 0);
    cases.push({ name, passed: true }); console.log('PASS ' + name);
  } finally { await context.close(); }
}
try {
  await run('recording is labelled and the static explorer sends no event writes', async page => {
    const requests = []; page.on('request', request => requests.push({ method: request.method(), url: request.url() }));
    await page.goto(url); await page.getByText('All 20 accepted IDs reconciled').waitFor();
    assert.ok(await page.getByText('Recorded run · 27 Oct 2026', { exact: true }).isVisible());
    assert.equal(await page.locator('#accepted').textContent(), '20');
    assert.equal(await page.locator('#visible').textContent(), '20');
    assert.equal(await page.locator('#unresolved').textContent(), '0');
    assert.ok(requests.every(request => request.method === 'GET' && !request.url.includes('/api/')));
    assert.equal(await page.locator('input[type="password"]').count(), 0);
    await fs.mkdir(reports, { recursive: true });
    await page.screenshot({ path: path.join(reports, 'recorded-overview.png'), fullPage: true });
  });
  await run('event selection preserves the captured identity and observation', async page => {
    await page.goto(url); await page.getByText('All 20 accepted IDs reconciled').waitFor();
    await page.getByRole('link', { name: 'Events', exact: true }).click();
    await page.getByRole('button', { name: 'Inspect event 3', exact: true }).click();
    assert.equal(await page.locator('#selected-id').textContent(), data.environments.compose.requests[2].event_id);
    assert.equal(await page.locator('#query-step').getAttribute('class'), 'complete');
    const row = data.environments.compose.requests[2];
    assert.match(await page.locator('#query-step p').textContent(), new RegExp((row.scheduled_s + row.observed_visibility_upper_bound_s).toFixed(3)));
    assert.match(await page.locator('#event-record').textContent(), /observed_visibility_upper_bound_s/);
  });
  await run('environment and chart selection use their own recorded values', async page => {
    await page.goto(url); await page.getByText('All 20 accepted IDs reconciled').waitFor();
    await page.getByLabel('Verification environment').selectOption('kind');
    assert.equal(await page.locator('#latency').textContent(), data.environments.kind.summary.successful_http_elapsed.p95_ms.toFixed(1) + ' ms');
    await page.getByLabel('Measure', { exact: true }).selectOption('observed_visibility_upper_bound_s');
    await page.getByRole('img', { name: 'Recorded visibility upper bounds' }).waitFor();
    assert.equal(await page.locator('#chart circle').count(), 20);
    await page.getByRole('link', { name: 'Events', exact: true }).click();
    assert.equal(await page.locator('#selected-id').textContent(), data.environments.kind.requests[0].event_id);
  });
  await run('unavailable recording retains unknown metrics', async page => {
    mode = 'unavailable'; await page.goto(url);
    await page.getByRole('alert').waitFor({ state: 'visible' });
    assert.equal(await page.locator('#accepted').textContent(), '—');
    assert.equal(await page.locator('#visible').textContent(), '—');
    assert.equal(await page.locator('#run-status').textContent(), 'Recording unavailable');
  });
  await run('recorded text remains inert in the raw observation view', async page => {
    mode = 'payload'; await page.goto(url);
    await page.getByText('All 20 accepted IDs reconciled').waitFor();
    await page.getByRole('link', { name: 'Events', exact: true }).click();
    await page.getByText('Captured client record', { exact: true }).click();
    assert.match(await page.locator('#event-record').textContent(), /<img/);
    assert.equal(await page.locator('#event-record img').count(), 0);
    assert.equal(await page.evaluate(() => Boolean(window.snapshotInjected)), false);
  });
  await run('mobile navigation and event details fit the viewport', async page => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(url); await page.getByText('All 20 accepted IDs reconciled').waitFor();
    assert.ok(await page.getByText('Recorded run · 27 Oct 2026', { exact: true }).isVisible());
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await page.screenshot({ path: path.join(reports, 'recorded-mobile-overview.png'), fullPage: true });
    await page.getByRole('link', { name: 'Events', exact: true }).click();
    await page.getByRole('button', { name: 'Inspect event 20', exact: true }).click();
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await page.getByRole('link', { name: 'Run locally', exact: true }).click();
    await page.getByRole('heading', { name: 'Run the full pipeline.' }).waitFor();
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await page.screenshot({ path: path.join(reports, 'recorded-mobile.png'), fullPage: true });
  });
  await fs.writeFile(path.join(reports, 'recorded-run-browser.json'), JSON.stringify({ recordedData: true, cases }, null, 2));
} finally {
  await browser.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
}
