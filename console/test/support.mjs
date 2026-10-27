import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import assert from 'node:assert/strict';

export const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
export const reports = path.resolve(process.env.TELEMETRY_BROWSER_REPORTS || path.join(root, '../data/console-browser'));

export async function launch() {
  const module = process.env.TELEMETRY_PLAYWRIGHT_PATH ? pathToFileURL(process.env.TELEMETRY_PLAYWRIGHT_PATH).href : 'playwright';
  const { chromium } = await import(module);
  return chromium.launch({ headless: true, ...(process.env.TELEMETRY_CHROMIUM_PATH ? { executablePath: process.env.TELEMETRY_CHROMIUM_PATH } : {}) });
}

export async function connect(page, token = 'synthetic-fixture-token') {
  await page.getByRole('button', { name: /Connect session|Replace credentials/ }).click();
  await page.getByRole('dialog', { name: 'Connect your session' }).waitFor();
  assert.equal(await page.getByLabel('Event access token', { exact: true }).getAttribute('type'), 'password');
  await page.getByLabel('Event access token', { exact: true }).fill(token);
  await page.getByRole('button', { name: 'Use credentials' }).click();
  await page.waitForFunction(() => !document.getElementById('session-dialog').open);
}

export async function fixture() {
  const state = { mode: 'visible', reads: 0, accepted: 40, metricsForbidden: false, queryCount: 0, events: new Map() };
  function json(response, status, value) {
    response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    response.end(JSON.stringify(value));
  }
  const server = http.createServer(async (request, response) => {
    const url = new URL(request.url, 'http://localhost');
    if (url.pathname.endsWith('/readiness')) return json(response, 200, { status: 'UP' });
    if (url.pathname.endsWith('/metrics')) {
      if (!request.headers.authorization || state.metricsForbidden) return json(response, 403, {});
      const worker = url.pathname.includes('/worker/');
      state.accepted++;
      return json(response, 200, {
        component: worker ? 'worker' : 'collector', instanceId: worker ? 'fixture-worker' : 'fixture-collector',
        observedAt: new Date().toISOString(), acceptedTotal: worker ? null : state.accepted,
        bufferRecords: worker ? null : 12, bufferBytes: worker ? null : 24576, oldestBufferedAgeSeconds: worker ? null : 1.4,
        stages: { ingress: { completed: 80, failures: 0 }, dispatch: { completed: 68, failures: 1 }, persistence: { completed: 68, failures: 0 } }
      });
    }
    if (url.pathname === '/api/collector/events' && request.method === 'POST') {
      let body = ''; for await (const chunk of request) body += chunk;
      const event = JSON.parse(body); state.events.set(event.eventId, event); state.reads = 0;
      state.traceparent = request.headers.traceparent;
      return json(response, 202, { eventId: state.mode === 'wrong-receipt' ? 'different-id' : event.eventId });
    }
    if (url.pathname.startsWith('/api/worker/events/')) {
      state.queryCount++;
      const id = url.pathname.split('/').at(-1);
      if (state.mode === 'wrong-query') return json(response, 200, { eventId: 'different-id' });
      if (state.mode === 'pending-query') return json(response, 202, { eventId: id });
      if (state.mode === 'xss') return json(response, 200, { eventId: id, attributes: { text: '<img src=x onerror="window.injected=true">' } });
      const event = state.events.get(id);
      if (!event || state.reads++ === 0) return json(response, 404, {});
      return json(response, 200, event);
    }
    const names = new Set(['index.html', 'app.js', 'model.js', 'style.css', 'mark.svg']);
    const name = url.pathname === '/' ? 'index.html' : url.pathname.slice(1);
    if (!names.has(name)) { response.writeHead(404); response.end(); return; }
    const type = name.endsWith('.js') ? 'text/javascript' : name.endsWith('.css') ? 'text/css' : name.endsWith('.svg') ? 'image/svg+xml' : 'text/html';
    response.writeHead(200, { 'content-type': type, 'content-security-policy': "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'" });
    response.end(await fs.readFile(path.join(root, 'public', name)));
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return { state, url: `http://127.0.0.1:${server.address().port}`, close: async () => { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); } };
}

export async function assertNoStoredCredentials(page) {
  assert.equal(await page.evaluate(() => localStorage.length + sessionStorage.length), 0);
}

export async function saveScreenshot(page, name) {
  await fs.mkdir(reports, { recursive: true });
  assert.equal(await page.locator('#session-dialog').evaluate(element => element.open), false);
  await page.screenshot({ path: path.join(reports, name), fullPage: true, animations: 'disabled' });
}
