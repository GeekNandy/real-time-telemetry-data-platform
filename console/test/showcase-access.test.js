import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { accessControl, cookieName, credentialRecord, parseUsers, sessionLifetimeMs } from '../showcase-access.js';
import { protectedServer } from '../showcase-server.mjs';
import { buildShowcase } from '../build-showcase.mjs';

const password = 'synthetic-test-password-never-used-for-hosted-access';
const records = [];
for (const name of ['fixture-owner', 'fixture-reviewer1', 'fixture-reviewer2']) records.push(await credentialRecord(name, password));
const usersJson = JSON.stringify(records);
async function fixture(body, options = {}) {
  const server = await protectedServer({ usersJson, ...options });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const url = 'http://127.0.0.1:' + server.address().port;
  const origin = 'https://127.0.0.1:' + server.address().port;
  const request = (route, options = {}) => fetch(url + route, { redirect: 'manual', ...options });
  const login = (username = records[0].username, supplied = password, extra = {}) => request('/login', {
    method: 'POST', headers: { origin, 'content-type': 'application/x-www-form-urlencoded', ...extra },
    body: new URLSearchParams({ username, password: supplied }).toString()
  });
  try { await body({ request, login, origin, url }); }
  finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}
const sessionCookie = response => response.headers.get('set-cookie').split(';')[0];

test('access configuration fails closed, including missing, duplicate and plaintext credentials', async () => {
  for (const value of [undefined, '', '[]', '{}', '[null]', JSON.stringify([records[0], records[0]]),
    JSON.stringify([{ ...records[0], password }]), JSON.stringify([{ ...records[0], username: null }]),
    JSON.stringify([{ ...records[0], username: true }]), JSON.stringify([{ ...records[0], hash: 'invalid' }])]) {
    assert.throws(() => parseUsers(value), /Invalid review access configuration/);
  }
  await assert.rejects(protectedServer(), /Invalid review access configuration/);
  assert.equal(parseUsers(usersJson).size, 3);
  assert.notEqual(records[0].salt, records[1].salt);
  assert.notEqual(records[0].hash, records[1].hash);
});
test('anonymous clients cannot retrieve any explorer asset or recording', () => fixture(async ({ request }) => {
  const root = await request('/');
  assert.equal(root.status, 303); assert.equal(root.headers.get('location'), '/login');
  for (const name of ['index.html', 'recorded-run.json', 'app.js', 'run-model.js', 'base.css', 'showcase.css', 'mark.svg']) {
    const response = await request('/' + name);
    assert.equal(response.status, 401);
    assert.doesNotMatch(await response.text(), /event_id|Recorded run|const /);
  }
  const head = await request('/recorded-run.json', { method: 'HEAD' });
  assert.equal(head.status, 401); assert.equal(await head.text(), '');
  const page = await request('/login');
  assert.equal(page.status, 200); assert.match(await page.text(), /type="password"/);
  assert.equal(page.headers.get('referrer-policy'), 'same-origin');
  assert.equal(await (await request('/healthz')).text(), 'OK');
}));
test('all three users receive isolated secure sessions and protected data', () => fixture(async ({ request, login }) => {
  const cookies = new Set();
  for (const record of records) {
    const response = await login(record.username);
    assert.equal(response.status, 303); assert.equal(response.headers.get('location'), '/');
    const header = response.headers.get('set-cookie');
    for (const flag of ['HttpOnly', 'Secure', 'SameSite=Strict', 'Path=/', 'Max-Age=28800']) assert.ok(header.includes(flag));
    const cookie = sessionCookie(response);
    assert.ok(cookie.startsWith(cookieName + '=')); assert.ok(!cookie.includes(record.username)); cookies.add(cookie);
    const data = await request('/recorded-run.json', { headers: { cookie } });
    assert.equal(data.status, 200);
    assert.equal(data.headers.get('cache-control'), 'private, no-store');
    assert.equal(data.headers.get('vary'), 'Cookie');
    assert.equal((await data.json()).environments.compose.requests.length, 20);
    const page = await request('/', { headers: { cookie } });
    const html = await page.text();
    assert.match(html, /action="\/logout"/);
    assert.match(html, /name="referrer" content="same-origin"/);
    assert.match(page.headers.get('content-security-policy'), /frame-ancestors 'none'/);
    assert.equal(page.headers.get('x-content-type-options'), 'nosniff');
  }
  assert.equal(cookies.size, 3);
}));
test('wrong and unknown users get the same failure and forged or ambiguous cookies fail', () => fixture(async ({ request, login }) => {
  const wrong = await login(records[0].username, 'incorrect-synthetic-password');
  const unknown = await login('missing-user', password);
  assert.equal(wrong.status, 303); assert.equal(unknown.status, 303);
  assert.equal(wrong.headers.get('location'), unknown.headers.get('location'));
  assert.equal(wrong.headers.get('set-cookie'), null);
  const valid = sessionCookie(await login());
  const altered = valid.slice(0, -1) + (valid.endsWith('A') ? 'B' : 'A');
  for (const cookie of [altered, valid + '; ' + valid, cookieName + '=malformed']) {
    assert.equal((await request('/recorded-run.json', { headers: { cookie } })).status, 401);
  }
}));
test('logout revokes the actual session and cross-origin requests cannot log a user in or out', () => fixture(async ({ request, login, origin }) => {
  const cookie = sessionCookie(await login());
  assert.equal((await login(records[0].username, password, { origin: 'https://unrelated.invalid' })).status, 403);
  assert.equal((await login(records[0].username, password, { origin: 'null' })).status, 403);
  assert.equal((await request('/logout', { method: 'POST', headers: { cookie, origin: 'https://unrelated.invalid' } })).status, 403);
  assert.equal((await request('/recorded-run.json', { headers: { cookie } })).status, 200);
  const logout = await request('/logout', { method: 'POST', headers: { cookie, origin } });
  assert.equal(logout.status, 303); assert.match(logout.headers.get('set-cookie'), /Max-Age=0/);
  assert.equal((await request('/recorded-run.json', { headers: { cookie } })).status, 401);
}));
test('sessions expire absolutely and are not recreated from a supplied cookie', async () => {
  let time = Date.now();
  await fixture(async ({ request, login }) => {
    const cookie = sessionCookie(await login());
    time += sessionLifetimeMs + 1;
    for (let i = 0; i < 2; i++) assert.equal((await request('/recorded-run.json', { headers: { cookie } })).status, 401);
  }, { now: () => time });
});
test('login work is capped at two concurrent scrypt operations and a bounded attempt budget', async () => {
  let time = 1000;
  const access = accessControl(parseUsers(usersJson), () => time);
  const results = await Promise.all(Array.from({ length: 32 }, () => access.login('unknown-user', password)));
  assert.equal(results.filter(r => r.status === 401).length, 2);
  assert.equal(results.filter(r => r.status === 503).length, 18);
  assert.equal(results.filter(r => r.status === 429).length, 12);
  time += 3000;
  assert.equal((await access.login(records[0].username, password)).status, 200);
});
test('oversized and unsupported login bodies, alternate file paths and event writes are rejected', () => fixture(async ({ request, login, origin, url }) => {
  assert.equal((await request('/login', { method: 'POST', body: 'x'.repeat(2048),
    headers: { origin, 'content-type': 'application/x-www-form-urlencoded' } })).status, 413);
  assert.equal((await request('/login', { method: 'POST', body: '{}',
    headers: { origin, 'content-type': 'application/json' } })).status, 415);
  assert.equal((await request('/login', { method: 'POST' })).status, 403);
  const cookie = sessionCookie(await login());
  for (const route of ['/showcase-server.mjs', '/%2e%2e/package.json', '/.env', '/anything']) {
    assert.equal((await request(route, { headers: { cookie } })).status, 404);
  }
  assert.equal((await request('/api/collector/events', { method: 'POST', body: '{}' })).status, 405);
  const chunkedStatus = await new Promise((resolve, reject) => {
    const outgoing = http.request(url + '/login', { method: 'POST', headers: {
      origin, 'content-type': 'application/x-www-form-urlencoded', 'transfer-encoding': 'chunked'
    } }, response => { response.resume(); resolve(response.statusCode); });
    outgoing.on('error', reject); outgoing.write('x'.repeat(600)); outgoing.end('x'.repeat(600));
  });
  assert.equal(chunkedStatus, 413);
}));
test('a static entry build contains no application assets or recording to bypass hosted login', async () => {
  const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'telemetry-entry-'));
  try {
    await fs.writeFile(path.join(directory, 'recorded-run.json'), '{"old":"data"}');
    await buildShowcase({ destination: directory, redirectUrl: 'https://protected-fixture.onrender.com/' });
    assert.deepEqual(await fs.readdir(directory), ['index.html']);
    assert.match(await fs.readFile(path.join(directory, 'index.html'), 'utf8'), /https:\/\/protected-fixture.onrender.com/);
    for (const redirectUrl of ['http://protected-fixture.onrender.com/', 'https://unrelated.invalid/', 'https://protected-fixture.onrender.com/?secret=value']) {
      await assert.rejects(buildShowcase({ destination: directory, redirectUrl }));
    }
  } finally { await fs.rm(directory, { recursive: true, force: true }); }
});
