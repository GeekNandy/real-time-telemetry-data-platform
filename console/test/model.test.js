import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { ApiError, acceptedRate, format, matchingEvent, request, validateEvent } from '../public/model.js';

const id = '00000000-0000-4000-8000-000000000001';
const event = () => ({ eventId: id, eventType: 'operator.sample', schemaVersion: 1,
  occurredAt: '2026-01-01T00:00:00Z', attributes: { kind: 'synthetic' } });

test('rates need comparable process samples and survive counter resets honestly', () => {
  const before = { instanceId: 'a', acceptedTotal: 10, sampledAt: 1000 };
  assert.equal(acceptedRate(before, { ...before, acceptedTotal: 20, sampledAt: 3000 }), 5);
  for (const next of [null, { ...before, instanceId: 'b' }, { ...before, acceptedTotal: 1 },
    { ...before, acceptedTotal: null }, { ...before, sampledAt: 0 }]) assert.equal(acceptedRate(before, next), null);
  assert.equal(acceptedRate(null, before), null);
});

test('missing operational values remain unknown, while a measured zero is visible', () => {
  for (const value of [null, undefined, NaN, Infinity, -1, '0']) assert.equal(format(value), '—');
  assert.equal(format(0), '0');
  assert.equal(format(1024, 'bytes'), '1.0 KiB');
});

test('event input cannot select a tenant or send an unbounded body', () => {
  assert.equal(validateEvent(event()).eventId, id);
  assert.throws(() => validateEvent({ ...event(), tenantId: 'other' }), /Tenant identity/);
  assert.throws(() => validateEvent({ ...event(), attributes: { secret: 'x'.repeat(17000) } }), /16 KiB/);
  assert.throws(() => validateEvent({ ...event(), attributes: { number: 1 } }), /string values/);
});

test('HTTP success alone does not establish event identity', () => {
  assert.equal(matchingEvent({ eventId: id }, id), true);
  assert.equal(matchingEvent({ eventId: 'other' }, id), false);
  assert.equal(matchingEvent(null, id), false);
  assert.equal(matchingEvent({}, id), false);
  assert.equal(matchingEvent({ eventId: id.toUpperCase() }, id), true);
  assert.equal(matchingEvent({ eventId: 'malformed' }, 'malformed'), false);
});

async function withServer(handler, run) {
  const server = http.createServer(handler);
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  try { await run(`http://127.0.0.1:${server.address().port}`); }
  finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}

test('an oversized streamed response cannot enter console state', async () => {
  await withServer((_, response) => { response.writeHead(200); response.end('x'.repeat(65537)); }, async url => {
    await assert.rejects(request(url, ''), error => error instanceof ApiError && /exceeded/.test(error.message));
  });
});

test('the absolute deadline covers a body that stalls after HTTP headers', async () => {
  await withServer((_, response) => { response.writeHead(200); response.write('{'); }, async url => {
    const started = performance.now();
    await assert.rejects(request(url, '', { timeout: 80 }), error => error instanceof ApiError && error.status === 0);
    assert.ok(performance.now() - started < 1500);
  });
});

test('authorization errors keep their meaning without parsing or exposing error bodies', async () => {
  await withServer((_, response) => { response.writeHead(403); response.end('private-server-details'); }, async url => {
    await assert.rejects(request(url, 'synthetic-token'), error => error.status === 403 && !error.message.includes('private-server-details'));
  });
});

test('a submission forwards the same event identity and explicit W3C context', async () => {
  const traceparent = '00-12345678901234567890123456789012-1234567890123456-01';
  await withServer(async (incoming, response) => {
    assert.equal(incoming.method, 'POST');
    assert.equal(incoming.headers.authorization, 'Bearer synthetic-token');
    assert.equal(incoming.headers.traceparent, traceparent);
    let body = ''; for await (const chunk of incoming) body += chunk;
    response.writeHead(202, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ eventId: JSON.parse(body).eventId }));
  }, async url => {
    const result = await request(url, 'synthetic-token', { body: event(), traceparent });
    assert.equal(result.status, 202); assert.equal(result.data.eventId, id);
  });
});
