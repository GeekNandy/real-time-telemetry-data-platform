import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { connect, launch, reports, saveScreenshot, assertNoStoredCredentials } from './support.mjs';

if (!process.env.TELEMETRY_CONSOLE_TOKEN_FILE) throw new Error('A synthetic demo token file is required');
const token = (await fs.readFile(process.env.TELEMETRY_CONSOLE_TOKEN_FILE, 'utf8')).trim();
const url = process.env.TELEMETRY_CONSOLE_URL || 'http://127.0.0.1:8088';
const browser = await launch();
let page;
const outcomes = [];
try {
  page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
  page.on('response', response => {
    const pathname = new URL(response.url()).pathname;
    if (pathname.startsWith('/api/')) outcomes.push({
      route: pathname.replace(/[0-9a-f]{8}-[0-9a-f-]{27}/ig, '{eventId}'), status: response.status()
    });
  });
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  const response = await page.goto(url);
  assert.match(response.headers()['content-security-policy'], /connect-src 'self'/);
  const unauthenticated = await page.request.get(url + '/api/collector/metrics');
  assert.equal(unauthenticated.status(), 401);
  const arbitraryProxy = await page.request.get(url + '/api/collector/actuator/env');
  assert.equal(arbitraryProxy.status(), 404);
  await page.getByText('Both applications ready', { exact: true }).waitFor();
  await connect(page, token);
  await page.waitForFunction(() => document.getElementById('buffer-records').textContent !== '—');
  await page.getByRole('link', { name: /Event playground/ }).click();
  const event = JSON.parse(await page.locator('#event-json').inputValue());
  await page.getByRole('button', { name: 'Submit event' }).click();
  await page.waitForFunction(() => {
    const text = document.getElementById('journey-submitted').textContent;
    return text.includes('HTTP ') || text.includes('outcome unconfirmed');
  }, null, { timeout: 10000 });
  assert.ok(await page.locator('#journey-accepted').evaluate(element => element.classList.contains('complete')),
    await page.locator('#event-notice').textContent());
  await page.waitForFunction(() => document.getElementById('journey-visible').classList.contains('complete'), null, { timeout: 40000 });
  assert.ok(await page.locator('#journey-accepted').evaluate(element => element.classList.contains('complete')));
  await saveScreenshot(page, 'live-event-journey.png');
  await page.getByRole('button', { name: 'Inspect event', exact: true }).click();
  await page.locator('#lookup-result').waitFor();
  const stored = JSON.parse(await page.locator('#lookup-result').textContent());
  assert.equal(stored.eventId, event.eventId);
  assert.equal(stored.attributes.platform, 'operator-console');
  await page.getByRole('link', { name: /Overview/ }).click();
  await page.waitForFunction(() => scrollY === 0);
  await page.waitForFunction(() => document.getElementById('buffer-records').textContent !== '—');
  await page.locator('#refresh').click();
  await page.waitForFunction(() => document.getElementById('accepted-rate').textContent !== '—');
  await saveScreenshot(page, 'live-overview.png');
  await page.setViewportSize({ width: 390, height: 844 });
  await page.waitForFunction(() => {
    const host = document.getElementById('chart'), svg = host.querySelector('svg');
    return svg && svg.viewBox.baseVal.width === host.clientWidth;
  });
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
  await saveScreenshot(page, 'live-mobile.png');
  await assertNoStoredCredentials(page); assert.deepEqual(errors, []);
  await page.getByRole('button', { name: 'Clear session' }).click();
  assert.equal(await page.locator('#buffer-records').textContent(), '—');
  await fs.mkdir(reports, { recursive: true });
  await fs.writeFile(path.join(reports, 'live-console.json'), JSON.stringify({
    eventId: event.eventId, acceptedReceipt: true, matchingQueryResult: true,
    observerAuthorization: true, restrictedProxy: true, mobileFits: true, credentialsPersisted: false
  }, null, 2));
  console.log('PASS actual browser -> gateway -> collector -> Kafka -> PostgreSQL -> worker query');
} catch (error) {
  await fs.mkdir(reports, { recursive: true });
  const state = page ? await page.evaluate(() => ({
    notice: document.getElementById('event-notice')?.textContent,
    stages: ['journey-submitted', 'journey-accepted', 'journey-visible'].map(id => ({
      id, text: document.getElementById(id)?.textContent, state: document.getElementById(id)?.className
    }))
  })).catch(() => ({})) : {};
  await fs.writeFile(path.join(reports, 'live-failure.json'), JSON.stringify({ state, outcomes: outcomes.slice(-40) }, null, 2));
  if (page && !await page.locator('#session-dialog').evaluate(element => element.open).catch(() => true))
    await saveScreenshot(page, 'live-failure.png');
  throw error;
} finally { await browser.close(); }
