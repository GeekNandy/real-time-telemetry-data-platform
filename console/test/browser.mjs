import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { assertNoStoredCredentials, connect, fixture, launch, reports, saveScreenshot } from './support.mjs';

const browser = await launch();
const results = [];
async function run(name, body) {
  const backend = await fixture();
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } });
  const page = await context.newPage();
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  try {
    await page.goto(backend.url);
    await body(page, backend.state);
    assert.deepEqual(errors, []);
    await assertNoStoredCredentials(page);
    results.push({ name, passed: true }); console.log(`PASS ${name}`);
  } finally { await context.close(); await backend.close(); }
}

try {
  await run('unknown data is not fabricated as healthy zeroes', async page => {
    await page.getByText('Both applications ready', { exact: true }).waitFor();
    assert.equal(await page.locator('#buffer-records').textContent(), '—');
    assert.equal(await page.locator('#accepted-rate').textContent(), '—');
  });
  await run('live snapshots render and rates require two process samples', async page => {
    await connect(page);
    await page.waitForFunction(() => document.getElementById('buffer-records').textContent === '12');
    await page.locator('#refresh').click();
    await page.waitForFunction(() => document.getElementById('accepted-rate').textContent !== '—');
    await page.locator('#refresh').click();
    await page.locator('#chart svg').waitFor();
    assert.equal(await page.locator('#buffer-bytes').textContent(), '24.0 KiB');
    await saveScreenshot(page, 'fixture-overview.png');
  });
  await run('a matching receipt and query result complete separate milestones', async (page, state) => {
    await connect(page); await page.getByRole('link', { name: /Event playground/ }).click();
    await page.getByRole('button', { name: 'Submit event' }).click();
    await page.waitForFunction(() => document.getElementById('journey-visible').classList.contains('complete'));
    assert.ok(await page.locator('#journey-accepted').evaluate(element => element.classList.contains('complete')));
    assert.match(state.traceparent, /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/);
    assert.ok(state.queryCount >= 2);
    await saveScreenshot(page, 'fixture-event-journey.png');
  });
  await run('a mismatched acceptance receipt never counts as accepted', async (page, state) => {
    state.mode = 'wrong-receipt'; await connect(page);
    await page.getByRole('link', { name: /Event playground/ }).click();
    await page.getByRole('button', { name: 'Submit event' }).click();
    await page.getByText(/acceptance receipt did not match/).waitFor();
    assert.equal(await page.locator('#journey-accepted').evaluate(element => element.classList.contains('complete')), false);
    assert.equal(state.queryCount, 0);
  });
  await run('wrong query identities are rejected and payload HTML stays inert', async (page, state) => {
    await connect(page); await page.getByRole('link', { name: /Event inspector/ }).click();
    await page.getByLabel('Event ID', { exact: true }).fill('00000000-0000-4000-8000-000000000001');
    state.mode = 'wrong-query'; await page.getByRole('button', { name: 'Look up event' }).click();
    await page.getByText(/response identity did not match/).waitFor();
    assert.ok(await page.locator('#lookup-result').isHidden());
    state.mode = 'pending-query'; await page.getByRole('button', { name: 'Look up event' }).click();
    await page.getByText(/visibility was not confirmed/).waitFor();
    assert.ok(await page.locator('#lookup-result').isHidden());
    state.mode = 'xss'; await page.getByRole('button', { name: 'Look up event' }).click();
    await page.locator('#lookup-result').waitFor();
    assert.match(await page.locator('#lookup-result').textContent(), /<img/);
    assert.equal(await page.evaluate(() => Boolean(window.injected)), false);
    assert.equal(await page.locator('#lookup-result img').count(), 0);
  });
  await run('observer scope failures do not masquerade as zero metrics', async (page, state) => {
    state.metricsForbidden = true; await connect(page);
    await page.waitForFunction(() => document.getElementById('metrics-status').textContent.includes('required scope'));
    assert.equal(await page.locator('#buffer-records').textContent(), '—');
  });
  await run('clearing a session prevents a late response from exposing old tenant data', async (page, state) => {
    state.mode = 'xss'; await connect(page);
    await page.getByRole('link', { name: /Event inspector/ }).click();
    await page.route('**/api/worker/events/**', async route => {
      await new Promise(resolve => setTimeout(resolve, 250));
      await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify({ eventId: '00000000-0000-4000-8000-000000000001', attributes: { private: 'old-tenant' } }) }).catch(() => {});
    });
    await page.getByLabel('Event ID', { exact: true }).fill('00000000-0000-4000-8000-000000000001');
    await page.getByRole('button', { name: 'Look up event' }).click();
    await page.getByRole('button', { name: 'Clear session' }).click();
    await page.waitForTimeout(400);
    assert.equal(await page.locator('#lookup-result').textContent(), '');
    assert.equal(await page.locator('#session-status').textContent(), 'No credentials');
    assert.equal(await page.getByText('old-tenant').count(), 0);
  });
  await run('mobile navigation fits and reload clears credentials', async page => {
    await page.setViewportSize({ width: 390, height: 844 }); await connect(page);
    await page.waitForFunction(() => document.getElementById('buffer-records').textContent === '12');
    assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
    await saveScreenshot(page, 'fixture-mobile.png');
    await page.getByRole('link', { name: /Event playground/ }).click();
    await page.getByRole('heading', { name: /Event playground/ }).waitFor({ state: 'visible' });
    await page.reload();
    assert.equal(await page.locator('#session-status').textContent(), 'No credentials');
  });
  await fs.mkdir(reports, { recursive: true });
  await fs.writeFile(path.join(reports, 'browser-contracts.json'), JSON.stringify({ syntheticFixtures: true, cases: results }, null, 2));
} finally { await browser.close(); }
