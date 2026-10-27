import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import { credentialRecord } from '../showcase-access.js';
import { protectedServer } from '../showcase-server.mjs';
import { launch, reports } from './support.mjs';

// These credentials and HTTP-only cookies belong solely to the local browser fixture.
const username = 'browser-fixture';
const password = 'synthetic-browser-password-never-deployed';
const usersJson = JSON.stringify([await credentialRecord(username, password)]);
const server = await protectedServer({ usersJson, secureCookie: false });
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const url = 'http://127.0.0.1:' + server.address().port;
const browser = await launch(), cases = [];
await fs.mkdir(reports, { recursive: true });
try {
  for (const [name, viewport] of [['desktop', { width: 1440, height: 1000 }], ['mobile', { width: 390, height: 844 }]]) {
    const context = await browser.newContext({ viewport });
    const page = await context.newPage(), errors = [];
    page.on('pageerror', error => errors.push(error.message));
    try {
      const anonymous = await context.request.get(url + '/recorded-run.json');
      assert.equal(anonymous.status(), 401);
      await page.goto(url);
      await page.getByRole('heading', { name: 'Sign in to the run explorer.' }).waitFor();
      assert.equal(await page.getByLabel('Password', { exact: true }).getAttribute('type'), 'password');
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      await page.screenshot({ path: path.join(reports, 'protected-login-' + name + '.png'), fullPage: true });
      await page.getByLabel('Username', { exact: true }).fill(username);
      await page.getByLabel('Password', { exact: true }).fill('incorrect-synthetic-password');
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await page.getByRole('alert').waitFor();
      assert.equal((await context.request.get(url + '/app.js')).status(), 401);
      await page.getByLabel('Username', { exact: true }).fill(username);
      await page.getByLabel('Password', { exact: true }).fill(password);
      await page.getByRole('button', { name: 'Sign in', exact: true }).click();
      await page.getByText('All 20 accepted IDs reconciled').waitFor();
      assert.ok(await page.getByRole('button', { name: 'Sign out', exact: true }).isVisible());
      assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth));
      assert.equal(await page.evaluate(() => localStorage.length + sessionStorage.length), 0);
      assert.equal(await page.evaluate(() => document.cookie), '');
      const cookies = await context.cookies();
      assert.equal(cookies.length, 1); assert.equal(cookies[0].httpOnly, true); assert.equal(cookies[0].sameSite, 'Strict');
      const recording = await context.request.get(url + '/recorded-run.json');
      assert.equal(recording.status(), 200);
      assert.equal((await recording.json()).environments.kind.requests.length, 20);
      await page.screenshot({ path: path.join(reports, 'protected-overview-' + name + '.png'), fullPage: true });
      await page.getByRole('button', { name: 'Sign out', exact: true }).click();
      await page.getByRole('heading', { name: 'Sign in to the run explorer.' }).waitFor();
      assert.equal((await context.request.get(url + '/recorded-run.json')).status(), 401);
      assert.equal((await context.cookies()).length, 0);
      assert.deepEqual(errors, []);
      cases.push({ name: name + ' anonymous denial, login, private session and logout', passed: true });
      console.log('PASS ' + cases.at(-1).name);
    } finally { await context.close(); }
  }
  await fs.writeFile(path.join(reports, 'protected-access-browser.json'), JSON.stringify({ cases }, null, 2));
} finally {
  await browser.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
}
