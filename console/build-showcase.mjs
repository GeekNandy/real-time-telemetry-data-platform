import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { validateRun } from './showcase/run-model.js';

const root = path.dirname(fileURLToPath(import.meta.url));
export async function buildShowcase({ destination = path.join(root, 'dist'), redirectUrl = process.env.SHOWCASE_REDIRECT_URL } = {}) {
  if (redirectUrl !== undefined) {
    const url = new URL(redirectUrl);
    if (url.protocol !== 'https:' || !url.hostname.endsWith('.onrender.com') || url.username || url.password ||
        url.pathname !== '/' || url.search || url.hash || url.port) throw new Error('Invalid protected explorer origin.');
    const html = '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
      '<meta name="robots" content="noindex,nofollow"><meta http-equiv="Content-Security-Policy" content="default-src &apos;none&apos;; base-uri &apos;none&apos;; form-action &apos;none&apos;">' +
      '<meta http-equiv="refresh" content="0; url=' + url.origin + '/"><title>Telemetry · Sign in</title></head>' +
      '<body><h1>Telemetry Run Explorer</h1><p><a href="' + url.origin + '/">Continue to sign in</a></p></body></html>';
    await fs.rm(destination, { recursive: true, force: true });
    await fs.mkdir(destination, { recursive: true });
    await fs.writeFile(path.join(destination, 'index.html'), html);
    return;
  }
  const evidence = path.resolve(root, '../evidence/hosted-run-2026-10-27');
  const manifest = JSON.parse(await fs.readFile(path.join(evidence, 'manifest.json'), 'utf8'));
  for (const [name, record] of Object.entries(manifest.files)) {
    if (path.basename(name) !== name) throw new Error('Invalid evidence filename.');
    const bytes = await fs.readFile(path.join(evidence, name));
    if (crypto.createHash('sha256').update(bytes).digest('hex') !== record.sha256) throw new Error('Evidence digest mismatch: ' + name);
  }
  const json = async name => JSON.parse(await fs.readFile(path.join(evidence, name), 'utf8'));
  const jsonl = async name => (await fs.readFile(path.join(evidence, name), 'utf8')).trim().split('\n').map(line => JSON.parse(line));
  const data = validateRun({
    schemaVersion: 1, provenance: manifest,
    environments: {
      compose: { summary: await json('compose-summary.json'), requests: await jsonl('compose-requests.jsonl') },
      kind: { summary: await json('kind-summary.json'), requests: await jsonl('kind-requests.jsonl') }
    },
    browser: await json('browser.json'), observability: await json('observability.json')
  });
  const recording = JSON.stringify(data);
  if (Buffer.byteLength(recording) > 65536) throw new Error('Recording exceeds the browser read budget.');
  await fs.rm(destination, { recursive: true, force: true });
  await fs.mkdir(destination, { recursive: true });
  for (const name of ['index.html', 'app.js', 'run-model.js', 'showcase.css']) {
    await fs.copyFile(path.join(root, 'showcase', name), path.join(destination, name));
  }
  await fs.copyFile(path.join(root, 'public/style.css'), path.join(destination, 'base.css'));
  await fs.copyFile(path.join(root, 'public/mark.svg'), path.join(destination, 'mark.svg'));
  await fs.writeFile(path.join(destination, 'recorded-run.json'), recording + '\n');
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await buildShowcase();
  console.log(process.env.SHOWCASE_REDIRECT_URL === undefined ? 'Built recorded-run explorer with verified evidence digests.' : 'Built protected explorer entry page.');
}
