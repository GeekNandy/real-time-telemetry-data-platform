import http from 'node:http';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { accessControl, cookieName, parseUsers, sessionLifetimeMs } from './showcase-access.js';
import { loginPage, loginStyleHash } from './showcase-login.js';

const root = path.dirname(fileURLToPath(import.meta.url));
const contentTypes = {
  'index.html': 'text/html; charset=utf-8', 'app.js': 'text/javascript; charset=utf-8',
  'run-model.js': 'text/javascript; charset=utf-8', 'base.css': 'text/css; charset=utf-8',
  'showcase.css': 'text/css; charset=utf-8', 'mark.svg': 'image/svg+xml', 'recorded-run.json': 'application/json'
};

export async function protectedServer({ usersJson, now = Date.now, secureCookie = true } = {}) {
  // The CLI always uses Secure + __Host-. Plain HTTP is only for loopback browser fixtures.
  const name = secureCookie ? cookieName : 'telemetry-review-fixture';
  const access = accessControl(parseUsers(usersJson), now, name);
  const assets = new Map();
  for (const [name, type] of Object.entries(contentTypes)) {
    let body = await fs.readFile(path.join(root, 'dist', name));
    if (name === 'index.html') {
      const html = body.toString();
      if (!html.includes('<!-- access-controls -->')) throw new Error('Access controls missing from built explorer.');
      body = Buffer.from(html.replace('<!-- access-controls -->',
        '<form class="access-controls" action="/logout" method="post"><button class="button small" type="submit">Sign out</button></form>'));
    }
    assets.set('/' + name, { type, body });
  }
  const cookie = (value, maxAge) => name + '=' + value + '; Path=/; HttpOnly; SameSite=Strict; Max-Age=' + maxAge + (secureCookie ? '; Secure' : '');
  const csp = "default-src 'none'; script-src 'self'; style-src 'self' 'sha256-" + loginStyleHash +
    "'; img-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; object-src 'none'; frame-ancestors 'none'";
  const server = http.createServer({ maxHeaderSize: 8192 }, async (request, response) => {
    const headers = {
      'cache-control': 'private, no-store', pragma: 'no-cache', 'content-security-policy': csp,
      'x-content-type-options': 'nosniff', 'x-frame-options': 'DENY', 'referrer-policy': 'same-origin',
      'permissions-policy': 'camera=(), microphone=(), geolocation=()', 'x-robots-tag': 'noindex, nofollow',
      'strict-transport-security': 'max-age=31536000', vary: 'Cookie'
    };
    function send(status, type, body = '', extra = {}) {
      response.writeHead(status, { ...headers, 'content-type': type, 'content-length': Buffer.byteLength(body), ...extra });
      response.end(request.method === 'HEAD' ? undefined : body);
    }
    const plain = (status, body, extra) => send(status, 'text/plain; charset=utf-8', body, extra);
    const redirect = (location, extra) => plain(303, '', { location, ...extra });
    try {
      const url = new URL(request.url, 'http://localhost');
      const read = request.method === 'GET' || request.method === 'HEAD';
      if (url.pathname === '/healthz' && read) return plain(200, 'OK');
      if (url.pathname === '/robots.txt' && read) return plain(200, 'User-agent: *\nDisallow: /\n');
      if (request.method === 'POST' && ['/login', '/logout'].includes(url.pathname)) {
        let origin;
        try { origin = new URL(request.headers.origin); } catch { return plain(403, 'Request origin required.'); }
        if (origin.host !== request.headers.host || origin.protocol !== (secureCookie ? 'https:' : 'http:')) return plain(403, 'Request origin rejected.');
        if (Number(request.headers['content-length']) > 1024) return plain(413, 'Request is too large.');
        if (url.pathname === '/logout') {
          access.logout(request.headers.cookie);
          request.resume();
          return redirect('/login', { 'set-cookie': cookie('', 0) });
        }
        if (!/^application\/x-www-form-urlencoded(?:;|$)/i.test(request.headers['content-type'] || '')) return plain(415, 'Form data required.');
        const chunks = [];
        let bytes = 0;
        for await (const chunk of request) {
          bytes += chunk.length;
          if (bytes > 1024) return plain(413, 'Request is too large.', { connection: 'close' });
          chunks.push(chunk);
        }
        const form = new URLSearchParams(Buffer.concat(chunks).toString('utf8'));
        const unique = form.getAll('username').length === 1 && form.getAll('password').length === 1;
        const result = await access.login(unique ? form.get('username') : null, unique ? form.get('password') : null, request.headers.cookie);
        if (result.status === 429 || result.status === 503) return plain(result.status, 'Sign-in is busy. Please try again shortly.', { 'retry-after': '3' });
        if (result.status !== 200) return redirect('/login?invalid=1');
        return redirect('/', { 'set-cookie': cookie(result.token, sessionLifetimeMs / 1000) });
      }
      if (!read) return plain(405, 'Method not allowed.', { allow: 'GET, HEAD' });
      const session = access.session(request.headers.cookie);
      if (url.pathname === '/login') {
        if (session) return redirect('/');
        return send(200, 'text/html; charset=utf-8', loginPage(url.searchParams.has('invalid')));
      }
      if (!session) return url.pathname === '/' ? redirect('/login') : plain(401, 'Sign in to access the explorer.');
      const asset = assets.get(url.pathname === '/' ? '/index.html' : url.pathname);
      if (!asset) return plain(404, 'Not found.');
      return send(200, asset.type, asset.body);
    } catch {
      if (!response.headersSent && !response.destroyed) plain(400, 'Request could not be processed.');
      else response.destroy();
    }
  });
  server.requestTimeout = 10000;
  server.headersTimeout = 5000;
  server.keepAliveTimeout = 5000;
  server.maxConnections = 64;
  server.maxRequestsPerSocket = 100;
  return server;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const server = await protectedServer({ usersJson: process.env.SHOWCASE_USERS_JSON });
    const port = Number(process.env.PORT || 10000);
    if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid port.');
    server.listen(port, '0.0.0.0', () => console.log('Protected recorded-run explorer is listening.'));
    process.once('SIGTERM', () => {
      server.close(() => process.exit(0));
      setTimeout(() => { server.closeAllConnections(); process.exit(0); }, 5000).unref();
    });
  } catch {
    console.error('Protected explorer could not start: check access configuration and built assets.');
    process.exitCode = 1;
  }
}
