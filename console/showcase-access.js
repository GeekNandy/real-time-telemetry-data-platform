import { randomBytes, scrypt, timingSafeEqual, createHash } from 'node:crypto';
import { promisify } from 'node:util';

const derive = promisify(scrypt);
const parameters = { N: 65536, r: 8, p: 2, maxmem: 128 * 1024 * 1024 };
const usernamePattern = /^[a-z][a-z0-9_-]{2,31}$/;
export const sessionLifetimeMs = 8 * 60 * 60 * 1000;
export const cookieName = '__Host-telemetry-review';

export async function credentialRecord(username, password) {
  if (typeof username !== 'string' || !usernamePattern.test(username) || typeof password !== 'string' ||
      Buffer.byteLength(password) < 16 || Buffer.byteLength(password) > 128) {
    throw new Error('Invalid review credential input.');
  }
  const salt = randomBytes(16);
  const hash = await derive(password, salt, 32, parameters);
  return { username, salt: salt.toString('hex'), hash: hash.toString('hex') };
}

export function parseUsers(value) {
  let rows;
  try { rows = JSON.parse(value); } catch { throw new Error('Invalid review access configuration.'); }
  if (!Array.isArray(rows) || rows.length < 1 || rows.length > 8) throw new Error('Invalid review access configuration.');
  const users = new Map();
  for (const row of rows) {
    if (!row || typeof row !== 'object' || Object.keys(row).sort().join(',') !== 'hash,salt,username' ||
        typeof row.username !== 'string' || !usernamePattern.test(row.username) || typeof row.salt !== 'string' || !/^[a-f0-9]{32}$/.test(row.salt) ||
        typeof row.hash !== 'string' || !/^[a-f0-9]{64}$/.test(row.hash) || users.has(row.username)) {
      throw new Error('Invalid review access configuration.');
    }
    users.set(row.username, { salt: Buffer.from(row.salt, 'hex'), hash: Buffer.from(row.hash, 'hex') });
  }
  return users;
}

export function accessControl(users, now = Date.now, name = cookieName) {
  const dummy = { salt: randomBytes(16), hash: randomBytes(32) };
  const sessions = new Map();
  let tokens = 20, refilledAt = now(), active = 0;
  const digest = value => createHash('sha256').update(value).digest('hex');
  function sessionKey(header) {
    const values = (header || '').split(';').map(part => part.trim()).filter(part => part.startsWith(name + '='));
    if (values.length !== 1) return null;
    const value = values[0].slice(name.length + 1);
    return /^[A-Za-z0-9_-]{43}$/.test(value) ? digest(value) : null;
  }
  function prune() {
    for (const [key, session] of sessions) if (session.expires <= now()) sessions.delete(key);
  }
  return {
    session(header) {
      prune();
      const key = sessionKey(header);
      return key && sessions.get(key);
    },
    logout(header) { sessions.delete(sessionKey(header)); },
    async login(username, password, previousCookie) {
      tokens = Math.min(20, tokens + Math.max(0, now() - refilledAt) / 3000);
      refilledAt = now();
      if (tokens < 1) return { status: 429 };
      tokens--;
      if (active >= 2) return { status: 503 };
      active++;
      try {
        const valid = typeof username === 'string' && usernamePattern.test(username) &&
          typeof password === 'string' && Buffer.byteLength(password) >= 16 && Buffer.byteLength(password) <= 128;
        const record = (valid && users.get(username)) || dummy;
        const hash = await derive(valid ? password : 'invalid-review-credential', record.salt, 32, parameters);
        const matches = timingSafeEqual(hash, record.hash);
        if (!valid || record === dummy || !matches) return { status: 401 };
        prune();
        this.logout(previousCookie);
        const owned = [...sessions].filter(([, session]) => session.username === username);
        if (owned.length >= 8) sessions.delete(owned[0][0]);
        const token = randomBytes(32).toString('base64url');
        sessions.set(digest(token), { username, expires: now() + sessionLifetimeMs });
        return { status: 200, token };
      } finally { active--; }
    }
  };
}
