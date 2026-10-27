import { createHash } from 'node:crypto';

const style = ':root{color-scheme:dark;font-family:system-ui,sans-serif;background:#101419;color:#ecf0f4}*{box-sizing:border-box}body{margin:0;min-height:100svh;display:grid;place-items:center;padding:24px}.card{width:min(100%,440px);padding:32px;background:#191f26;border:1px solid #303842;border-radius:12px}.brand{color:#b7f66b;font-weight:700;letter-spacing:.04em;margin-bottom:44px}.eyebrow{font-size:11px;letter-spacing:.14em;color:#b7f66b}h1{font-size:30px;line-height:1.15;margin:12px 0}p{font-size:14px;line-height:1.6;color:#a7b4c5}label{display:block;font-size:13px;margin-top:20px}input{display:block;width:100%;margin-top:8px;padding:12px;border:1px solid #46515f;border-radius:6px;background:#101419;color:#ecf0f4;font:inherit}input:focus-visible,button:focus-visible,a:focus-visible{outline:2px solid #b7f66b;outline-offset:3px}button{width:100%;margin-top:24px;padding:12px;border:0;border-radius:6px;background:#b7f66b;color:#182310;font:inherit;font-weight:700;cursor:pointer}.note{font-size:12px;margin-top:24px}.error{padding:12px;background:#372524;border:1px solid #75453d;border-radius:6px;color:#ffd1c8}footer{font-size:11px;color:#8390a1;margin-top:32px;border-top:1px solid #303842;padding-top:20px}';
export const loginStyleHash = createHash('sha256').update(style).digest('base64');

export function loginPage(invalid = false) {
  return '<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
    '<meta name="color-scheme" content="dark"><meta name="robots" content="noindex,nofollow"><title>Sign in · Telemetry</title><style>' + style +
    '</style></head><body><main class="card"><div class="brand">telemetry / DATA PLATFORM</div><div class="eyebrow">PRIVATE REVIEW</div>' +
    '<h1>Sign in to the run explorer.</h1><p>Use the review credentials shared with you.</p>' +
    (invalid ? '<p class="error" role="alert">Unable to sign in with those credentials.</p>' : '') +
    '<form action="/login" method="post"><label for="username">Username</label><input id="username" name="username" autocomplete="username" required minlength="3" maxlength="32">' +
    '<label for="password">Password</label><input id="password" name="password" type="password" autocomplete="current-password" required maxlength="128">' +
    '<button type="submit">Sign in</button></form><p class="note">A recorded verification run. Live pipeline operations are available in the local console.</p>' +
    '<footer>REAL-TIME TELEMETRY DATA PLATFORM</footer></main></body></html>';
}
