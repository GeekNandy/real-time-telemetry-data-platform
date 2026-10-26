export const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isNumber = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;

export function syntheticEvent({ eventId = crypto.randomUUID(), occurredAt = new Date().toISOString() } = {}) {
  return { eventId, eventType: 'performance.sample', schemaVersion: 1, occurredAt,
    attributes: { platform: 'operator-console', metric: 'frame_ms', value: '16' } };
}

export class ApiError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}

const messages = {
  400: 'The API rejected this event. Check the schema and values.',
  401: 'The API could not verify this token. Load a valid, unexpired token.',
  403: 'This token does not have the required scope.',
  404: 'This event is not queryable in the current tenant.',
  409: 'This event ID conflicts with an existing payload.',
  413: 'The event exceeds the API payload limit.',
  429: 'Admission is busy. Back off before retrying the same event.',
  503: 'The service is unavailable or its durable buffer is full.'
};

// One absolute budget covers headers and the bounded response body.
export async function request(path, token, { body, signal, timeout = 6000, traceparent } = {}) {
  const deadline = AbortSignal.timeout(timeout);
  const combined = signal ? AbortSignal.any([signal, deadline]) : deadline;
  try {
    const headers = { Accept: 'application/json' };
    if (token) headers.Authorization = `Bearer ${token}`;
    if (traceparent && /^00-[0-9a-f]{32}-[0-9a-f]{16}-01$/.test(traceparent)) headers.traceparent = traceparent;
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    const response = await fetch(path, {
      method: body === undefined ? 'GET' : 'POST', headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      cache: 'no-store', credentials: 'omit', redirect: 'error', signal: combined
    });
    if (!response.ok) {
      await response.body?.cancel();
      throw new ApiError(response.status, messages[response.status] || `Service returned HTTP ${response.status}.`);
    }
    const reader = response.body?.getReader();
    const parts = []; let size = 0;
    if (!reader) throw new ApiError(0, 'The service returned an empty response.');
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        size += value.byteLength;
        if (size > 65536) {
          await reader.cancel();
          throw new ApiError(0, 'The service response exceeded the console limit.');
        }
        parts.push(value);
      }
    } finally { reader.releaseLock(); }
    const bytes = new Uint8Array(size); let offset = 0;
    for (const part of parts) { bytes.set(part, offset); offset += part.byteLength; }
    let data;
    try { data = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); }
    catch { throw new ApiError(0, 'The service returned invalid JSON.'); }
    return { status: response.status, data };
  } catch (error) {
    if (signal?.aborted) throw error;
    if (error instanceof ApiError) throw error;
    throw new ApiError(0, 'The request could not be confirmed. Its delivery outcome may be ambiguous.');
  }
}

export function matchingEvent(value, id) {
  return value !== null && typeof value === 'object' && typeof value.eventId === 'string' &&
    UUID.test(value.eventId) && UUID.test(id) && value.eventId.toLowerCase() === id.toLowerCase();
}

export function acceptedRate(previous, current) {
  if (!previous || !current || typeof current.instanceId !== 'string' || !current.instanceId || previous.instanceId !== current.instanceId ||
      !isNumber(previous.acceptedTotal) || !isNumber(current.acceptedTotal) ||
      current.acceptedTotal < previous.acceptedTotal || current.sampledAt <= previous.sampledAt) return null;
  return (current.acceptedTotal - previous.acceptedTotal) / ((current.sampledAt - previous.sampledAt) / 1000);
}

export function format(value, unit = 'number') {
  if (!isNumber(value)) return '—';
  if (unit === 'bytes') {
    if (value >= 1048576) return `${(value / 1048576).toFixed(1)} MiB`;
    if (value >= 1024) return `${(value / 1024).toFixed(1)} KiB`;
    return `${Math.round(value)} B`;
  }
  if (unit === 'seconds') return `${value.toFixed(1)} s`;
  if (unit === 'rate') return value.toFixed(1);
  return new Intl.NumberFormat('en', { maximumFractionDigits: 0 }).format(value);
}

export function validateEvent(value) {
  // UX validation of core Event v1; the backend remains the authority.
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('An event must be a JSON object.');
  if (Object.keys(value).some(key => !['eventId', 'eventType', 'schemaVersion', 'occurredAt', 'attributes'].includes(key)))
    throw new Error('Use only the event schema fields. Tenant identity comes from your token.');
  if (!UUID.test(value.eventId || '')) throw new Error('Use a valid UUID for eventId.');
  if (!['session.started', 'session.ended', 'performance.sample'].includes(value.eventType))
    throw new Error('Use session.started, session.ended or performance.sample as the eventType.');
  if (value.schemaVersion !== 1) throw new Error('This event contract supports schemaVersion 1.');
  if (typeof value.occurredAt !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value.occurredAt) ||
      !Number.isFinite(Date.parse(value.occurredAt)) || new Date(value.occurredAt).getUTCFullYear() < 1 || new Date(value.occurredAt).getUTCFullYear() > 9999)
    throw new Error('Use an ISO timestamp within years 0001 through 9999 for occurredAt.');
  if (!value.attributes || typeof value.attributes !== 'object' || Array.isArray(value.attributes) ||
      Object.values(value.attributes).some(item => typeof item !== 'string')) throw new Error('Attributes must be an object containing string values.');
  const allowed = new Set(['platform', 'region', 'sdkVersion', 'statusCode', 'durationMs', 'metric', 'value']);
  if (Object.keys(value.attributes).length > 7 || Object.entries(value.attributes).some(([key, item]) =>
    !allowed.has(key) || item.length > 128 || item.includes('\u0000') || !item.isWellFormed()))
    throw new Error('Use only platform, region, sdkVersion, statusCode, durationMs, metric or value attributes, each at most 128 UTF-16 code units with valid Unicode.');
  if (new TextEncoder().encode(JSON.stringify(value)).length > 16384) throw new Error('The event exceeds the 16 KiB request limit.');
  return { ...value, eventId: value.eventId.toLowerCase() };
}
