// webhook-store.js — Persistent webhook storage to Google Drive
// Uses lightweight google-auth-library + raw fetch (no googleapis SDK)

const { GoogleAuth } = require('google-auth-library');
const cron = require('node-cron');

const REDIS_KEY = 'webhook:archive:buffer';
const DRIVE_ID = process.env.GOOGLE_DRIVE_ID;
const DRIVE_API = 'https://www.googleapis.com/drive/v3';
const UPLOAD_API = 'https://www.googleapis.com/upload/drive/v3';

let authClient = null;
let redis = null;
let folderCache = {}; // path -> folderId
let totalBuffered = 0;
let totalFlushed = 0;

function init(redisClient) {
  redis = redisClient;

  const saJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!saJson || !DRIVE_ID) {
    console.log('⚠️ Webhook store: Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_DRIVE_ID — disabled');
    return false;
  }

  try {
    const sa = JSON.parse(saJson);
    const auth = new GoogleAuth({
      credentials: sa,
      scopes: ['https://www.googleapis.com/auth/drive'],
    });
    authClient = auth;
    console.log('📦 Webhook store: Google Drive connected');

    // Flush every hour at :05
    cron.schedule('5 * * * *', () => flushToDrive());
    console.log('📦 Webhook store: Hourly flush scheduled');

    return true;
  } catch (e) {
    console.error('❌ Webhook store init failed:', e.message);
    return false;
  }
}

async function getHeaders() {
  const client = await authClient.getClient();
  const token = await client.getAccessToken();
  return {
    'Authorization': `Bearer ${token.token || token}`,
    'Content-Type': 'application/json',
  };
}

// Buffer a webhook event
async function buffer(event, accountId, payload) {
  if (!redis) return;
  const entry = JSON.stringify({
    event,
    account_id: accountId,
    payload,
    ts: new Date().toISOString(),
  });
  await redis.rpush(REDIS_KEY, entry);
  totalBuffered++;
}

// Find or create a folder
async function getOrCreateFolder(name, parentId) {
  const cacheKey = `${parentId}/${name}`;
  if (folderCache[cacheKey]) return folderCache[cacheKey];

  const headers = await getHeaders();
  const q = encodeURIComponent(`name='${name}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`);
  const res = await fetch(`${DRIVE_API}/files?q=${q}&driveId=${DRIVE_ID}&corpora=drive&includeItemsFromAllDrives=true&supportsAllDrives=true&fields=files(id,name)`, { headers });
  const data = await res.json();

  if (data.files && data.files.length > 0) {
    folderCache[cacheKey] = data.files[0].id;
    return data.files[0].id;
  }

  // Create folder
  const createRes = await fetch(`${DRIVE_API}/files?supportsAllDrives=true`, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      name,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId],
    }),
  });
  const folder = await createRes.json();
  folderCache[cacheKey] = folder.id;
  return folder.id;
}

// Append to existing file or create new
async function appendOrCreateFile(fileName, folderId, content) {
  const headers = await getHeaders();
  const q = encodeURIComponent(`name='${fileName}' and '${folderId}' in parents and trashed=false`);
  const res = await fetch(`${DRIVE_API}/files?q=${q}&driveId=${DRIVE_ID}&corpora=drive&includeItemsFromAllDrives=true&supportsAllDrives=true&fields=files(id,name)`, { headers });
  const data = await res.json();

  if (data.files && data.files.length > 0) {
    // Download existing, append, re-upload
    const fileId = data.files[0].id;
    const dlRes = await fetch(`${DRIVE_API}/files/${fileId}?alt=media&supportsAllDrives=true`, { headers });
    const existing = await dlRes.text();
    const merged = existing + content;

    await fetch(`${UPLOAD_API}/files/${fileId}?uploadType=media&supportsAllDrives=true`, {
      method: 'PATCH',
      headers: { ...headers, 'Content-Type': 'application/x-ndjson' },
      body: merged,
    });
    return fileId;
  } else {
    // Create new file (multipart upload)
    const boundary = 'webhook_boundary_123';
    const metadata = JSON.stringify({ name: fileName, parents: [folderId] });
    const multipart = [
      `--${boundary}`,
      'Content-Type: application/json; charset=UTF-8',
      '',
      metadata,
      `--${boundary}`,
      'Content-Type: application/x-ndjson',
      '',
      content,
      `--${boundary}--`,
    ].join('\r\n');

    const createRes = await fetch(`${UPLOAD_API}/files?uploadType=multipart&supportsAllDrives=true`, {
      method: 'POST',
      headers: {
        ...headers,
        'Content-Type': `multipart/related; boundary=${boundary}`,
      },
      body: multipart,
    });
    const file = await createRes.json();
    return file.id;
  }
}

// Flush buffered webhooks to Google Drive
async function flushToDrive() {
  if (!authClient || !redis) return;

  try {
    const len = await redis.llen(REDIS_KEY);
    if (len === 0) {
      console.log('📦 Webhook store: Nothing to flush');
      return { flushed: 0 };
    }

    // Pop all buffered events
    const entries = [];
    for (let i = 0; i < len; i++) {
      const entry = await redis.lpop(REDIS_KEY);
      if (entry) entries.push(typeof entry === 'string' ? entry : JSON.stringify(entry));
    }

    if (entries.length === 0) return { flushed: 0 };

    // Group by date
    const byDate = {};
    for (const entry of entries) {
      try {
        const parsed = typeof entry === 'string' ? JSON.parse(entry) : entry;
        const date = parsed.ts?.slice(0, 10) || new Date().toISOString().slice(0, 10);
        if (!byDate[date]) byDate[date] = [];
        byDate[date].push(typeof entry === 'string' ? entry : JSON.stringify(entry));
      } catch {
        const today = new Date().toISOString().slice(0, 10);
        if (!byDate[today]) byDate[today] = [];
        byDate[today].push(typeof entry === 'string' ? entry : JSON.stringify(entry));
      }
    }

    // Upload
    const webhooksFolderId = await getOrCreateFolder('webhooks', DRIVE_ID);

    for (const [date, dateEntries] of Object.entries(byDate)) {
      const month = date.slice(0, 7);
      const monthFolderId = await getOrCreateFolder(month, webhooksFolderId);
      const fileName = `${date}.jsonl`;
      const content = dateEntries.join('\n') + '\n';
      await appendOrCreateFile(fileName, monthFolderId, content);
      console.log(`📦 Webhook store: Flushed ${dateEntries.length} events to ${month}/${fileName}`);
    }

    totalFlushed += entries.length;
    console.log(`📦 Webhook store: Total flushed ${entries.length} events`);
    return { flushed: entries.length };
  } catch (e) {
    console.error('❌ Webhook store flush failed:', e.message);
    return { error: e.message };
  }
}

function getStats() {
  return {
    driveConnected: !!authClient,
    driveId: DRIVE_ID,
    totalBuffered,
    totalFlushed,
  };
}

module.exports = { init, buffer, flushToDrive, getStats };
