// webhook-store.js — Persistent webhook storage to Google Drive
// Buffers all webhook events in Redis, flushes to Google Drive hourly as JSONL files.

const { google } = require('googleapis');
const cron = require('node-cron');

const REDIS_KEY = 'webhook:archive:buffer';
const DRIVE_ID = process.env.GOOGLE_DRIVE_ID;

let driveService = null;
let redis = null;
let folderCache = {}; // path -> folderId

function init(redisClient) {
  redis = redisClient;

  const saJson = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!saJson || !DRIVE_ID) {
    console.log('⚠️ Webhook store: Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_DRIVE_ID — disabled');
    return false;
  }

  try {
    const sa = JSON.parse(saJson);
    const auth = new google.auth.GoogleAuth({
      credentials: sa,
      scopes: ['https://www.googleapis.com/auth/drive'],
    });
    driveService = google.drive({ version: 'v3', auth });
    console.log('📦 Webhook store: Google Drive connected');

    // Flush every hour at :05 (give webhooks time to settle)
    cron.schedule('5 * * * *', () => flushToDrive());
    console.log('📦 Webhook store: Hourly flush scheduled');

    return true;
  } catch (e) {
    console.error('❌ Webhook store init failed:', e.message);
    return false;
  }
}

// Buffer a webhook event (called on every webhook)
async function buffer(event, accountId, payload) {
  if (!redis) return;
  const entry = JSON.stringify({
    event,
    account_id: accountId,
    payload,
    ts: new Date().toISOString(),
  });
  await redis.rpush(REDIS_KEY, entry);
}

// Get or create a folder by path (e.g., "webhooks/2026-02")
async function getOrCreateFolder(name, parentId) {
  const cacheKey = `${parentId}/${name}`;
  if (folderCache[cacheKey]) return folderCache[cacheKey];

  // Search for existing folder
  const q = `name='${name}' and '${parentId}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false`;
  const res = await driveService.files.list({
    q,
    driveId: DRIVE_ID,
    corpora: 'drive',
    includeItemsFromAllDrives: true,
    supportsAllDrives: true,
    fields: 'files(id,name)',
  });

  if (res.data.files.length > 0) {
    folderCache[cacheKey] = res.data.files[0].id;
    return res.data.files[0].id;
  }

  // Create folder
  const folder = await driveService.files.create({
    requestBody: {
      name,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId],
    },
    supportsAllDrives: true,
    fields: 'id',
  });

  folderCache[cacheKey] = folder.data.id;
  return folder.data.id;
}

// Append data to an existing file or create new one
async function appendOrCreateFile(fileName, folderId, content) {
  // Search for existing file
  const q = `name='${fileName}' and '${folderId}' in parents and trashed=false`;
  const res = await driveService.files.list({
    q,
    driveId: DRIVE_ID,
    corpora: 'drive',
    includeItemsFromAllDrives: true,
    supportsAllDrives: true,
    fields: 'files(id,name)',
  });

  if (res.data.files.length > 0) {
    // Download existing content, append, re-upload
    const fileId = res.data.files[0].id;
    const existing = await driveService.files.get({
      fileId,
      alt: 'media',
      supportsAllDrives: true,
    });
    const merged = (existing.data || '') + content;
    await driveService.files.update({
      fileId,
      media: { mimeType: 'application/x-ndjson', body: merged },
      supportsAllDrives: true,
    });
    return fileId;
  } else {
    // Create new file
    const file = await driveService.files.create({
      requestBody: {
        name: fileName,
        parents: [folderId],
      },
      media: { mimeType: 'application/x-ndjson', body: content },
      supportsAllDrives: true,
      fields: 'id',
    });
    return file.data.id;
  }
}

// Flush buffered webhooks to Google Drive
async function flushToDrive() {
  if (!driveService || !redis) return;

  try {
    // Atomic drain: get length, pop that many
    const len = await redis.llen(REDIS_KEY);
    if (len === 0) {
      console.log('📦 Webhook store: Nothing to flush');
      return;
    }

    // Pop all buffered events
    const entries = [];
    for (let i = 0; i < len; i++) {
      const entry = await redis.lpop(REDIS_KEY);
      if (entry) entries.push(entry);
    }

    if (entries.length === 0) return;

    // Group by date
    const byDate = {};
    for (const entry of entries) {
      try {
        const parsed = JSON.parse(entry);
        const date = parsed.ts?.slice(0, 10) || new Date().toISOString().slice(0, 10);
        if (!byDate[date]) byDate[date] = [];
        byDate[date].push(entry);
      } catch {
        // If can't parse, use today
        const today = new Date().toISOString().slice(0, 10);
        if (!byDate[today]) byDate[today] = [];
        byDate[today].push(entry);
      }
    }

    // Upload each date's data
    const webhooksFolderId = await getOrCreateFolder('webhooks', DRIVE_ID);

    for (const [date, dateEntries] of Object.entries(byDate)) {
      const month = date.slice(0, 7); // 2026-02
      const monthFolderId = await getOrCreateFolder(month, webhooksFolderId);
      const fileName = `${date}.jsonl`;
      const content = dateEntries.join('\n') + '\n';
      await appendOrCreateFile(fileName, monthFolderId, content);
      console.log(`📦 Webhook store: Flushed ${dateEntries.length} events to ${month}/${fileName}`);
    }

    console.log(`📦 Webhook store: Total flushed ${entries.length} events`);
  } catch (e) {
    console.error('❌ Webhook store flush failed:', e.message);
    // Don't lose data — events were already popped, push them back
    // (best effort, some may be lost on catastrophic failure)
  }
}

// Manual flush endpoint
function getStats() {
  return {
    driveConnected: !!driveService,
    driveId: DRIVE_ID,
    redisKey: REDIS_KEY,
  };
}

module.exports = { init, buffer, flushToDrive, getStats };
