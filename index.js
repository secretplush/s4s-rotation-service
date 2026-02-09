require('dotenv').config();
const express = require('express');
const cron = require('node-cron');
const { Redis } = require('@upstash/redis');

const app = express();
app.use(express.json());

// Config
const OF_API_KEY = process.env.OF_API_KEY;
const OF_API_BASE = 'https://app.onlyfansapi.com/api';
const PORT = process.env.PORT || 3000;

// Redis for state persistence
const redis = new Redis({
  url: process.env.KV_REST_API_URL,
  token: process.env.KV_REST_API_TOKEN,
});

// Keys for Redis persistence
const REDIS_KEYS = {
  PENDING_DELETES: 's4s:pending-deletes',
  ROTATION_STATE: 's4s:rotation-state',
  SCHEDULE: 's4s:schedule',
};

// In-memory state
let isRunning = false;
let rotationState = {
  lastTagTime: {},
  executedTags: [],
  dailySchedule: {},
  stats: {
    totalTags: 0,
    totalDeletes: 0,
    startedAt: null,
  }
};

// Ghost captions
const GHOST_CAPTIONS = [
  "obsessed with her 🥹💕 @{target}",
  "ok but she's actually so pretty @{target} 💗",
  "bestie check 🫶 @{target}",
  "everyone needs to follow her rn @{target} ✨",
  "my fav girl @{target} 💕",
  "she's too cute @{target} 🥰",
  "go show her some love @{target} 💗",
  "literally the sweetest @{target} 🫶",
  "can't stop staring @{target} 😍",
  "you need her on your feed @{target} ✨",
  "drop everything and follow @{target} 💕",
  "she's everything @{target} 🔥",
  "prettiest girl @{target} 💗",
  "bestie appreciation post @{target} 🥹",
  "heart eyes for @{target} 😍",
];

// === REDIS-BACKED PENDING DELETES ===

async function addPendingDelete(postId, accountId, promoter, target) {
  const deleteAt = Date.now() + 5 * 60 * 1000; // 5 minutes
  const entry = { postId, accountId, promoter, target, deleteAt, createdAt: Date.now() };
  
  // Persist to Redis IMMEDIATELY
  try {
    const existing = await redis.get(REDIS_KEYS.PENDING_DELETES) || [];
    existing.push(entry);
    await redis.set(REDIS_KEYS.PENDING_DELETES, existing);
    console.log(`📌 Persisted pending delete: ${postId} (deletes at ${new Date(deleteAt).toISOString()})`);
    return entry;
  } catch (e) {
    console.error('❌ CRITICAL: Failed to persist pending delete:', e);
    // Still return entry for in-memory backup
    return entry;
  }
}

async function getPendingDeletes() {
  try {
    return await redis.get(REDIS_KEYS.PENDING_DELETES) || [];
  } catch (e) {
    console.error('Failed to get pending deletes:', e);
    return [];
  }
}

async function removePendingDelete(postId) {
  try {
    const pending = await redis.get(REDIS_KEYS.PENDING_DELETES) || [];
    const updated = pending.filter(p => p.postId !== postId);
    await redis.set(REDIS_KEYS.PENDING_DELETES, updated);
    return true;
  } catch (e) {
    console.error('Failed to remove pending delete:', e);
    return false;
  }
}

async function processOverdueDeletes() {
  const now = Date.now();
  const pending = await getPendingDeletes();
  let processed = 0;
  
  for (const del of pending) {
    if (now >= del.deleteAt) {
      console.log(`🗑️ Processing overdue delete: ${del.postId} (was due ${Math.round((now - del.deleteAt) / 1000)}s ago)`);
      const success = await deletePost(del.postId, del.accountId);
      if (success) {
        await removePendingDelete(del.postId);
        rotationState.stats.totalDeletes++;
        processed++;
      }
    }
  }
  
  if (processed > 0) {
    console.log(`✅ Processed ${processed} overdue deletes`);
  }
  return processed;
}

// === CORE FUNCTIONS ===

async function loadVaultMappings() {
  try {
    const data = await redis.get('vault_mappings');
    console.log('Loaded vault mappings:', Object.keys(data || {}).length, 'models');
    return data || {};
  } catch (e) {
    console.error('Failed to load vault mappings:', e);
    return {};
  }
}

async function loadModelAccounts() {
  try {
    const res = await fetch(`${OF_API_BASE}/accounts`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    const accounts = await res.json();
    
    const accountMap = {};
    for (const acct of accounts) {
      if (acct.onlyfans_username) {
        accountMap[acct.onlyfans_username] = acct.id;
      }
    }
    return accountMap;
  } catch (e) {
    console.error('Failed to load accounts:', e);
    return {};
  }
}

function generateDailySchedule(models, vaultMappings) {
  const schedule = {};
  const now = Date.now();
  
  for (const model of models) {
    const targets = Object.keys(vaultMappings[model] || {});
    if (targets.length === 0) continue;
    
    const shuffledTargets = [...targets].sort(() => Math.random() - 0.5);
    const tagsPerDay = 56;
    const baseInterval = 25.7 * 60 * 1000;
    
    schedule[model] = [];
    let currentTime = now + (1 + Math.random() * 4) * 60 * 1000;
    
    for (let i = 0; i < tagsPerDay; i++) {
      const target = shuffledTargets[i % shuffledTargets.length];
      const jitter = (Math.random() - 0.5) * 6 * 60 * 1000;
      const scheduledTime = currentTime + jitter;
      
      schedule[model].push({
        target,
        scheduledTime,
        executed: false,
      });
      
      currentTime += baseInterval;
    }
  }
  
  console.log(`📅 Generated schedule for ${models.length} models, starting from now`);
  return schedule;
}

function getRandomCaption(targetUsername) {
  const template = GHOST_CAPTIONS[Math.floor(Math.random() * GHOST_CAPTIONS.length)];
  return template.replace('{target}', targetUsername);
}

async function executeTag(promoter, target, vaultId, accountId) {
  const caption = getRandomCaption(target);
  
  try {
    const res = await fetch(`${OF_API_BASE}/${accountId}/posts`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OF_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        text: caption,
        mediaFiles: [vaultId]
      })
    });
    
    if (!res.ok) {
      const err = await res.text();
      console.error(`Failed to create post for ${promoter} → ${target}:`, err);
      return null;
    }
    
    const data = await res.json();
    const postId = data.id || data.post_id || data.postId || data.data?.id;
    
    console.log(`✅ Posted: ${promoter} → @${target} (post ${postId})`);
    return postId;
  } catch (e) {
    console.error(`Error posting ${promoter} → ${target}:`, e);
    return null;
  }
}

async function deletePost(postId, accountId) {
  try {
    const res = await fetch(`${OF_API_BASE}/${accountId}/posts/${postId}`, {
      method: 'DELETE',
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    
    if (res.ok) {
      console.log(`🗑️ Deleted post ${postId}`);
      return true;
    } else {
      const err = await res.text();
      console.error(`Failed to delete post ${postId}:`, err);
      return false;
    }
  } catch (e) {
    console.error(`Error deleting post ${postId}:`, e);
    return false;
  }
}

// === MAIN LOOP ===

async function runRotationCycle() {
  if (!isRunning) return;
  
  const now = Date.now();
  const vaultMappings = await loadVaultMappings();
  const accountMap = await loadModelAccounts();
  
  // ALWAYS process pending deletes first (from Redis)
  const pending = await getPendingDeletes();
  
  for (const del of pending) {
    if (now >= del.deleteAt) {
      const success = await deletePost(del.postId, del.accountId);
      if (success) {
        await removePendingDelete(del.postId);
        rotationState.stats.totalDeletes++;
        // Update executedTags status
        const tag = rotationState.executedTags.find(t => t.postId === del.postId);
        if (tag) tag.status = 'deleted';
      }
    }
  }
  
  // Check scheduled tags
  for (const [model, schedules] of Object.entries(rotationState.dailySchedule)) {
    const accountId = accountMap[model];
    if (!accountId) continue;
    
    for (const sched of schedules) {
      if (sched.executed) continue;
      if (now < sched.scheduledTime) continue;
      
      const vaultId = vaultMappings[model]?.[sched.target];
      if (!vaultId) {
        console.log(`⚠️ No vault ID for ${model} → ${sched.target}`);
        sched.executed = true;
        continue;
      }
      
      const postId = await executeTag(model, sched.target, vaultId, accountId);
      sched.executed = true;
      
      if (postId) {
        // Track executed tag
        rotationState.executedTags.push({
          postId,
          promoter: model,
          target: sched.target,
          createdAt: now,
          status: 'active'
        });
        if (rotationState.executedTags.length > 50) {
          rotationState.executedTags = rotationState.executedTags.slice(-50);
        }
        
        // PERSIST pending delete to Redis immediately
        await addPendingDelete(postId, accountId, model, sched.target);
        rotationState.stats.totalTags++;
      }
      
      // Rate limit protection
      await new Promise(r => setTimeout(r, 2000));
    }
  }
}

// === SCHEDULING ===

// Run rotation cycle every minute
cron.schedule('* * * * *', runRotationCycle);

// Also run delete check every 30 seconds for faster cleanup
cron.schedule('*/30 * * * * *', async () => {
  if (!isRunning) return;
  
  const pending = await getPendingDeletes();
  const now = Date.now();
  const overdue = pending.filter(p => now >= p.deleteAt);
  
  if (overdue.length > 0) {
    console.log(`⏰ Found ${overdue.length} overdue deletes, processing...`);
    await processOverdueDeletes();
  }
});

// Regenerate daily schedule at midnight
cron.schedule('0 0 * * *', async () => {
  if (!isRunning) return;
  console.log('🔄 Regenerating daily schedule...');
  const vaultMappings = await loadVaultMappings();
  const models = Object.keys(vaultMappings);
  rotationState.dailySchedule = generateDailySchedule(models, vaultMappings);
});

// === STARTUP RECOVERY ===

async function startupRecovery() {
  console.log('🔧 Running startup recovery...');
  
  // Check for any pending deletes that should have been processed
  const overdue = await processOverdueDeletes();
  console.log(`   Recovered ${overdue} overdue deletes`);
  
  // Load previous state if exists
  try {
    const savedState = await redis.get(REDIS_KEYS.ROTATION_STATE);
    if (savedState) {
      rotationState.stats = savedState.stats || rotationState.stats;
      console.log(`   Restored stats: ${rotationState.stats.totalTags} tags, ${rotationState.stats.totalDeletes} deletes`);
    }
  } catch (e) {
    console.log('   No previous state found');
  }
  
  console.log('✅ Startup recovery complete');
}

// === API ENDPOINTS ===

app.get('/', (req, res) => {
  res.json({
    service: 'S4S Rotation Service',
    status: isRunning ? 'running' : 'stopped',
    stats: rotationState.stats,
  });
});

app.post('/start', async (req, res) => {
  if (isRunning) {
    return res.json({ status: 'already running' });
  }
  
  console.log('🚀 Starting rotation service...');
  isRunning = true;
  rotationState.stats.startedAt = new Date().toISOString();
  
  // Process any overdue deletes first
  await processOverdueDeletes();
  
  // Generate initial schedule
  const vaultMappings = await loadVaultMappings();
  const models = Object.keys(vaultMappings);
  rotationState.dailySchedule = generateDailySchedule(models, vaultMappings);
  
  console.log(`📅 Schedule generated for ${models.length} models`);
  
  res.json({ 
    status: 'started',
    models: models.length,
    message: `Rotation started for ${models.length} models`
  });
});

app.post('/stop', async (req, res) => {
  console.log('⏹️ Stopping rotation service...');
  isRunning = false;
  
  // Process all remaining deletes before stopping
  console.log('🗑️ Cleaning up pending deletes...');
  const pending = await getPendingDeletes();
  
  for (const del of pending) {
    await deletePost(del.postId, del.accountId);
    await removePendingDelete(del.postId);
    rotationState.stats.totalDeletes++;
  }
  
  console.log(`   Deleted ${pending.length} remaining posts`);
  
  res.json({ status: 'stopped', cleaned: pending.length });
});

// Receive schedule from app (single source of truth)
app.post('/schedule', async (req, res) => {
  const { schedule } = req.body;
  
  if (!schedule) {
    return res.status(400).json({ error: 'schedule required' });
  }
  
  rotationState.dailySchedule = schedule;
  await redis.set(REDIS_KEYS.SCHEDULE, schedule);
  
  const totalTags = Object.values(schedule).reduce((sum, s) => sum + s.length, 0);
  console.log(`📅 Received schedule: ${Object.keys(schedule).length} models, ${totalTags} total tags`);
  
  res.json({ 
    status: 'schedule updated',
    models: Object.keys(schedule).length,
    totalTags
  });
});

app.get('/schedule', (req, res) => {
  const now = Date.now();
  const upcoming = [];
  
  for (const [model, schedules] of Object.entries(rotationState.dailySchedule)) {
    for (const sched of schedules) {
      if (!sched.executed && sched.scheduledTime > now) {
        upcoming.push({
          model,
          target: sched.target,
          scheduledTime: new Date(sched.scheduledTime).toISOString(),
          inMinutes: Math.round((sched.scheduledTime - now) / 60000),
        });
      }
    }
  }
  
  upcoming.sort((a, b) => a.inMinutes - b.inMinutes);
  
  res.json({
    upcoming: upcoming.slice(0, 50),
    total: upcoming.length,
  });
});

app.get('/stats', async (req, res) => {
  const pending = await getPendingDeletes();
  
  res.json({
    isRunning,
    stats: rotationState.stats,
    modelsActive: Object.keys(rotationState.dailySchedule).length,
    pendingDeletes: pending.length,
    pendingDeletesList: pending.map(p => ({
      postId: p.postId,
      promoter: p.promoter,
      target: p.target,
      deleteAt: new Date(p.deleteAt).toISOString(),
      overdueBy: Math.max(0, Math.round((Date.now() - p.deleteAt) / 1000))
    }))
  });
});

app.get('/active', async (req, res) => {
  const pending = await getPendingDeletes();
  const now = Date.now();
  
  const activeTags = pending.map(p => ({
    promoter: p.promoter,
    target: p.target,
    postId: p.postId,
    createdAt: new Date(p.createdAt).toISOString(),
    ageSeconds: Math.round((now - p.createdAt) / 1000),
    deletesIn: Math.max(0, Math.round((p.deleteAt - now) / 1000))
  })).sort((a, b) => b.ageSeconds - a.ageSeconds);
  
  res.json({
    count: activeTags.length,
    tags: activeTags
  });
});

// Force cleanup endpoint
app.post('/cleanup', async (req, res) => {
  console.log('🧹 Force cleanup triggered...');
  const pending = await getPendingDeletes();
  let cleaned = 0;
  
  for (const del of pending) {
    const success = await deletePost(del.postId, del.accountId);
    if (success) {
      await removePendingDelete(del.postId);
      cleaned++;
    }
  }
  
  res.json({ cleaned, message: `Deleted ${cleaned} posts` });
});

app.get('/health', (req, res) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

// === START SERVER ===

app.listen(PORT, async () => {
  console.log(`🚀 S4S Rotation Service running on port ${PORT}`);
  console.log(`   POST /start    - Start rotation`);
  console.log(`   POST /stop     - Stop rotation (cleans up)`);
  console.log(`   POST /schedule - Receive schedule from app`);
  console.log(`   POST /cleanup  - Force delete all pending`);
  console.log(`   GET /schedule  - View upcoming tags`);
  console.log(`   GET /stats     - View statistics`);
  console.log(`   GET /active    - View active (undeleted) posts`);
  
  // Run startup recovery
  await startupRecovery();
});
