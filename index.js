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

// In-memory state (backed by Redis)
let isRunning = false;
let rotationState = {
  lastTagTime: {},      // { modelUsername: timestamp }
  pendingDeletes: [],   // { postId, account, deleteAt }
  dailySchedule: {},    // { modelUsername: [{ targetUsername, scheduledTime }] }
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

// === CORE FUNCTIONS ===

async function loadVaultMappings() {
  try {
    const data = await redis.get('s4s:vault-mappings');
    return data || {};
  } catch (e) {
    console.error('Failed to load vault mappings:', e);
    return {};
  }
}

async function loadModelAccounts() {
  try {
    // Fetch accounts from OF API
    const res = await fetch(`${OF_API_BASE}/accounts`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    const accounts = await res.json();
    
    // Map username to account ID
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
  const now = new Date();
  const startOfDay = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  
  for (const model of models) {
    const targets = Object.keys(vaultMappings[model] || {});
    if (targets.length === 0) continue;
    
    // Shuffle targets for randomization
    const shuffledTargets = [...targets].sort(() => Math.random() - 0.5);
    
    // 56 tags per day = one every ~25.7 minutes
    // Spread across 24 hours with some randomness
    const tagsPerDay = 56;
    const baseInterval = (24 * 60) / tagsPerDay; // ~25.7 min
    
    schedule[model] = [];
    let currentTime = startOfDay.getTime() + Math.random() * 10 * 60 * 1000; // Random start within first 10 min
    
    for (let i = 0; i < tagsPerDay; i++) {
      const target = shuffledTargets[i % shuffledTargets.length];
      // Add randomness: ±5 minutes
      const jitter = (Math.random() - 0.5) * 10 * 60 * 1000;
      const scheduledTime = currentTime + jitter;
      
      schedule[model].push({
        target,
        scheduledTime,
        executed: false,
      });
      
      currentTime += baseInterval * 60 * 1000;
    }
  }
  
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
      console.error(`Failed to delete post ${postId}`);
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
  
  // Process pending deletes first
  const pendingDeletes = [...rotationState.pendingDeletes];
  rotationState.pendingDeletes = [];
  
  for (const del of pendingDeletes) {
    if (now >= del.deleteAt) {
      await deletePost(del.postId, del.account);
      rotationState.stats.totalDeletes++;
    } else {
      rotationState.pendingDeletes.push(del);
    }
  }
  
  // Check scheduled tags
  for (const [model, schedules] of Object.entries(rotationState.dailySchedule)) {
    const accountId = accountMap[model];
    if (!accountId) continue;
    
    for (const sched of schedules) {
      if (sched.executed) continue;
      if (now < sched.scheduledTime) continue;
      
      // Execute the tag
      const vaultId = vaultMappings[model]?.[sched.target];
      if (!vaultId) {
        console.log(`⚠️ No vault ID for ${model} → ${sched.target}`);
        sched.executed = true;
        continue;
      }
      
      const postId = await executeTag(model, sched.target, vaultId, accountId);
      sched.executed = true;
      
      if (postId) {
        // Schedule deletion in 5 minutes
        rotationState.pendingDeletes.push({
          postId,
          account: accountId,
          deleteAt: now + 5 * 60 * 1000,
        });
        rotationState.stats.totalTags++;
      }
      
      // Small delay between posts to avoid rate limits
      await new Promise(r => setTimeout(r, 2000));
    }
  }
  
  // Save state to Redis
  await redis.set('s4s:rotation-state', JSON.stringify(rotationState));
}

// === SCHEDULING ===

// Run rotation cycle every minute
cron.schedule('* * * * *', runRotationCycle);

// Regenerate daily schedule at midnight
cron.schedule('0 0 * * *', async () => {
  if (!isRunning) return;
  console.log('🔄 Regenerating daily schedule...');
  const vaultMappings = await loadVaultMappings();
  const models = Object.keys(vaultMappings);
  rotationState.dailySchedule = generateDailySchedule(models, vaultMappings);
  console.log(`📅 New schedule generated for ${models.length} models`);
});

// === API ENDPOINTS ===

app.get('/', (req, res) => {
  res.json({
    service: 'S4S Rotation Service',
    status: isRunning ? 'running' : 'stopped',
    stats: rotationState.stats,
    pendingDeletes: rotationState.pendingDeletes.length,
  });
});

app.post('/start', async (req, res) => {
  if (isRunning) {
    return res.json({ status: 'already running' });
  }
  
  console.log('🚀 Starting rotation service...');
  isRunning = true;
  rotationState.stats.startedAt = new Date().toISOString();
  
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
  
  // Process remaining deletes
  for (const del of rotationState.pendingDeletes) {
    await deletePost(del.postId, del.account);
  }
  rotationState.pendingDeletes = [];
  
  res.json({ status: 'stopped' });
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
    pendingDeletes: rotationState.pendingDeletes.length,
  });
});

app.get('/stats', (req, res) => {
  res.json({
    isRunning,
    stats: rotationState.stats,
    modelsActive: Object.keys(rotationState.dailySchedule).length,
    pendingDeletes: rotationState.pendingDeletes.length,
  });
});

// Health check for Railway
app.get('/health', (req, res) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

// === START SERVER ===

app.listen(PORT, () => {
  console.log(`🚀 S4S Rotation Service running on port ${PORT}`);
  console.log(`   POST /start - Start rotation`);
  console.log(`   POST /stop  - Stop rotation`);
  console.log(`   GET /schedule - View upcoming tags`);
  console.log(`   GET /stats - View statistics`);
});
