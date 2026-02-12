require('dotenv').config();
const express = require('express');
const cron = require('node-cron');
const { Redis } = require('@upstash/redis');

const app = express();
app.use(express.json({ limit: '10mb' }));

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

// Models that promote others but are NEVER tagged/promoted themselves (no promo image)
const PROMOTER_ONLY = new Set(['taylorskully']);

function generateDailySchedule(models, vaultMappings) {
  const schedule = {};
  const now = Date.now();
  
  for (const model of models) {
    // Filter targets: exclude promoter-only models (they have no image to tag with)
    const allTargets = Object.keys(vaultMappings[model] || {});
    const targets = allTargets.filter(t => !PROMOTER_ONLY.has(t));
    if (targets.length === 0) continue;
    
    const shuffledTargets = [...targets].sort(() => Math.random() - 0.5);
    const tagsPerDay = 57;
    const baseInterval = (24 * 60 * 60 * 1000) / tagsPerDay; // ~25.3 min
    
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

// === PINNED POST SYSTEM ===
// 5 featured girls × 6 accounts each = 30 pinned posts/day
// Created at 6am AST (10:00 UTC), expire after 24h via OF
// Stopping rotation does NOT remove pinned posts

const PINNED_REDIS_KEY = 's4s:pinned-state';
const PINNED_FEATURED_PER_DAY = 5;
const PINNED_ACCOUNTS_PER_GIRL = 6;

const PINNED_CAPTIONS = [
  "my girl 💕 go follow @{target} rn",
  "she's literally perfect @{target} 🥰",
  "everyone go subscribe to @{target} 💗",
  "you're welcome 😍 @{target}",
  "obsessed w her @{target} 🫶",
  "bestie goals @{target} ✨",
  "trust me, follow @{target} 💕",
  "she's that girl @{target} 🔥",
  "my fav human @{target} 💗",
  "go show love to @{target} 🥹",
];

function getPinnedCaption(targetUsername) {
  const template = PINNED_CAPTIONS[Math.floor(Math.random() * PINNED_CAPTIONS.length)];
  return template.replace('{target}', targetUsername);
}

async function getPinnedState() {
  try {
    return await redis.get(PINNED_REDIS_KEY) || { dayIndex: 0, activePosts: [], lastRun: null };
  } catch (e) {
    return { dayIndex: 0, activePosts: [], lastRun: null };
  }
}

async function savePinnedState(state) {
  try {
    await redis.set(PINNED_REDIS_KEY, state);
  } catch (e) {
    console.error('Failed to save pinned state:', e);
  }
}

async function createPinnedPost(promoterAccountId, targetUsername, vaultId) {
  const caption = getPinnedCaption(targetUsername);
  
  try {
    // Step 1: Create the post with 24hr expiry
    const res = await fetch(`${OF_API_BASE}/${promoterAccountId}/posts`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OF_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        text: caption,
        mediaFiles: [vaultId],
        expireDays: 1
      })
    });
    
    if (!res.ok) {
      const err = await res.text();
      console.error(`Failed to create pinned post: ${err}`);
      return null;
    }
    
    const data = await res.json();
    const postId = data.id || data.post_id || data.postId || data.data?.id;
    
    if (!postId) {
      console.error('No post ID returned');
      return null;
    }
    
    // Step 2: Pin the post (separate API call)
    await new Promise(r => setTimeout(r, 1000));
    const pinRes = await fetch(`${OF_API_BASE}/${promoterAccountId}/posts/${postId}/pin`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OF_API_KEY}`,
        'Content-Type': 'application/json'
      }
    });
    
    if (pinRes.ok) {
      console.log(`📌 Pinned: @${targetUsername} on ${promoterAccountId} (post ${postId})`);
    } else {
      console.log(`⚠️ Posted but pin failed: @${targetUsername} on ${promoterAccountId} (post ${postId})`);
    }
    
    return postId;
  } catch (e) {
    console.error(`Error creating pinned post:`, e);
    return null;
  }
}

async function runPinnedPostRotation() {
  console.log('📌 === PINNED POST ROTATION ===');
  
  const vaultMappings = await loadVaultMappings();
  const accountMap = await loadModelAccounts();
  const allModels = Object.keys(vaultMappings).sort();
  // Models eligible to be FEATURED (promoted) — exclude promoter-only models
  const targetableModels = allModels.filter(m => !PROMOTER_ONLY.has(m));
  
  if (allModels.length === 0) {
    console.log('❌ No models with vault mappings');
    return { success: false, error: 'No models' };
  }
  
  const pinnedState = await getPinnedState();
  
  // Determine which 5 girls are featured today (from targetable models only)
  const dayIndex = pinnedState.dayIndex || 0;
  const startIdx = (dayIndex * PINNED_FEATURED_PER_DAY) % targetableModels.length;
  const featuredGirls = [];
  for (let i = 0; i < PINNED_FEATURED_PER_DAY; i++) {
    featuredGirls.push(targetableModels[(startIdx + i) % targetableModels.length]);
  }
  
  console.log(`📌 Today's featured (day ${dayIndex + 1}): ${featuredGirls.join(', ')}`);
  
  // For each featured girl, pick 6 accounts to pin on (not their own)
  // Track history so the same girl doesn't get pinned on the same accounts repeatedly
  const activePosts = [];
  const allOtherModels = [...allModels]; // pool of promoters
  
  // Load pin history: { featuredUsername: [promoter1, promoter2, ...] } (last used promoters)
  const pinHistory = (await redis.get('s4s:pin-history')) || {};
  
  for (const featured of featuredGirls) {
    // Get available promoters (not the featured girl herself, and not already assigned today)
    const usedPromoters = new Set(activePosts.map(p => p.promoter));
    const available = allOtherModels.filter(m => m !== featured && !usedPromoters.has(m));
    
    // Sort: prioritize accounts that HAVEN'T promoted this girl recently
    const recentPromoters = new Set(pinHistory[featured] || []);
    const fresh = available.filter(m => !recentPromoters.has(m));
    const stale = available.filter(m => recentPromoters.has(m));
    
    // Pick from fresh first, then stale if not enough fresh
    const shuffledFresh = fresh.sort(() => Math.random() - 0.5);
    const shuffledStale = stale.sort(() => Math.random() - 0.5);
    const promoters = [...shuffledFresh, ...shuffledStale].slice(0, PINNED_ACCOUNTS_PER_GIRL);
    
    // Update history: store this round's promoters (keep last 2 rounds = 12 accounts)
    const prevHistory = pinHistory[featured] || [];
    pinHistory[featured] = [...promoters, ...prevHistory].slice(0, PINNED_ACCOUNTS_PER_GIRL * 2);
    
    for (const promoter of promoters) {
      const accountId = accountMap[promoter];
      const vaultId = vaultMappings[promoter]?.[featured];
      
      if (!accountId || !vaultId) {
        console.log(`⚠️ Missing account/vault for ${promoter} → ${featured}`);
        continue;
      }
      
      let postId = await createPinnedPost(accountId, featured, vaultId);
      
      // Retry once if rate limited
      if (!postId) {
        console.log(`⏳ Retrying ${promoter} → @${featured} after 12s...`);
        await new Promise(r => setTimeout(r, 12000));
        postId = await createPinnedPost(accountId, featured, vaultId);
      }
      
      if (postId) {
        activePosts.push({
          postId,
          promoter,
          featured,
          accountId,
          createdAt: Date.now()
        });
      }
      
      // 10-20s random spacing between posts to avoid rate limits and stagger naturally
      const spacing = 10000 + Math.floor(Math.random() * 10000);
      await new Promise(r => setTimeout(r, spacing));
    }
  }
  
  // Save pin history so next rotation uses different accounts
  await redis.set('s4s:pin-history', pinHistory);
  
  // Save state
  const newState = {
    dayIndex: dayIndex + 1,
    activePosts,
    lastRun: new Date().toISOString(),
    featuredGirls
  };
  await savePinnedState(newState);
  
  console.log(`📌 Created ${activePosts.length} pinned posts for ${featuredGirls.length} featured girls`);
  return { success: true, posts: activePosts.length, featured: featuredGirls };
}

async function removePinnedPosts() {
  const pinnedState = await getPinnedState();
  const posts = pinnedState.activePosts || [];
  let removed = 0;
  
  for (const post of posts) {
    const success = await deletePost(post.postId, post.accountId);
    if (success) removed++;
    await new Promise(r => setTimeout(r, 1000));
  }
  
  pinnedState.activePosts = [];
  await savePinnedState(pinnedState);
  
  console.log(`📌 Removed ${removed}/${posts.length} pinned posts`);
  return { removed, total: posts.length };
}

// Run pinned posts at 6am AST = 10:00 UTC
cron.schedule('0 10 * * *', async () => {
  if (!isRunning) return;
  
  // Check if pinned posts are enabled
  const pinnedEnabled = await redis.get('s4s:pinned-enabled');
  if (pinnedEnabled === false) {
    console.log('📌 Pinned posts disabled, skipping');
    return;
  }
  
  // Stagger start by 0-2 min random offset to avoid predictability
  const staggerMs = Math.floor(Math.random() * 2 * 60 * 1000);
  console.log(`📌 Pinned posts scheduled, starting in ${Math.round(staggerMs/1000)}s...`);
  setTimeout(() => runPinnedPostRotation(), staggerMs);
});

// === MASS DM PROMO SYSTEM ===

const MASS_DM_CAPTIONS = [
  "Have you seen my friend @{target}? 😍",
  "You NEED to check out my girl @{target} 🔥",
  "My friend @{target} is so hot omg go see her 😩",
  "Go say hi to my bestie @{target} 💕",
  "Ok but have you seen @{target} yet?? 👀",
  "My girl @{target} is so fine it's not even fair 🥵",
  "You'd love my friend @{target} trust me 😘",
  "Obsessed with my girl @{target} rn go follow her 💋",
  "If you like me you'll LOVE @{target} 😍",
  "Go show some love to @{target} for me babe 💗",
  "My friend @{target} just started and she's already killing it 🔥",
  "Seriously go check out @{target} before everyone else does 👀",
  "I can't stop looking at @{target}'s page omg 🥵",
  "Do me a favor and go follow my girl @{target} 😘",
  "You're welcome in advance… @{target} 🫣",
  "My bestie @{target} is too fine to not share 💕",
  "Tell @{target} I sent you 😏",
  "Just wait until you see @{target} 🤤",
  "Go subscribe to my girl @{target} you won't regret it 😍",
  "Sharing my fav girl @{target} with you because I'm nice like that 😘",
  "My college roommate @{target} finally made one 😍",
  "This girl from my class @{target} just started… go look 👀",
  "My sorority sister @{target} is so bad omg 🥵",
  "Ok so @{target} just turned 18 and made an OF… you're welcome 🫣",
  "@{target} literally just started posting and she's already so hot 🔥",
  "My friend @{target} from school finally caved and made one 😩",
  "College girls do it better… go see @{target} 💋",
  "This freshman @{target} is about to blow up go follow now 👀",
  "@{target} just turned 18 and I can't believe her page 🤤",
  "My dorm mate @{target} started an OF and I'm obsessed 😍",
  "She just turned 18 last week… go see @{target} before she blows up 🔥",
  "@{target} is brand new and already hotter than everyone 🥵",
  "My study buddy @{target} finally made a page go show her love 💕",
  "Just found out @{target} from my campus made one… omg 👀",
  "Newest girl on campus @{target} just dropped her first posts 😘",
  "This college girl @{target} is unreal go look 🫣",
  "@{target} just started her page and she's so nervous go be nice 🥺",
  "My girl @{target} is fresh out of high school and already killing it 🔥",
  "Campus cutie @{target} finally joined… trust me on this one 😏",
  "She's barely 18 and already this fine?? go see @{target} 😩",
];

// 12 time windows in UTC hours (AST + 4)
const MASS_DM_WINDOWS_UTC = [
  { startHour: 4 },   // 12:00 AM - 1:00 AM AST
  { startHour: 6 },   // 2:00 AM - 3:00 AM AST
  { startHour: 8 },   // 4:00 AM - 5:00 AM AST
  { startHour: 10 },  // 6:00 AM - 7:00 AM AST
  { startHour: 12 },  // 8:00 AM - 9:00 AM AST
  { startHour: 14 },  // 10:00 AM - 11:00 AM AST
  { startHour: 16 },  // 12:00 PM - 1:00 PM AST
  { startHour: 18 },  // 2:00 PM - 3:00 PM AST
  { startHour: 20 },  // 4:00 PM - 5:00 PM AST
  { startHour: 22 },  // 6:00 PM - 7:00 PM AST
  { startHour: 0 },   // 8:00 PM - 9:00 PM AST (next UTC day)
  { startHour: 2 },   // 10:00 PM - 11:00 PM AST (next UTC day)
];

function getMassDmCaption(targetUsername) {
  const template = MASS_DM_CAPTIONS[Math.floor(Math.random() * MASS_DM_CAPTIONS.length)];
  return template.replace('{target}', targetUsername);
}

async function generateMassDmSchedule() {
  console.log('📨 Generating mass DM schedule...');
  
  const vaultMappings = await loadVaultMappings();
  const allModels = Object.keys(vaultMappings).sort();
  
  if (allModels.length < 2) {
    console.log('❌ Not enough models for mass DM schedule');
    return;
  }
  
  // Load promotion history to avoid repeats
  const history = await redis.get('s4s:mass-dm-history') || {};
  
  const now = new Date();
  const todayStr = now.toISOString().slice(0, 10);
  
  // Build target assignments per model (12 targets each)
  // Promoter-only models can SEND mass DMs but are never promoted as targets
  const modelTargets = {};
  for (const model of allModels) {
    const others = allModels.filter(m => m !== model && !PROMOTER_ONLY.has(m));
    const recentlyPromoted = new Set(history[model] || []);
    const fresh = others.filter(m => !recentlyPromoted.has(m));
    const stale = others.filter(m => recentlyPromoted.has(m));
    const shuffledFresh = fresh.sort(() => Math.random() - 0.5);
    const shuffledStale = stale.sort(() => Math.random() - 0.5);
    const pool = [...shuffledFresh, ...shuffledStale];
    const targets = pool.slice(0, MASS_DM_WINDOWS_UTC.length);
    
    const prevHistory = history[model] || [];
    history[model] = [...targets, ...prevHistory].slice(0, others.length);
    modelTargets[model] = targets;
  }
  
  // Build schedule: for each window, space all models evenly across the hour
  // interval = 60 minutes / number of models
  const intervalMinutes = 60 / allModels.length;
  const schedule = {};
  
  // Shuffle model order for each window so the same model isn't always first
  for (let windowIdx = 0; windowIdx < MASS_DM_WINDOWS_UTC.length; windowIdx++) {
    const window = MASS_DM_WINDOWS_UTC[windowIdx];
    const shuffledModels = [...allModels].sort(() => Math.random() - 0.5);
    
    for (let modelIdx = 0; modelIdx < shuffledModels.length; modelIdx++) {
      const model = shuffledModels[modelIdx];
      const target = modelTargets[model]?.[windowIdx];
      if (!target) continue;
      
      // Space evenly: model 0 at :00, model 1 at :02, model 2 at :04, etc.
      const offsetMinutes = Math.round(modelIdx * intervalMinutes);
      
      const scheduled = new Date(now);
      scheduled.setUTCHours(window.startHour, offsetMinutes, 0, 0);
      
      // Windows 0 and 2 (startHour 0, 2) are actually next UTC day for AST evening
      if (window.startHour < 4) {
        scheduled.setUTCDate(scheduled.getUTCDate() + 1);
      }
      
      const vaultId = vaultMappings[model]?.[target];
      
      if (!schedule[model]) schedule[model] = [];
      schedule[model].push({
        target,
        windowIndex: windowIdx,
        scheduledTime: scheduled.toISOString(),
        vaultId: vaultId || null,
        executed: false,
        failed: false,
        sentAt: null,
      });
    }
  }
  
  // Save schedule and history
  await redis.set('s4s:mass-dm-schedule', { date: todayStr, schedule });
  await redis.set('s4s:mass-dm-history', history);
  
  const totalDms = Object.values(schedule).reduce((sum, entries) => sum + entries.length, 0);
  console.log(`📨 Mass DM schedule generated: ${allModels.length} models × ${intervalMinutes.toFixed(1)} min intervals × ${MASS_DM_WINDOWS_UTC.length} windows = ${totalDms} DMs for ${todayStr}`);
}

// "NEW SFS Exclude" list IDs per account (username → list ID)
// "NEW SFS Exclude" list IDs per account — using POPULATED lists (verified 2026-02-11)
const SFS_EXCLUDE_LISTS = {
  "skyyroseee": "1261988346",       // 0 users (team hasn't populated yet)
  "yourrfavblondie": "1261988351",   // 0 users
  "thesarasky": "1261988365",       // 21 users ✅
  "chelseapaige": "1261988375",     // 0 users
  "dollyrhodesss": "1261988388",    // 4 users ✅
  "lilyyymonroee": "1261498701",    // 14 users ✅ (was 1261988410)
  "lindamarievip": "1260524216",    // 9 users ✅ (was 1261988425)
  "laceythomass": "1260552953",     // 15 users ✅ (was 1261988445)
  "kaliblakexo": "1261524694",      // 4 users ✅ (was 1261988476)
  "jessicaparkerrr": "1261988558",  // 31 users ✅ (was 1261988499)
  "tyybabyy": "1261988505",        // 6 users ✅
  "itsmealexisrae": "1261988522",   // 13 users ✅
  "lolaxmae": "1261988531",         // 0 users
  "rebeccabrownn": "1262027725",    // 2 users ✅ (was 1261988546)
  "oliviabrookess": "1261988558",   // shared ID with jessicaparkerrr?
  "milliexhart": "1256700429",      // 7 users ✅ (was 1261988563)
  "zoepriceee": "1262020857",       // 0 users (was 1261988574)
  "novaleighh": "1257095557",       // 34 users ✅ (was 1261988587)
  "lucymonroee": "1258839857",      // 14 users ✅ (was 1261988600)
  "chloecookk": "1261988618",       // not in our accounts
  "jackiesmithh": "1260548516",     // 10 users ✅ (was 1261988627)
  "brookeewest": "1262020881",      // 2 users ✅ (was 1261988637)
  "ayaaann": "1261988660",          // not in our accounts
  "chloeecavalli": "1262020825",    // 0 users (was 1261988667)
  "sadieeblake": "1262020580",      // 0 users (was 1261988675)
  "lolasinclairr": "1261988697",    // 9 users ✅
  "maddieharperr": "1256821855",    // 12 users ✅ (was 1261988712)
  "zoeemonroe": "1262020818",       // 0 users (was 1261988718)
  "biancaawoods": "1262025288",     // 4 users ✅ (was 1261988726)
  "aviannaarose": "1256700115",     // 10 users ✅ (was 1261988737)
};

async function sendMassDm(promoterUsername, targetUsername, vaultId, accountId) {
  const caption = getMassDmCaption(targetUsername);
  
  try {
    const excludeListId = SFS_EXCLUDE_LISTS[promoterUsername];
    const body = {
      text: caption,
      mediaFiles: [vaultId],
      userLists: ['fans', 'following'],
      ...(excludeListId ? { excludedLists: [excludeListId] } : {}),
    };
    
    const res = await fetch(`${OF_API_BASE}/${accountId}/mass-messaging`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OF_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });
    
    if (res.status === 429) {
      console.log(`⏳ Rate limited on mass DM ${promoterUsername} → @${targetUsername}, retrying in 15s...`);
      await new Promise(r => setTimeout(r, 15000));
      
      const retry = await fetch(`${OF_API_BASE}/${accountId}/mass-messaging`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${OF_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
      });
      
      if (!retry.ok) {
        const err = await retry.text();
        console.error(`❌ Mass DM retry failed ${promoterUsername} → @${targetUsername}: ${err}`);
        return false;
      }
      
      const retryData = await retry.json();
      const retryQueueId = retryData?.data?.[0]?.id || retryData?.id || null;
      console.log(`📨 Mass DM sent (after retry): ${promoterUsername} → @${targetUsername} (queue: ${retryQueueId})`);
      return { success: true, queueId: retryQueueId };
    }
    
    if (!res.ok) {
      const err = await res.text();
      console.error(`❌ Mass DM failed ${promoterUsername} → @${targetUsername}: ${err}`);
      return false;
    }
    
    const data = await res.json();
    const queueId = data?.data?.[0]?.id || data?.id || null;
    console.log(`📨 Mass DM sent: ${promoterUsername} → @${targetUsername} (queue: ${queueId})`);
    return { success: true, queueId };
  } catch (e) {
    console.error(`❌ Mass DM error ${promoterUsername} → @${targetUsername}:`, e);
    return false;
  }
}

let massDmProcessing = false;

async function processMassDmSchedule() {
  if (massDmProcessing) return; // Prevent overlapping runs
  massDmProcessing = true;
  
  try {
    await _processMassDmScheduleInner();
  } finally {
    massDmProcessing = false;
  }
}

async function _processMassDmScheduleInner() {
  const enabled = await redis.get('s4s:mass-dm-enabled');
  console.log(`📨 Mass DM cron tick — enabled=${enabled} (type: ${typeof enabled})`);
  if (enabled === false) return;
  
  const data = await redis.get('s4s:mass-dm-schedule');
  if (!data || !data.schedule) {
    console.log(`📨 Mass DM: no schedule data — data=${!!data}, schedule=${!!(data && data.schedule)}`);
    return;
  }
  
  const now = Date.now();
  console.log(`📨 Mass DM: loading accounts...`);
  const accountMap = await loadModelAccounts();
  console.log(`📨 Mass DM: loaded ${Object.keys(accountMap).length} accounts, loading vault...`);
  const vaultMappings = await loadVaultMappings();
  console.log(`📨 Mass DM: loaded vault, processing ${Object.keys(data.schedule).length} models`);
  
  let modified = false;
  let sentCount = 0;
  
  for (const [model, entries] of Object.entries(data.schedule)) {
    const accountId = accountMap[model];
    if (!accountId) continue;
    
    for (const entry of entries) {
      if (entry.executed || entry.failed) continue;
      
      // Parse scheduled time (stored as ISO string)
      const scheduledMs = new Date(entry.scheduledTime).getTime();
      
      // Not yet time — skip
      if (scheduledMs > now) continue;
      
      // If more than 5 minutes past the scheduled time, skip it (no catch-up)
      const overdueMs = now - scheduledMs;
      if (overdueMs > 5 * 60 * 1000) {
        entry.executed = true;
        entry.failed = true;
        entry.error = 'missed_window';
        modified = true;
        continue;
      }
      
      // Resolve vault ID if not cached
      const vaultId = entry.vaultId || vaultMappings[model]?.[entry.target];
      if (!vaultId) {
        console.log(`⚠️ No vault ID for mass DM: ${model} → ${entry.target}`);
        entry.failed = true;
        entry.error = 'no_vault_id';
        modified = true;
        continue;
      }
      
      const result = await sendMassDm(model, entry.target, vaultId, accountId);
      const success = result && result.success;
      const queueId = result && result.queueId;
      entry.executed = true;
      entry.sentAt = new Date().toISOString();
      entry.queueId = queueId || null;
      modified = true;
      
      if (success) {
        sentCount++;
        // Track in sent log with queue ID for unsending
        try {
          const sentLog = await redis.get('s4s:mass-dm-sent') || [];
          sentLog.push({
            promoter: model,
            target: entry.target,
            accountId,
            queueId: queueId || null,
            sentAt: entry.sentAt,
            success: true,
          });
          // Keep last 500 entries
          await redis.set('s4s:mass-dm-sent', sentLog.slice(-500));
        } catch (e) { /* best effort */ }
      } else {
        entry.failed = true;
        entry.error = 'api_error';
        // Track failure
        try {
          const sentLog = await redis.get('s4s:mass-dm-sent') || [];
          sentLog.push({
            promoter: model,
            target: entry.target,
            sentAt: entry.sentAt,
            success: false,
          });
          await redis.set('s4s:mass-dm-sent', sentLog.slice(-500));
        } catch (e) { /* best effort */ }
      }
      
      // 10 second delay between sends
      await new Promise(r => setTimeout(r, 10000));
    }
  }
  
  if (modified) {
    await redis.set('s4s:mass-dm-schedule', data);
  }
  
  if (sentCount > 0) {
    console.log(`📨 Processed ${sentCount} mass DMs this cycle`);
  }
}

// Mass DM cron: every minute
let massDmLastError = null;
let massDmCronRuns = 0;
cron.schedule('* * * * *', async () => {
  massDmCronRuns++;
  try {
    await processMassDmSchedule();
  } catch (e) {
    massDmLastError = { message: e.message, stack: e.stack, at: new Date().toISOString() };
    console.error('❌ Mass DM cron error:', e);
  }
});

// Generate mass DM schedule at midnight UTC (8pm AST)
cron.schedule('0 0 * * *', async () => {
  const enabled = await redis.get('s4s:mass-dm-enabled');
  if (enabled === false) return;
  await generateMassDmSchedule();
});

// === MASS DM ENDPOINTS ===

app.get('/mass-dm', async (req, res) => {
  const enabled = await redis.get('s4s:mass-dm-enabled');
  const data = await redis.get('s4s:mass-dm-schedule');
  const sentLog = await redis.get('s4s:mass-dm-sent') || [];
  
  let todaySent = 0, todayPending = 0, todayFailed = 0, todayTotal = 0;
  if (data && data.schedule) {
    for (const entries of Object.values(data.schedule)) {
      for (const e of entries) {
        todayTotal++;
        if (e.executed && !e.failed) todaySent++;
        else if (e.failed) todayFailed++;
        else todayPending++;
      }
    }
  }
  
  const lastSent = sentLog.length > 0 ? sentLog[sentLog.length - 1].sentAt : null;
  
  res.json({
    enabled: enabled !== false,
    date: data?.date || null,
    todaySent,
    todayPending,
    todayFailed,
    todayTotal,
    lastSent,
    models: data?.schedule ? Object.keys(data.schedule).length : 0,
    schedule: '12 windows, ' + (data?.schedule ? Object.keys(data.schedule).length : 0) + ' models',
    debug: {
      cronRuns: massDmCronRuns,
      lastError: massDmLastError,
      processing: massDmProcessing,
    }
  });
});

app.get('/mass-dm/schedule', async (req, res) => {
  const data = await redis.get('s4s:mass-dm-schedule');
  if (!data || !data.schedule) {
    return res.json({ date: null, schedule: {} });
  }
  
  const now = Date.now();
  const summary = {};
  for (const [model, entries] of Object.entries(data.schedule)) {
    summary[model] = entries.map(e => ({
      target: e.target,
      scheduledTime: e.scheduledISO,
      status: e.failed ? 'failed' : e.executed ? 'sent' : (new Date(e.scheduledTime).getTime() <= now ? 'ready' : 'pending'),
      sentAt: e.sentAt,
      error: e.error || null,
    }));
  }
  
  res.json({ date: data.date, schedule: summary });
});

app.post('/mass-dm/enable', async (req, res) => {
  await redis.set('s4s:mass-dm-enabled', true);
  
  // Always regenerate schedule on enable so timing starts fresh from NOW
  await generateMassDmSchedule();
  
  const data = await redis.get('s4s:mass-dm-schedule');
  const totalDms = data ? Object.values(data.schedule).reduce((sum, entries) => sum + entries.length, 0) : 0;
  
  // Count how many are still in the future
  const now = Date.now();
  let futureDms = 0;
  if (data?.schedule) {
    for (const entries of Object.values(data.schedule)) {
      for (const e of entries) {
        if (new Date(e.scheduledTime).getTime() > now) futureDms++;
      }
    }
  }
  
  res.json({ enabled: true, message: 'Mass DM system enabled', totalDms, futureDms });
});

app.post('/mass-dm/test-cron', async (req, res) => {
  console.log('📨 Manual cron test triggered');
  try {
    await _processMassDmScheduleInner();
    res.json({ ok: true, message: 'Cron function ran' });
  } catch (e) {
    res.json({ ok: false, error: e.message, stack: e.stack });
  }
});

// Unsend all tracked mass DMs
app.post('/mass-dm/unsend-all', async (req, res) => {
  const sentLog = await redis.get('s4s:mass-dm-sent') || [];
  const toUnsend = sentLog.filter(e => e.success && e.queueId && e.accountId);
  let unsent = 0;
  let failed = 0;
  
  for (const entry of toUnsend) {
    try {
      const delRes = await fetch(`${OF_API_BASE}/${entry.accountId}/mass-messaging/${entry.queueId}`, {
        method: 'DELETE',
        headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
      });
      if (delRes.ok) {
        unsent++;
        console.log(`🗑️ Unsent mass DM: ${entry.promoter} → @${entry.target} (queue: ${entry.queueId})`);
      } else {
        failed++;
        const err = await delRes.text();
        console.log(`⚠️ Failed to unsend ${entry.queueId}: ${err.slice(0, 100)}`);
      }
    } catch (e) {
      failed++;
    }
    await new Promise(r => setTimeout(r, 300));
  }
  
  res.json({ unsent, failed, total: toUnsend.length, message: `Unsent ${unsent} mass DMs` });
});

// Get sent log with queue IDs
app.get('/mass-dm/sent', async (req, res) => {
  const sentLog = await redis.get('s4s:mass-dm-sent') || [];
  res.json({ 
    total: sentLog.length,
    withQueueId: sentLog.filter(e => e.queueId).length,
    entries: sentLog.slice(-50) 
  });
});

app.post('/mass-dm/disable', async (req, res) => {
  await redis.set('s4s:mass-dm-enabled', false);
  res.json({ enabled: false, message: 'Mass DM system disabled — pending DMs will not be sent' });
});

app.post('/mass-dm/run', async (req, res) => {
  const enabled = await redis.get('s4s:mass-dm-enabled');
  if (enabled === false) {
    return res.json({ error: 'Mass DM system is disabled' });
  }
  
  const data = await redis.get('s4s:mass-dm-schedule');
  if (!data || !data.schedule) {
    return res.json({ error: 'No schedule found — enable first' });
  }
  
  const accountMap = await loadModelAccounts();
  const vaultMappings = await loadVaultMappings();
  const now = Date.now();
  
  // Find next pending DM (even if not yet due, for testing)
  for (const [model, entries] of Object.entries(data.schedule)) {
    for (const entry of entries) {
      if (entry.executed || entry.failed) continue;
      
      const accountId = accountMap[model];
      const vaultId = entry.vaultId || vaultMappings[model]?.[entry.target];
      
      if (!accountId || !vaultId) continue;
      
      const success = await sendMassDm(model, entry.target, vaultId, accountId);
      entry.executed = true;
      entry.sentAt = new Date().toISOString();
      if (!success) {
        entry.failed = true;
        entry.error = 'manual_run_failed';
      }
      
      await redis.set('s4s:mass-dm-schedule', data);
      
      return res.json({
        sent: true,
        promoter: model,
        target: entry.target,
        success,
        scheduledTime: entry.scheduledISO,
      });
    }
  }
  
  res.json({ sent: false, message: 'No pending DMs to send' });
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
  
  // Generate mass DM schedule if needed
  const massDmEnabled = await redis.get('s4s:mass-dm-enabled');
  if (massDmEnabled !== false) {
    const massDmData = await redis.get('s4s:mass-dm-schedule');
    const todayStr = new Date().toISOString().slice(0, 10);
    if (!massDmData || massDmData.date !== todayStr) {
      await generateMassDmSchedule();
    } else {
      console.log(`   Mass DM schedule already exists for ${todayStr}`);
    }
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
  const pinnedState = await getPinnedState();
  const pinnedEnabled = await redis.get('s4s:pinned-enabled');
  
  // Mass DM stats
  const massDmEnabled = await redis.get('s4s:mass-dm-enabled');
  const massDmData = await redis.get('s4s:mass-dm-schedule');
  const massDmSentLog = await redis.get('s4s:mass-dm-sent') || [];
  let massDmSent = 0, massDmPending = 0, massDmTotal = 0;
  if (massDmData && massDmData.schedule) {
    for (const entries of Object.values(massDmData.schedule)) {
      for (const e of entries) {
        massDmTotal++;
        if (e.executed && !e.failed) massDmSent++;
        else if (!e.executed && !e.failed) massDmPending++;
      }
    }
  }
  const massDmLastSent = massDmSentLog.length > 0 ? massDmSentLog[massDmSentLog.length - 1].sentAt : null;

  res.json({
    isRunning,
    stats: rotationState.stats,
    modelsActive: Object.keys(rotationState.dailySchedule).length,
    pendingDeletes: pending.length,
    massDm: {
      enabled: massDmEnabled !== false,
      todaySent: massDmSent,
      todayPending: massDmPending,
      todayTotal: massDmTotal,
      lastSent: massDmLastSent,
      schedule: '12 windows, ' + (massDmData?.schedule ? Object.keys(massDmData.schedule).length : 0) + ' models',
    },
    pinned: {
      enabled: pinnedEnabled !== false,
      activePosts: (pinnedState.activePosts || []).length,
      lastRun: pinnedState.lastRun,
      featuredGirls: pinnedState.featuredGirls || [],
      dayIndex: pinnedState.dayIndex || 0
    },
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

// === PINNED POST ENDPOINTS ===

app.get('/pinned', async (req, res) => {
  const state = await getPinnedState();
  const enabled = await redis.get('s4s:pinned-enabled');
  res.json({
    enabled: enabled !== false,
    ...state,
    activeCount: (state.activePosts || []).length
  });
});

app.post('/pinned/run', async (req, res) => {
  console.log('📌 Manual pinned post trigger...');
  const result = await runPinnedPostRotation();
  res.json(result);
});

app.post('/pinned/remove', async (req, res) => {
  console.log('📌 Manual pinned post removal...');
  const result = await removePinnedPosts();
  res.json(result);
});

app.post('/pinned/enable', async (req, res) => {
  await redis.set('s4s:pinned-enabled', true);
  res.json({ enabled: true, message: 'Pinned posts enabled — will run at 6am AST' });
});

app.post('/pinned/disable', async (req, res) => {
  await redis.set('s4s:pinned-enabled', false);
  res.json({ enabled: false, message: 'Pinned posts disabled — existing pins stay up until they expire' });
});

app.get('/health', (req, res) => {
  res.json({ ok: true, timestamp: new Date().toISOString() });
});

// === START SERVER ===

app.listen(PORT, async () => {
  console.log(`🚀 S4S Rotation Service running on port ${PORT}`);
  console.log(`   POST /start         - Start rotation`);
  console.log(`   POST /stop          - Stop rotation (cleans up ghost tags, keeps pins)`);
  console.log(`   POST /schedule      - Receive schedule from app`);
  console.log(`   POST /cleanup       - Force delete all pending ghost tags`);
  console.log(`   GET  /schedule      - View upcoming tags`);
  console.log(`   GET  /stats         - View statistics`);
  console.log(`   GET  /active        - View active (undeleted) posts`);
  console.log(`   GET  /pinned        - View pinned post status`);
  console.log(`   POST /pinned/run    - Manually trigger pinned posts now`);
  console.log(`   POST /pinned/remove - Force-remove all pinned posts`);
  console.log(`   POST /pinned/enable - Enable daily pinned rotation`);
  console.log(`   POST /pinned/disable- Disable (existing pins stay up)`);
  console.log(`   GET  /mass-dm       - Mass DM status & stats`);
  console.log(`   GET  /mass-dm/schedule - View today's full schedule`);
  console.log(`   POST /mass-dm/enable  - Enable mass DM system`);
  console.log(`   POST /mass-dm/disable - Disable mass DM system`);
  console.log(`   POST /mass-dm/run     - Manually trigger next pending DM`);
  
  // Run startup recovery
  await startupRecovery();
});
