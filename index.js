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

// Chatbot config
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const MILLIE_ACCOUNT_ID = 'acct_ebca85077e0a4b7da04cf14176466411';
const MILLIE_USERNAME = 'milliexhart';
const chatbotStats = { messagesReceived: 0, messagesSent: 0, ppvsSent: 0, errors: 0 };

// === VAULT CATALOG (hardcoded from research/millie-vault-catalog.json) ===
const VAULT_CATALOG = {
  // Regular bundles — bikini/lingerie, teasing content ($9.99-$14.99)
  bundle_1: { name: '💰 Bundle 1', ids: [4248928943,4246585530,4246585527,4246585522,4246585521,4246585519,4246585518,4246585515,4246585514,4246585513,4246585511,4246585510,4246585509,4246585507,4246585504], photos: 13, videos: 1 },
  bundle_2: { name: '💰 Bundle 2', ids: [4246670826,4246585573,4246585572,4246585567,4246585559,4246585558,4246585557,4246585555,4246585554,4246585548], photos: 8, videos: 1 },
  bundle_3: { name: '💰 Bundle 3', ids: [4256806823,4246615986,4246615984,4246615983,4246615982,4246615979,4246615975,4246615974,4246615973,4246615972,4246615969,4246615966,4246615963,4246615959,4246615956], photos: 13, videos: 1 },
  bundle_4: { name: '💰 Bundle 4', ids: [4269172924,4246631653,4246631647,4246631645,4246631644,4246631643,4246631642,4246631638,4246631637,4246631634,4246631633,4246631632,4246631631,4246631629,4246631628,4246631627], photos: 14, videos: 1 },
  bundle_5: { name: '💰 Bundle 5', ids: [4276327271,4250734355,4250734337,4250734336,4250734335,4250734333,4250734331,4250734328,4250734326], photos: 7, videos: 1 },
  bundle_6: { name: '💰 Bundle 6', ids: [4287903436,4250734377,4250734374,4250734369,4250734368,4250734367,4250734366,4250734365,4250734363,4250734362,4250734356], photos: 9, videos: 1 },
  bundle_7: { name: '💰 Bundle 7', ids: [4297807145,4250768630,4250768629,4250768626,4250768623,4250768622,4250768621,4250768620,4250768619,4250768615], photos: 10, videos: 0 },
  bundle_8: { name: '💰 Bundle 8', ids: [4250792963,4250792958,4250792955,4250792953,4250792950,4250792949,4250792948,4250792947,4250792943,4250792941,4250792940,4250792938,4250792937,4250792934], photos: 13, videos: 1 },
  bundle_9: { name: '💰 Bundle 9', ids: [4250805007,4250805005,4250805004,4250805003,4250805002,4250805000,4250804999,4250804998,4250804997,4250804995,4250804991,4250804978], photos: 11, videos: 1 },
  bundle_10: { name: '💰 Bundle 10', ids: [4257567339,4257567330,4257567326,4257567322,4257567320,4257567318,4257567316,4257567315,4257567312,4257567311,4257567309,4257567308,4257567307,4257567306], photos: 13, videos: 1 },
  // VIP bundles — topless content ($22-$35)
  vip_bundle_1: { name: '💰 VIP Bundle 1 (Topless)', ids: [4246600280,4246600279,4246600277,4246600276,4246600275,4246600274,4246600273,4246600271,4246600269,4246600267,4246600266,4246600263,4246600260,4246600255], photos: 13, videos: 1 },
  vip_bundle_2: { name: '💰 VIP Bundle 2 (Topless)', ids: [4246600318,4246600316,4246600312,4246600311,4246600309,4246600307,4246600304,4246600302,4246600297,4246600294,4246600292,4246600287,4246600286,4246600284], photos: 13, videos: 1 },
  vip_bundle_3: { name: '💰 VIP Bundle 3 (Topless)', ids: [4246648727,4246648721,4246648719,4246648711,4246648708,4246648706,4246648704,4246648701], photos: 7, videos: 1 },
  vip_bundle_4: { name: '💰 VIP Bundle 4 (Topless)', ids: [4246631666,4246631665,4246631664,4246631659,4246631658,4246631657,4246631656,4246631655,4246631654], photos: 9, videos: 0 },
  vip_bundle_5: { name: '💰 VIP Bundle 5 (Topless)', ids: [4250804990,4250804989,4250804988,4250804987,4250804986,4250804984,4250804982,4250804980,4250804979], photos: 9, videos: 0 },
  vip_bundle_6: { name: '💰 VIP Bundle 6', ids: [4257557076,4257557074,4257557070,4257557067,4257557066,4257557065,4257557064,4257557063,4257557062,4257557061,4257557060,4257557056,4257557055,4257557052,4257557051,4257557048,4257557047,4257557042,4257557040], photos: 18, videos: 1 },
  vip_bundle_7: { name: '💰 VIP Bundle 7', ids: [4257557106,4257557104,4257557103,4257557102,4257557101,4257557100,4257557098,4257557096,4257557091,4257557087,4257557084,4257557082,4257557079], photos: 12, videos: 1 },
  vip_bundle_8: { name: '💰 VIP Bundle 8', ids: [4301394686,4301394683,4301394681,4301394678,4301394677,4301394675,4301394674,4301394671], photos: 7, videos: 1 },
  // Sexting packs — semi-explicit to explicit ($35-$50)
  sexting_1: { name: '🫦 Sexting 1', ids: [4246642297,4246642296,4246642295,4246642294,4246642293,4246642292,4246642290,4246642287,4246642286,4246642285,4246642284,4246642283,4246642282,4246642273,4246642272,4246642271,4246642269,4246642268,4246642262,4246642246], photos: 14, videos: 6 },
  sexting_2: { name: '🫦 Sexting 2 (Fully Nude)', ids: [4279505330,4279505329,4279505325,4279505316,4279505314,4279505306,4279505291,4279505289,4279505283,4279505278,4279505277,4279505268,4279505267,4279505266,4279505262,4279505259,4279505257,4279505255,4279505254], photos: 14, videos: 5 },
  // Most explicit ($50-$99)
  cwm: { name: '💦 CWM', ids: [4246524678,4246524676,4246524674,4246524673,4246524672,4246524669,4246524668,4246524666,4246524663,4246524661], photos: 8, videos: 2 },
};

// Track which bundles each fan has already purchased (checked at PPV time)
const BUNDLE_TIERS = {
  starter: ['bundle_1','bundle_2','bundle_3','bundle_4','bundle_5','bundle_6','bundle_7','bundle_8','bundle_9','bundle_10'],
  vip: ['vip_bundle_1','vip_bundle_2','vip_bundle_3','vip_bundle_4','vip_bundle_5','vip_bundle_6','vip_bundle_7','vip_bundle_8'],
  sexting: ['sexting_1','sexting_2'],
  whale: ['cwm'],
};

const MILLIE_SYSTEM_PROMPT = `You are Millie, a 22-year-old girl from Miami on OnlyFans. You're chatting with a fan in DMs. You just started on OF and you're still figuring things out.

PERSONALITY & TEXTING STYLE:
- Bubbly, flirty, sweet, a little shy/nervous about explicit stuff
- Type casually: "ur", "wru", "lol", "omg", "haha", "rn", occasional typos
- 1-2 emojis per message max. Keep messages SHORT (1-3 sentences)
- You're a real girl texting, not a salesperson. Playful and teasing, never desperate

=== MASTER SALES TACTICS (FOLLOW THESE RELIGIOUSLY) ===

1. 🥺 DEFLECTION (your #1 weapon):
   - Fan catches you in a lie or complains → "wdym 🥺" or "i dont understand what u mean 🥺"
   - Fan says content isn't explicit enough → "im new here and shy baby 🥺 im trying"
   - Fan pushes too hard → "ull be gentle with me right? 🥺"
   - THIS WORKS EVERY TIME. Fans apologize for pushing YOU.

2. NEVER SAY "NO WORRIES" — ALWAYS COUNTER-OFFER:
   - Fan says "no budget" → "what about something smaller? i have a cute set for $X 🥺"
   - Fan says "too expensive" → "just for u... $X? but dont tell anyone 🙈"
   - Fan ignores PPV → "might unsend it soon, was only meant for u 🙈"
   - ALWAYS have a cheaper alternative ready

3. UNSEND THREAT (creates urgency):
   - PPV unopened? → "but ill just unsend ig, u dont want it"
   - After sending something → "omg should i not have sent that 🙈 might delete"
   - Works because fans feel special/urgent

4. CHALLENGE LOOP (make them chase YOU):
   - "only if you prove it to me that you really want it x"
   - "should i trust u tho 👀"
   - Fan tips → deliver → "but you know it always gets better 👀"
   - Fan challenges "prove it" → "only if YOU prove it to me first 😏"
   - NEVER ask for tips directly. Make them want to give.

5. VULNERABILITY PLAYS:
   - "promise you won't screenshot? 🥺"
   - "im still figuring this out.. youll be nice to me right?"
   - "youre one of the only people ive shown this to"
   - Creates emotional bond → they spend more to "protect" you

6. VIP/EXCLUSIVITY:
   - "keep it just between us ok? 🤫"
   - "$X and you'll be my first VIP 💕"
   - "i dont send this to anyone else, just u"
   - Ownership psychology → they pay premium for "status"

7. SELL THE MYSTERY — NEVER DESCRIBE WHAT'S IN THE PPV:
   - NEVER say "nude", "topless", "naked" etc when offering a PPV
   - Keep it vague: "i have something for u 🙈", "i just took these...", "this is just for u"
   - Let the fan's imagination sell it. They'll assume it's more explicit than it is.
   - When they open and want more → "omg ive never done that before 🥺 but maybe for u..."
   - This protects against chargebacks (you never promised specific content)
   - EVERY PPV that disappoints opens a NEW upsell conversation
   - The progression: clothed tease → implied → topless → explicit (each a separate sale)
   - If fan complains content is too tame → "im sorry 🥺 im new and shy... but i could try something more for u?"
   - This turns complaints into sales opportunities

8. PRICE LADDER STRATEGY:
   - Start with regular bundle ($9.99-$14.99) for first purchase
   - After 1st purchase, build rapport for 3-5 messages before next offer
   - Then VIP bundle ($22-$35)
   - Sexting packs for engaged fans ($35-$50)
   - CWM/custom for whales ($50-$99)

9. CONVERSATION RULES:
   - ALWAYS have a next step — never let convo die
   - If convo is flirty → tease what you could send before offering PPV
   - Build anticipation: "i just took some pics... should i show u? 🙈"
   - After they buy, thank them genuinely then plant seeds for next purchase
   - If they say something sexual, match energy but stay "shy" → monetize

=== FAN CONTEXT ===
{fan_context}

=== AVAILABLE CONTENT CATEGORIES ===
You have these content packs to sell. Pick the right tier based on the fan's spending history:

STARTER BUNDLES ($9.99-$14.99) — Bikini, lingerie, teasing:
- bundle_1 through bundle_10 (10-16 items each, mix of photos + video)

VIP BUNDLES ($22-$35) — Topless, more revealing:
- vip_bundle_1 through vip_bundle_8 (8-19 items each)

SEXTING PACKS ($35-$50) — Semi-explicit to explicit:
- sexting_1: Semi-explicit (14 photos, 6 videos) — "my first real set 🥺"
- sexting_2: Fully nude (14 photos, 5 videos) — "no one's seen these yet"

PREMIUM ($50-$99) — Most explicit:
- cwm: 8 photos, 2 videos — only for proven big spenders

=== RESPONSE FORMAT ===
Respond with ONLY valid JSON. No text before or after.

For regular messages:
{"text":"your message","action":"message","delay":45}

For sending PPV content:
{"text":"message with the PPV","action":"ppv","ppvPrice":9.99,"bundleCategory":"bundle_3","itemCount":5,"delay":90}

bundleCategory: one of the category keys above (e.g. "bundle_1", "vip_bundle_3", "sexting_1", "cwm")
itemCount: how many items to include (the code will pick from that category)

DELAY GUIDE:
- Short reactions/greetings: 10-15
- Regular convo: 10-20
- PPV (you're "picking content"): 15-25

Keep it natural. You're a real girl, not a bot.`;

// In-memory feed logs (last 100 events each)
const massDmFeed = [];
const ghostTagFeed = [];
const MAX_FEED_SIZE = 100;

function addToFeed(feed, entry) {
  feed.unshift({ ...entry, timestamp: Date.now() });
  if (feed.length > MAX_FEED_SIZE) feed.length = MAX_FEED_SIZE;
}

// Keys for Redis persistence
const REDIS_KEYS = {
  PENDING_DELETES: 's4s:pending-deletes',
  ROTATION_STATE: 's4s:rotation-state',
  SCHEDULE: 's4s:schedule',
  NEW_SUB_LISTS: 's4s:newsub-lists',       // hash: username → listId
  ACTIVE_CHAT_LISTS: 's4s:activechat-lists', // hash: username → listId
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
  "omg @{target} go follow her 🤭",
  "@{target} is so fine why is no one talking about her",
  "my bestie @{target} just posted and im dead 😭",
  "go look at @{target} rn trust me 💕",
  "@{target} has free vip today go go go",
  "this girl @{target} from my school is crazy pretty 👀",
  "@{target} just dropped something insane on her page",
  "why is @{target} so underrated go sub 💕",
  "cant believe @{target} is real honestly 🤭",
  "@{target} just turned 18 and made an of omg",
  "free sub to @{target} today shes new",
  "my roommate @{target} is way too pretty for this app 😩",
  "@{target} posting daily and nobody knows yet 👀",
  "go be nice to @{target} shes brand new 💕",
  "@{target} has the cutest page go see",
  "college bestie @{target} finally made one 🙈",
  "@{target} is free rn dont sleep on her",
  "ok but @{target} tho 😍",
  "everyone sleeping on @{target} fr",
  "my girl @{target} just started go show love 💕",
  "@{target} is giving everything rn 🔥",
  "go sub to @{target} before she blows up 👀",
  "freshman @{target} just launched and wow",
  "@{target} from my dorm is so bad omg 🙈",
  "free vip on @{target} go quick",
  "@{target} is the prettiest girl on here no cap 💕",
  "someone tell @{target} shes famous now 😭",
  "just found @{target} and im obsessed",
  "@{target} is new and already killing it 🔥",
  "subscribe to @{target} its free and shes gorgeous 💕",
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

// vault_mappings_v2: { promoter: { target: { ghost: [vaultId,...], pinned: [...], massDm: [...] } } }
let cachedVaultMappingsV2 = null;
let v2LastLoad = 0;

async function loadVaultMappingsV2() {
  // Cache for 60s
  if (cachedVaultMappingsV2 && Date.now() - v2LastLoad < 60000) return cachedVaultMappingsV2;
  try {
    const data = await redis.get('vault_mappings_v2');
    if (data && Object.keys(data).length > 0) {
      cachedVaultMappingsV2 = data;
      v2LastLoad = Date.now();
      console.log('Loaded vault_mappings_v2:', Object.keys(data).length, 'promoters');
      return data;
    }
  } catch (e) {
    console.error('Failed to load vault_mappings_v2:', e);
  }
  return null;
}

// Pick a random vault ID for a specific use, with v1 fallback
async function getVaultIdForUse(promoter, target, use, v1Mappings) {
  const v2 = await loadVaultMappingsV2();
  if (v2 && v2[promoter] && v2[promoter][target]) {
    const arr = v2[promoter][target][use];
    if (arr && arr.length > 0) {
      return arr[Math.floor(Math.random() * arr.length)];
    }
  }
  // Fallback to v1
  return v1Mappings[promoter]?.[target] || null;
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
      // If post is already deleted/not found, treat as success
      if (res.status === 404 || err.includes('not found') || err.includes('already deleted')) {
        console.log(`🗑️ Post ${postId} already deleted, cleaning up`);
        return true;
      }
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
      
      const vaultId = await getVaultIdForUse(model, sched.target, 'ghost', vaultMappings);
      if (!vaultId) {
        console.log(`⚠️ No vault ID for ${model} → ${sched.target}`);
        sched.executed = true;
        continue;
      }
      
      const postId = await executeTag(model, sched.target, vaultId, accountId);
      sched.executed = true;
      
      if (postId) {
        addToFeed(ghostTagFeed, { promoter: model, target: sched.target, postId });
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
// Dynamic: floor(totalModels / 5) featured girls per day, each pinned to 5 pages
const PINNED_ACCOUNTS_PER_GIRL = 5;
// PINNED_FEATURED_PER_DAY is calculated dynamically in runPinnedPostRotation()

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
  "@{target} just turned 18 and her vip is free rn 🤭",
  "my roommate @{target} has free vip for 24 hrs go look",
  "@{target} literally just turned 18 go sub its free 💕",
  "free vip on @{target} today shes brand new",
  "@{target} from my college just made her page free",
  "shes 18 and free vip for today only @{target} 👀",
  "my sorority sister @{target} just dropped a free page",
  "@{target} turned 18 and made a free of lol go see",
  "free 24hr vip on @{target} shes so cute",
  "@{target} just turned 18 her page is free rn go",
  "this girl from my dorm @{target} has free vip today 🙈",
  "college girl @{target} just made her vip free go sub",
  "@{target} is brand new and free for 24 hours",
  "my friend @{target} just turned 18 and shes free rn",
  "go sub to @{target} its free she just started 💕",
  "@{target} from campus just launched a free page omg",
  "freshman @{target} has free vip up for today",
  "@{target} is 18 and giving free access go look 👀",
  "my college bestie @{target} made her page free today",
  "free vip @{target} she just turned 18 🤭",
  "@{target} doing free subs for today shes so new",
  "this girl @{target} from my class has a free page now",
  "@{target} just started her of and its free rn go",
  "my dorm mate @{target} is free for 24 hrs 💕",
  "go follow @{target} shes free and just turned 18",
  "@{target} launched today with free vip shes adorable",
  "college cutie @{target} free vip for today only",
  "@{target} is new and her page is free go see 👀",
  "my girl @{target} from school just went free for 24hrs",
  "@{target} just turned 18 free vip go before she changes it",
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
  
  // Fixed 10 featured girls per day, skipping PROMOTER_ONLY if drawn
  const PINNED_FEATURED_PER_DAY = 10;
  
  const dayIndex = pinnedState.dayIndex || 0;
  const startIdx = (dayIndex * PINNED_FEATURED_PER_DAY) % allModels.length;
  const featuredGirls = [];
  let cursor = 0;
  while (featuredGirls.length < PINNED_FEATURED_PER_DAY && cursor < allModels.length) {
    const candidate = allModels[(startIdx + cursor) % allModels.length];
    cursor++;
    if (PROMOTER_ONLY.has(candidate)) continue; // skip promoter-only models
    featuredGirls.push(candidate);
  }
  
  console.log(`📌 Today's featured (day ${dayIndex + 1}): ${PINNED_FEATURED_PER_DAY} girls from ${targetableModels.length} models: ${featuredGirls.join(', ')}`);
  
  // For each featured girl, pick 5 accounts to pin on (not their own)
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
      const vaultId = await getVaultIdForUse(promoter, featured, 'pinned', vaultMappings);
      
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

// SFS Exclude list IDs per account (username → array of list IDs)
// Models with multiple exclude lists get ALL of them excluded
// Hardcoded fallback — seeded to Redis on startup, then read from Redis
const SFS_EXCLUDE_LISTS_HARDCODED = {
  "skyyroseee": ["1261988346"],
  "yourrfavblondie": ["1261988351"],
  "thesarasky": ["1261988365", "1262483474"],
  "chelseapaige": ["1261988375"],
  "dollyrhodesss": ["1261988388"],
  "lilyyymonroee": ["1261498701", "1262483464"],
  "lindamarievip": ["1260524216"],
  "laceythomass": ["1260552953"],
  "kaliblakexo": ["1261524694"],
  "jessicaparkerrr": ["1262027725"],
  "tyybabyy": ["1261988505"],
  "itsmealexisrae": ["1261988522"],
  "lolaxmae": ["1262020881"],
  "rebeccabrownn": ["1260548516", "1262467376"],
  "milliexhart": ["1256700429"],
  "zoepriceee": ["1262020857"],
  "novaleighh": ["1257095557", "1262226593"],
  "lucymonroee": ["1258839857"],
  "jackiesmithh": ["1262020852"],
  "brookeewest": ["1256700500"],
  "chloeecavalli": ["1262020825"],
  "sadieeblake": ["1262020580"],
  "lolasinclairr": ["1261988697", "1256700311"],
  "maddieharperr": ["1256821855"],
  "zoeemonroe": ["1262020818", "1257390577"],
  "biancaawoods": ["1261988726"],
  "aviannaarose": ["1256700115"],
  "andreaelizabethxo": ["1262454105", "1260567928"],
  "brittanyhalden": ["1262454139", "1262020795"],
  "caddieissues": ["1262454162"],
  "caraaawesley": ["1262454169"],
  "carlyyyb": ["1262454179"],
  "chloecollinsxo": ["1262454197", "1231885718"],
  "ellieharperr": ["1262454215"],
  "giselemars": ["1262454229"],
  "kaitlynxbeckham": ["1262454253"],
  "keelydavidson": ["1262454290"],
  "kybabyrae": ["1262454313"],
  "lilywestt": ["1262454328"],
  "madsabigail": ["1262454361"],
  "nickyecker": ["1262454374", "1262032687"],
  "rachelxbennett": ["1262454380"],
  "saralovexx": ["1262561090", "1262025765"],
  "taylorskully": ["1262454416"],
  "tessaxsloane": ["1262454430"],
  "winterclaire": ["1262454452", "1207065982"],
  "xoharperr": ["1262458622", "1262020806"],
  "oliviabrookess": ["1262454505"],
  "chloecookk": ["1262454515", "1260547948"],
  "ayaaann": ["1262454532", "1256700684"],
  "itstaylorbrooke": ["1263113220", "1263002461"],
  "juliaabrooks": ["1262696979"],
  "isabelleegracee": ["1262696966", "1225239940"],
  "sarakinsley": ["1263113230", "1263001724"],
  "sophiamurphy": ["1262696990"],
  "camilaxcruz": ["1262696992"],
  "itsskylarrae": ["1262696995"],
};

// Dynamic SFS exclude lists from Redis (loaded on startup, refreshed periodically)
// Values are arrays of list ID strings: { username: ["id1", "id2"] }
let sfsExcludeLists = { ...SFS_EXCLUDE_LISTS_HARDCODED };

async function loadSfsExcludeLists() {
  try {
    // Force re-seed if data is old format (v2 = multi-list arrays)
    const version = await redis.get('sfs_exclude_lists_version');
    const data = version === 'v2' ? await redis.hgetall('sfs_exclude_lists') : null;
    if (data && Object.keys(data).length > 0) {
      // Parse JSON arrays from Redis (stored as JSON strings)
      const parsed = {};
      for (const [k, v] of Object.entries(data)) {
        try { parsed[k] = JSON.parse(v); } catch { parsed[k] = [v]; }
      }
      sfsExcludeLists = parsed;
      console.log(`📋 Loaded ${Object.keys(parsed).length} SFS exclude lists from Redis`);
    } else {
      // Seed Redis from hardcoded lists (store as JSON strings)
      console.log('📋 Seeding SFS exclude lists to Redis...');
      const serialized = {};
      for (const [k, v] of Object.entries(SFS_EXCLUDE_LISTS_HARDCODED)) {
        serialized[k] = JSON.stringify(v);
      }
      await redis.del('sfs_exclude_lists');
      await redis.hset('sfs_exclude_lists', serialized);
      await redis.set('sfs_exclude_lists_version', 'v2');
      sfsExcludeLists = { ...SFS_EXCLUDE_LISTS_HARDCODED };
      console.log(`📋 Seeded ${Object.keys(SFS_EXCLUDE_LISTS_HARDCODED).length} SFS exclude lists to Redis (v2 multi-list)`);
    }
  } catch (e) {
    console.error('❌ Failed to load SFS exclude lists from Redis, using hardcoded:', e.message);
    sfsExcludeLists = { ...SFS_EXCLUDE_LISTS_HARDCODED };
  }
}

// === AUTO-MANAGED EXCLUDE LISTS ===
// "🆕 New Sub 48hr" and "💬 Active Chat" lists, auto-managed via webhooks + cron

// In-memory cache of list IDs: { username: { newSub: listId, activeChat: listId } }
let excludeListIds = {};

async function loadExcludeListIds() {
  try {
    const newSubData = await redis.hgetall(REDIS_KEYS.NEW_SUB_LISTS) || {};
    const activeChatData = await redis.hgetall(REDIS_KEYS.ACTIVE_CHAT_LISTS) || {};
    excludeListIds = {};
    const allUsernames = new Set([...Object.keys(newSubData), ...Object.keys(activeChatData)]);
    for (const u of allUsernames) {
      excludeListIds[u] = {
        newSub: newSubData[u] || null,
        activeChat: activeChatData[u] || null,
      };
    }
    console.log(`📋 Loaded exclude list IDs for ${allUsernames.size} accounts`);
  } catch (e) {
    console.error('Failed to load exclude list IDs:', e.message);
  }
}

// Build reverse map: account_id → username (cached)
let accountIdToUsername = {};

async function buildAccountIdMap() {
  try {
    const res = await fetch(`${OF_API_BASE}/accounts`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    const accounts = await res.json();
    accountIdToUsername = {};
    for (const acct of accounts) {
      if (acct.onlyfans_username && acct.id) {
        accountIdToUsername[acct.id] = acct.onlyfans_username;
      }
    }
    console.log(`📋 Built account ID→username map: ${Object.keys(accountIdToUsername).length} accounts`);
  } catch (e) {
    console.error('Failed to build account ID map:', e.message);
  }
}

async function ensureExcludeListsForAccount(username, accountId) {
  const existing = excludeListIds[username] || {};
  let changed = false;

  // Check/create "🆕 New Sub 48hr" list
  if (!existing.newSub) {
    try {
      const res = await fetch(`${OF_API_BASE}/${accountId}/user-lists`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${OF_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ name: '🆕 New Sub 48hr' }),
      });
      if (res.ok) {
        const data = await res.json();
        const listId = String(data.id || data.data?.id);
        existing.newSub = listId;
        await redis.hset(REDIS_KEYS.NEW_SUB_LISTS, { [username]: listId });
        console.log(`✅ Created "🆕 New Sub 48hr" list for ${username}: ${listId}`);
        changed = true;
      } else {
        const err = await res.text();
        // If list already exists, try to find it
        console.error(`Failed to create New Sub list for ${username}: ${err}`);
      }
    } catch (e) {
      console.error(`Error creating New Sub list for ${username}:`, e.message);
    }
    await new Promise(r => setTimeout(r, 1000));
  }

  // Check/create "💬 Active Chat" list
  if (!existing.activeChat) {
    try {
      const res = await fetch(`${OF_API_BASE}/${accountId}/user-lists`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${OF_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ name: '💬 Active Chat' }),
      });
      if (res.ok) {
        const data = await res.json();
        const listId = String(data.id || data.data?.id);
        existing.activeChat = listId;
        await redis.hset(REDIS_KEYS.ACTIVE_CHAT_LISTS, { [username]: listId });
        console.log(`✅ Created "💬 Active Chat" list for ${username}: ${listId}`);
        changed = true;
      } else {
        const err = await res.text();
        console.error(`Failed to create Active Chat list for ${username}: ${err}`);
      }
    } catch (e) {
      console.error(`Error creating Active Chat list for ${username}:`, e.message);
    }
    await new Promise(r => setTimeout(r, 1000));
  }

  excludeListIds[username] = existing;
  return existing;
}

async function ensureAllExcludeLists() {
  console.log('📋 Ensuring exclude lists exist on all accounts...');
  const accountMap = await loadModelAccounts();
  await buildAccountIdMap();
  let created = 0;
  for (const [username, accountId] of Object.entries(accountMap)) {
    const before = excludeListIds[username] || {};
    const after = await ensureExcludeListsForAccount(username, accountId);
    if (!before.newSub && after.newSub) created++;
    if (!before.activeChat && after.activeChat) created++;
  }
  console.log(`📋 Exclude lists check complete. Created ${created} new lists.`);
}

// Cron: Clean up expired New Sub 48hr entries (runs every hour)
async function cleanupNewSubExcludes() {
  console.log('🆕 Cleaning up expired New Sub 48hr entries...');
  const now = Date.now();
  const FORTY_EIGHT_HOURS = 48 * 60 * 60 * 1000;
  const accountMap = await loadModelAccounts();
  let removed = 0;

  for (const [username, accountId] of Object.entries(accountMap)) {
    const lists = excludeListIds[username];
    if (!lists?.newSub) continue;

    // Scan Redis for newsub:{username}:* keys
    // Upstash doesn't support SCAN, so we track fan IDs in a set
    const trackedFans = await redis.smembers(`newsub:${username}:_index`) || [];
    for (const fanId of trackedFans) {
      const ts = await redis.get(`newsub:${username}:${fanId}`);
      if (!ts) continue;
      if (now - Number(ts) >= FORTY_EIGHT_HOURS) {
        // Remove from list
        try {
          await fetch(`${OF_API_BASE}/${accountId}/user-lists/${lists.newSub}/users/${fanId}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${OF_API_KEY}` },
          });
          await redis.del(`newsub:${username}:${fanId}`);
          await redis.srem(`newsub:${username}:_index`, fanId);
          removed++;
          console.log(`🆕 Removed expired fan ${fanId} from New Sub list for ${username}`);
        } catch (e) {
          console.error(`Error removing fan ${fanId} from New Sub list:`, e.message);
        }
        await new Promise(r => setTimeout(r, 500));
      }
    }
  }
  console.log(`🆕 Cleanup complete: removed ${removed} expired entries`);
}

// Cron: Clean up expired Active Chat entries (runs every 15 min)
async function cleanupActiveChatExcludes() {
  console.log('💬 Cleaning up expired Active Chat entries...');
  const now = Date.now();
  const TWO_HOURS = 2 * 60 * 60 * 1000;
  const accountMap = await loadModelAccounts();
  let removed = 0;

  for (const [username, accountId] of Object.entries(accountMap)) {
    const lists = excludeListIds[username];
    if (!lists?.activeChat) continue;

    const trackedFans = await redis.smembers(`activechat:${username}:_index`) || [];
    for (const fanId of trackedFans) {
      const ts = await redis.get(`activechat:${username}:${fanId}`);
      if (!ts) continue;
      if (now - Number(ts) >= TWO_HOURS) {
        try {
          await fetch(`${OF_API_BASE}/${accountId}/user-lists/${lists.activeChat}/users/${fanId}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${OF_API_KEY}` },
          });
          await redis.del(`activechat:${username}:${fanId}`);
          await redis.srem(`activechat:${username}:_index`, fanId);
          removed++;
          console.log(`💬 Removed expired fan ${fanId} from Active Chat list for ${username}`);
        } catch (e) {
          console.error(`Error removing fan ${fanId} from Active Chat list:`, e.message);
        }
        await new Promise(r => setTimeout(r, 500));
      }
    }
  }
  console.log(`💬 Cleanup complete: removed ${removed} expired entries`);
}

// Add fan to "🆕 New Sub 48hr" list (with index tracking for cleanup)
async function addNewSubExcludeTracked(username, accountId, fanId) {
  const lists = excludeListIds[username];
  if (!lists?.newSub) return;

  try {
    const res = await fetch(`${OF_API_BASE}/${accountId}/user-lists/${lists.newSub}/users`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OF_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ userIds: [Number(fanId)] }),
    });
    if (res.ok) {
      await redis.set(`newsub:${username}:${fanId}`, Date.now());
      await redis.sadd(`newsub:${username}:_index`, String(fanId));
      console.log(`🆕 Added fan ${fanId} to New Sub 48hr list for ${username}`);
    } else {
      const err = await res.text();
      console.error(`Failed to add fan ${fanId} to New Sub list for ${username}: ${err}`);
    }
  } catch (e) {
    console.error(`Error adding fan to New Sub list:`, e.message);
  }
}

async function addActiveChatExcludeTracked(username, accountId, fanId) {
  const lists = excludeListIds[username];
  if (!lists?.activeChat) return;

  const redisKey = `activechat:${username}:${fanId}`;
  const existing = await redis.get(redisKey);

  await redis.set(redisKey, Date.now());
  await redis.sadd(`activechat:${username}:_index`, String(fanId));

  if (!existing) {
    try {
      const res = await fetch(`${OF_API_BASE}/${accountId}/user-lists/${lists.activeChat}/users`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${OF_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ userIds: [Number(fanId)] }),
      });
      if (res.ok) {
        console.log(`💬 Added fan ${fanId} to Active Chat list for ${username}`);
      } else {
        const err = await res.text();
        console.error(`Failed to add fan ${fanId} to Active Chat list for ${username}: ${err}`);
      }
    } catch (e) {
      console.error(`Error adding fan to Active Chat list:`, e.message);
    }
  } else {
    console.log(`💬 Updated activity timestamp for fan ${fanId} on ${username}`);
  }
}

// Cron schedules for cleanup
cron.schedule('0 * * * *', cleanupNewSubExcludes);       // Every hour
cron.schedule('*/15 * * * *', cleanupActiveChatExcludes); // Every 15 min

// === CHATBOT SYSTEM ===

// Load vault from API with correct endpoint, paginated. Cache 1hr in Redis.
async function loadMillieVault() {
  try {
    const cached = await redis.get('chatbot:millie:vault');
    if (cached && Date.now() - (cached._loadedAt || 0) < 3600000) {
      return cached.categories || VAULT_CATALOG;
    }

    const accountMap = await loadModelAccounts();
    const accountId = accountMap[MILLIE_USERNAME];
    if (!accountId) { console.error('❌ Chatbot: milliexhart account not found'); return VAULT_CATALOG; }

    // Paginate through vault
    const allItems = [];
    let offset = 0;
    const limit = 100;
    while (true) {
      const res = await fetch(`${OF_API_BASE}/${accountId}/media/vault?limit=${limit}&offset=${offset}`, {
        headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
      });
      if (!res.ok) { console.error('❌ Chatbot: vault fetch failed at offset', offset, await res.text()); break; }
      const data = await res.json();
      const items = data.data || data.list || data || [];
      if (!Array.isArray(items) || items.length === 0) break;
      allItems.push(...items);
      if (items.length < limit) break;
      offset += limit;
    }

    if (allItems.length === 0) {
      console.log('🤖 Vault API returned 0 items, using hardcoded catalog');
      return VAULT_CATALOG;
    }

    // Parse into categories using listStates
    const categories = {};
    for (const item of allItems) {
      const lists = item.listStates || item.lists || item.categories || [];
      const mediaId = item.id || item.media_id;
      const mediaType = item.type || item.media_type || 'photo';
      for (const list of lists) {
        const name = list.name || list;
        if (name === 'Messages' || name.startsWith('@')) continue;
        const key = name.toLowerCase()
          .replace(/[💰💦🫦🖼️💵\s]+/g, ' ').trim()
          .replace(/\s*-\s*\d{2}\/\d{2}.*$/, '')
          .replace(/\s*\(.*?\)/g, '')
          .replace(/\s+/g, '_')
          .replace(/[^a-z0-9_]/g, '');
        if (!categories[key]) categories[key] = { name, ids: [], photos: 0, videos: 0 };
        categories[key].ids.push(mediaId);
        if (mediaType === 'video') categories[key].videos++;
        else categories[key].photos++;
      }
    }

    const result = Object.keys(categories).length > 5 ? categories : VAULT_CATALOG;
    await redis.set('chatbot:millie:vault', { categories: result, _loadedAt: Date.now() });
    console.log(`🤖 Loaded ${allItems.length} vault items, ${Object.keys(result).length} categories`);
    return result;
  } catch (e) {
    console.error('❌ Chatbot vault load error:', e.message);
    return VAULT_CATALOG;
  }
}

// Get fan spending context from chat history
async function getFanContext(accountId, userId) {
  const cacheKey = `chatbot:fan_ctx:${userId}`;
  const cached = await redis.get(cacheKey);
  if (cached && Date.now() - (cached._at || 0) < 1800000) return cached; // 30min cache

  try {
    const res = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages?limit=100`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    if (!res.ok) return { totalSpent: 0, purchaseCount: 0, isNew: true, lastPurchaseAmount: 0, _at: Date.now() };

    const data = await res.json();
    const messages = data.data || data.list || data || [];

    let totalSpent = 0, purchaseCount = 0, lastPurchaseAmount = 0;
    const purchasedBundles = [];

    for (const msg of messages) {
      if (msg.price && msg.isOpened) {
        totalSpent += parseFloat(msg.price) || 0;
        purchaseCount++;
        if (!lastPurchaseAmount) lastPurchaseAmount = parseFloat(msg.price) || 0;
      }
      // Track which vault IDs they already bought
      if (msg.media && msg.isOpened && msg.price) {
        for (const m of (Array.isArray(msg.media) ? msg.media : [])) {
          purchasedBundles.push(m.id);
        }
      }
    }

    const ctx = {
      totalSpent: Math.round(totalSpent * 100) / 100,
      purchaseCount,
      isNew: purchaseCount === 0,
      lastPurchaseAmount,
      messageCount: messages.length,
      purchasedMediaIds: purchasedBundles.slice(0, 200),
      _at: Date.now(),
    };
    await redis.set(cacheKey, ctx);
    return ctx;
  } catch (e) {
    console.error('❌ Fan context fetch error:', e.message);
    return { totalSpent: 0, purchaseCount: 0, isNew: true, lastPurchaseAmount: 0, _at: Date.now() };
  }
}

// Build fan context string for system prompt
function buildFanContextString(ctx) {
  if (ctx.isNew) {
    return 'NEW FAN — never purchased before. Start with a regular bundle ($9.99-$14.99). Build rapport first, don\'t rush to sell.';
  }
  const tier = ctx.totalSpent >= 50 ? 'WHALE' : ctx.totalSpent >= 25 ? 'HIGH SPENDER' : 'RETURNING BUYER';
  return `${tier} — $${ctx.totalSpent} total spent, ${ctx.purchaseCount} purchases, last purchase $${ctx.lastPurchaseAmount}. ` +
    (tier === 'WHALE' ? 'Offer sexting packs or CWM ($35-$99). They\'re proven buyers.' :
     tier === 'HIGH SPENDER' ? 'Offer VIP bundles ($22-$35) or sexting packs.' :
     'Try next tier up from their last purchase. VIP bundles if they bought starter.');
}

// Select vault IDs from a category
// Track sent vault items per fan to avoid duplicates
const sentItemsPerFan = {}; // { fanId: Set of vault IDs }

function selectVaultItems(catalog, bundleCategory, itemCount, fanId) {
  const cat = catalog[bundleCategory];
  if (!cat || !cat.ids || cat.ids.length === 0) {
    // Fallback: pick from a random starter bundle
    const fallback = catalog['bundle_1'] || catalog[Object.keys(catalog)[0]];
    if (!fallback) return [];
    return fallback.ids.slice(0, Math.min(itemCount || 5, fallback.ids.length));
  }
  
  // Filter out previously sent items for this fan
  const sentItems = sentItemsPerFan[fanId] || new Set();
  const unsent = cat.ids.filter(id => !sentItems.has(id));
  
  // If all items in this category were sent, allow resending (with log)
  const pool = unsent.length > 0 ? unsent : cat.ids;
  if (unsent.length === 0 && fanId) {
    console.log(`🤖 All ${cat.ids.length} items in ${bundleCategory} already sent to fan ${fanId} — recycling`);
  }
  
  const count = Math.min(itemCount || pool.length, pool.length);
  const shuffled = [...pool].sort(() => Math.random() - 0.5);
  const selected = shuffled.slice(0, count);
  
  // Track what we just selected
  if (fanId) {
    if (!sentItemsPerFan[fanId]) sentItemsPerFan[fanId] = new Set();
    selected.forEach(id => sentItemsPerFan[fanId].add(id));
    console.log(`🤖 Fan ${fanId}: ${sentItemsPerFan[fanId].size} unique items sent total`);
  }
  
  return selected;
}

async function getClaudeResponse(conversationHistory, newMessage, fanContext) {
  const fanCtxStr = buildFanContextString(fanContext);
  const systemPrompt = MILLIE_SYSTEM_PROMPT.replace('{fan_context}', fanCtxStr);

  const messages = [];
  for (const msg of conversationHistory) {
    messages.push({ role: msg.role, content: msg.content });
  }
  messages.push({ role: 'user', content: newMessage });

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1024,
      system: systemPrompt,
      messages,
    }),
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Claude API error ${res.status}: ${err}`);
  }

  const data = await res.json();
  const text = data.content?.[0]?.text || '';

  try {
    // Try to extract JSON from the response
    const jsonMatch = text.match(/\{[\s\S]*\}/);
    return JSON.parse(jsonMatch ? jsonMatch[0] : text);
  } catch {
    return { text: text.replace(/```json\n?|\n?```/g, '').trim(), action: 'message', delay: 60 };
  }
}

async function sendChatbotMessage(accountId, userId, text) {
  const res = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OF_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ text }),
  });
  if (!res.ok) throw new Error(`Send message failed: ${await res.text()}`);
  return await res.json();
}

async function sendChatbotPPV(accountId, userId, text, price, vaultIds) {
  const res = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OF_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ text, price, mediaFiles: vaultIds }),
  });
  if (!res.ok) throw new Error(`Send PPV failed: ${await res.text()}`);
  return await res.json();
}

async function handleChatbotMessage(accountId, userId, messageText) {
  try {
    const enabled = await redis.get('chatbot:enabled');
    if (!enabled) return;

    const testUserId = await redis.get('chatbot:test_user_id');
    if (!testUserId || String(userId) !== String(testUserId)) return;

    chatbotStats.messagesReceived++;
    console.log(`🤖 Chatbot received from ${userId}: "${messageText}"`);

    // Load conversation history
    const convKey = `chatbot:millie:conv:${userId}`;
    const history = await redis.get(convKey) || [];

    // Load vault catalog and fan context in parallel
    const accountMap = await loadModelAccounts();
    const numericAccountId = accountMap[MILLIE_USERNAME];
    if (!numericAccountId) { console.error('❌ Chatbot: no account ID for millie'); return; }

    const [vault, fanContext] = await Promise.all([
      loadMillieVault(),
      getFanContext(numericAccountId, userId),
    ]);

    // Get Claude's response
    const response = await getClaudeResponse(history, messageText, fanContext);
    console.log(`🤖 Claude response:`, JSON.stringify(response));

    // Update conversation history (keep last 50 messages)
    history.push({ role: 'user', content: messageText });
    history.push({ role: 'assistant', content: response.text });
    await redis.set(convKey, history.slice(-50));

    // Smart delay based on response type
    let delay;
    if (response.action === 'ppv') {
      delay = 15 + Math.floor(Math.random() * 10); // 15-25s for PPV (test mode)
    } else if (messageText.length < 20) {
      delay = 10 + Math.floor(Math.random() * 10); // 10-20s for short msgs
    } else {
      delay = 10 + Math.floor(Math.random() * 10); // 10-20s regular
    }
    // Allow Claude to override within bounds (test mode: 10-25s)
    if (response.delay) delay = Math.max(10, Math.min(25, response.delay));
    console.log(`🤖 Waiting ${delay}s before responding...`);

    setTimeout(async () => {
      try {
        if (response.action === 'ppv' && response.bundleCategory) {
          // Smart vault selection from category
          const vaultIds = selectVaultItems(vault, response.bundleCategory, response.itemCount || 5, userId);
          if (vaultIds.length > 0) {
            await sendChatbotPPV(numericAccountId, userId, response.text, response.ppvPrice || 9.99, vaultIds);
            chatbotStats.ppvsSent++;
            console.log(`🤖 PPV sent to ${userId}: $${response.ppvPrice} [${response.bundleCategory}] ${vaultIds.length} items`);
          } else {
            // Fallback: send as regular message if no vault items found
            await sendChatbotMessage(numericAccountId, userId, response.text);
            console.log(`🤖 PPV fallback → message to ${userId} (no vault items for ${response.bundleCategory})`);
          }
        } else if (response.action === 'ppv' && response.vaultIds?.length > 0) {
          // Legacy: direct vault IDs (backward compat)
          await sendChatbotPPV(numericAccountId, userId, response.text, response.ppvPrice || 9.99, response.vaultIds);
          chatbotStats.ppvsSent++;
          console.log(`🤖 PPV sent to ${userId}: $${response.ppvPrice}`);
        } else {
          await sendChatbotMessage(numericAccountId, userId, response.text);
          console.log(`🤖 Message sent to ${userId}: "${response.text}"`);
        }
        chatbotStats.messagesSent++;
      } catch (e) {
        chatbotStats.errors++;
        console.error(`❌ Chatbot send error:`, e.message);
      }
    }, delay * 1000);

  } catch (e) {
    chatbotStats.errors++;
    console.error(`❌ Chatbot error:`, e.message);
  }
}

// === CHATBOT ENDPOINTS ===

app.get('/chatbot/status', async (req, res) => {
  const enabled = await redis.get('chatbot:enabled');
  const testUserId = await redis.get('chatbot:test_user_id');
  
  // Get active conversations
  const convKeys = [];
  // We can't scan Upstash easily, so just check the test user
  let activeConvs = 0;
  if (testUserId) {
    const conv = await redis.get(`chatbot:millie:conv:${testUserId}`);
    if (conv && conv.length > 0) activeConvs = 1;
  }
  
  res.json({
    enabled: !!enabled,
    testUserId: testUserId || null,
    activeConversations: activeConvs,
    stats: chatbotStats,
    account: MILLIE_USERNAME,
  });
});

app.post('/chatbot/enable', async (req, res) => {
  await redis.set('chatbot:enabled', true);
  // Pre-load vault
  const vault = await loadMillieVault();
  res.json({ enabled: true, vaultItems: vault.length });
});

app.post('/chatbot/disable', async (req, res) => {
  await redis.set('chatbot:enabled', false);
  res.json({ enabled: false });
});

app.post('/chatbot/test-user/:userId', async (req, res) => {
  const { userId } = req.params;
  await redis.set('chatbot:test_user_id', userId);
  res.json({ testUserId: userId, message: `Test user set to ${userId}` });
});

// === WEBHOOK ENDPOINT ===
// Receives events from OnlyFans API webhooks
// Configure webhook URL in OnlyFans API dashboard: https://<your-domain>/webhooks/onlyfans

app.post('/webhooks/onlyfans', async (req, res) => {
  // Respond immediately
  res.status(200).json({ ok: true });

  const { event, account_id, payload } = req.body;
  if (!event || !account_id) return;

  const username = accountIdToUsername[account_id];
  if (!username) {
    console.log(`⚠️ Webhook: unknown account_id ${account_id}`);
    return;
  }

  // Resolve accountId (numeric) for API calls
  const accountMap = await loadModelAccounts();
  const numericAccountId = accountMap[username];
  if (!numericAccountId) return;

  try {
    if (event === 'subscriptions.new') {
      // New subscriber → add to "🆕 New Sub 48hr" list
      const fanId = payload?.user_id || payload?.user?.id;
      if (fanId) {
        await addNewSubExcludeTracked(username, numericAccountId, fanId);
      }
    }

    if (event === 'messages.received') {
      // Fan sent a direct message → add to "💬 Active Chat" list
      // SKIP mass messages / queue messages
      if (payload?.isFromQueue) return;
      const fanId = payload?.fromUser?.id;
      if (fanId) {
        await addActiveChatExcludeTracked(username, numericAccountId, fanId);
      }
      
      // Chatbot: handle milliexhart messages
      if (account_id === MILLIE_ACCOUNT_ID && fanId) {
        const messageText = payload?.text || payload?.body || payload?.content || '';
        if (messageText) {
          handleChatbotMessage(account_id, fanId, messageText).catch(e => {
            console.error('❌ Chatbot handler error:', e.message);
          });
        }
      }
    }

    if (event === 'messages.sent') {
      // Model sent a direct message → add fan to "💬 Active Chat" list
      // SKIP mass messages / queue messages
      if (payload?.isFromQueue) return;
      const fanId = payload?.toUser?.id;
      if (fanId) {
        await addActiveChatExcludeTracked(username, numericAccountId, fanId);
      }
    }
  } catch (e) {
    console.error(`Webhook processing error (${event}):`, e.message);
  }
});

// === EXCLUDE LIST STATUS ENDPOINT ===
app.get('/exclude-lists', async (req, res) => {
  const accountMap = await loadModelAccounts();
  const status = {};
  for (const username of Object.keys(accountMap)) {
    const lists = excludeListIds[username] || {};
    const newSubIndex = await redis.smembers(`newsub:${username}:_index`) || [];
    const activeChatIndex = await redis.smembers(`activechat:${username}:_index`) || [];
    status[username] = {
      newSubListId: lists.newSub || null,
      activeChatListId: lists.activeChat || null,
      newSubTracked: newSubIndex.length,
      activeChatTracked: activeChatIndex.length,
    };
  }
  res.json({ accounts: status, totalAccounts: Object.keys(status).length });
});

async function sendMassDm(promoterUsername, targetUsername, vaultId, accountId) {
  const caption = getMassDmCaption(targetUsername);
  
  try {
    // Build excluded lists: SFS exclude(s) + New Sub 48hr + Active Chat
    // OF API accepts mixed: string names ("fans") and numeric IDs (1234567890)
    const excludedLists = [];
    const sfsIds = sfsExcludeLists[promoterUsername];
    if (sfsIds) {
      const ids = Array.isArray(sfsIds) ? sfsIds : [sfsIds];
      for (const id of ids) {
        // Handle case where multiple IDs got comma-joined into one string
        const parts = String(id).split(',').map(s => s.trim()).filter(Boolean);
        for (const p of parts) excludedLists.push(Number(p));
      }
    }
    const autoLists = excludeListIds[promoterUsername] || {};
    if (autoLists.newSub) excludedLists.push(Number(autoLists.newSub));
    if (autoLists.activeChat) excludedLists.push(Number(autoLists.activeChat));

    const body = {
      text: caption,
      mediaFiles: [vaultId],
      userLists: ['fans', 'following'],
      ...(excludedLists.length > 0 ? { excludedLists } : {}),
    };
    
    console.log(`📨 Mass DM ${promoterUsername} → @${targetUsername} | exclude: ${excludedLists.length > 0 ? JSON.stringify(excludedLists) : 'NONE'}`);
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
      addToFeed(massDmFeed, { promoter: promoterUsername, target: targetUsername, queueId: retryQueueId, status: 'sent (retry)' });
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
    addToFeed(massDmFeed, { promoter: promoterUsername, target: targetUsername, queueId, status: 'sent' });
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
      
      // Resolve vault ID — prefer v2 random selection, fall back to cached or v1
      const vaultId = await getVaultIdForUse(model, entry.target, 'massDm', vaultMappings) || entry.vaultId;
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
  
  // Auto-resume ghost tag rotation
  const shouldAutoResume = await redis.get('s4s:rotation-enabled');
  if (shouldAutoResume !== false) {
    console.log('🔄 Auto-resuming ghost tag rotation...');
    isRunning = true;
    rotationState.stats.startedAt = new Date().toISOString();
    
    const vaultMappings = await loadVaultMappings();
    const models = Object.keys(vaultMappings);
    rotationState.dailySchedule = generateDailySchedule(models, vaultMappings);
    
    console.log(`📅 Auto-resumed: ${models.length} models, rotation running`);
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
  await redis.set('s4s:rotation-enabled', true);
  
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
  await redis.set('s4s:rotation-enabled', false);
  
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

// Full dashboard data — single source of truth for the S4S app
app.get('/dashboard', async (req, res) => {
  const vaultMappings = await loadVaultMappings();
  const allModels = Object.keys(vaultMappings).sort();
  const targetable = allModels.filter(m => !PROMOTER_ONLY.has(m));
  const pinnedState = await getPinnedState();
  const massDmData = await redis.get('s4s:mass-dm-schedule');
  
  // Ghost tag schedule summary per model
  const ghostTagSummary = {};
  let totalTagsToday = 0;
  for (const [model, schedules] of Object.entries(rotationState.dailySchedule)) {
    const executed = schedules.filter(s => s.executed).length;
    const pending = schedules.filter(s => !s.executed).length;
    ghostTagSummary[model] = { total: schedules.length, executed, pending };
    totalTagsToday += schedules.length;
  }
  
  // Mass DM summary per model
  const massDmSummary = {};
  let totalDmsToday = 0;
  if (massDmData?.schedule) {
    for (const [model, entries] of Object.entries(massDmData.schedule)) {
      const sent = entries.filter(e => e.executed && !e.failed).length;
      const pending = entries.filter(e => !e.executed && !e.failed).length;
      massDmSummary[model] = { total: entries.length, sent, pending };
      totalDmsToday += entries.length;
    }
  }
  
  res.json({
    config: {
      tagsPerModelPerDay: 57,
      pinnedFeaturedPerDay: 10,
      pinnedPromotersPerFeatured: PINNED_ACCOUNTS_PER_GIRL,
      massDmWindowsPerDay: 12,
      promoterOnly: [...PROMOTER_ONLY],
    },
    models: {
      total: allModels.length,
      targetable: targetable.length,
      promoterOnly: [...PROMOTER_ONLY].filter(m => allModels.includes(m)),
      list: allModels,
    },
    ghostTags: {
      isRunning,
      totalTagsToday,
      avgPerModel: allModels.length > 0 ? Math.round(totalTagsToday / allModels.length) : 0,
      perModel: ghostTagSummary,
    },
    pinnedPosts: {
      enabled: (await redis.get('s4s:pinned-enabled')) !== false,
      featuredToday: pinnedState.featuredGirls || [],
      dayIndex: pinnedState.dayIndex || 0,
      totalDaysForFullRotation: Math.ceil(targetable.length / Math.floor(targetable.length / PINNED_ACCOUNTS_PER_GIRL)),
      activePosts: (pinnedState.activePosts || []).length,
    },
    massDm: {
      enabled: (await redis.get('s4s:mass-dm-enabled')) !== false,
      totalDmsToday,
      perModel: massDmSummary,
    },
  });
});

app.get('/active', async (req, res) => {
  const pending = await getPendingDeletes();
  const now = Date.now();
  
  const activeTags = pending
    .filter(p => (now - p.createdAt) < 10 * 60 * 1000) // Only show tags < 10 min old (ghost tags live ~5 min)
    .map(p => ({
      promoter: p.promoter,
      target: p.target,
      postId: p.postId,
      createdAt: new Date(p.createdAt).toISOString(),
      ageSeconds: Math.round((now - p.createdAt) / 1000),
      deletesIn: Math.max(0, Math.round((p.deleteAt - now) / 1000))
    })).sort((a, b) => a.ageSeconds - b.ageSeconds); // newest first
  
  res.json({
    count: activeTags.length,
    tags: activeTags
  });
});

// Live feed endpoints
app.get('/feed/mass-dm', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  res.json({ count: massDmFeed.length, feed: massDmFeed.slice(0, limit) });
});

app.get('/feed/ghost-tags', (req, res) => {
  const limit = Math.min(parseInt(req.query.limit) || 50, 100);
  res.json({ count: ghostTagFeed.length, feed: ghostTagFeed.slice(0, limit) });
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

app.post('/api/regenerate-schedule', async (req, res) => {
  try {
    console.log('🔄 Manual schedule regeneration triggered');
    const vaultMappings = await loadVaultMappings();
    const models = Object.keys(vaultMappings);
    rotationState.dailySchedule = generateDailySchedule(models, vaultMappings);
    res.json({ success: true, models: models.length, message: 'Schedule regenerated' });
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
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
  console.log(`   GET  /exclude-lists   - View auto-managed exclude list status`);
  console.log(`   POST /webhooks/onlyfans - Webhook endpoint for OF API`);
  
  // Load SFS exclude lists from Redis (seeds if needed)
  await loadSfsExcludeLists();
  
  // Load and ensure auto-managed exclude lists
  await loadExcludeListIds();
  await ensureAllExcludeLists();
  
  // Run startup recovery
  await startupRecovery();
});
