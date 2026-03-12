/**
 * Biancawoods AI Chatbot Engine
 * Phase 1 — Autonomous chat, bump, welcome, retarget
 * 
 * Requires env vars:
 *   CHATBOT_BIANCA_ENABLED=true  (master kill switch)
 *   ANTHROPIC_API_KEY             (Claude Sonnet 4)
 *   OF_API_KEY                    (OnlyFans API)
 *   KV_REST_API_URL + KV_REST_API_TOKEN (Upstash Redis)
 */

const cron = require('node-cron');
const fs = require('fs');
const path = require('path');

// ── Config ──────────────────────────────────────────────────────────────────

const OF_API_KEY = process.env.OF_API_KEY;
const OF_API_BASE = 'https://app.onlyfansapi.com/api';
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;
const BIANCA_ACCOUNT_ID = 'acct_54e3119e77da4429b6537f7dd2883a05';
const BIANCA_USERNAME = 'biancaawoods';

// Excluded whale fan IDs (hardcoded per spec)
const EXCLUDED_WHALE_IDS = {
  '483664969': 'Antonio — $1000+ whale, human-only',
  '482383508': 'Brandon — $1000+ whale, human-only',
};
// nij444 and tylerd34 resolved at startup
let resolvedExcludeIds = {}; // filled by resolveExcludedUsernames()

const NEVER_SELL_CATEGORIES = new Set([25942142, 25942144, 27174580, 26278056]);

// OF list IDs
const LIST_IDS = {
  timewasters: 1231455148,
  brokeStudent: 1232110158,
  lowballers: 1258116798,
  newSubs: 1239250956,
  spenders101to500: 1254929000,
};

// Bump vault category
const BUMP_VAULT_CATEGORY_ID = 27535987;

const BUMP_TEXTS = [
  'heyy how are u 😊',
  'hey babe what are u up to rn',
  'hiii 💕',
  'heyyy whatcha doing 😊',
  'miss talking to u 🥺',
  'hey handsome 😏',
  'bored rn... entertain me? 😊',
  'heyy stranger 💕',
  'thinking about u rn 😊',
  'hey cutie wyd 💕',
];

const WELCOME_TEMPLATES = [
  'hiii 🥰 omg thank u for subbing, i actually get so excited when someone new joins lol',
  'heyyy welcome babe 💕 tell me about urself, what made u sub?',
  'omg hi!! i was literally just posting new stuff, ur timing is perfect haha',
];

// ── Restricted Words (Section 10 of brain v3) ──────────────────────────────

const RESTRICTED_WORDS = [
  'abduct','abducted','abducting','abduction','admireme','animal','asphyxia','asphyxiate',
  'asphyxiation','asphyxicate','asphyxication','bait','ballbusting','bareback','beastiality',
  'bestiality','blacked','blackmail','bleeding','blood','bloodplay','bukkake','caned','caning',
  'cannibal','cbt','cervics','cerviks','cervix','child','chloroform','chloroformed',
  'chloroforming','choking','coma','comatose','consent','cp','cycle','diapers','dog','doze',
  'drinking','drunk','drunken','enema','entrance','escort','escorting','fanfuck','farm',
  'fecal','fetal','flogging','foetal','forced','forcedbi','forceful','forcing','fuckafan',
  'fuckfan','gangbang','gangbangs','gaping','golden','hardsports','hooker','hypno',
  'hypnotize','hypnotized','hypnotizing','inbreed','inbreeded','inbreeding','incapacitate',
  'incapacitation','incest','intox','intoxicated','inzest','jail','jailbait','kidnap',
  'kidnapped','kidnapping','knock','knocked','lactate','lactation','lolicon','lolita',
  'medicalplay','meet','meeting','meetup','menstrate','menstrual','menstruate',
  'menstruating','menstruation','molest','molested','molesting','mutilate','mutilation',
  'necrophilia','nigger','paddling','paralyzed','passed','pedo','pedophile','pedophilia',
  'pee','peeplay','pegging','piss','pissing','poo','poop','preteen','prostituted',
  'prostituting','prostitution','pse','rape','raping','rapist','scat','showers','skat',
  'snuff','ss','strangled','strangling','strangulation','suffocate','suffocation','teen',
  'toilet','toiletslave','toiletslavery','torture','tortured','trance','unconscious',
  'underage','unwilling','vomit','vomitted','vomitting','watersports','whipping','young',
  'zoophilia',
];

function containsRestricted(text) {
  const lower = text.toLowerCase();
  for (const word of RESTRICTED_WORDS) {
    if (lower.includes(word)) return word;
  }
  return null;
}

// ── Content Map (loaded at startup) ─────────────────────────────────────────

let CONTENT_MAP = null;

function loadContentMap() {
  try {
    const raw = fs.readFileSync(path.join(__dirname, 'biancawoods-content-map.json'), 'utf8');
    CONTENT_MAP = JSON.parse(raw);
    console.log('🗺️ [bianca] Content map loaded');
  } catch (e) {
    console.error('❌ [bianca] Failed to load content map:', e.message);
    CONTENT_MAP = null;
  }
}

// ── Bianca Prompt (slim decision-only version) ──────────────────────────────

let BIANCA_PROMPT_TEXT = '';

function loadBiancaPrompt() {
  try {
    BIANCA_PROMPT_TEXT = fs.readFileSync(path.join(__dirname, 'bianca-prompt-slim.md'), 'utf8');
    console.log('🧠 [bianca] Prompt loaded (' + BIANCA_PROMPT_TEXT.length + ' chars)');
  } catch (e) {
    console.error('❌ [bianca] Failed to load prompt:', e.message);
    BIANCA_PROMPT_TEXT = 'You are Bianca, a flirty confident woman on OnlyFans.';
  }
}

// ── Module State ────────────────────────────────────────────────────────────

let redis = null; // Set via init()
let biancaAccountId = null; // numeric account ID resolved at startup
let isRunning = false;
let startedAt = null;

// Loop handles
let messagePollingInterval = null;
let welcomeCheckInterval = null;
let bumpCronJob = null;
let retargetCronJob = null;
let timewasterCronJob = null;
let profileCleanupCronJob = null;

// Debounce
const DEBOUNCE_MS = 3000;
const pendingBiancaMessages = {};

// Rate limiting
const rateLimits = {
  msgsThisHour: 0,
  hourKey: '',
  retargetsToday: 0,
  retargetDate: '',
};

// Stats
const stats = {
  messagesReceived: 0,
  messagesSent: 0,
  ppvsSent: 0,
  ppvRevenue: 0,
  bumpsSent: 0,
  welcomesSent: 0,
  retargetsSent: 0,
  errors: 0,
  lastActivityAt: null,
};

// Vault item cache: { categoryId: { items: [...], loadedAt } }
const vaultCache = {};
const VAULT_CACHE_TTL = 3600000; // 1 hour

// ── Redis Helpers ───────────────────────────────────────────────────────────

const R = {
  async get(key) { try { return await redis.get(key); } catch (e) { console.error('Redis GET error:', key, e.message); return null; } },
  async set(key, val) { try { await redis.set(key, val); } catch (e) { console.error('Redis SET error:', key, e.message); } },
  async hget(key, field) { try { return await redis.hget(key, field); } catch (e) { return null; } },
  async hset(key, obj) { try { await redis.hset(key, obj); } catch (e) { console.error('Redis HSET error:', key, e.message); } },
  async hdel(key, field) { try { await redis.hdel(key, field); } catch (e) {} },
  async hgetall(key) { try { return await redis.hgetall(key) || {}; } catch (e) { return {}; } },
  async lpush(key, val) { try { await redis.lpush(key, JSON.stringify(val)); } catch (e) {} },
  async ltrim(key, start, stop) { try { await redis.ltrim(key, start, stop); } catch (e) {} },
  async lrange(key, start, stop) { try { const r = await redis.lrange(key, start, stop); return (r || []).map(v => { try { return typeof v === 'string' ? JSON.parse(v) : v; } catch { return v; } }); } catch (e) { return []; } },
  async sismember(key, member) { try { return await redis.sismember(key, String(member)); } catch (e) { return false; } },
  async sadd(key, member) { try { await redis.sadd(key, String(member)); } catch (e) {} },
  async smembers(key) { try { return await redis.smembers(key) || []; } catch (e) { return []; } },
  async incr(key) { try { return await redis.incr(key); } catch (e) { return 0; } },
  async expire(key, seconds) { try { await redis.expire(key, seconds); } catch (e) {} },
};

// ── OF API Helpers ──────────────────────────────────────────────────────────

async function ofApi(endpoint, options = {}) {
  const url = `${OF_API_BASE}/${biancaAccountId}${endpoint}`;
  const res = await fetch(url, {
    method: options.method || 'GET',
    headers: {
      'Authorization': `Bearer ${OF_API_KEY}`,
      ...(options.body ? { 'Content-Type': 'application/json' } : {}),
      ...options.headers,
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`OF API ${res.status} ${endpoint}: ${err}`);
  }
  return res.json();
}

async function resolveAccountId() {
  try {
    const res = await fetch(`${OF_API_BASE}/accounts`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` },
    });
    const accounts = await res.json();
    for (const acct of accounts) {
      if (acct.onlyfans_username === BIANCA_USERNAME || acct.id === BIANCA_ACCOUNT_ID) {
        biancaAccountId = acct.id;
        console.log(`🔑 [bianca] Account resolved: ${biancaAccountId}`);
        return biancaAccountId;
      }
    }
    console.error('❌ [bianca] Account not found in OF API');
    return null;
  } catch (e) {
    console.error('❌ [bianca] Account resolution failed:', e.message);
    return null;
  }
}

// ── Excluded Fans ───────────────────────────────────────────────────────────

async function resolveExcludedUsernames() {
  const toResolve = { nij444: 'Ferdy — DO NOT MESSAGE', tylerd34: 'VIP Tyler — $1000+ whale, human-only' };
  for (const [username, reason] of Object.entries(toResolve)) {
    try {
      const data = await ofApi(`/users?search=${username}`);
      const users = data.data || data.list || data || [];
      if (Array.isArray(users) && users.length > 0) {
        const uid = String(users[0].id || users[0].userId);
        resolvedExcludeIds[uid] = reason;
        console.log(`🔍 [bianca] Resolved ${username} → ${uid}`);
      } else {
        console.warn(`⚠️ [bianca] Could not resolve ${username}`);
      }
    } catch (e) {
      console.error(`❌ [bianca] Failed to resolve ${username}:`, e.message);
    }
  }
  // Seed to Redis
  const allExcluded = { ...EXCLUDED_WHALE_IDS, ...resolvedExcludeIds };
  for (const [id, reason] of Object.entries(allExcluded)) {
    await R.hset('chatbot:bianca:excluded_fans', { [id]: reason });
  }
  console.log(`🚫 [bianca] ${Object.keys(allExcluded).length} fans excluded`);
}

async function isExcludedFan(fanId) {
  const id = String(fanId);
  if (EXCLUDED_WHALE_IDS[id] || resolvedExcludeIds[id]) return true;
  const reason = await R.hget('chatbot:bianca:excluded_fans', id);
  return !!reason;
}

// ── Vault Fetching ──────────────────────────────────────────────────────────

async function fetchVaultItems(categoryId, limit = 20) {
  if (NEVER_SELL_CATEGORIES.has(categoryId)) {
    console.error(`🚫 [bianca] BLOCKED: attempted fetch from never-sell category ${categoryId}`);
    return [];
  }

  // Check in-memory cache
  const cached = vaultCache[categoryId];
  if (cached && Date.now() - cached.loadedAt < VAULT_CACHE_TTL) return cached.items;

  // Check Redis cache
  const redisCached = await R.get(`chatbot:bianca:vault_cache:${categoryId}`);
  if (redisCached && Array.isArray(redisCached) && redisCached.length > 0) {
    vaultCache[categoryId] = { items: redisCached, loadedAt: Date.now() };
    return redisCached;
  }

  try {
    const data = await ofApi(`/media/vault?categoryId=${categoryId}&limit=${limit}`);
    const items = data.data || data.list || data || [];
    const ids = items.map(item => item.id).filter(Boolean);
    if (ids.length > 0) {
      vaultCache[categoryId] = { items: ids, loadedAt: Date.now() };
      await R.set(`chatbot:bianca:vault_cache:${categoryId}`, ids);
      // Set TTL via expire
      await R.expire(`chatbot:bianca:vault_cache:${categoryId}`, 3600);
    }
    return ids;
  } catch (e) {
    console.error(`❌ [bianca] Vault fetch error for category ${categoryId}:`, e.message);
    return [];
  }
}

// ── Fan Profile Management ──────────────────────────────────────────────────

function defaultFanProfile(fanId) {
  return {
    fanId: String(fanId),
    username: null,
    buyerType: 'unknown',
    totalSpent: 0,
    purchaseCount: 0,
    avgPurchasePrice: 0,
    lastPurchasePrice: 0,
    estimatedCeiling: 15,
    lastTestedAbove: 0,
    priceHistory: [],
    sextingProgress: {
      sexting1: { currentStep: 0, lastStepAt: null },
      sexting2: { currentStep: 0, lastStepAt: null },
      sexting3: { currentStep: 0, lastStepAt: null },
    },
    sentBundles: [],
    sentUpsellScreenshots: [],
    sentCustomUpsells: [],
    lastMessageAt: null,
    lastBotReplyAt: null,
    firstSeenAt: new Date().toISOString(),
    isTimewaster: false,
    timewasterScore: 0,
    botMessageCount: 0,
    fanMessageCount: 0,
    retargetCount: 0,
    lastRetargetAt: null,
    preferredHook: null,
    preferredTime: null,
    welcomed: false,
    flags: { humanHandoff: false, humanHandoffReason: null },
  };
}

async function getFanProfile(fanId) {
  const data = await R.get(`chatbot:bianca:fan:${fanId}`);
  if (data) return data;
  return defaultFanProfile(fanId);
}

async function saveFanProfile(profile) {
  await R.set(`chatbot:bianca:fan:${profile.fanId}`, profile);
}

// ── Content Summary Builder (per-fan) ──────────────────────────────────────

function buildContentSummary(fanProfile) {
  if (!CONTENT_MAP) return 'Content map not loaded.';

  const lines = [];
  lines.push(`=== FAN CONTENT CONTEXT ===`);
  lines.push(`Total spent: $${fanProfile.totalSpent.toFixed(2)} | Purchase count: ${fanProfile.purchaseCount || 0}`);
  lines.push(`Estimated ceiling: $${fanProfile.estimatedCeiling || 15}`);
  
  const sentBundles = new Set(fanProfile.sentBundles || []);
  const sentBundleCount = sentBundles.size;
  lines.push(`Bundles sent: ${sentBundleCount}/${CONTENT_MAP.bundles?.length || 0}`);

  // Show sexting progress
  if (fanProfile.sextingProgress) {
    for (const [chain, progress] of Object.entries(fanProfile.sextingProgress)) {
      if (progress.currentStep > 0) {
        lines.push(`${chain}: step ${progress.currentStep}`);
      }
    }
  }

  lines.push('\nContent will be selected based on fan spend and conversation context.');
  lines.push('Available levels: clothed/bikini → implied nude → topless → explicit');
  lines.push(`Never sell categories: ${CONTENT_MAP.neverSell?.join(', ')}`);

  return lines.join('\n');
}

// ── Claude Integration ──────────────────────────────────────────────────────

function buildSystemPrompt(fanProfile, conversation) {
  const now = new Date();
  const astTime = now.toLocaleString('en-US', { timeZone: 'America/Puerto_Rico', hour: 'numeric', minute: '2-digit', hour12: true, weekday: 'short' });

  // Build conversation context
  let conversationText = '';
  if (conversation && conversation.length > 0) {
    conversationText = '\n\n=== CONVERSATION HISTORY ===\n';
    conversation.slice(-10).forEach(msg => {
      const role = msg.role === 'user' ? 'FAN' : 'BIANCA';
      conversationText += `${role}: ${msg.content}\n`;
    });
  }

  return `${BIANCA_PROMPT_TEXT}

## CURRENT CONTEXT
Time: ${astTime}
Fan ID: ${fanProfile.fanId}
Fan spent: $${fanProfile.totalSpent.toFixed(2)} (${fanProfile.purchaseCount || 0} purchases)
Estimated ceiling: $${fanProfile.estimatedCeiling || 15}
Buyer type: ${fanProfile.buyerType || 'unknown'}
Timewaster: ${fanProfile.isTimewaster ? 'YES' : 'NO'}

${buildContentSummary(fanProfile)}${conversationText}`;
}

// ── Direct Anthropic API ───────────────────────────────────────────────────

async function callAnthropicDirect(fanProfile, conversationHistory, newMessage) {
  // Model selection based on fan value
  const useOpus = fanProfile.totalSpent >= 50;
  const model = useOpus ? 'claude-opus-4-20250514' : 'claude-sonnet-4-20250514';
  
  console.log(`🧠 [bianca] Using ${model} for fan ${fanProfile.fanId} ($${fanProfile.totalSpent})`);

  // Build conversation for API
  const messages = conversationHistory.map(m => ({ role: m.role, content: m.content }));
  messages.push({ role: 'user', content: newMessage });

  const systemPrompt = buildSystemPrompt(fanProfile, conversationHistory);

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': ANTHROPIC_API_KEY,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      model,
      max_tokens: 1024,
      system: systemPrompt,
      messages,
    }),
  });

  if (res.status === 429) {
    console.error('🚫 [bianca] Anthropic 429 rate limit');
    throw new Error('RATE_LIMIT_429');
  }
  if (!res.ok) {
    const err = await res.text();
    console.error(`🚫 [bianca] Anthropic ${res.status}: ${err}`);
    throw new Error(`Anthropic API error: ${res.status}`);
  }

  const data = await res.json();
  const text = data.content?.[0]?.text || '';
  
  // Parse JSON response
  try {
    // Strip markdown code fences if present
    const cleaned = text.replace(/^```json?\s*\n?/i, '').replace(/\n?```\s*$/i, '').trim();
    const parsed = JSON.parse(cleaned);
    
    // Convert the prompt's action format to our internal format
    if (parsed.action) {
      const converted = { messages: [], flag: null };
      
      if (parsed.action === 'text') {
        converted.messages.push({ text: parsed.message_text, action: 'message' });
      } else if (parsed.action === 'ppv') {
        converted.messages.push({
          text: parsed.message_text,
          action: 'ppv',
          contentCategory: parsed.content_key,
          price: parsed.price
        });
      } else if (parsed.action === 'free_media') {
        converted.messages.push({
          text: parsed.message_text,
          action: 'free_media',
          contentCategory: parsed.content_key
        });
      } else if (parsed.action === 'skip') {
        return { skip: true, reason: parsed.reason };
      }
      
      return converted;
    }
    
    return parsed;
  } catch (e) {
    console.error('🚫 [bianca] Failed to parse Anthropic response as JSON:', text.substring(0, 200));
    // Return as plain text message
    return { messages: [{ text: text.substring(0, 300), action: 'message' }], flag: null };
  }
}

// ── Content Resolution ──────────────────────────────────────────────────────

async function resolveContentCategory(contentCategory, fanProfile) {
  let categoryId = null;
  let isFreePic = false;

  if (!contentCategory) return { categoryId: null, vaultItems: [] };

  // Direct category ID
  if (typeof contentCategory === 'number') {
    categoryId = contentCategory;
  } else if (typeof contentCategory === 'string') {
    // Try parsing as number
    const asNum = parseInt(contentCategory);
    if (!isNaN(asNum) && String(asNum) === contentCategory) {
      categoryId = asNum;
    }
    // Content map keys
    else if (CONTENT_MAP) {
      // Free content
      if (CONTENT_MAP.freeContent?.[contentCategory]) {
        categoryId = CONTENT_MAP.freeContent[contentCategory].categoryId;
        isFreePic = true;
      }
      // Bundles by key (combo1, combo2, etc.)
      else if (contentCategory.startsWith('combo')) {
        const num = parseInt(contentCategory.replace('combo', ''));
        const bundle = CONTENT_MAP.bundles?.find(b => b.name && b.name.includes(`Bundle ${num}`));
        if (bundle) categoryId = bundle.id;
      }
      // Sexting chains
      else if (contentCategory.startsWith('sexting')) {
        const match = contentCategory.match(/^(sexting\d)(?:_(?:pic|vid)(\d+))?$/);
        if (match) {
          const chain = match[1];
          const sextingData = CONTENT_MAP.sextingChains?.[chain];
          if (sextingData) {
            const progress = fanProfile.sextingProgress?.[chain] || { currentStep: 0 };
            const step = progress.currentStep;
            const stepData = sextingData.steps?.[step];
            if (stepData) {
              categoryId = stepData.categoryId;
              isFreePic = stepData.price === 0;
            }
          }
        }
      }
      // Body categories
      else if (CONTENT_MAP.bodyCategories?.[contentCategory]) {
        categoryId = CONTENT_MAP.bodyCategories[contentCategory].categoryId;
      }
      // Custom upsells
      else if (CONTENT_MAP.customUpsells?.[contentCategory]) {
        categoryId = CONTENT_MAP.customUpsells[contentCategory].categoryId;
      }
      // Custom tiers (custom_tier1, etc.)
      else if (contentCategory.startsWith('custom_tier')) {
        const tierNum = contentCategory.replace('custom_tier', '');
        const tier = CONTENT_MAP.customUpsells?.[`tier${tierNum}`];
        if (tier) categoryId = tier.categoryId;
      }
      // Bundle by ID
      else if (contentCategory.startsWith('bundle_')) {
        const bundleId = parseInt(contentCategory.replace('bundle_', ''));
        if (!isNaN(bundleId)) categoryId = bundleId;
      }
      // GFE selfie shorthand
      else if (contentCategory === 'gfe_selfie') {
        categoryId = CONTENT_MAP.freeContent?.gfeSelfies?.categoryId;
        isFreePic = true;
      }
    }
  }

  if (!categoryId) {
    console.warn(`⚠️ [bianca] Could not resolve content category: ${contentCategory}`);
    return { categoryId: null, vaultItems: [] };
  }

  // Check never-sell list
  if (CONTENT_MAP?.neverSell?.includes(categoryId)) {
    console.error(`🚫 [bianca] BLOCKED: Claude tried to send from never-sell category ${categoryId}`);
    return { categoryId: null, vaultItems: [] };
  }

  const vaultItems = await fetchVaultItems(categoryId);
  return { categoryId, vaultItems, isFreePic };
}

// ── Message Sending ─────────────────────────────────────────────────────────

async function sendTextMessage(fanId, text) {
  return ofApi(`/chats/${fanId}/messages`, {
    method: 'POST',
    body: { text },
  });
}

async function sendMediaMessage(fanId, text, mediaFileIds) {
  return ofApi(`/chats/${fanId}/messages`, {
    method: 'POST',
    body: { text, mediaFiles: mediaFileIds },
  });
}

async function sendPPVMessage(fanId, text, price, mediaFileIds) {
  return ofApi(`/chats/${fanId}/messages`, {
    method: 'POST',
    body: { text, price, mediaFiles: mediaFileIds },
  });
}

// ── Rate Limiting ───────────────────────────────────────────────────────────

function getHourKey() {
  const now = new Date();
  return `${now.getUTCFullYear()}${String(now.getUTCMonth()+1).padStart(2,'0')}${String(now.getUTCDate()).padStart(2,'0')}${String(now.getUTCHours()).padStart(2,'0')}`;
}

async function checkGlobalRateLimit() {
  const hourKey = getHourKey();
  if (rateLimits.hourKey !== hourKey) {
    rateLimits.hourKey = hourKey;
    rateLimits.msgsThisHour = 0;
  }
  return rateLimits.msgsThisHour < 50;
}

async function checkFanPPVRate(fanId) {
  const hourKey = getHourKey();
  const key = `chatbot:bianca:rate:ppv:${fanId}:${hourKey}`;
  const count = await R.get(key);
  return (parseInt(count) || 0) < 3;
}

async function incrementMsgRate() {
  rateLimits.msgsThisHour++;
}

async function incrementFanPPVRate(fanId) {
  const hourKey = getHourKey();
  const key = `chatbot:bianca:rate:ppv:${fanId}:${hourKey}`;
  const val = await R.incr(key);
  if (val === 1) await R.expire(key, 7200);
}

// ── Conversation Logging ────────────────────────────────────────────────────

async function logConversation(entry) {
  await R.lpush('chatbot:bianca:log', entry);
  await R.ltrim('chatbot:bianca:log', 0, 4999);
}

// ── Core: Process a Fan Message (Phase 1 — Push to Relay Queue) ─────────────

async function processFanMessage(fanId, messageText) {
  try {
    // Check excluded
    if (await isExcludedFan(fanId)) {
      console.log(`🚫 [bianca] Skipping excluded fan ${fanId}`);
      return;
    }

    stats.messagesReceived++;
    stats.lastActivityAt = new Date().toISOString();

    // Load fan profile
    const fanProfile = await getFanProfile(fanId);
    fanProfile.lastMessageAt = new Date().toISOString();
    fanProfile.fanMessageCount++;

    // Mark as active conversation
    await R.sadd('chatbot:bianca:active_convos', String(fanId));
    await R.set(`chatbot:bianca:active_ts:${fanId}`, Date.now());

    // Load conversation history from Redis
    const convHistory = await R.get(`chatbot:bianca:conv:${fanId}`) || [];

    // Add new message
    convHistory.push({ role: 'user', content: messageText });
    await R.set(`chatbot:bianca:conv:${fanId}`, convHistory.slice(-50));

    // Check human handoff
    if (fanProfile.flags?.humanHandoff) {
      console.log(`🚩 [bianca] Fan ${fanId} is in human handoff, sending template`);
      await sendTextMessage(fanId, "heyy 💕 give me a sec, i'll get back to u soon");
      stats.messagesSent++;
      await saveFanProfile(fanProfile);
      return;
    }

    // Call Claude API
    console.log(`🧠 [bianca] Calling Claude for fan ${fanId}...`);
    const aiResponse = await callAnthropicDirect(fanProfile, convHistory.slice(-20), messageText);
    
    if (aiResponse.skip) {
      console.log(`⏭️ [bianca] Skipping fan ${fanId}: ${aiResponse.reason}`);
      await saveFanProfile(fanProfile);
      return;
    }
    
    console.log(`🧠 [bianca] Got response for fan ${fanId}: ${JSON.stringify(aiResponse).substring(0, 200)}`);

    // Execute the response (send messages + PPVs)
    const result = await executeRelayResponse(fanId, aiResponse);
    console.log(`✅ [bianca] Fan ${fanId} handled: ${JSON.stringify(result).substring(0, 200)}`);

  } catch (e) {
    stats.errors++;
    console.error(`❌ [bianca] Error processing fan ${fanId}:`, e.message);
  }
}

// ── Process AI Response (Phase 2 — Execute actions from OpenClaw relay) ─────

async function executeRelayResponse(fanId, response) {
  try {
    const fanProfile = await getFanProfile(fanId);
    const convHistory = await R.get(`chatbot:bianca:conv:${fanId}`) || [];

    // Check for restricted words
    const allTexts = (response.messages || []).map(m => m.text).join(' ');
    const restricted = containsRestricted(allTexts);
    if (restricted) {
      console.error(`🚫 [bianca] Restricted word "${restricted}" in relay response for fan ${fanId}, dropping`);
      await logConversation({
        ts: new Date().toISOString(), fanId, direction: 'blocked',
        botResponse: null, reason: `restricted_word: ${restricted}`,
      });
      return { ok: false, reason: 'restricted_word', word: restricted };
    }

    // Handle flags (human handoff)
    if (response.flag) {
      console.log(`🚩 [bianca] Flag for fan ${fanId}:`, response.flag);
      fanProfile.flags.humanHandoff = true;
      fanProfile.flags.humanHandoffReason = response.flag.reason;
      await sendTextMessage(fanId, "heyy 💕 give me a min, i wanna give u a proper response");
      stats.messagesSent++;
      await saveFanProfile(fanProfile);
      return { ok: true, flagged: true };
    }

    // Execute messages
    const messages = response.messages || [response];
    let ppvSentThisTurn = false;
    const botResponseParts = [];

    for (const msg of messages) {
      if (!msg.text && !msg.action) continue;

      if ((msg.action === 'ppv' || msg.action === 'free_media') && !ppvSentThisTurn) {
        const { categoryId, vaultItems, isFreePic } = await resolveContentCategory(msg.contentCategory, fanProfile);

        if (vaultItems.length > 0 && categoryId) {
          try {
            if (msg.action === 'free_media' || isFreePic) {
              // Send as free media
              await sendMediaMessage(fanId, msg.text || 'just for u 💕', vaultItems);
              botResponseParts.push({ text: msg.text, action: 'free_media', category: categoryId, itemCount: vaultItems.length });
              console.log(`💕 [bianca] Free media sent to ${fanId} [cat ${categoryId}] ${vaultItems.length} items`);
            } else {
              // Send as PPV
              const price = Math.min(Math.max(msg.price || 15, 1), 100);
              await sendPPVMessage(fanId, msg.text || 'just for u 🙈', price, vaultItems);
              ppvSentThisTurn = true;
              stats.ppvsSent++;
              stats.ppvRevenue += price;

              fanProfile.priceHistory = fanProfile.priceHistory || [];
              fanProfile.priceHistory.push({
                offered: price, opened: false,
                ts: new Date().toISOString(), category: String(categoryId),
              });

              botResponseParts.push({ text: msg.text, action: 'ppv', category: categoryId, price, itemCount: vaultItems.length });
              console.log(`💰 [bianca] PPV sent to ${fanId}: $${price} [cat ${categoryId}] ${vaultItems.length} items`);
            }

            // Update sexting progress
            if (msg.contentCategory && typeof msg.contentCategory === 'string' && msg.contentCategory.startsWith('sexting')) {
              const chainMatch = msg.contentCategory.match(/^(sexting\d)/);
              if (chainMatch && fanProfile.sextingProgress[chainMatch[1]]) {
                fanProfile.sextingProgress[chainMatch[1]].currentStep++;
                fanProfile.sextingProgress[chainMatch[1]].lastStepAt = new Date().toISOString();
              }
            }

            if (categoryId && !fanProfile.sentBundles.includes(categoryId)) {
              fanProfile.sentBundles.push(categoryId);
            }

          } catch (e) {
            console.error(`❌ [bianca] Media send failed:`, e.message);
            if (msg.text) {
              await sendTextMessage(fanId, msg.text);
              botResponseParts.push({ text: msg.text, action: 'message' });
            }
            stats.errors++;
          }
        } else {
          if (msg.text) {
            await sendTextMessage(fanId, msg.text);
            botResponseParts.push({ text: msg.text, action: 'message' });
          }
          console.warn(`⚠️ [bianca] No vault items for category ${msg.contentCategory}`);
        }
      } else if ((msg.action === 'ppv' || msg.action === 'free_media') && ppvSentThisTurn) {
        if (msg.text) {
          await sendTextMessage(fanId, msg.text);
          botResponseParts.push({ text: msg.text, action: 'message (media_skipped)' });
        }
      } else {
        if (msg.text) {
          await sendTextMessage(fanId, msg.text);
          botResponseParts.push({ text: msg.text, action: 'message' });
        }
      }

      stats.messagesSent++;

      // Small delay between messages for realism
      if (messages.indexOf(msg) < messages.length - 1) {
        const words = (msg.text || '').split(/\s+/).length;
        const delayMs = Math.max(2000, (words / 40) * 60000 + 1000 + Math.random() * 2000);
        await new Promise(r => setTimeout(r, delayMs));
      }
    }

    // Update conversation history
    const assistantContent = botResponseParts.map(p => {
      if (p.action === 'ppv' || p.action?.includes('ppv')) {
        return `${p.text || ''} [SYSTEM: PPV SENT — category=${p.category}, price=$${p.price}, items=${p.itemCount || '?'}. DO NOT re-pitch this same content.]`;
      }
      return p.text;
    }).join(' ... ');

    if (assistantContent) {
      convHistory.push({ role: 'assistant', content: assistantContent });
    }
    await R.set(`chatbot:bianca:conv:${fanId}`, convHistory.slice(-50));

    // Update fan profile
    fanProfile.lastBotReplyAt = new Date().toISOString();
    fanProfile.botMessageCount += botResponseParts.length;
    await saveFanProfile(fanProfile);

    // Log
    await logConversation({
      ts: new Date().toISOString(), fanId, direction: 'inbound',
      botResponse: botResponseParts,
      contentSent: botResponseParts.find(p => p.category)?.category || null,
      priceSent: botResponseParts.find(p => p.price)?.price || null,
      buyerType: fanProfile.buyerType,
    });

    console.log(`✅ [bianca] Executed relay response for fan ${fanId}: ${botResponseParts.length} messages sent`);
    return { ok: true, messagesSent: botResponseParts.length };

  } catch (e) {
    stats.errors++;
    console.error(`❌ [bianca] Error executing relay response for fan ${fanId}:`, e.message);
    return { ok: false, error: e.message };
  }
}

// ── Debounce Handler ────────────────────────────────────────────────────────

function handleIncomingMessage(fanId, messageText) {
  if (!isRunning) return;
  if (!pendingBiancaMessages[fanId]) {
    pendingBiancaMessages[fanId] = { messages: [], timer: null };
  }
  pendingBiancaMessages[fanId].messages.push(messageText);

  if (pendingBiancaMessages[fanId].timer) clearTimeout(pendingBiancaMessages[fanId].timer);
  pendingBiancaMessages[fanId].timer = setTimeout(() => {
    const batch = pendingBiancaMessages[fanId];
    delete pendingBiancaMessages[fanId];
    const combined = batch.messages.join('\n');
    processFanMessage(fanId, combined).catch(e => {
      console.error('❌ [bianca] Debounced handler error:', e.message);
    });
  }, DEBOUNCE_MS);
}

// ── Loop 1: Message Polling (30s) ───────────────────────────────────────────

async function pollMessages() {
  if (!isRunning) return;
  try {
    const data = await ofApi('/chats?limit=50&order=recent');
    const chats = data.data || data.list || data || [];

    for (const chat of chats) {
      if (!chat.hasUnread && !chat.unreadCount) continue;
      const fanId = chat.withUser?.id || chat.userId;
      if (!fanId) continue;

      // Fetch unread messages
      try {
        const msgData = await ofApi(`/chats/${fanId}/messages?limit=10`);
        const messages = msgData.data || msgData.list || msgData || [];

        for (const msg of messages) {
          // Only process fan messages (not our own)
          if (msg.isFromMe || msg.isMine) continue;
          // Only unread
          if (msg.isRead) continue;

          const text = msg.text || msg.body || msg.content || '';
          if (!text) continue;

          handleIncomingMessage(String(fanId), text);
        }

        // Mark as read (best effort)
        try {
          await ofApi(`/chats/${fanId}/messages/read`, { method: 'POST', body: {} });
        } catch {}
      } catch (e) {
        console.error(`❌ [bianca] Error fetching messages for fan ${fanId}:`, e.message);
      }
    }
  } catch (e) {
    stats.errors++;
    console.error('❌ [bianca] Poll error:', e.message);
  }
}

// ── Loop 2: Hourly Bump ─────────────────────────────────────────────────────

async function runBumpLoop() {
  if (!isRunning) return;
  try {
    console.log('📢 [bianca] Running hourly bump...');

    // Load bump state
    const bumpState = await R.get('chatbot:bianca:bump_state') || {
      lastBumpMessageId: null,
      lastBumpAt: null,
      recentTexts: [],
      totalBumpsSent: 0,
    };

    // Delete previous bump
    if (bumpState.lastBumpMessageId) {
      try {
        await ofApi(`/mass-messaging/${bumpState.lastBumpMessageId}`, { method: 'DELETE' });
        console.log(`🗑️ [bianca] Deleted previous bump ${bumpState.lastBumpMessageId}`);
      } catch (e) {
        console.log(`⚠️ [bianca] Could not delete previous bump: ${e.message}`);
      }
    }

    // Fetch a random photo from bump vault
    const vaultItems = await fetchVaultItems(BUMP_VAULT_CATEGORY_ID);
    if (vaultItems.length === 0) {
      console.warn('⚠️ [bianca] No bump vault items found');
      return;
    }
    const randomPhoto = vaultItems[Math.floor(Math.random() * vaultItems.length)];

    // Pick bump text (avoid recent repeats)
    const recentTexts = new Set(bumpState.recentTexts || []);
    const available = BUMP_TEXTS.filter(t => !recentTexts.has(t));
    const pool = available.length > 0 ? available : BUMP_TEXTS;
    const bumpText = pool[Math.floor(Math.random() * pool.length)];

    // Get active conversation fan IDs to exclude
    const activeConvoFans = await R.smembers('chatbot:bianca:active_convos');
    // Filter: only fans with activity in last 2 hours
    const now = Date.now();
    const activeExcludes = [];
    for (const fid of activeConvoFans) {
      const ts = await R.get(`chatbot:bianca:active_ts:${fid}`);
      if (ts && now - Number(ts) < 2 * 3600000) {
        activeExcludes.push(Number(fid));
      }
    }

    // Get all excluded fan IDs
    const excludedFans = await R.hgetall('chatbot:bianca:excluded_fans');
    const excludeUserIds = [
      ...Object.keys(EXCLUDED_WHALE_IDS).map(Number),
      ...Object.keys(resolvedExcludeIds).map(Number),
      ...Object.keys(excludedFans).map(Number),
      ...activeExcludes,
    ];

    // Send mass message
    const body = {
      text: bumpText,
      mediaFiles: [randomPhoto],
      excludedLists: [LIST_IDS.timewasters, LIST_IDS.brokeStudent, LIST_IDS.lowballers],
      excludeUserIds: [...new Set(excludeUserIds)],
    };

    const result = await ofApi('/mass-messaging', { method: 'POST', body });
    const messageId = result?.data?.[0]?.id || result?.id || null;

    // Update bump state
    const newRecentTexts = [bumpText, ...(bumpState.recentTexts || [])].slice(0, 3);
    await R.set('chatbot:bianca:bump_state', {
      lastBumpMessageId: messageId,
      lastBumpAt: new Date().toISOString(),
      lastBumpText: bumpText,
      recentTexts: newRecentTexts,
      totalBumpsSent: (bumpState.totalBumpsSent || 0) + 1,
    });

    stats.bumpsSent++;
    stats.lastActivityAt = new Date().toISOString();
    console.log(`📢 [bianca] Bump sent: "${bumpText}" (msg ${messageId})`);

  } catch (e) {
    stats.errors++;
    console.error('❌ [bianca] Bump loop error:', e.message);
  }
}

// ── Loop 3: New Subscriber Welcome (2min) ───────────────────────────────────

async function checkNewSubscribers() {
  if (!isRunning) return;
  try {
    // Fetch "New Subs (Clear every 8AM)" list
    const data = await ofApi(`/user-lists/${LIST_IDS.newSubs}/users`);
    const users = data.data || data.list || data || [];

    for (const user of users) {
      const fanId = String(user.id || user.userId);
      if (!fanId) continue;

      // Already welcomed?
      if (await R.sismember('chatbot:bianca:welcomed_fans', fanId)) continue;

      // Excluded?
      if (await isExcludedFan(fanId)) {
        await R.sadd('chatbot:bianca:welcomed_fans', fanId);
        continue;
      }

      // Send welcome (template, no Claude)
      const welcomeText = WELCOME_TEMPLATES[Math.floor(Math.random() * WELCOME_TEMPLATES.length)];

      try {
        await sendTextMessage(fanId, welcomeText);
        await R.sadd('chatbot:bianca:welcomed_fans', fanId);

        // Init fan profile
        const profile = defaultFanProfile(fanId);
        profile.username = user.username || user.name || null;
        profile.welcomed = true;
        profile.firstSeenAt = new Date().toISOString();
        await saveFanProfile(profile);

        // Init conversation history
        await R.set(`chatbot:bianca:conv:${fanId}`, [
          { role: 'assistant', content: welcomeText },
        ]);

        stats.welcomesSent++;
        stats.messagesSent++;
        stats.lastActivityAt = new Date().toISOString();
        console.log(`👋 [bianca] Welcomed new sub ${fanId} (${profile.username})`);
      } catch (e) {
        console.error(`❌ [bianca] Welcome send error for ${fanId}:`, e.message);
      }

      // Small delay between welcomes
      await new Promise(r => setTimeout(r, 3000));
    }
  } catch (e) {
    stats.errors++;
    console.error('❌ [bianca] Welcome loop error:', e.message);
  }
}

// ── Loop 4: Spender Retarget (daily 9pm AST = 01:00 UTC) ───────────────────

async function runRetargetLoop() {
  if (!isRunning) return;
  try {
    console.log('🎯 [bianca] Running spender retarget...');

    const todayStr = new Date().toISOString().slice(0, 10);
    let retargetState = await R.get('chatbot:bianca:retarget_state') || {
      lastRunDate: null,
      retargetedToday: [],
      retargetCount: 0,
      fanCooldowns: {},
    };

    // Reset daily counter
    if (retargetState.lastRunDate !== todayStr) {
      retargetState.retargetedToday = [];
      retargetState.retargetCount = 0;
      retargetState.lastRunDate = todayStr;
    }

    if (retargetState.retargetCount >= 10) {
      console.log('🎯 [bianca] Retarget limit reached for today (10)');
      return;
    }

    // Fetch $101-$500 spender list
    const data = await ofApi(`/user-lists/${LIST_IDS.spenders101to500}/users`);
    const users = data.data || data.list || data || [];

    for (const user of users) {
      if (retargetState.retargetCount >= 10) break;

      const fanId = String(user.id || user.userId);
      if (!fanId) continue;

      // Skip excluded
      if (await isExcludedFan(fanId)) continue;

      // Skip already retargeted today
      if (retargetState.retargetedToday.includes(fanId)) continue;

      // Check cooldown (2 attempts max, 30-day cooldown)
      const cooldown = retargetState.fanCooldowns[fanId];
      if (cooldown) {
        if (cooldown.attempts >= 2 && cooldown.cooldownUntil) {
          if (new Date(cooldown.cooldownUntil) > new Date()) continue;
        }
      }

      // Check if fan has been active recently (skip if active in last 7 days)
      const profile = await getFanProfile(fanId);
      if (profile.lastMessageAt) {
        const daysSinceMsg = (Date.now() - new Date(profile.lastMessageAt).getTime()) / (86400000);
        if (daysSinceMsg < 7) continue;
      }

      // Build retarget message via Claude
      const convHistory = await R.get(`chatbot:bianca:conv:${fanId}`) || [];
      
      // For retargeting, we'll use a simple template rather than calling Claude
      const retargetMessages = [
        'heyy been thinking about u 💕',
        'miss talking to u 🥺',
        'hey stranger... remember me? 😏',
        'bored rn and thought of u 💕 how have u been?',
        'hiii 👋 been forever since we talked',
      ];
      
      const retargetText = retargetMessages[Math.floor(Math.random() * retargetMessages.length)];

      try {
        // Check restricted
        const restricted = containsRestricted(retargetText);
        if (restricted) {
          console.warn(`⚠️ [bianca] Restricted word in retarget template, skipping`);
          continue;
        }

        await sendTextMessage(fanId, retargetText);
        stats.retargetsSent++;
        stats.messagesSent++;
        stats.lastActivityAt = new Date().toISOString();

        // Update retarget state
        retargetState.retargetedToday.push(fanId);
        retargetState.retargetCount++;
        retargetState.fanCooldowns[fanId] = retargetState.fanCooldowns[fanId] || { attempts: 0 };
        retargetState.fanCooldowns[fanId].attempts++;
        retargetState.fanCooldowns[fanId].lastAt = new Date().toISOString();
        if (retargetState.fanCooldowns[fanId].attempts >= 2) {
          const cooldownDate = new Date();
          cooldownDate.setDate(cooldownDate.getDate() + 30);
          retargetState.fanCooldowns[fanId].cooldownUntil = cooldownDate.toISOString().slice(0, 10);
        }

        // Update profile
        profile.retargetCount = (profile.retargetCount || 0) + 1;
        profile.lastRetargetAt = new Date().toISOString();
        await saveFanProfile(profile);

        // Update conversation
        convHistory.push({ role: 'assistant', content: retargetText });
        await R.set(`chatbot:bianca:conv:${fanId}`, convHistory.slice(-50));

        console.log(`🎯 [bianca] Retargeted fan ${fanId}: "${retargetText}"`);
      } catch (e) {
        console.error(`❌ [bianca] Retarget send error for ${fanId}:`, e.message);
      }

      await new Promise(r => setTimeout(r, 5000)); // Pace retargets
    }

    await R.set('chatbot:bianca:retarget_state', retargetState);
    console.log(`🎯 [bianca] Retarget complete: ${retargetState.retargetCount} fans today`);

  } catch (e) {
    stats.errors++;
    console.error('❌ [bianca] Retarget loop error:', e.message);
  }
}

// ── Timewaster Detection (every 6 hours) ────────────────────────────────────

async function checkTimewasters() {
  if (!isRunning) return;
  // This runs periodically to re-score fans. Full implementation would scan
  // known fan profiles and update timewaster scores.
  // For now, the scoring happens inline when processing messages.
  console.log('🔍 [bianca] Timewaster check (placeholder — scoring happens inline)');
}

// ── Save/Load Global State ──────────────────────────────────────────────────

async function saveGlobalState() {
  await R.set('chatbot:bianca:state', {
    enabled: isRunning,
    startedAt,
    stats,
  });
}

// ── Start / Stop ────────────────────────────────────────────────────────────

async function startChatbot(redisClient) {
  if (isRunning) return { ok: true, message: 'Already running' };

  redis = redisClient;

  // Check both env var and Redis flag
  if (process.env.CHATBOT_BIANCA_ENABLED !== 'true') {
    console.log('⏸️ [bianca] CHATBOT_BIANCA_ENABLED is not true, not starting');
    return { ok: false, message: 'CHATBOT_BIANCA_ENABLED env var is not true' };
  }

  if (!ANTHROPIC_API_KEY) {
    console.error('❌ [bianca] ANTHROPIC_API_KEY not set');
    return { ok: false, message: 'ANTHROPIC_API_KEY not set' };
  }

  console.log('🚀 [bianca] Starting chatbot engine...');

  // Load resources
  loadContentMap();
  loadBiancaPrompt();

  // Resolve account
  await resolveAccountId();
  if (!biancaAccountId) {
    return { ok: false, message: 'Could not resolve bianca account ID' };
  }

  // Resolve excluded usernames
  await resolveExcludedUsernames();

  // Set Redis enabled flag
  await R.set('chatbot:bianca:enabled', true);

  isRunning = true;
  startedAt = new Date().toISOString();

  // Start loops
  messagePollingInterval = setInterval(pollMessages, 30000);
  welcomeCheckInterval = setInterval(checkNewSubscribers, 120000);

  // Hourly bump (on the hour)
  bumpCronJob = cron.schedule('0 * * * *', runBumpLoop);

  // Daily retarget at 9pm AST = 01:00 UTC
  retargetCronJob = cron.schedule('0 1 * * *', runRetargetLoop);

  // Timewaster check every 6 hours
  timewasterCronJob = cron.schedule('0 */6 * * *', checkTimewasters);

  await saveGlobalState();

  console.log('✅ [bianca] Chatbot engine started');
  console.log('   📨 Message polling: every 30s');
  console.log('   📢 Bump loop: every hour');
  console.log('   👋 Welcome check: every 2min');
  console.log('   🎯 Retarget: daily 9pm AST');

  return { ok: true, message: 'Biancawoods chatbot started', startedAt };
}

async function stopChatbot() {
  if (!isRunning) return { ok: true, message: 'Already stopped', stats };

  console.log('⏹️ [bianca] Stopping chatbot engine...');

  isRunning = false;

  if (messagePollingInterval) { clearInterval(messagePollingInterval); messagePollingInterval = null; }
  if (welcomeCheckInterval) { clearInterval(welcomeCheckInterval); welcomeCheckInterval = null; }
  if (bumpCronJob) { bumpCronJob.stop(); bumpCronJob = null; }
  if (retargetCronJob) { retargetCronJob.stop(); retargetCronJob = null; }
  if (timewasterCronJob) { timewasterCronJob.stop(); timewasterCronJob = null; }
  if (profileCleanupCronJob) { profileCleanupCronJob.stop(); profileCleanupCronJob = null; }

  // Clear pending debounces
  for (const fanId of Object.keys(pendingBiancaMessages)) {
    if (pendingBiancaMessages[fanId].timer) clearTimeout(pendingBiancaMessages[fanId].timer);
    delete pendingBiancaMessages[fanId];
  }

  await R.set('chatbot:bianca:enabled', false);
  await saveGlobalState();

  console.log('⏹️ [bianca] Chatbot engine stopped');
  return { ok: true, message: 'Biancawoods chatbot stopped', stats };
}

// ── API Endpoint Handlers ───────────────────────────────────────────────────

async function statusHandler(req, res) {
  const globalState = await R.get('chatbot:bianca:state');
  const bumpState = await R.get('chatbot:bianca:bump_state');
  const retargetState = await R.get('chatbot:bianca:retarget_state');
  const excludedFans = await R.hgetall('chatbot:bianca:excluded_fans');

  const uptime = startedAt ? `${Math.round((Date.now() - new Date(startedAt).getTime()) / 60000)}m` : null;

  res.json({
    enabled: isRunning,
    uptime,
    startedAt,
    stats,
    loops: {
      messagePolling: { status: messagePollingInterval ? 'running' : 'stopped', interval: '30s' },
      bumpLoop: { status: bumpCronJob ? 'running' : 'stopped', interval: '60m', lastBump: bumpState?.lastBumpAt },
      welcomeLoop: { status: welcomeCheckInterval ? 'running' : 'stopped', interval: '2m' },
      retargetLoop: { status: retargetCronJob ? 'running' : 'stopped', nextRun: 'daily 01:00 UTC', todayCount: retargetState?.retargetCount || 0 },
    },
    excludedFans: Object.keys(excludedFans).length,
    bumpState: bumpState ? { lastBumpAt: bumpState.lastBumpAt, totalBumpsSent: bumpState.totalBumpsSent } : null,
  });
}

async function startHandler(req, res) {
  const result = await startChatbot(redis);
  res.json(result);
}

async function stopHandler(req, res) {
  const result = await stopChatbot();
  res.json(result);
}

async function fansHandler(req, res) {
  const limit = parseInt(req.query.limit) || 50;
  const offset = parseInt(req.query.offset) || 0;

  // We can't scan all keys with Upstash easily, so return what we know from logs
  // In practice, you'd maintain a fan index
  const logs = await R.lrange('chatbot:bianca:log', 0, 500);
  const fanIds = [...new Set(logs.map(l => l.fanId).filter(Boolean))];

  const fans = [];
  const slice = fanIds.slice(offset, offset + limit);
  for (const fanId of slice) {
    const profile = await getFanProfile(fanId);
    fans.push({
      fanId: profile.fanId,
      username: profile.username,
      buyerType: profile.buyerType,
      totalSpent: profile.totalSpent,
      purchaseCount: profile.purchaseCount,
      lastMessageAt: profile.lastMessageAt,
      isTimewaster: profile.isTimewaster,
      sextingProgress: {
        sexting1: profile.sextingProgress?.sexting1?.currentStep || 0,
        sexting2: profile.sextingProgress?.sexting2?.currentStep || 0,
        sexting3: profile.sextingProgress?.sexting3?.currentStep || 0,
      },
    });
  }

  res.json({ total: fanIds.length, fans });
}

async function logsHandler(req, res) {
  const limit = parseInt(req.query.limit) || 100;
  const fanId = req.query.fanId;

  let logs = await R.lrange('chatbot:bianca:log', 0, limit - 1);
  if (fanId) {
    logs = logs.filter(l => l.fanId === fanId);
  }

  res.json({ total: logs.length, logs });
}

async function excludeHandler(req, res) {
  const { fanId } = req.params;
  const { reason } = req.body || {};
  await R.hset('chatbot:bianca:excluded_fans', { [fanId]: reason || 'manual exclude' });
  res.json({ ok: true, fanId, excluded: true });
}

async function unexcludeHandler(req, res) {
  const { fanId } = req.params;
  // Don't allow removing hardcoded whales
  if (EXCLUDED_WHALE_IDS[fanId] || resolvedExcludeIds[fanId]) {
    return res.status(400).json({ ok: false, message: 'Cannot remove hardcoded whale exclusion' });
  }
  await R.hdel('chatbot:bianca:excluded_fans', fanId);
  res.json({ ok: true, fanId, excluded: false });
}

// ── Webhook Integration ─────────────────────────────────────────────────────
// Called from index.js when a webhook event arrives for biancawoods

function handleWebhookEvent(event, payload) {
  if (!isRunning) return;

  if (event === 'subscriptions.new') {
    const fanId = String(payload?.fanId || payload?.user_id || payload?.user?.id || '');
    if (fanId) {
      (async () => {
        try {
          // Check if already welcomed
          if (await R.sismember('chatbot:bianca:welcomed_fans', fanId)) return;
          if (await isExcludedFan(fanId)) { await R.sadd('chatbot:bianca:welcomed_fans', fanId); return; }

          // Send welcome with GFE selfie (from free content)
          const gfeSelfieId = CONTENT_MAP?.freeContent?.gfeSelfies?.categoryId;
          if (gfeSelfieId) {
            const vaultItems = await fetchVaultItems(gfeSelfieId, 5);
            if (vaultItems.length > 0) {
              const randomSelfie = vaultItems[Math.floor(Math.random() * vaultItems.length)];
              const welcomeText = WELCOME_TEMPLATES[Math.floor(Math.random() * WELCOME_TEMPLATES.length)];
              
              await sendMediaMessage(fanId, welcomeText, [randomSelfie]);
              await R.sadd('chatbot:bianca:welcomed_fans', fanId);
              stats.welcomesSent++;
              console.log(`👋 [bianca] Welcomed new sub ${fanId} with GFE selfie`);
            }
          }
        } catch (e) {
          console.error(`❌ [bianca] Welcome error for ${fanId}:`, e.message);
        }
      })();
    }
  }

  if (event === 'messages.received') {
    const fanId = payload?.fromUser?.id;
    const text = payload?.text || payload?.body || payload?.content || '';
    if (fanId && text) {
      handleIncomingMessage(String(fanId), text);
    }
  }

  if (event === 'messages.ppv.unlocked') {
    const fanId = String(payload?.fromUser?.id || payload?.user?.id || payload?.userId || '');
    const price = parseFloat(payload?.price || payload?.amount || 0);
    if (fanId) {
      // Update fan profile with purchase
      (async () => {
        try {
          const profile = await getFanProfile(fanId);
          profile.totalSpent += price;
          profile.purchaseCount++;
          profile.lastPurchasePrice = price;
          if (profile.purchaseCount > 0) {
            profile.avgPurchasePrice = Math.round((profile.totalSpent / profile.purchaseCount) * 100) / 100;
          }
          // Update price history: mark last offered as opened
          if (profile.priceHistory && profile.priceHistory.length > 0) {
            const last = profile.priceHistory[profile.priceHistory.length - 1];
            if (!last.opened && Math.abs(last.offered - price) < 1) {
              last.opened = true;
            }
          }
          // Update estimated ceiling
          if (price > profile.estimatedCeiling) {
            profile.estimatedCeiling = Math.round(price * 1.2);
          }
          await saveFanProfile(profile);
          stats.ppvRevenue += price;
          console.log(`💰 [bianca] PPV purchased by ${fanId}: $${price} (total: $${profile.totalSpent})`);
        } catch (e) {
          console.error(`❌ [bianca] PPV tracking error:`, e.message);
        }
      })();
    }
  }

  if (event === 'webhooks.test') {
    console.log(`🔔 [bianca] Webhook test received:`, payload);
  }
}

// ── Exports ─────────────────────────────────────────────────────────────────

// ── Relay Endpoints ─────────────────────────────────────────────────────────

async function relayPollHandler(req, res) {
  const queue = await R.get('chatbot:bianca:relay:incoming') || [];
  if (queue.length > 0) {
    await R.set('chatbot:bianca:relay:incoming', []);
  }
  res.json({ messages: queue, polledAt: new Date().toISOString() });
}

async function relayRespondHandler(req, res) {
  const { fanId, response } = req.body;
  if (!fanId || !response) {
    return res.status(400).json({ error: 'fanId and response required' });
  }

  try {
    const result = await executeRelayResponse(fanId, response);
    res.json(result);
  } catch (e) {
    console.error(`❌ [bianca] Relay respond error:`, e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
}

module.exports = {
  // Lifecycle
  startChatbot,
  stopChatbot,
  getChatbotStatus: () => ({ enabled: isRunning, startedAt, stats }),
  handleWebhookEvent,

  // For index.js to set redis
  init(redisClient) { redis = redisClient; },

  // Express handlers
  statusHandler,
  startHandler,
  stopHandler,
  fansHandler,
  logsHandler,
  excludeHandler,
  unexcludeHandler,

  // Relay handlers (OpenClaw integration)
  relayPollHandler,
  relayRespondHandler,

  // Constants (for external use)
  BIANCA_ACCOUNT_ID,
  BIANCA_USERNAME,
};
