/**
 * Chatbot Brain v2 — Intelligent Decision Engine
 * Used by the relay system to generate context-aware responses
 */

// Fan States
const STATES = {
  WELCOME: 'welcome',       // Just subscribed
  RAPPORT: 'rapport',       // Building connection
  TEASE: 'tease',          // Building sexual tension
  PPV_SENT: 'ppv_sent',    // Bundle sent, waiting
  PURCHASED: 'purchased',   // Fan bought something
  UPSELL: 'upsell',        // Ready for next tier
  DECLINED: 'declined',     // Fan said no
  SILENT: 'silent',         // No response
  RE_ENGAGE: 're_engage',  // Returning after gap
};

// Fan Types
const FAN_TYPES = {
  UNKNOWN: 'unknown',
  SILENT_BUYER: 'silent_buyer',
  EMOTIONAL: 'emotional',
  KINK: 'kink',
  SELF_DEPRECATING: 'self_deprecating',
  NEGOTIATOR: 'negotiator',
  SKEPTIC: 'skeptic',
  SEXUAL: 'sexual',
  GFE: 'gfe',
};

// Bundle tier progression
const TIER_PROGRESSION = {
  starter: { price: 18, nextTier: 'vip', bundles: ['bundle_1','bundle_2','bundle_3','bundle_4','bundle_5','bundle_6','bundle_7','bundle_8','bundle_9','bundle_10'] },
  vip: { price: 28, nextTier: 'sexting', bundles: ['vip_bundle_1','vip_bundle_2','vip_bundle_3','vip_bundle_4','vip_bundle_5','vip_bundle_6','vip_bundle_7','vip_bundle_8'] },
  sexting: { price: 45, nextTier: 'whale', bundles: ['sexting_1','sexting_2'] },
  whale: { price: 75, nextTier: null, bundles: ['cwm'] },
};

// Auto-detect fan type from messages
function detectFanType(messages) {
  const fanMessages = messages.filter(m => m.role === 'user').map(m => (m.content || '').toLowerCase());
  const allText = fanMessages.join(' ');
  
  // Kink detection
  const kinkKeywords = ['cage', 'bdsm', 'dom', 'sub', 'punish', 'spank', 'choke', 'collar', 'leash', 'slave', 'master', 'mistress', 'feet', 'worship', 'humiliat', 'cock cage', 'pay pig', 'findom'];
  if (kinkKeywords.some(k => allText.includes(k))) return FAN_TYPES.KINK;
  
  // Self-deprecating
  const selfDepKeywords = ['small dick', 'not good enough', 'dont deserve', 'im ugly', 'out of my league', 'too good for me', 'pathetic'];
  if (selfDepKeywords.some(k => allText.includes(k))) return FAN_TYPES.SELF_DEPRECATING;
  
  // Sexual from the start
  const sexualKeywords = ['hard', 'stroking', 'cum', 'dick', 'cock', 'horny', 'jerk', 'fuck me', 'nude', 'naked', 'pussy'];
  if (fanMessages.length <= 3 && sexualKeywords.some(k => allText.includes(k))) return FAN_TYPES.SEXUAL;
  
  // Negotiator
  const negotiatorKeywords = ['how much', 'what do i get', 'discount', 'deal', 'cheaper', 'too expensive', 'thats a lot'];
  if (negotiatorKeywords.some(k => allText.includes(k))) return FAN_TYPES.NEGOTIATOR;
  
  // Skeptic
  const skepticKeywords = ['scam', 'fake', 'same thing', 'already seen', 'not worth', 'waste', 'rip off', 'bs'];
  if (skepticKeywords.some(k => allText.includes(k))) return FAN_TYPES.SKEPTIC;
  
  // Emotional / GFE (longer messages, personal details)
  const avgLength = fanMessages.reduce((sum, m) => sum + m.length, 0) / (fanMessages.length || 1);
  const personalKeywords = ['lonely', 'miss', 'connection', 'beautiful', 'gorgeous', 'love', 'girlfriend', 'talk to', 'company', 'before bed'];
  if (avgLength > 50 && personalKeywords.some(k => allText.includes(k))) return FAN_TYPES.GFE;
  if (avgLength > 80) return FAN_TYPES.EMOTIONAL;
  
  // Silent buyer — few messages but has purchases
  if (fanMessages.length <= 3 && messages.some(m => m.content && m.content.includes('[PPV'))) return FAN_TYPES.SILENT_BUYER;
  
  return FAN_TYPES.UNKNOWN;
}

// Determine fan's current state from conversation
function detectFanState(messages, sentBundles = []) {
  if (!messages || messages.length === 0) return STATES.WELCOME;
  
  const lastMsg = messages[messages.length - 1];
  const fanMessages = messages.filter(m => m.role === 'user');
  const botMessages = messages.filter(m => m.role === 'assistant');
  
  // Check if we've sent a PPV recently
  const lastBotMsg = botMessages.length ? botMessages[botMessages.length - 1] : null;
  const hasPendingPPV = lastBotMsg && lastBotMsg.content && lastBotMsg.content.includes('[PPV SENT');
  
  // Check if fan just purchased
  const lastFanMsg = fanMessages.length ? fanMessages[fanMessages.length - 1] : null;
  if (lastFanMsg && lastFanMsg.content && lastFanMsg.type === 'purchase') return STATES.PURCHASED;
  if (lastFanMsg && lastFanMsg.content && lastFanMsg.content.includes('PURCHASED')) return STATES.PURCHASED;
  
  // Check for declines
  const declineKeywords = ['no thanks', 'not interested', 'too much', 'cant afford', 'maybe later', 'no budget', 'nah'];
  if (lastFanMsg && declineKeywords.some(k => (lastFanMsg.content || '').toLowerCase().includes(k))) return STATES.DECLINED;
  
  // If we sent PPV and fan hasn't bought yet
  if (hasPendingPPV && lastMsg.role === 'user') {
    // Fan responded but didn't buy — still in tease/pressure mode
    return STATES.TEASE;
  }
  
  // If we have sent bundles and fan has bought some
  if (sentBundles.length > 0) return STATES.UPSELL;
  
  // Based on message count
  if (fanMessages.length <= 1) return STATES.WELCOME;
  if (fanMessages.length <= 3) return STATES.RAPPORT;
  
  return STATES.TEASE;
}

// Pick the next bundle to send
function pickNextBundle(sentBundleNames = [], currentTier = 'starter') {
  const tier = TIER_PROGRESSION[currentTier];
  if (!tier) return null;
  
  // Find first unsent bundle in current tier
  const available = tier.bundles.filter(b => !sentBundleNames.includes(b));
  if (available.length > 0) {
    return { bundle: available[0], price: tier.price, tier: currentTier };
  }
  
  // Current tier exhausted, move to next
  if (tier.nextTier) {
    return pickNextBundle(sentBundleNames, tier.nextTier);
  }
  
  return null; // All bundles sent
}

// Determine which tier fan should be on based on purchase history
function getCurrentTier(purchases = []) {
  if (purchases.length === 0) return 'starter';
  
  const maxPrice = Math.max(...purchases.map(p => p.price || 0));
  if (maxPrice >= 50) return 'whale';
  if (maxPrice >= 35) return 'sexting';
  if (maxPrice >= 22) return 'vip';
  return 'starter'; // First purchase was starter, try vip next
}

// Generate context summary for the AI brain
function generateFanContext(messages, sentBundles = [], purchases = []) {
  const fanType = detectFanType(messages);
  const state = detectFanState(messages, sentBundles);
  const tier = getCurrentTier(purchases);
  const nextBundle = pickNextBundle(sentBundles.map(s => s.bundle), tier === 'starter' && purchases.length > 0 ? 'vip' : tier);
  
  const fanMsgCount = messages.filter(m => m.role === 'user').length;
  const totalSpent = purchases.reduce((sum, p) => sum + (p.price || 0), 0);
  
  return {
    fanType,
    state,
    currentTier: tier,
    nextBundle,
    messageCount: fanMsgCount,
    purchaseCount: purchases.length,
    totalSpent,
    sentBundleCount: sentBundles.length,
    summary: `Fan type: ${fanType} | State: ${state} | Tier: ${tier} | Messages: ${fanMsgCount} | Purchases: ${purchases.length} ($${totalSpent}) | Bundles sent: ${sentBundles.length}${nextBundle ? ` | Next: ${nextBundle.bundle} at $${nextBundle.price}` : ' | ALL BUNDLES EXHAUSTED'}`
  };
}

module.exports = {
  STATES,
  FAN_TYPES,
  TIER_PROGRESSION,
  detectFanType,
  detectFanState,
  pickNextBundle,
  getCurrentTier,
  generateFanContext,
};
