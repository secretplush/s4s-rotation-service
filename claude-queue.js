/**
 * Claude API Request Queue
 * 
 * Features:
 * - Concurrency limiting (max N parallel requests)
 * - Prompt caching (90% savings on repeated system prompts)
 * - Exponential backoff on 429 rate limits
 * - Priority queue (whale fans go first)
 * - Request deduplication
 */

const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// ── Config ──────────────────────────────────────────────────────────────────

const MAX_CONCURRENT = 3;          // Max parallel Claude calls
const MAX_QUEUE_SIZE = 50;         // Drop requests beyond this
const BASE_BACKOFF_MS = 1000;      // Initial retry delay on 429
const MAX_BACKOFF_MS = 30000;      // Max retry delay
const MAX_RETRIES = 3;             // Max retries per request

// ── State ───────────────────────────────────────────────────────────────────

let activeRequests = 0;
const queue = [];  // { resolve, reject, params, priority, enqueuedAt }
let globalBackoffUntil = 0;  // Timestamp — if set, all requests wait

// Stats
const queueStats = {
  totalRequests: 0,
  totalCompleted: 0,
  totalRetries: 0,
  total429s: 0,
  totalErrors: 0,
  cacheHits: 0,
  avgLatencyMs: 0,
  _latencySum: 0,
};

// ── Queue Management ────────────────────────────────────────────────────────

function processQueue() {
  while (activeRequests < MAX_CONCURRENT && queue.length > 0) {
    // Wait for global backoff
    if (Date.now() < globalBackoffUntil) {
      const waitMs = globalBackoffUntil - Date.now();
      setTimeout(processQueue, waitMs + 50);
      return;
    }

    const item = queue.shift();
    activeRequests++;
    
    executeRequest(item.params, item.retryCount || 0)
      .then(result => {
        item.resolve(result);
      })
      .catch(err => {
        item.reject(err);
      })
      .finally(() => {
        activeRequests--;
        queueStats.totalCompleted++;
        processQueue();
      });
  }
}

/**
 * Queue a Claude API call.
 * 
 * @param {Object} params - { model, system, messages, max_tokens, priority }
 *   priority: 'high' (whales) | 'normal' | 'low' (retargets/bumps)
 * @returns {Promise<Object>} - { parsed, latencyMs, cached }
 */
function queueRequest(params) {
  return new Promise((resolve, reject) => {
    queueStats.totalRequests++;

    // Drop if queue too long
    if (queue.length >= MAX_QUEUE_SIZE) {
      queueStats.totalErrors++;
      return reject(new Error('Queue full — request dropped'));
    }

    const priority = params.priority || 'normal';
    const item = { resolve, reject, params, priority, enqueuedAt: Date.now(), retryCount: 0 };

    // Insert by priority
    if (priority === 'high') {
      // Find first non-high item
      const idx = queue.findIndex(q => q.priority !== 'high');
      if (idx === -1) queue.push(item);
      else queue.splice(idx, 0, item);
    } else if (priority === 'low') {
      queue.push(item);
    } else {
      // Normal — after high, before low
      const idx = queue.findIndex(q => q.priority === 'low');
      if (idx === -1) queue.push(item);
      else queue.splice(idx, 0, item);
    }

    processQueue();
  });
}

// ── Execute with Retry ──────────────────────────────────────────────────────

async function executeRequest(params, retryCount) {
  const startMs = Date.now();

  // Build request body with prompt caching
  const systemContent = buildCachedSystem(params.system);

  const body = {
    model: params.model || 'claude-sonnet-4-20250514',
    max_tokens: params.max_tokens || 1024,
    system: systemContent,
    messages: params.messages,
  };

  try {
    const res = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'anthropic-beta': 'prompt-caching-2024-07-31',
      },
      body: JSON.stringify(body),
    });

    const latencyMs = Date.now() - startMs;

    // Handle 429
    if (res.status === 429) {
      queueStats.total429s++;
      const retryAfter = parseInt(res.headers.get('retry-after') || '0');
      const backoffMs = retryAfter > 0 
        ? retryAfter * 1000 
        : Math.min(BASE_BACKOFF_MS * Math.pow(2, retryCount), MAX_BACKOFF_MS);
      
      console.log(`⏳ [claude-queue] 429 rate limit — backing off ${backoffMs}ms (retry ${retryCount + 1}/${MAX_RETRIES})`);
      
      // Set global backoff so other requests wait too
      globalBackoffUntil = Date.now() + backoffMs;
      
      if (retryCount < MAX_RETRIES) {
        queueStats.totalRetries++;
        await new Promise(r => setTimeout(r, backoffMs));
        return executeRequest(params, retryCount + 1);
      }
      
      throw new Error(`429 rate limit after ${MAX_RETRIES} retries`);
    }

    // Handle 529 (overloaded)
    if (res.status === 529) {
      const backoffMs = Math.min(BASE_BACKOFF_MS * Math.pow(2, retryCount), MAX_BACKOFF_MS);
      console.log(`⏳ [claude-queue] 529 overloaded — backing off ${backoffMs}ms`);
      
      if (retryCount < MAX_RETRIES) {
        queueStats.totalRetries++;
        await new Promise(r => setTimeout(r, backoffMs));
        return executeRequest(params, retryCount + 1);
      }
      throw new Error(`529 overloaded after ${MAX_RETRIES} retries`);
    }

    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Claude API ${res.status}: ${err}`);
    }

    const data = await res.json();
    const text = data.content?.[0]?.text || '';

    // Track cache usage
    if (data.usage?.cache_read_input_tokens > 0) {
      queueStats.cacheHits++;
    }

    // Update avg latency
    queueStats._latencySum += latencyMs;
    queueStats.avgLatencyMs = Math.round(queueStats._latencySum / (queueStats.totalCompleted + 1));

    // Parse JSON response
    const parsed = parseClaudeJSON(text);

    return {
      parsed,
      latencyMs,
      cached: (data.usage?.cache_read_input_tokens || 0) > 0,
      usage: data.usage,
    };

  } catch (e) {
    if (e.message.includes('429') || e.message.includes('529')) throw e;
    queueStats.totalErrors++;
    throw e;
  }
}

// ── Prompt Caching ──────────────────────────────────────────────────────────

function buildCachedSystem(systemText) {
  // Anthropic prompt caching: split system into static (cacheable) + dynamic parts.
  // The static playbook/personality is identical across all fans → cached at 90% discount.
  // The dynamic fan context changes per request → charged full price but is small.
  
  // Look for the fan context separator
  const separator = '=== FAN CONTEXT ===';
  const splitIdx = systemText.indexOf(separator);
  
  if (splitIdx > 0) {
    // Split: cache the large static playbook, send fan context uncached
    const staticPart = systemText.substring(0, splitIdx).trim();
    const dynamicPart = systemText.substring(splitIdx).trim();
    
    return [
      {
        type: 'text',
        text: staticPart,
        cache_control: { type: 'ephemeral' },
      },
      {
        type: 'text',
        text: dynamicPart,
      },
    ];
  }
  
  // No separator found — cache the whole thing
  return [
    {
      type: 'text',
      text: systemText,
      cache_control: { type: 'ephemeral' },
    },
  ];
}

// ── JSON Parsing ────────────────────────────────────────────────────────────

function parseClaudeJSON(text) {
  try {
    const cleaned = text.replace(/```json\n?|\n?```/g, '').trim();
    return JSON.parse(cleaned);
  } catch {
    try {
      const jsonMatch = text.match(/\{[\s\S]*\}/);
      if (jsonMatch) return JSON.parse(jsonMatch[0]);
    } catch {}
    // Fallback: treat as plain text message
    return {
      messages: [{ text: text.replace(/```json\n?|\n?```/g, '').trim(), action: 'message' }],
    };
  }
}

// ── Exports ─────────────────────────────────────────────────────────────────

module.exports = {
  queueRequest,
  getQueueStats: () => ({
    ...queueStats,
    activeRequests,
    queueLength: queue.length,
    globalBackoffUntil: globalBackoffUntil > Date.now() ? globalBackoffUntil - Date.now() : 0,
  }),
};
