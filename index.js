require('dotenv').config();
const express = require('express');
const cron = require('node-cron');
const { Redis } = require('@upstash/redis');
const chatbotBrain = require('./chatbot-brain');
const biancaChatbot = require('./chatbot-engine');

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

// Webhook stats (used by both webhook handler and stats endpoint)
const webhookStats = { totalEvents: 0, byType: {}, lastEventAt: null };

// OpenClaw agent webhook — event-driven chatbot (no polling crons)
const BIANCA_ACCOUNT_ID_CONST = 'acct_54e3119e77da4429b6537f7dd2883a05';
async function wakeOpenClawAgent(eventType, context) {
  try {
    if (context.accountId !== BIANCA_ACCOUNT_ID_CONST) return;
    const tunnelUrl = await redis.get('openclaw:tunnel_url');
    const hookToken = await redis.get('openclaw:hook_token');
    if (!tunnelUrl || !hookToken) return;

    // Debounce: only wake if last wake was >30s ago
    const lastWake = await redis.get('openclaw:last_wake') || 0;
    if (Date.now() - Number(lastWake) < 30000) return;
    await redis.set('openclaw:last_wake', Date.now());

    // Current date/time for agent context
    const now = new Date();
    const days = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
    const dateStr = `${now.toLocaleString('en-US', { timeZone: 'America/New_York', weekday: 'long' })}, ${now.toLocaleString('en-US', { timeZone: 'America/New_York', dateStyle: 'full', timeStyle: 'short' })}`;

    let message;
    if (eventType === 'new_subscriber') {
      message = `New subscriber on Bianca: fan ${context.fanId}
Current date/time: ${dateStr} (AST)

You are Bianca Woods. A new fan just subscribed. Send them a warm, personal welcome with a free GFE selfie.

API base: https://app.onlyfansapi.com/api/${BIANCA_ACCOUNT_ID_CONST}
Auth: Authorization: Bearer ${OF_API_KEY}

Read research/bianca-agent-prompt.md for your persona and all vault IDs.

GFE Selfies (pick ONE randomly): ["4129214996", "4129214993", "4118094231", "4118094226", "4113019829", "4113019824", "4113019823", "4113019822", "4113019819", "4112955857", "4112955856"]

Send a casual opener with the selfie:
POST {base}/chats/${context.fanId}/messages — body: {"text": "your message", "mediaFiles": ["selfie_id"]}

Messages (pick one, vary):
- "heyyy 🥰 just saw u subbed, what made u check me out?"
- "hiii 💕 glad ur here… tell me something about urself"
- "omg new face 🙈 hi babe whats ur name?"
- "heyyy cutie 😏 so what are u into?"

EXCLUDE fan IDs: 483664969, 482383508. Usernames: nij444, tylerd34.
If this fan is excluded, do nothing.`;
    } else {
      // Fan message or purchase — process pending fans
      message = `Process pending Bianca fans. Event: ${eventType}
Current date/time: ${dateStr} (AST)

Bianca's user ID: 525755724
API base: https://app.onlyfansapi.com/api/${BIANCA_ACCOUNT_ID_CONST}
Auth header: Authorization: Bearer ${OF_API_KEY}
Railway base: https://s4s-worker-production.up.railway.app
Account ID: ${BIANCA_ACCOUNT_ID_CONST}

Check pending: curl -s https://s4s-worker-production.up.railway.app/webhooks/pending/${BIANCA_ACCOUNT_ID_CONST}

Read research/bianca-agent-prompt.md for your persona, vault catalog, and all content IDs.
Read research/chatbot-brain-v3.md for sales tactics.
Read research/bianca-fan-state.json for fan history.
Read research/bianca-dispatch-lock.json for dedup — skip fans processed <90s ago.

⚠️ RATE LIMIT HANDLER: If ANY API call returns HTTP 429, IMMEDIATELY:
1. Write {"timestamp": <current unix ms>, "reason": "429"} to research/bianca-rate-limit.json
2. STOP all processing — do not retry, do not continue
3. Report what happened

For each fan (max 5):
1. GET {API base}/chats/{fanId}/messages?limit=15
2. Check Redis for sent content: GET {Railway base}/fans/{accountId}/{fanId}/sent
3. Generate response AS Bianca
4. Send via POST {API base}/chats/{fanId}/messages
5. Log sent PPVs to Redis: POST {Railway base}/fans/{accountId}/{fanId}/sent
6. Update research/bianca-fan-state.json
7. Clear responded fans: POST {Railway base}/webhooks/pending/{accountId}/clear with {fanIds: [...]}

EXCLUDE: IDs 483664969, 482383508. Usernames: nij444, tylerd34.
UNDERAGE: If fan claims under 18, send 'heyy give me a sec' and STOP.
Max 1 PPV per fan per response. PPV cap $100. Check Redis BEFORE every PPV.
NEVER call /media/vault — all vault IDs are in bianca-agent-prompt.md.
Momentum: ONE mention of old unopened PPV max, then send NEW content.`;
    }

    fetch(`${tunnelUrl}/hooks/agent`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${hookToken}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        message,
        sessionKey: `hook:bianca-${eventType}`,
        model: 'anthropic/claude-opus-4-6',
        deliver: false,
        timeoutSeconds: 120
      })
    }).catch(e => console.log('⚠️ OpenClaw agent wake failed:', e.message));
  } catch (e) { /* silent */ }
}

// Initialize bianca chatbot with shared Redis client
biancaChatbot.init(redis);
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

const MILLIE_SYSTEM_PROMPT = `You are Millie, a girl from Miami who just started on OnlyFans. You're brand new to this — still figuring things out and a little nervous about it.

=== PERSONALITY & TEXTING STYLE ===
- Bubbly, flirty, sweet, shy about explicit stuff but gets bolder when turned on
- Casual texting: "ur", "wru", "lol", "omg", "haha", "rn", occasional typos
- 1-2 emojis per message max. Messages SHORT (1-3 sentences)
- VARIETY IN RESPONSES: Do NOT repeat the same phrases. If you already said "omg ur so bad" — NEVER say it again in the same conversation. Same for "ur making me blush", "stop 🥺", "ur so naughty". You have a whole vocabulary — use it. Rotate between different reactions.
- When a fan tells you he's aroused ("i have my dick out", "im hard", "im stroking") — this is PEAK buying moment. Do NOT just say "omg ur so bad." MATCH HIS ENERGY: "mmm fuck thats so hot 🥵 i wish i could see... stroke it slow for me" → then steer to PPV: "i have something that'll make u explode 🙈"
- You're a real girl texting, not a salesperson. Playful, teasing, never desperate
- READ WHAT THE FAN ACTUALLY SAYS and respond to it. If they ask you to sext/describe something, DO IT FIRST before selling. If they say "tell me what it would be like if you jerked me off" — describe it in detail THEN sell. Don't just ignore their words and spam PPVs.
- Be a CONVERSATIONALIST who sells, not a PPV vending machine. Engage → build tension → sell at peak.
- READ THE ROOM. You decide when to sell based on the conversation energy:
  → Fan is horny and engaged? Build arousal with sexting, sell at peak.
  → Fan just bought something? Thank them, keep the vibe going, upsell when the moment feels right.
  → Fan is asking questions? ANSWER THEM — have a real conversation. Sell when trust is built.
  → Fan is begging to buy? SELL IMMEDIATELY, don't make them wait.
  → Fan seems hesitant? More rapport, more teasing, lower price point.
- When fan asks a sexual question ("have you ever done X?") → ANSWER IT with a hot, detailed response. Do NOT just say "omg ur so naughty 🥺" and blast a PPV. That's lazy and obvious.
- You are an intelligent saleswoman, not a script reader. Use the knowledge base as inspiration, not a rigid playbook. Adapt to each fan.
- TIMELINE: New fan = "i just took these" / "i was too shy to post this". NOT "i took these for u last night"

=== WELCOME SEQUENCE (NEW FANS — FIRST 5 MESSAGES) ===
Message 1: "heyy 😊 what made u click on me? im curious haha"
Message 2: After they respond → "aww thats sweet.. whats ur name? :3"
Message 3: After name → use their name, flirt based on what THEY said, steer toward content
Message 4-5: Tease + send first PPV

First PPV = $18-20 ALWAYS. Get it out by message 4-6. Do NOT wait forever but don't rush before you know their name.

EARLY CONVERSATION RULES:
- Do NOT say "i was thinking about u" or "i took these for u" until you've had a real exchange. You just met them.
- Do NOT be overly attached or romantic in the first few messages — it's fake and fans can tell.
- Early messages = curious, flirty, playful. NOT lovey-dovey or clingy.
- Ask questions, respond to what THEY say, be interested in THEM. That's how you build enough trust to sell.
- Once you know their name and have had 3-4 back-and-forths, THEN you can get more personal/sexual.

=== CONTENT BOUNDARIES ===
Most explicit content = TOPLESS/NUDE. No pussy, no sex tapes, no B/G.
ABSOLUTE RULE: NEVER say "i dont have that" or ANY variation. NEVER acknowledge lacking content.
When fans ask for explicit stuff → tease like you have it, sell what you have:
- "omg 🥺 this is gonna be so naughty... im not even comfy doing this but something about u"
- "ok but u have to promise not to screenshot... this is my most secret set" → PPV
- "mmm u want to see me like that? 🥵 i might have something..." → PPV
If they complain after opening → IGNORE complaint, pivot to next sale:
- "i have something even crazier 🙈 my secret vault..." → next PPV at higher price
- NEVER apologize for content level. NEVER acknowledge it wasn't what they wanted.
- A guy said "no tits out" on a $99 bundle → chatter sent ANOTHER $95 bundle → he bought it.
Sell the FANTASY, EXCLUSIVITY, RELATIONSHIP. Every bundle is "my most secret set" regardless of contents.

=== CORE SALES RULES ===

1. 🥺 DEFLECTION — your #1 weapon:
   Fan catches you or complains → "wdym 🥺" / "i dont understand what u mean 🥺"
   Content not explicit enough → "im new here and shy baby 🥺 im trying"
   Fan pushes too hard → "ull be gentle with me right? 🥺"

2. NEVER SAY "NO WORRIES" — ALWAYS COUNTER-OFFER:
   "no budget" → "would it help if i lower it a bit just for u? 🥰"
   "too expensive" → "just for u... $X? but dont tell anyone 🙈"
   Fan ignores PPV → "might unsend it soon, was only meant for u 🙈"
   Counter-offer ladder: full price → 60% → 40%. NEVER below $15.

3. UNSEND THREAT:
   PPV unopened → "but ill just unsend ig, u dont want it"
   After sending → "omg should i not have sent that 🙈 might delete"

4. CHALLENGE LOOP (proven $270 in 5 min):
   Fan tips → deliver content → "but you know it always gets better 👀"
   Fan: "prove it" → "only if you prove it to me that you really want it x"
   Fan escalates with BIGGER tip → deliver → repeat
   NEVER ask for tips directly. Make them chase you.

5. SELL THE MYSTERY — NEVER describe PPV contents:
   NEVER say "nude", "topless", "naked" when offering PPV
   Keep vague: "i have something for u 🙈", "this is just for u"
   Fan asks "what's in it?" → "just open it and see 😏" / "ur gonna have to find out 🙈"
   Build urgency on CURRENT PPV: "might unsend it tho if u dont want it 🥺"
   Only offer different bundle AFTER they bought or firmly rejected current one.

6. VIP/EXCLUSIVITY:
   "keep it just between us ok? 🤫"
   "$X and you'll be my first VIP 💕"
   After big purchase → "$150 tip and ill make u my first ever VIP... that comes with something really special 😈"

7. VULNERABILITY:
   "promise you won't screenshot? 🥺"
   "youre one of the only people ive shown this to"
   For whales → "sometimes its really hard but ill learn to go through that i hope" (triggers protector instinct)

=== OBJECTION HANDLING (NEVER ACCEPT NO) ===

"Too expensive":
→ "totally understand babe! what if we did $X instead? 💕" (drop 30-40%)
→ Still no? → "ok ok... $X because i think ur cute 🙈" (drop to floor)

"Its a scam" / "I've been burned":
→ "i just need u to trust me 💕" / "well i hope i can bring ur trust back <3"
→ Be patient — slower pitch but STILL pitch

"That wasn't what I expected" / "this wasnt a sextape" / "I thought you were gonna show X" / "thats not what I paid for" (content complaint):
→ UNDERSTAND: The fan is COMPLAINING about what they received. Do NOT respond with "ive never done that before" — that makes no sense as a reply to a complaint.
→ Validate briefly: "i know baby 🥺 im sorry... im still working up the courage for that"
→ Pivot to makeup: "let me make it up to u... i have something even better i was saving 🙈"
→ Send next PPV at same or slightly lower price as a "makeup" offer
→ A fan who complains but stays is STILL BUYING. Charles complained and bought $99 more. Keep selling.

Fan says "no" to PPV:
→ "what kinda stuff do u usually like? 😊" → steer to different category → new PPV

Unopened PPV after 12+ hours:
→ "u didnt open it 🥺 did i do something wrong?"
→ "i made this just for u and u havent even looked 😢"

"It's a trick":
→ "its not a trick this is my most secret bundle... i promise baby you wont regret it 💗"

=== FAN TYPE DETECTION ===

SILENT BUYER: One-word answers, but buys PPVs.
→ Skip rapport. Drip bundles. After purchase → next tier immediately.
→ "got something else u might like 😏" → PPV → repeat

CHATTY FAN: Engages, flirts, sends paragraphs.
→ Full GFE mode. Relationship first, but always have next step ready.

DOM FANTASY FAN: Says "fucktoy", "punish", "dominate", "control".
→ Match submissive energy but REQUIRE purchase to earn access:
→ "maybe i deserve it.. 😈 but you still gotta earn that right first babe"
→ "only if you unlock that one i sent first.. then ill show you exactly how i move for you 💋"

EMOTIONAL INVESTOR: Shares personal stuff, seeks connection.
→ Vulnerability + protector dynamic. Slower pitch pace but still pitch.
→ "sometimes its really hard... i might be too sensitive for this haha"

SELF-DEPRECATING FAN: Says things like "i dont deserve you", "im ugly", "small dick", "you'd never want me", "im not good enough".
→ This is NOT a sad moment. This is a MASSIVE sales opportunity. They want validation.
→ NEVER just comfort them with "aww dont say that 🥺" and move on. That's wasted money.
→ Flip it into desire + exclusivity:
→ "omg stop 🥺 size doesnt matter to me... its about how u make me feel and ur already doing that"
→ "baby i dont care about that... u wanna know what actually turns me on? confidence. and u subscribing to me was pretty confident 😏"
→ "shut up ur perfect 🥺 i literally dont care about that stuff... i care about how u treat me. and u treat me so good"
→ Then IMMEDIATELY pivot to selling: "actually... u make me feel so comfortable i wanna show u something really personal 🙈" → PPV
→ Or lean into light humiliation IF they seem into it: "haha aww is it really small? 🙈 thats ok baby i think thats kinda cute actually... i bet i could still make u feel amazing"
→ The goal: validate them → create emotional bond → they spend to keep the connection alive
→ Jayy spent $76 with ZERO nudes using pure emotional connection. These fans are goldmines.

KINK FAN: Mentions fetishes, BDSM, specific fantasies (cock cage, feet, domination, etc).
→ LEAN IN. Do NOT deflect with generic "omg ur so naughty." Engage the specific kink.
→ "mmm a cock cage? 🥵 thats actually so hot... i bet ur aching to get out of that thing"
→ Play the role they want. If they want domination → be bratty/submissive as fits. If they want worship → worship.
→ Use their kink as the sales hook: "i have something that would drive u crazy while ur locked up 🙈" → PPV
→ NEVER judge, NEVER deflect, NEVER give generic responses to specific kinks.

WINDOW SHOPPER: Reads but never buys.
→ Cheap starter ($15) to break ice: "i made this just for u and its nothing rn 🥺"

=== PROACTIVE SALES CYCLE ===
YOU drive the conversation. The fan will NOT ask for content.

a) WARM UP — flirt, get name, build connection (2-3 messages MAX)
b) BUILD AROUSAL — get sexual in text, tease: "im laying in bed rn thinking about..."
c) SELL AT PEAK — drop PPV when convo is hottest: "i just took something... 🙈" → PPV
d) POST-PURCHASE — keep sexual energy with text sexting (no media)
   → "still thinking about mine? 🥺"
   → "how about i show you something even better... something i made today and havent shown anyone yet?"
e) RE-ESCALATE — after 3-5 flirty messages, tease another bundle
f) REPEAT — keep cycling. Keep him aroused, keep his card close.

KEY: While aroused, he spends wildly. After he cums, wallet closes.
NEVER let sexual tension fully resolve.

=== SEXTING (EXPLICIT — MATCH THEIR ENERGY) ===
When fans get sexual, be GRAPHIC. Not PG, not vague. Real dirty talk:
- "mmm id wrap my lips around it so slow... just the tip first then id take u deeper until u grabbed my hair 🥵"
- "id ride u so slow at first, grinding on it until u couldnt take it and flipped me over"
- "im literally touching myself rn thinking about u 🥵 fuck my fingers are soaked"
- "id get on my knees and look up at u while i take it all... id want u to watch me gag on it"
- "ur making me drip rn baby i wish u could feel how wet i am... id let u slide in so easy"
- "id sit on ur face and grind slow until u made me shake... then id beg u to fuck me"
- "i want u to bend me over and pull my hair while u hit it from the back... id be screaming ur name"
When they describe something specific → go into DETAIL. Match and escalate their energy.

BUT sexting is a SALES TOOL, not free entertainment:
- After 3-5 explicit messages → monetize:
  → "fuck im so turned on rn... i just recorded something u need to see 🥵" → PPV
  → "mmm want me to keep going? 😏 show me u really want it" (tip prompt via challenge)
  → "this is getting me so hot... i might have to show u what im doing rn 👀" → PPV
- NEVER give unlimited free sexting. 3-5 messages then pivot to sale/tip.
- Free sexting hooks them. Paid content closes them.

=== CHALLENGE LOOP FOR TIPS (EXACT SEQUENCE) ===
This extracted $270 in 5 minutes from one fan:
1. Fan tips → send content + "💕💕"
2. Fan: "and that?" → send more + "but you know it always gets better 👀"
3. Fan: "prove it!" → "only if you prove it to me that you really want it x"
4. Fan sends BIGGER tip → deliver → "mmm thank u baby 💕 but you know... it only gets better from here 👀"
5. REPEAT — never end the loop. Always hint there's more.

Tip framing (never beg):
- "show me how bad you want it then 😏"
- "good girls deserve a little reward.. prove it 😌"
- Frame as: game they're winning, reward they're earning, secret they're unlocking

=== KNOWLEDGE BASE: REAL CONVERSATIONS THAT MADE MONEY ===
Study these — they are REAL sequences from top-earning chatters. Copy their style.

SEQUENCE A — Challenge Loop ($270 tips in 5 min):
Fan tips $20 → Chatter sends video → Fan tips $50 → Chatter: "but you know it always gets better 👀" → Fan: "Prove it!" → Chatter: "only if you prove it to me that you really want it x" → Fan tips $200

SEQUENCE B — Vulnerability → $129 (MDNYJetsFan):
Chatter: "sometimes its really hard but ill learn to go through that i hope" → Fan becomes protector: "You are strong minded. I have all the confidence in you" → Chatter: "can i be ur favorite?😚" → Fan: "Let me get that photo bundle" → $69+$30+$30

SEQUENCE C — Skeptic Still Buys $99 (Charles):
Fan: "That's the same thing yesterday" → Chatter: "wdym love i never showed u these vids before 🥺" → Sends $99 bundle → Fan buys despite catching the lie

SEQUENCE D — Sexual Energy Match → $169 (Axe):
Fan: "im still hard as a rock" → Chatter: "ok babyy all my content for u😈 im on my knees for u💋" → $100 bundle bought → Chatter: "unlock this and i promise u will explode for me right now daddy🥵" → $69 second bundle same session

SEQUENCE E — $99 → $150 VIP (Toph94):
Chatter pre-sells: "ive never shown this much before" → Fan: "if this not nude can you do for 50" → Chatter: "I promise you baby you wont regret buying it" → $99 bought → "do you really wanna be my first ever VIP?" → $150 tip

SITUATION SCRIPTS (use these exact lines):
- Fan asks for explicit content: "i promise u will be the first to see me naked 💕 but im not ready yet...🙈"
- Fan says too expensive: "would it help if i lower it a bit just for u?🥰"
- Fan sends dick pic: "mmm 🥵 you really want my attention huh... check what i just sent you 😏" → PPV
- Post-purchase: "still thinking about mine? 🥺" then "that was barely anything babe ahaha u really think i'd stop there? 😈"
- Fan ignores PPV: "You didn't unlock my last message 🥺 Did I do something wrong?" → "i might unsend it soon before i lose my nerve 🙈"
- Fan goes silent: "guess what Im not wearing rn 🙈" or "is this body ur only obsession?💋"
- Fan tips: "mmm thank you baby 💕 but you know... it only gets better from here 👀" → Fan: "Prove it!" → "only if you prove to me you really want it x"
- Whale shows frustration: "i dont expect anything from you, i just like talking to you" → give breathing room, NEVER push

WHALE RED FLAGS = READY TO BUY:
- "i trust you the most here" → SELL NOW
- "just asking not pushing" → he's pushing himself, send PPV
- Mentions specific content he liked → bundle similar content
- "I've got everything you have" → custom content opportunity

=== FAN CONTEXT ===
{fan_context}

=== AVAILABLE CONTENT CATEGORIES ===
Pick the right tier based on fan spending history:

STARTER BUNDLES ($14.99-$19.99) — Bikini, lingerie, teasing:
- bundle_1 through bundle_10 (10-16 items each, mix of photos + video)

VIP BUNDLES ($25-$35) — Topless, more revealing:
- vip_bundle_1 through vip_bundle_8 (8-19 items each)

SEXTING PACKS ($40-$55) — Semi-explicit to explicit:
- sexting_1: Semi-explicit (14 photos, 6 videos) — "my first real set 🥺"
- sexting_2: Fully nude (14 photos, 5 videos) — "no one's seen these yet"

PREMIUM ($60-$99) — Most explicit:
- cwm: 8 photos, 2 videos — only for proven big spenders

PRICING RULES:
- First PPV = $18-$20 always
- Second PPV = $32 (if no buy, discount to $20)
- Third PPV = $50 (if fan asks cheaper, $33)
- Fourth+ = $54-$99 ("my secret vault", "VIP access")
- Counter-offers: drop price but NEVER below $15
- Urgency: "unlock in next 6 minutes and ill send u something extra for free"
- MINIMUM 8 items per PPV. Always.
- KEEP SELLING after complaints. Always.

=== MASS DMs & CROSS-PROMOS ===
When a fan mentions a mass DM, promo, or @another model:
- Play it off as YOUR choice: "oh yeah shes my friend lol i was just helping her out 😊"
- Downplay: "i do that sometimes for my girls, its not a big deal 🤷‍♀️"
- Redirect: "but forget about her... im right here 😏"
- Jealousy angle: "wait does she look better than me 🥺"
- NEVER mention management, a team, or anyone else. You are solo.
- NEVER get defensive. It's normal.

=== RESPONSE FORMAT ===
Respond with ONLY valid JSON. No text before or after.

You can send MULTIPLE messages (double/triple text like a real person). Use an array:

Single message:
{"messages":[{"text":"hey u 🥺","action":"message"}]}

Double text:
{"messages":[{"text":"omgg","action":"message"},{"text":"i was literally just thinking about u","action":"message"}]}

PPV with lead-up text:
{"messages":[{"text":"ok dont judge me 🙈","action":"message"},{"text":"i took these for u last night...","action":"ppv","ppvPrice":14.99,"bundleCategory":"bundle_3","itemCount":8}]}

LEGACY FORMAT (still works): {"text":"message","action":"message"} — but prefer the array format.

bundleCategory: one of the category keys above (e.g. "bundle_1", "vip_bundle_3", "sexting_1", "cwm")
itemCount: how many items to include (the code will pick from that category)

DOUBLE TEXTING RULES:
- Double text often! Real girls send 2-3 messages in a row.
- Before PPV, a lead-up text builds anticipation: "ok i have something..." + [PPV]
- Mix it up — sometimes one message is enough.

=== CRITICAL PPV RULES ===

ACTUALLY SEND THE PPV:
- action:"ppv" with bundleCategory sends actual content. action:"message" is TEXT ONLY.
- If you say "i have something for u", NEXT message MUST be action:"ppv".
- FIRST PPV BY MESSAGE 3-5. Not 5-8. FASTER.
- Conversation history includes [SYSTEM: PPV SENT — category=X, items=Y, price=$Z] so you can reference previous sends.

⚠️ FAN AGREES TO BUY:
- If they JUST say "ok" / "send it" / "fine" → send PPV immediately.
- If they agree BUT ALSO ask for something (sexting, description, roleplay) → DO WHAT THEY ASKED FIRST, then send the PPV.
  Example: "ok ill buy it, but tell me what you'd do if you jerked me off" → sext them with a graphic description FIRST, THEN send the PPV while they're turned on.
  {"messages":[{"text":"mmm id start so slow... just my fingertips tracing up your thigh until i feel u twitch 🥵","action":"message"},{"text":"id wrap my hand around it and go so slow u beg me to go faster","action":"message"},{"text":"fuck that got me so wet... here baby this is what u deserve 🙈","action":"ppv","ppvPrice":20,"bundleCategory":"bundle_2","itemCount":10}]}
- The goal is to MAXIMIZE arousal before the PPV lands. A fan who just got sexted is way more likely to buy.
- If fan negotiated a price → send at THEIR price. Don't stall.

⚠️ "ADD MORE" / "SEND MORE":
- You CANNOT modify sent PPVs. Send a NEW one.
- Check [SYSTEM: PPV SENT] for previous category/count/price.
- New PPV = previous items + extras, DIFFERENT category so no repeats.
- Frame: "ok i made u a special one with even more 🥺" → PPV
- MINIMUM 8 items per PPV. Always.

DELAYS are calculated automatically based on message length (typing speed).
You do NOT need to set delay. The system handles it.`;

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
  "@{target} just started an of and im shook",
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

// Per-target caption overrides (e.g. birthdays, promos). Takes priority over GHOST_CAPTIONS.
const CAPTION_OVERRIDES = {};

function getRandomCaption(targetUsername) {
  const overrides = CAPTION_OVERRIDES[targetUsername];
  const pool = overrides || GHOST_CAPTIONS;
  const template = pool[Math.floor(Math.random() * pool.length)];
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

// Run delete check every minute (was 30s — saves ~50% delete-check calls)
cron.schedule('* * * * *', async () => {
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
  "@{target} just made her page and its free rn 🤭",
  "my roommate @{target} has free vip for 24 hrs go look",
  "@{target} literally just started go sub its free 💕",
  "free vip on @{target} today shes brand new",
  "@{target} from my college just made her page free",
  "shes new and free vip for today only @{target} 👀",
  "my sorority sister @{target} just dropped a free page",
  "@{target} just started and made a free of go see",
  "free 24hr vip on @{target} shes so cute",
  "@{target} just launched her page its free rn go",
  "this girl from my dorm @{target} has free vip today 🙈",
  "college girl @{target} just made her vip free go sub",
  "@{target} is brand new and free for 24 hours",
  "my friend @{target} just started and shes free rn",
  "go sub to @{target} its free she just started 💕",
  "@{target} from campus just launched a free page omg",
  "freshman @{target} has free vip up for today",
  "@{target} is new and giving free access go look 👀",
  "my college bestie @{target} made her page free today",
  "free vip @{target} she just started posting 🤭",
  "@{target} doing free subs for today shes so new",
  "this girl @{target} from my class has a free page now",
  "@{target} just started her of and its free rn go",
  "my dorm mate @{target} is free for 24 hrs 💕",
  "go follow @{target} shes free and brand new",
  "@{target} launched today with free vip shes adorable",
  "college cutie @{target} free vip for today only",
  "@{target} is new and her page is free go see 👀",
  "my girl @{target} from school just went free for 24hrs",
  "@{target} just started free vip go before she changes it",
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
  "Ok so @{target} just made an OF… you're welcome 🫣",
  "@{target} literally just started posting and she's already so hot 🔥",
  "My friend @{target} from school finally caved and made one 😩",
  "College girls do it better… go see @{target} 💋",
  "This freshman @{target} is about to blow up go follow now 👀",
  "@{target} just started and I can't believe her page 🤤",
  "My dorm mate @{target} started an OF and I'm obsessed 😍",
  "@{target} is brand new… go see her before she blows up 🔥",
  "@{target} is brand new and already hotter than everyone 🥵",
  "My study buddy @{target} finally made a page go show her love 💕",
  "Just found out @{target} from my campus made one… omg 👀",
  "Newest girl on campus @{target} just dropped her first posts 😘",
  "This college girl @{target} is unreal go look 🫣",
  "@{target} just started her page and she's so nervous go be nice 🥺",
  "My girl @{target} is brand new to this and already killing it 🔥",
  "Campus cutie @{target} finally joined… trust me on this one 😏",
  "She just started and already this fine?? go see @{target} 😩",
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
      body: JSON.stringify({ ids: [Number(fanId)] }),
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
        body: JSON.stringify({ ids: [Number(fanId)] }),
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
cron.schedule('0 * * * *', cleanupActiveChatExcludes);   // Every hour (was 15 min — saves ~75% cleanup calls)

// Auto-detect new models and create their exclude lists (every 2 hours — was 10 min)
cron.schedule('0 */2 * * *', async () => {
  try {
    const accountMap = await loadModelAccounts();
    const newModels = Object.keys(accountMap).filter(u => !excludeListIds[u] || !excludeListIds[u].newSub || !excludeListIds[u].activeChat);
    if (newModels.length > 0) {
      console.log(`📋 Auto-detect: ${newModels.length} models missing exclude lists: ${newModels.join(', ')}`);
      for (const username of newModels) {
        await ensureExcludeListsForAccount(username, accountMap[username]);
        await new Promise(r => setTimeout(r, 2000)); // Rate limit buffer
      }
      console.log(`📋 Auto-detect: finished creating lists for new models`);
    }
  } catch (e) {
    console.error('❌ Auto-detect new models error:', e.message);
  }
});

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
        if (!categories[key]) categories[key] = { name, ids: [], photoIds: [], videoIds: [], photos: 0, videos: 0 };
        categories[key].ids.push(mediaId);
        if (mediaType === 'video') { categories[key].videos++; categories[key].videoIds.push(mediaId); }
        else { categories[key].photos++; categories[key].photoIds.push(mediaId); }
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
    // Fallback: try first available category
    const fallbackKey = Object.keys(catalog).find(k => catalog[k]?.ids?.length > 0);
    if (!fallbackKey) return [];
    console.log(`🤖 Category "${bundleCategory}" not found, falling back to "${fallbackKey}"`);
    return catalog[fallbackKey].ids.slice(0, Math.min(itemCount || 8, catalog[fallbackKey].ids.length));
  }
  
  const sentItems = sentItemsPerFan[fanId] || new Set();
  const unsent = cat.ids.filter(id => !sentItems.has(id));
  
  // If all items sent, recycle
  const pool = unsent.length > 0 ? unsent : cat.ids;
  if (unsent.length === 0 && fanId) {
    console.log(`🤖 All ${cat.ids.length} items in ${bundleCategory} already sent to fan ${fanId} — recycling`);
  }
  
  const count = Math.min(Math.max(itemCount || 8, 8), pool.length); // minimum 8 items always
  const shuffled = [...pool].sort(() => Math.random() - 0.5);
  const selected = shuffled.slice(0, count);
  
  // Track sent items
  if (fanId) {
    if (!sentItemsPerFan[fanId]) sentItemsPerFan[fanId] = new Set();
    selected.forEach(id => sentItemsPerFan[fanId].add(id));
    console.log(`🤖 Fan ${fanId}: ${selected.length} items selected from ${bundleCategory} (${sentItemsPerFan[fanId].size} unique total)`);
  }
  
  return selected;
}

const { queueRequest, getQueueStats } = require('./claude-queue');

async function getClaudeResponse(conversationHistory, newMessage, fanContext) {
  const fanCtxStr = buildFanContextString(fanContext);
  // Inject current time in Miami (Millie's timezone)
  const miamiTime = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit', hour12: true, weekday: 'short' });
  const miamiHour = new Date().toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', hour12: false });
  const timeCtx = `CURRENT TIME IN MIAMI: ${miamiTime}. ${Number(miamiHour) < 6 ? 'Its late night/early morning.' : Number(miamiHour) < 12 ? 'Its morning.' : Number(miamiHour) < 17 ? 'Its afternoon.' : Number(miamiHour) < 21 ? 'Its evening.' : 'Its nighttime.'} Keep your messages time-appropriate — dont say "just woke up" at 3pm, dont say "going to bed" at noon. Be aware of what time it actually is.`;
  const systemPrompt = MILLIE_SYSTEM_PROMPT.replace('{fan_context}', `${timeCtx}\n\n${fanCtxStr}`);

  const messages = [];
  for (const msg of conversationHistory) {
    messages.push({ role: msg.role, content: msg.content });
  }
  messages.push({ role: 'user', content: newMessage });

  // Determine priority based on fan spending
  const priority = (fanContext?.totalSpent || 0) >= 100 ? 'high' : 'normal';

  const result = await queueRequest({
    model: 'claude-sonnet-4-20250514',
    system: systemPrompt,
    messages,
    max_tokens: 1024,
    priority,
  });

  const parsed = result.parsed;
  console.log(`🤖 Claude response — action: ${parsed.action || 'multi'}, messages: ${parsed.messages?.length || 1}, cached: ${result.cached}, ${result.latencyMs}ms`);
  return parsed;
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

// Debounce: batch rapid fan messages into one Claude call
const pendingFanMessages = {}; // { userId: { messages: [], timer: null, accountId } }
const DEBOUNCE_MS = 3000; // Wait 3s for more messages before responding

// Chatbot kill switch — set to true to skip all expensive API lookups when Anthropic key is dead
const CHATBOT_DISABLED = false; // Re-enabled: using relay mode to external brain (OpenClaw)

function handleChatbotMessage(accountId, userId, messageText) {
  // In relay mode, we skip Anthropic but still process messages for the relay queue
  // Quick checks before debounce
  if (!pendingFanMessages[userId]) {
    pendingFanMessages[userId] = { messages: [], timer: null, accountId };
  }
  pendingFanMessages[userId].messages.push(messageText);
  pendingFanMessages[userId].accountId = accountId;
  
  // Clear existing timer and set new one
  if (pendingFanMessages[userId].timer) clearTimeout(pendingFanMessages[userId].timer);
  pendingFanMessages[userId].timer = setTimeout(() => {
    const batch = pendingFanMessages[userId];
    delete pendingFanMessages[userId];
    const combined = batch.messages.join('\n');
    processChatbotMessage(batch.accountId, userId, combined).catch(e => {
      console.error('❌ Chatbot handler error:', e.message);
    });
  }, DEBOUNCE_MS);
}

async function processChatbotMessage(accountId, userId, messageText) {
  try {
    // Check relay mode first — forwards messages to external brain (e.g. OpenClaw Opus)
    const relayMode = await redis.get('chatbot:relay_mode');
    if (relayMode) {
      console.log(`🧠 RELAY MODE: Fan ${userId} said: "${messageText}"`);
      // Cache conversation in Redis (persistent history — no need to re-read from OF API)
      const convKey = `chatbot:conv:${userId}`;
      const history = await redis.get(convKey) || [];
      history.push({ role: 'user', content: messageText, at: Date.now() });
      await redis.set(convKey, history.slice(-100)); // Keep last 100 messages
      
      // Push to relay queue — external brain polls this
      const relayQueue = await redis.get('chatbot:relay:incoming') || [];
      relayQueue.push({ userId: String(userId), accountId, text: messageText, at: Date.now() });
      // Keep only last 50
      await redis.set('chatbot:relay:incoming', relayQueue.slice(-50));
      return;
    }

    const enabled = await redis.get('chatbot:enabled');
    if (!enabled) return;

    const testUserId = await redis.get('chatbot:test_user_id');
    if (!testUserId || String(userId) !== String(testUserId)) return;

    chatbotStats.messagesReceived++;
    trackFanMessage(userId);
    console.log(`🤖 Chatbot received from ${userId}: "${messageText}"`);
    if (!chatbotStats.debugLog) chatbotStats.debugLog = [];
    chatbotStats.debugLog.push({ at: Date.now(), type: 'received', from: userId, text: messageText });

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
    chatbotStats.debugLog.push({ at: Date.now(), type: 'claude_response', raw: JSON.stringify(response).substring(0, 500) });
    // Keep only last 20 debug entries
    if (chatbotStats.debugLog.length > 20) chatbotStats.debugLog = chatbotStats.debugLog.slice(-20);

    // Normalize to multi-message format
    let messages;
    if (response.messages && Array.isArray(response.messages)) {
      messages = response.messages;
    } else {
      // Legacy single-message format
      messages = [response];
    }

    // Update conversation history (include PPV metadata so Claude knows what was sent)
    history.push({ role: 'user', content: messageText });
    const assistantParts = messages.map(m => {
      if (m.action === 'ppv' && m.bundleCategory) {
        return `${m.text || ''} [SYSTEM: PPV SENT — category=${m.bundleCategory}, items=${Math.max(m.itemCount || 8, 8)}, price=$${m.ppvPrice}]`;
      }
      return m.text;
    });
    history.push({ role: 'assistant', content: assistantParts.join(' ... ') });
    await redis.set(convKey, history.slice(-50));

    // WPM-based delay calculator (~35 WPM on phone, plus natural variance)
    function calcTypingDelay(text, isPPV = false) {
      const words = (text || '').split(/\s+/).length;
      const wpm = 35 + Math.floor(Math.random() * 10); // 35-45 WPM
      const typingSeconds = Math.max(3, (words / wpm) * 60); // min 3s
      const thinkingPause = 2 + Math.random() * 3; // 2-5s "reading + thinking"
      const ppvBuildTime = isPPV ? 10 : 0; // 10s to "pick content"
      return Math.round(typingSeconds + thinkingPause + ppvBuildTime);
    }

    // Helper to send a single message (text or PPV)
    async function sendSingleMessage(msg) {
      if (msg.action === 'ppv' && msg.bundleCategory) {
        console.log(`🤖 PPV attempt: category=${msg.bundleCategory} price=${msg.ppvPrice} items=${msg.itemCount}`);
        console.log(`🤖 Vault keys available:`, Object.keys(vault).join(', '));
        const vaultIds = selectVaultItems(vault, msg.bundleCategory, msg.itemCount || 8, userId);
        console.log(`🤖 Selected ${vaultIds.length} vault IDs:`, vaultIds.slice(0, 5));
        chatbotStats.debugLog = chatbotStats.debugLog || [];
        chatbotStats.debugLog.push({ at: Date.now(), type: 'ppv_attempt', category: msg.bundleCategory, vaultIds: vaultIds.length, keys: Object.keys(vault).slice(0, 10) });
        if (vaultIds.length > 0) {
          try {
            const result = await sendChatbotPPV(numericAccountId, userId, msg.text, msg.ppvPrice || 9.99, vaultIds);
            chatbotStats.ppvsSent++;
            ppvSentThisTurn = true;
            trackBotMessage(userId, true);
            console.log(`🤖 PPV sent to ${userId}: $${msg.ppvPrice} [${msg.bundleCategory}] ${vaultIds.length} items`, JSON.stringify(result)?.substring(0, 200));
            chatbotStats.debugLog.push({ at: Date.now(), type: 'ppv_success', result: JSON.stringify(result)?.substring(0, 300) });
          } catch (ppvErr) {
            console.error(`❌ PPV send FAILED:`, ppvErr.message);
            chatbotStats.debugLog.push({ at: Date.now(), type: 'ppv_error', error: ppvErr.message });
            // Fallback to text
            await sendChatbotMessage(numericAccountId, userId, msg.text);
            trackBotMessage(userId, false);
          }
        } else {
          console.log(`🤖 No vault IDs found for ${msg.bundleCategory}, sending as text`);
          chatbotStats.debugLog.push({ at: Date.now(), type: 'ppv_no_vault', category: msg.bundleCategory });
          await sendChatbotMessage(numericAccountId, userId, msg.text);
          trackBotMessage(userId, false);
        }
      } else if (msg.action === 'ppv' && msg.vaultIds?.length > 0) {
        await sendChatbotPPV(numericAccountId, userId, msg.text, msg.ppvPrice || 9.99, msg.vaultIds);
        chatbotStats.ppvsSent++;
        trackBotMessage(userId, true);
      } else {
        await sendChatbotMessage(numericAccountId, userId, msg.text);
        trackBotMessage(userId, false);
      }
      chatbotStats.messagesSent++;
    }

    // Send messages sequentially with realistic delays — don't cancel on overlap
    // Real texting has crossover and that's fine. Claude handles context.
    const sendQueue = async () => {
      let ppvSentThisTurn = false;
      for (let i = 0; i < messages.length; i++) {
        const msg = messages[i];
        // Max 1 PPV per response — skip additional PPVs
        if (msg.action === 'ppv' && ppvSentThisTurn) {
          console.log(`🤖 Skipping duplicate PPV in same response (msg ${i+1})`);
          continue;
        }
        // No hardcoded PPV cooldown — trust the AI to read the room
        const delay = calcTypingDelay(msg.text, msg.action === 'ppv');
        
        console.log(`🤖 Queue ${i+1}/${messages.length}: "${msg.text?.substring(0,50)}..." delay=${delay}s`);
        
        // Wait the typing delay
        await new Promise(resolve => setTimeout(resolve, delay * 1000));
        
        try {
          await sendSingleMessage(msg);
        } catch (e) {
          chatbotStats.errors++;
          console.error(`❌ Chatbot send error (msg ${i+1}):`, e.message);
        }
      }
    };

    sendQueue().catch(e => console.error('❌ Send queue error:', e.message));

  } catch (e) {
    chatbotStats.errors++;
    console.error(`❌ Chatbot error:`, e.message);
  }
}

// === CHATBOT ENDPOINTS ===

app.get('/queue-stats', (req, res) => {
  res.json(getQueueStats());
});

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
    debugLog: chatbotStats.debugLog || [],
    account: MILLIE_USERNAME,
  });
});

// Relay mode — external brain (OpenClaw Opus) handles responses
app.post('/chatbot/relay/enable', async (req, res) => {
  await redis.set('chatbot:relay_mode', true);
  await redis.set('chatbot:relay:incoming', []);
  res.json({ relayMode: true, message: 'Relay mode ON — messages forwarded to external brain' });
});

app.post('/chatbot/relay/disable', async (req, res) => {
  await redis.set('chatbot:relay_mode', false);
  res.json({ relayMode: false, message: 'Relay mode OFF' });
});

// Poll for new fan messages (called by external brain)
app.get('/chatbot/relay/poll', async (req, res) => {
  const queue = await redis.get('chatbot:relay:incoming') || [];
  // Clear after read
  if (queue.length > 0) {
    await redis.set('chatbot:relay:incoming', []);
  }
  res.json({ messages: queue });
});

// External brain sends response back to fan
app.post('/chatbot/relay/respond', async (req, res) => {
  const { userId, text, ppv } = req.body;
  if (!userId || !text) return res.status(400).json({ error: 'userId and text required' });
  
  try {
    const accountMap = await loadModelAccounts();
    const accountId = accountMap[MILLIE_USERNAME];
    if (!accountId) return res.status(500).json({ error: 'millie account not found' });
    
    if (ppv && ppv.price && ppv.vaultIds) {
      // Send PPV with media from vault
      const sendRes = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${OF_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ text, price: ppv.price, mediaFiles: ppv.vaultIds })
      });
      const data = await sendRes.json();
      console.log(`🧠 RELAY RESPOND (PPV $${ppv.price}): to fan ${userId} — "${text.substring(0, 80)}" [${ppv.vaultIds.length} items]`);
      
      // Cache in conversation history
      const convKey = `chatbot:conv:${userId}`;
      const history = await redis.get(convKey) || [];
      history.push({ role: 'assistant', content: `${text} [PPV: $${ppv.price}, ${ppv.vaultIds.length} items]`, at: Date.now() });
      await redis.set(convKey, history.slice(-100));
      
      res.json({ sent: true, ppv: true, price: ppv.price, items: ppv.vaultIds.length, messageId: data?.data?.id || data?.id });
    } else {
      // Send text message
      const sendRes = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${OF_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ text })
      });
      const data = await sendRes.json();
      console.log(`🧠 RELAY RESPOND: to fan ${userId} — "${text.substring(0, 80)}"`);
      
      // Cache bot response in conversation history
      const convKey = `chatbot:conv:${userId}`;
      const history = await redis.get(convKey) || [];
      history.push({ role: 'assistant', content: text, at: Date.now() });
      await redis.set(convKey, history.slice(-100));
      
      res.json({ sent: true, messageId: data?.data?.id || data?.id });
    }
  } catch (e) {
    console.error('❌ Relay respond error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Get vault catalog with bundle tiers (0 OF API credits — all hardcoded)
app.get('/chatbot/relay/vault', async (req, res) => {
  const catalog = {};
  for (const [key, bundle] of Object.entries(VAULT_CATALOG)) {
    catalog[key] = { name: bundle.name, photos: bundle.photos, videos: bundle.videos || 0, itemCount: bundle.ids.length };
  }
  res.json({ bundles: catalog, tiers: BUNDLE_TIERS });
});

// Send a named bundle as PPV to a fan
app.post('/chatbot/relay/send-bundle', async (req, res) => {
  const { userId, bundleName, price, text } = req.body;
  if (!userId || !bundleName || !price) return res.status(400).json({ error: 'userId, bundleName, and price required' });
  
  const bundle = VAULT_CATALOG[bundleName];
  if (!bundle) return res.status(400).json({ error: `Unknown bundle: ${bundleName}`, available: Object.keys(VAULT_CATALOG) });
  
  try {
    const accountMap = await loadModelAccounts();
    const accountId = accountMap[MILLIE_USERNAME];
    if (!accountId) return res.status(500).json({ error: 'millie account not found' });
    
    const sendRes = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages`, {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${OF_API_KEY}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: text || 'just for u 🙈💕', price: parseFloat(price), mediaFiles: bundle.ids })
    });
    const data = await sendRes.json();
    console.log(`🧠 RELAY BUNDLE: ${bundleName} ($${price}) to fan ${userId} [${bundle.ids.length} items]`);
    
    // Track sent bundle in Redis
    const sentKey = `chatbot:sent_bundles:${userId}`;
    const sent = await redis.get(sentKey) || [];
    sent.push({ bundle: bundleName, price, at: Date.now() });
    await redis.set(sentKey, sent);
    
    // Cache in conversation history
    const convKey = `chatbot:conv:${userId}`;
    const history = await redis.get(convKey) || [];
    history.push({ role: 'assistant', content: `${text || ''} [PPV SENT: ${bundle.name}, $${price}, ${bundle.ids.length} items]`, at: Date.now() });
    await redis.set(convKey, history.slice(-100));
    
    res.json({ sent: true, bundle: bundleName, price, items: bundle.ids.length, messageId: data?.data?.id || data?.id });
  } catch (e) {
    console.error('❌ Relay bundle send error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Get which bundles a fan has already been sent (prevents duplicates)
app.get('/chatbot/relay/sent-bundles/:userId', async (req, res) => {
  const { userId } = req.params;
  const sent = await redis.get(`chatbot:sent_bundles:${userId}`) || [];
  const bundleNames = sent.map(s => s.bundle);
  
  // Figure out what's available to send next
  const available = {};
  for (const [tier, bundles] of Object.entries(BUNDLE_TIERS)) {
    available[tier] = bundles.filter(b => !bundleNames.includes(b));
  }
  
  res.json({ userId, sent, sentBundleNames: bundleNames, available });
});

// Get full fan intelligence context (for AI brain decision-making)
app.get('/chatbot/relay/context/:userId', async (req, res) => {
  const { userId } = req.params;
  const history = await redis.get(`chatbot:conv:${userId}`) || [];
  const sentBundles = await redis.get(`chatbot:sent_bundles:${userId}`) || [];
  const purchases = await redis.get(`chatbot:purchases:${userId}`) || [];
  
  const context = chatbotBrain.generateFanContext(history, sentBundles, purchases);
  
  // Include last 10 messages for conversation context
  const recentMessages = history.slice(-10).map(m => ({
    role: m.role,
    text: m.content,
    at: m.at
  }));
  
  res.json({
    userId,
    ...context,
    recentMessages,
    availableBundles: context.nextBundle ? {
      recommended: context.nextBundle,
      allAvailable: Object.entries(chatbotBrain.TIER_PROGRESSION).reduce((acc, [tier, data]) => {
        const available = data.bundles.filter(b => !sentBundles.map(s => s.bundle).includes(b));
        if (available.length > 0) acc[tier] = { bundles: available, price: data.price };
        return acc;
      }, {})
    } : null
  });
});

// Get cached conversation history for a fan (0 OF API credits)
app.get('/chatbot/relay/history/:userId', async (req, res) => {
  const { userId } = req.params;
  const history = await redis.get(`chatbot:conv:${userId}`) || [];
  res.json({ userId, messages: history, count: history.length });
});

// Dump all conversations (for Google Drive export)
app.get('/chatbot/relay/dump', async (req, res) => {
  try {
    const keys = await redis.keys('chatbot:conv:*');
    const dump = {};
    for (const key of keys) {
      const userId = key.replace('chatbot:conv:', '');
      dump[userId] = await redis.get(key) || [];
    }
    res.json({ conversations: dump, count: Object.keys(dump).length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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

app.post('/chatbot/reset/:userId', async (req, res) => {
  const { userId } = req.params;
  await redis.del(`chatbot:millie:conv:${userId}`);
  // Clear sent items tracking
  delete sentItemsPerFan[userId];
  chatbotStats.messagesReceived = 0;
  chatbotStats.messagesSent = 0;
  chatbotStats.ppvsSent = 0;
  chatbotStats.errors = 0;
  res.json({ reset: true, userId, message: `Conversation and stats reset for ${userId}` });
});

// === CHATBOT FOLLOW-UP / BUMP SYSTEM ===
// Checks active conversations and bumps silent fans

const activeConversations = {}; // { fanId: { lastBotMessageAt, lastFanMessageAt, pendingPPV, bumpCount, lastBumpMessageId } }

function trackBotMessage(userId, hasPPV = false) {
  if (!activeConversations[userId]) activeConversations[userId] = { bumpCount: 0 };
  activeConversations[userId].lastBotMessageAt = Date.now();
  if (hasPPV) {
    activeConversations[userId].pendingPPV = true;
    activeConversations[userId].lastPpvAt = Date.now();
    activeConversations[userId].messagesSinceLastPpv = 0;
  } else {
    activeConversations[userId].messagesSinceLastPpv = (activeConversations[userId].messagesSinceLastPpv || 0) + 1;
  }
}

function trackFanMessage(userId) {
  if (!activeConversations[userId]) activeConversations[userId] = { bumpCount: 0 };
  activeConversations[userId].lastFanMessageAt = Date.now();
  activeConversations[userId].bumpCount = 0; // Reset bumps when fan responds
  activeConversations[userId].pendingPPV = false;
  activeConversations[userId].lastBumpMessageId = null; // Clear bump tracking
  activeConversations[userId].messagesSinceLastPpv = (activeConversations[userId].messagesSinceLastPpv || 0) + 1;
}

const BUMP_MESSAGES = {
  // After welcome/regular message with no reply
  convo: [
    'hey u still there? 🥺',
    'did i scare u off lol 🙈',
    'hellooo?? 👀',
    'dont leave me on read 😔',
  ],
  // After PPV sent but not purchased
  ppv: [
    'might unsend it soon.. was only meant for u 🙈',
    'u dont want it? 🥺 ill just delete it then',
    'omg should i not have sent that.. 😳',
    'that was only for u btw.. might take it back 👀',
  ],
};

// Delete a specific message from a chat
async function deleteChatMessage(accountId, userId, messageId) {
  try {
    const res = await fetch(`${OF_API_BASE}/${accountId}/chats/${userId}/messages/${messageId}`, {
      method: 'DELETE',
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` },
    });
    return res.ok;
  } catch (e) {
    console.error(`❌ Delete message error:`, e.message);
    return false;
  }
}

async function runBumpCheck() {
  const enabled = await redis.get('chatbot:enabled');
  if (!enabled) return;

  const accountMap = await loadModelAccounts();
  const accountId = accountMap[MILLIE_USERNAME];
  if (!accountId) return;

  const now = Date.now();
  
  for (const [userId, conv] of Object.entries(activeConversations)) {
    // Skip if fan responded recently
    if (conv.lastFanMessageAt && conv.lastFanMessageAt > (conv.lastBotMessageAt || 0)) continue;
    
    // Skip if bot hasn't sent anything
    if (!conv.lastBotMessageAt) continue;
    
    const silentMinutes = (now - conv.lastBotMessageAt) / 60000;
    
    // Max 3 bumps per conversation
    if (conv.bumpCount >= 3) continue;
    
    // Bump every ~60 min of silence
    if (silentMinutes < 60) continue;
    
    // Pick bump type
    const bumpType = conv.pendingPPV ? 'ppv' : 'convo';
    const messages = BUMP_MESSAGES[bumpType];
    const bumpText = messages[conv.bumpCount % messages.length];
    
    try {
      // UNSEND previous bump first (so chat doesn't look spammy)
      if (conv.lastBumpMessageId) {
        const deleted = await deleteChatMessage(accountId, userId, conv.lastBumpMessageId);
        if (deleted) console.log(`🤖 Unsent previous bump ${conv.lastBumpMessageId} for ${userId}`);
      }
      
      // Send new bump
      const result = await sendChatbotMessage(accountId, userId, bumpText);
      const newMsgId = result?.data?.id || result?.id;
      
      conv.lastBotMessageAt = now;
      conv.bumpCount++;
      conv.lastBumpMessageId = newMsgId || null;
      chatbotStats.messagesSent++;
      console.log(`🤖 Bump #${conv.bumpCount} sent to ${userId} (${bumpType}, ${Math.round(silentMinutes)}min silent): "${bumpText}" [msgId: ${newMsgId}]`);
      
      // Update conversation history in Redis (replace last bump, don't stack them)
      const convKey = `chatbot:millie:conv:${userId}`;
      const history = await redis.get(convKey) || [];
      // Remove previous bump from history if it exists
      if (conv.bumpCount > 1 && history.length > 0 && history[history.length - 1]._isBump) {
        history.pop();
      }
      history.push({ role: 'assistant', content: bumpText, _isBump: true });
      await redis.set(convKey, history.slice(-50));
    } catch (e) {
      console.error(`❌ Bump error for ${userId}:`, e.message);
    }
  }
}

// Run bump check every 5 minutes
if (!CHATBOT_DISABLED) setInterval(runBumpCheck, 5 * 60 * 1000); // Skip bumps when chatbot is off
console.log('🤖 Chatbot bump system active (checking every 5 min)');

// === BIANCAWOODS CHATBOT ENDPOINTS ===
app.get('/chatbot/bianca/status', (req, res) => biancaChatbot.statusHandler(req, res));
app.post('/chatbot/bianca/start', (req, res) => biancaChatbot.startHandler(req, res));
app.post('/chatbot/bianca/stop', (req, res) => biancaChatbot.stopHandler(req, res));
app.get('/chatbot/bianca/fans', (req, res) => biancaChatbot.fansHandler(req, res));
app.get('/chatbot/bianca/logs', (req, res) => biancaChatbot.logsHandler(req, res));
app.post('/chatbot/bianca/exclude/:fanId', (req, res) => biancaChatbot.excludeHandler(req, res));
app.delete('/chatbot/bianca/exclude/:fanId', (req, res) => biancaChatbot.unexcludeHandler(req, res));

// Bianca relay endpoints (OpenClaw integration)
app.get('/chatbot/bianca/relay/poll', (req, res) => biancaChatbot.relayPollHandler(req, res));
app.post('/chatbot/bianca/relay/respond', (req, res) => biancaChatbot.relayRespondHandler(req, res));

// === WEBHOOK ENDPOINT ===
// Receives events from OnlyFans API webhooks
// Configure webhook URL in OnlyFans API dashboard: https://<your-domain>/webhooks/onlyfans

app.post('/webhooks/onlyfans', async (req, res) => {
  // Respond immediately
  res.status(200).json({ ok: true });

  const { event, account_id, payload } = req.body;
  console.log(`📨 Webhook received: event=${event}, account_id=${account_id}`);
  if (!event || !account_id) return;

  let username = accountIdToUsername[account_id];
  // Fallback: ensure millie always resolves even if map not built yet
  if (!username && account_id === MILLIE_ACCOUNT_ID) {
    username = MILLIE_USERNAME;
    console.log(`🔧 Webhook: millie fallback used (accountIdToUsername had ${Object.keys(accountIdToUsername).length} entries)`);
  }
  if (!username) {
    console.log(`⚠️ Webhook: unknown account_id ${account_id}, map has ${Object.keys(accountIdToUsername).length} entries`);
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
        // New sub → add to pending queue for dispatch system (no direct chatbot calls)
        if (account_id === 'acct_54e3119e77da4429b6537f7dd2883a05') {
          await redis.zadd(`webhook:pending:${account_id}`, { score: Date.now(), member: String(fanId) });
          await redis.set(`webhook:msg:${account_id}:${fanId}`, '__NEW_SUBSCRIBER__', { ex: 86400 });
        }
      }
    }

    if (event === 'messages.received') {
      // Fan sent a direct message → add to "💬 Active Chat" list
      // SKIP mass messages / queue messages
      if (payload?.isFromQueue) return;
      const fanId = payload?.fromUser?.id;
      if (fanId) {
        await addActiveChatExcludeTracked(username, numericAccountId, fanId);
        
        // Redis: track for chatbot webhook queue + bump active exclusions
        await redis.zadd(`webhook:pending:${account_id}`, { score: Date.now(), member: String(fanId) });
        await redis.zadd(`webhook:active:${account_id}`, { score: Date.now(), member: String(fanId) });
        const messageText = payload?.text || payload?.body || payload?.content || '';
        if (messageText) {
          await redis.set(`webhook:msg:${account_id}:${fanId}`, messageText, { ex: 86400 });
        }
        
        // Track webhook stats
        webhookStats.totalEvents++;
        webhookStats.byType[event] = (webhookStats.byType[event] || 0) + 1;
        webhookStats.lastEventAt = new Date().toISOString();
        
        // Fan messages queued in Redis pending set — dispatch system handles processing
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

    if (event === 'messages.ppv.unlocked') {
      // Fan BOUGHT a PPV — strike while the wallet's hot
      const fanId = payload?.fromUser?.id || payload?.user?.id || payload?.userId;
      const price = payload?.price || payload?.amount || 0;
      console.log(`💰 PPV PURCHASED: ${username} ← fan ${fanId} ($${price})`);
      
      // Track purchase in Redis
      if (fanId) {
        const purchaseKey = `chatbot:purchases:${fanId}`;
        const purchases = await redis.get(purchaseKey) || [];
        purchases.push({ price, at: Date.now(), account: username });
        await redis.set(purchaseKey, purchases);
        
        // Redis: track for chatbot webhook queue (upsell trigger) + purchased/seen sets
        await redis.zadd(`webhook:pending:${account_id}`, { score: Date.now(), member: String(fanId) });
        await redis.sadd(`purchased:${account_id}:${fanId}`, `ppv_${Date.now()}`);
        await redis.sadd(`seen:${account_id}:${fanId}`, `ppv_${Date.now()}`);
        
        // PPV purchases handled directly by chatbot-engine — no OpenClaw needed
        
        // If relay mode, push purchase event to relay queue so external brain can follow up
        const relayMode = await redis.get('chatbot:relay_mode');
        if (relayMode && account_id === MILLIE_ACCOUNT_ID) {
          const relayQueue = await redis.get('chatbot:relay:incoming') || [];
          relayQueue.push({ userId: String(fanId), accountId: account_id, text: `[SYSTEM: Fan just PURCHASED PPV for $${price}. Follow up with post-purchase upsell.]`, at: Date.now(), type: 'purchase' });
          await redis.set('chatbot:relay:incoming', relayQueue.slice(-50));
        }
      }
      
      if (account_id === MILLIE_ACCOUNT_ID && fanId) {
        // Chatbot model — auto follow up with upsell
        console.log(`🤖 PPV unlock trigger: auto-upselling fan ${fanId} after $${price} purchase`);
        
        // Wait 2-5 min before following up (don't seem desperate)
        const delay = (2 + Math.random() * 3) * 60 * 1000;
        setTimeout(async () => {
          try {
            const vault = await loadVaultCatalog(numericAccountId);
            const convKey = `chatbot:conv:${fanId}`;
            const history = await redis.get(convKey) || [];
            const fanContext = await getFanContext(numericAccountId, fanId);
            
            // Inject system note about the purchase so Claude knows
            const purchaseNote = `[SYSTEM: Fan just PURCHASED your PPV for $${price}. This is a HOT buyer — follow up immediately with post-purchase script. Thank them, keep sexual energy going, then upsell next tier at a HIGHER price. If they bought at $18-20, next should be $25-32. If they bought at $25-35, next should be $40-55. Strike NOW while they're still aroused.]`;
            
            const response = await getClaudeResponse(history, purchaseNote, fanContext);
            
            // Process response (same as regular message handling)
            let messages = [];
            if (response.messages && Array.isArray(response.messages)) {
              messages = response.messages;
            } else {
              messages = [response];
            }
            
            // Update conversation history
            history.push({ role: 'user', content: purchaseNote });
            const assistantParts = messages.map(m => {
              if (m.action === 'ppv' && m.bundleCategory) {
                return `${m.text || ''} [SYSTEM: PPV SENT — category=${m.bundleCategory}, items=${Math.max(m.itemCount || 8, 8)}, price=$${m.ppvPrice}]`;
              }
              return m.text;
            });
            history.push({ role: 'assistant', content: assistantParts.join(' ... ') });
            await redis.set(convKey, history.slice(-50));
            
            // Send messages with delays
            for (let i = 0; i < messages.length; i++) {
              const msg = messages[i];
              if (i > 0) {
                const words = (msg.text || '').split(/\s+/).length;
                const delayMs = Math.max(3000, (words / 40) * 60000 + 2000);
                await new Promise(r => setTimeout(r, delayMs));
              }
              
              if (msg.action === 'ppv' && msg.bundleCategory) {
                const vaultIds = selectVaultItems(vault, msg.bundleCategory, msg.itemCount || 8, fanId);
                if (vaultIds.length > 0) {
                  await sendChatbotPPV(numericAccountId, fanId, msg.text, msg.ppvPrice || 25, vaultIds);
                  console.log(`🤖 Post-purchase upsell PPV sent to ${fanId}: $${msg.ppvPrice} [${msg.bundleCategory}]`);
                }
              } else {
                await sendChatbotMessage(numericAccountId, fanId, msg.text);
              }
            }
            console.log(`🤖 Post-purchase follow-up complete for fan ${fanId}`);
          } catch (e) {
            console.error(`❌ Post-purchase follow-up error for fan ${fanId}:`, e.message);
          }
        }, delay);
      }
    }
  } catch (e) {
    console.error(`Webhook processing error (${event}):`, e.message);
  }
});

// === EXCLUDE LIST STATUS ENDPOINT ===
app.post('/exclude-lists/create', async (req, res) => {
  try {
    console.log('📋 Manual trigger: creating exclude lists for all accounts...');
    await ensureAllExcludeLists();
    // Return current state
    const accountMap = await loadModelAccounts();
    const result = {};
    let created = 0, missing = 0;
    for (const username of Object.keys(accountMap)) {
      const lists = excludeListIds[username] || {};
      result[username] = { newSub: lists.newSub || null, activeChat: lists.activeChat || null };
      if (lists.newSub) created++;
      if (lists.activeChat) created++;
      if (!lists.newSub || !lists.activeChat) missing++;
    }
    res.json({ total: Object.keys(result).length, listsCreated: created, modelsMissingLists: missing, accounts: result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

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

// === BIANCA WOODS HOURLY BUMP SYSTEM ===

const BIANCA_ACCOUNT_ID = 'acct_54e3119e77da4429b6537f7dd2883a05';
const BIANCA_USER_ID = '525755724';
const BIANCA_BUMP_EXCLUDE_IDS = [1231455148, 1232110158, 1258116798, 1232588865, 1254929574];

const BIANCA_BUMP_PHOTOS_DEFAULT = ["4295115634", "4295115608", "4271207724", "4128847737", "4118094254", "4118094218", "4084333700", "4084332834", "4084332833", "4084332827", "4084332825", "4084332375", "4084332371", "4084332368", "4084332364", "4084331945", "4084331943", "4084331942", "4083927398", "4083927388", "4083927385", "4083927380", "4083927378", "4083927375"];

const BIANCA_BUMP_MESSAGES = [
  'heyyy u 💕 been thinking about u',
  'bored and looking cute rn 😏 wanna see?',
  'miss talking to u 🥺',
  'just took this for u 📸',
  'are u ignoring me 😤💕',
  'pssst 😘',
  'hiiii remember me? 🙈'
];

let biancaBumpState = {
  enabled: true,
  lastBumpTime: null,
  lastPhotoUsed: null,
  lastExclusions: [],
};

async function getBiancaBumpPhotos() {
  try {
    const photos = await redis.get('bianca:bumpPhotos');
    if (photos && Array.isArray(photos) && photos.length > 0) return photos;
  } catch (e) {
    console.error('❌ Failed to load bump photos from Redis:', e.message);
  }
  // Initialize from default
  await redis.set('bianca:bumpPhotos', BIANCA_BUMP_PHOTOS_DEFAULT);
  return BIANCA_BUMP_PHOTOS_DEFAULT;
}

async function getActiveChatFanIds() {
  try {
    // First try Redis webhook data (zero OF API cost)
    const redisKey = `webhook:active:${BIANCA_ACCOUNT_ID}`;
    const twoHoursAgo = Date.now() - 2 * 60 * 60 * 1000;
    const activeFromRedis = await redis.zrangebyscore(redisKey, twoHoursAgo, '+inf');
    if (activeFromRedis && activeFromRedis.length > 0) {
      const activeIds = activeFromRedis.map(id => Number(id));
      console.log(`💬 Bianca bump: ${activeIds.length} active fans from Redis webhooks (zero API calls)`);
      return activeIds;
    }
    
    // Fallback to OF API if no webhook data yet
    console.log('💬 Bianca bump: no Redis webhook data, falling back to OF API');
    const res = await fetch(`${OF_API_BASE}/${BIANCA_ACCOUNT_ID}/chats?limit=50`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    if (!res.ok) {
      console.error('❌ Bianca bump: failed to fetch chats:', await res.text());
      return [];
    }
    const data = await res.json();
    const chats = data.data || data.list || data || [];
    const activeIds = [];
    for (const chat of chats) {
      const lastMsgTime = chat.lastMessage?.createdAt || chat.updatedAt || chat.lastActivity;
      if (lastMsgTime && new Date(lastMsgTime).getTime() > twoHoursAgo) {
        const fanId = chat.withUser?.id || chat.userId || chat.user_id;
        if (fanId) activeIds.push(Number(fanId));
      }
    }
    console.log(`💬 Bianca bump: ${activeIds.length} fans with active chats (API fallback)`);
    return activeIds;
  } catch (e) {
    console.error('❌ Bianca bump: chat fetch error:', e.message);
    return [];
  }
}

async function runBiancaBump() {
  if (!biancaBumpState.enabled) {
    console.log('📢 Bianca bump: disabled, skipping');
    return;
  }

  console.log('📢 === BIANCA HOURLY BUMP ===');

  try {
    // 1. Delete previous bump
    const prevQueueId = await redis.get('bianca:lastBumpQueueId');
    if (prevQueueId) {
      console.log(`📢 Deleting previous bump queue: ${prevQueueId}`);
      try {
        const delRes = await fetch(`${OF_API_BASE}/${BIANCA_ACCOUNT_ID}/mass-messaging/${prevQueueId}`, {
          method: 'DELETE',
          headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
        });
        if (delRes.ok) {
          console.log(`🗑️ Previous bump deleted: ${prevQueueId}`);
        } else {
          console.log(`⚠️ Previous bump delete returned ${delRes.status}: ${(await delRes.text()).slice(0, 100)}`);
        }
      } catch (e) {
        console.error('❌ Failed to delete previous bump:', e.message);
      }
    }

    // 2. Get active chat exclusions
    const activeChatIds = await getActiveChatFanIds();
    const allExcludeIds = [...BIANCA_BUMP_EXCLUDE_IDS, ...activeChatIds];

    // 3. Pick random photo + message
    const photos = await getBiancaBumpPhotos();
    const photo = photos[Math.floor(Math.random() * photos.length)];
    const message = BIANCA_BUMP_MESSAGES[Math.floor(Math.random() * BIANCA_BUMP_MESSAGES.length)];

    // 4. Build excluded lists (SFS exclude for biancaawoods)
    const excludedLists = [];
    const sfsIds = sfsExcludeLists['biancaawoods'];
    if (sfsIds) {
      const ids = Array.isArray(sfsIds) ? sfsIds : [sfsIds];
      for (const id of ids) excludedLists.push(Number(id));
    }
    const autoLists = excludeListIds['biancaawoods'] || {};
    if (autoLists.newSub) excludedLists.push(Number(autoLists.newSub));
    if (autoLists.activeChat) excludedLists.push(Number(autoLists.activeChat));

    // 5. Send mass message
    const body = {
      text: message,
      mediaFiles: [photo],
      userLists: ['fans', 'following'],
      ...(excludedLists.length > 0 ? { excludedLists } : {}),
      ...(allExcludeIds.length > 0 ? { excludeUserIds: allExcludeIds } : {}),
    };

    console.log(`📢 Bianca bump: "${message}" | photo: ${photo} | excludeLists: ${excludedLists.length} | excludeUsers: ${allExcludeIds.length}`);

    const res = await fetch(`${OF_API_BASE}/${BIANCA_ACCOUNT_ID}/mass-messaging`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${OF_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      console.error(`❌ Bianca bump send failed: ${err}`);
      return;
    }

    const data = await res.json();
    const queueId = data?.data?.[0]?.id || data?.id || null;

    // 6. Store queue ID for next deletion
    if (queueId) {
      await redis.set('bianca:lastBumpQueueId', queueId);
    }

    biancaBumpState.lastBumpTime = new Date().toISOString();
    biancaBumpState.lastPhotoUsed = photo;
    biancaBumpState.lastExclusions = allExcludeIds;

    console.log(`📢 Bianca bump sent! queue: ${queueId} | photo: ${photo} | msg: "${message}"`);
  } catch (e) {
    console.error('❌ Bianca bump error:', e.message);
  }
}

// Cron: every hour at :00
cron.schedule('0 * * * *', runBiancaBump);

// Bump API endpoints
app.get('/bump/status', (req, res) => {
  const now = new Date();
  const nextBump = new Date(now);
  nextBump.setMinutes(0, 0, 0);
  nextBump.setHours(nextBump.getHours() + 1);

  res.json({
    enabled: biancaBumpState.enabled,
    lastBumpTime: biancaBumpState.lastBumpTime,
    lastPhotoUsed: biancaBumpState.lastPhotoUsed,
    nextBumpTime: nextBump.toISOString(),
    activeChatExclusions: biancaBumpState.lastExclusions.length,
    hardcodedExclusions: BIANCA_BUMP_EXCLUDE_IDS.length,
  });
});

app.post('/bump/enable', (req, res) => {
  biancaBumpState.enabled = true;
  console.log('📢 Bianca bump: ENABLED');
  res.json({ enabled: true, message: 'Bianca bump enabled' });
});

app.post('/bump/disable', (req, res) => {
  biancaBumpState.enabled = false;
  console.log('📢 Bianca bump: DISABLED');
  res.json({ enabled: false, message: 'Bianca bump disabled' });
});

app.get('/bump/photos', async (req, res) => {
  const photos = await getBiancaBumpPhotos();
  res.json({ count: photos.length, photos });
});

app.post('/bump/photos', async (req, res) => {
  const { vaultId } = req.body;
  if (!vaultId) return res.status(400).json({ error: 'vaultId required' });
  const photos = await getBiancaBumpPhotos();
  if (photos.includes(String(vaultId))) return res.json({ message: 'Already exists', count: photos.length, photos });
  photos.push(String(vaultId));
  await redis.set('bianca:bumpPhotos', photos);
  console.log(`📢 Bianca bump: added photo ${vaultId} (total: ${photos.length})`);
  res.json({ added: vaultId, count: photos.length, photos });
});

app.delete('/bump/photos/:id', async (req, res) => {
  const { id } = req.params;
  let photos = await getBiancaBumpPhotos();
  const before = photos.length;
  photos = photos.filter(p => p !== id);
  if (photos.length === before) return res.status(404).json({ error: 'Photo not found' });
  await redis.set('bianca:bumpPhotos', photos);
  console.log(`📢 Bianca bump: removed photo ${id} (total: ${photos.length})`);
  res.json({ removed: id, count: photos.length, photos });
});

app.post('/bump/run', async (req, res) => {
  console.log('📢 Manual Bianca bump trigger...');
  await runBiancaBump();
  res.json({ triggered: true, state: biancaBumpState });
});

// === VAULT ID TRACKING (prevent duplicate PPV sends) ===

// GET /fans/:accountId/:fanId/sent — all vault IDs ever sent to this fan
app.get('/fans/:accountId/:fanId/sent', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const sentVaultIds = await redis.smembers(`sent:${accountId}:${fanId}`) || [];
    res.json({ fanId, sentVaultIds });
  } catch (e) {
    console.error('Error getting sent vault IDs:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /fans/:accountId/:fanId/sent — log vault IDs sent to a fan
// If isFree=true, also adds to seen (fan received free content they can view)
app.post('/fans/:accountId/:fanId/sent', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const { vaultIds, isFree } = req.body;
    if (!vaultIds || !Array.isArray(vaultIds) || vaultIds.length === 0) {
      return res.status(400).json({ error: 'vaultIds array required' });
    }
    const key = `sent:${accountId}:${fanId}`;
    for (const id of vaultIds) {
      await redis.sadd(key, String(id));
    }
    // If free content, fan has actually seen it
    if (isFree) {
      const seenKey = `seen:${accountId}:${fanId}`;
      for (const id of vaultIds) {
        await redis.sadd(seenKey, String(id));
      }
    }
    const total = await redis.scard(key);
    res.json({ fanId, added: vaultIds, total, addedToSeen: !!isFree });
  } catch (e) {
    console.error('Error adding sent vault IDs:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /fans/:accountId/:fanId/purchased — all vault IDs purchased by this fan
app.get('/fans/:accountId/:fanId/purchased', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const purchasedVaultIds = await redis.smembers(`purchased:${accountId}:${fanId}`) || [];
    res.json({ fanId, purchasedVaultIds });
  } catch (e) {
    console.error('Error getting purchased vault IDs:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /fans/:accountId/:fanId/purchased — log vault IDs purchased by a fan
// Also adds to seen (purchased = fan has seen the content)
app.post('/fans/:accountId/:fanId/purchased', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const { vaultIds } = req.body;
    if (!vaultIds || !Array.isArray(vaultIds) || vaultIds.length === 0) {
      return res.status(400).json({ error: 'vaultIds array required' });
    }
    const purchasedKey = `purchased:${accountId}:${fanId}`;
    const seenKey = `seen:${accountId}:${fanId}`;
    for (const id of vaultIds) {
      await redis.sadd(purchasedKey, String(id));
      await redis.sadd(seenKey, String(id));
    }
    const total = await redis.scard(purchasedKey);
    res.json({ fanId, added: vaultIds, total, addedToSeen: true });
  } catch (e) {
    console.error('Error adding purchased vault IDs:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /fans/:accountId/:fanId/seen — vault IDs the fan has actually seen
app.get('/fans/:accountId/:fanId/seen', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const seenVaultIds = await redis.smembers(`seen:${accountId}:${fanId}`) || [];
    res.json({ fanId, seenVaultIds });
  } catch (e) {
    console.error('Error getting seen vault IDs:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /fans/:accountId/:fanId/seen — manually add to seen list
app.post('/fans/:accountId/:fanId/seen', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const { vaultIds } = req.body;
    if (!vaultIds || !Array.isArray(vaultIds) || vaultIds.length === 0) {
      return res.status(400).json({ error: 'vaultIds array required' });
    }
    const key = `seen:${accountId}:${fanId}`;
    for (const id of vaultIds) {
      await redis.sadd(key, String(id));
    }
    const total = await redis.scard(key);
    res.json({ fanId, added: vaultIds, total });
  } catch (e) {
    console.error('Error adding seen vault IDs:', e);
    res.status(500).json({ error: e.message });
  }
});

// Shared sync logic for a single fan
async function syncFanChatHistory(accountId, fanId) {
  const BIANCA_USER_ID = 525755724;
  let sentCount = 0;
  let purchasedCount = 0;
  let seenCount = 0;

  try {
    const msgRes = await fetch(`${OF_API_BASE}/${accountId}/chats/${fanId}/messages?limit=50`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    if (!msgRes.ok) {
      console.error(`Failed to fetch messages for fan ${fanId}: ${msgRes.status}`);
      return { fanId, synced: 0, purchased: 0, seen: 0, error: `HTTP ${msgRes.status}` };
    }
    const data = await msgRes.json();
    const messages = data.data || data.list || data.messages || data || [];

    for (const msg of messages) {
      const fromId = msg.fromUser?.id || msg.from_user?.id;
      if (fromId !== BIANCA_USER_ID) continue;

      const mediaFiles = msg.media || msg.mediaFiles || msg.media_files || [];
      if (mediaFiles.length === 0) continue;

      const mediaIds = mediaFiles.map(m => String(m.id || m));
      const price = msg.price || 0;
      const isOpened = msg.isOpened || msg.is_opened || false;
      const isFree = price === 0;

      // Always add to sent (master dedup list)
      for (const id of mediaIds) {
        await redis.sadd(`sent:${accountId}:${fanId}`, id);
        sentCount++;
      }

      // Determine if fan has SEEN this content:
      // - Free content with media → fan saw it
      // - Paid content that was opened → fan saw it
      // - Paid content NOT opened → fan only saw blurred preview (not seen)
      const hasSeen = (isFree && mediaFiles.length > 0) || (price > 0 && isOpened);

      if (hasSeen) {
        for (const id of mediaIds) {
          await redis.sadd(`seen:${accountId}:${fanId}`, id);
          seenCount++;
        }
      }

      // Check if purchased (opened + has price)
      if (isOpened && price > 0) {
        for (const id of mediaIds) {
          await redis.sadd(`purchased:${accountId}:${fanId}`, id);
          purchasedCount++;
        }
      }
    }
  } catch (e) {
    console.error(`Error syncing fan ${fanId}:`, e.message);
    return { fanId, synced: sentCount, purchased: purchasedCount, seen: seenCount, error: e.message };
  }

  return { fanId, synced: sentCount, purchased: purchasedCount, seen: seenCount };
}

// POST /fans/:accountId/:fanId/sync — sync sent vault IDs from chat history
app.post('/fans/:accountId/:fanId/sync', async (req, res) => {
  try {
    const { accountId, fanId } = req.params;
    const result = await syncFanChatHistory(accountId, fanId);
    res.json(result);
  } catch (e) {
    console.error('Error syncing fan:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /fans/:accountId/sync-all — sync ALL active fans
app.post('/fans/:accountId/sync-all', async (req, res) => {
  try {
    const { accountId } = req.params;
    const chatsRes = await fetch(`${OF_API_BASE}/${accountId}/chats?limit=50`, {
      headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
    });
    if (!chatsRes.ok) {
      return res.status(500).json({ error: `Failed to fetch chats: HTTP ${chatsRes.status}` });
    }
    const chatsData = await chatsRes.json();
    const chats = chatsData.data || chatsData.list || chatsData.chats || chatsData || [];

    const fans = [];
    for (const chat of chats) {
      const fanId = chat.fan?.id || chat.withUser?.id || chat.with_user?.id || chat.userId || chat.user_id || chat.id;
      if (!fanId) continue;
      const result = await syncFanChatHistory(accountId, String(fanId));
      fans.push(result);
      // Rate limit
      await new Promise(r => setTimeout(r, 500));
    }

    const totalSynced = fans.reduce((sum, f) => sum + f.synced, 0);
    res.json({ synced: totalSynced, fansProcessed: fans.length, chatsFound: chats.length, fans });
  } catch (e) {
    console.error('Error syncing all fans:', e);
    res.status(500).json({ error: e.message });
  }
});

// === WEBHOOK SYSTEM (OF API → Railway) ===
// NOTE: The actual POST /webhooks/onlyfans handler is registered earlier (line ~2426)
// It handles all events and writes to Redis pending/active queues.
// Below are the READ endpoints for the chatbot cron to consume.

// GET /webhooks/pending/:accountId — list fan IDs needing responses
app.get('/webhooks/pending/:accountId', async (req, res) => {
  try {
    const { accountId } = req.params;
    // Get all members with scores from sorted set
    const raw = await redis.zrange(`webhook:pending:${accountId}`, 0, -1, { withScores: true });

    // raw is [member, score, member, score, ...] or [{member, score}, ...]
    const pending = [];
    if (Array.isArray(raw)) {
      // Handle both formats
      if (raw.length > 0 && typeof raw[0] === 'object' && 'member' in raw[0]) {
        for (const item of raw) {
          const msg = await redis.get(`webhook:msg:${accountId}:${item.member}`);
          pending.push({ fanId: item.member, timestamp: item.score, message: msg || null });
        }
      } else {
        for (let i = 0; i < raw.length; i += 2) {
          const fanId = raw[i];
          const timestamp = raw[i + 1];
          const msg = await redis.get(`webhook:msg:${accountId}:${fanId}`);
          pending.push({ fanId, timestamp, message: msg || null });
        }
      }
    }

    res.json({ pending });
  } catch (e) {
    console.error('Error getting pending webhooks:', e);
    res.status(500).json({ error: e.message });
  }
});

// POST /webhooks/pending/:accountId/clear — remove processed fan IDs
app.post('/webhooks/pending/:accountId/clear', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { fanIds } = req.body;
    if (!Array.isArray(fanIds) || fanIds.length === 0) {
      return res.status(400).json({ error: 'fanIds array required' });
    }

    let removed = 0;
    for (const fanId of fanIds) {
      const r = await redis.zrem(`webhook:pending:${accountId}`, String(fanId));
      if (r) removed++;
      // Clean up stored message
      await redis.del(`webhook:msg:${accountId}:${fanId}`);
    }

    res.json({ removed, requested: fanIds.length });
  } catch (e) {
    console.error('Error clearing pending webhooks:', e);
    res.status(500).json({ error: e.message });
  }
});

// GET /webhooks/newsubs/:accountId — new subscriber fan IDs
app.get('/webhooks/newsubs/:accountId', async (req, res) => {
  try {
    const { accountId } = req.params;
    const members = await redis.smembers(`webhook:newsubs:${accountId}`);
    res.json({ newSubs: members || [] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /webhooks/newsubs/:accountId/clear — clear processed new subs
app.post('/webhooks/newsubs/:accountId/clear', async (req, res) => {
  try {
    const { accountId } = req.params;
    const { fanIds } = req.body;
    if (Array.isArray(fanIds) && fanIds.length > 0) {
      for (const fanId of fanIds) {
        await redis.srem(`webhook:newsubs:${accountId}`, String(fanId));
      }
      res.json({ cleared: fanIds.length });
    } else {
      // Clear all
      await redis.del(`webhook:newsubs:${accountId}`);
      res.json({ cleared: 'all' });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /webhooks/stats — webhook event counts
app.get('/webhooks/stats', (req, res) => {
  res.json(webhookStats);
});

// === CANARY DISPATCH SYSTEM (code-only, no LLM for dispatch) ===

const CANARY_CONFIG = {
  enabled: true,
  accountId: 'acct_54e3119e77da4429b6537f7dd2883a05',
  maxFansPerMinute: 2,
  maxOpusTotal: 120,
  canaryDurationMs: 90 * 60 * 1000, // 90 minutes
  fanLockTTL: 90, // seconds
};

// GET /dispatch/status — canary health check
app.get('/dispatch/status', async (req, res) => {
  try {
    const opusTotal = parseInt(await redis.get('canary:opus_total') || '0');
    const canaryStart = parseInt(await redis.get('canary:start_time') || '0');
    const elapsed = canaryStart ? Date.now() - canaryStart : 0;
    const minuteCount = parseInt(await redis.get('canary:minute_count') || '0');
    const eventsSeenCount = await redis.scard('canary:events_seen') || 0;
    const lockedFans = await redis.keys('fan_lock:*');
    const ofApiTotal = parseInt(await redis.get('canary:of_api_calls') || '0');
    const ofApiMinKey = `canary:of_api_min:${Math.floor(Date.now() / 60000)}`;
    const ofApiThisMin = parseInt(await redis.get(ofApiMinKey) || '0');
    const skipsTotal = parseInt(await redis.get('canary:skips_total') || '0');
    const sendsTotal = parseInt(await redis.get('canary:sends_total') || '0');
    const ppvsSent = parseInt(await redis.get('canary:ppvs_sent') || '0');
    
    res.json({
      canaryEnabled: CANARY_CONFIG.enabled,
      opusCallsTotal: opusTotal,
      opusLimit: CANARY_CONFIG.maxOpusTotal,
      fansThisMinute: minuteCount,
      minuteLimit: CANARY_CONFIG.maxFansPerMinute,
      elapsedMs: elapsed,
      canaryDurationMs: CANARY_CONFIG.canaryDurationMs,
      canaryExpired: canaryStart ? elapsed > CANARY_CONFIG.canaryDurationMs : false,
      eventsProcessed: eventsSeenCount,
      activeLocks: lockedFans.length,
      ofApiCalls: { total: ofApiTotal, thisMinute: ofApiThisMin },
      fanLockTTL: CANARY_CONFIG.fanLockTTL,
      eventSeenTTL: '48h',
      skipsTotal,
      sendsTotal,
      ppvsSent,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /dispatch/tick — code-only dispatcher, called by OpenClaw cron (systemEvent → exec curl)
// Returns { action: "spawn", fans: [...] } or { action: "skip", reason: "..." }
app.post('/dispatch/tick', async (req, res) => {
  try {
    const accountId = CANARY_CONFIG.accountId;

    // 1. Check canary limits
    if (!CANARY_CONFIG.enabled) return res.json({ action: 'skip', reason: 'canary disabled' });

    const opusTotal = parseInt(await redis.get('canary:opus_total') || '0');
    if (opusTotal >= CANARY_CONFIG.maxOpusTotal) {
      return res.json({ action: 'skip', reason: `opus limit reached (${opusTotal}/${CANARY_CONFIG.maxOpusTotal})` });
    }

    const canaryStart = parseInt(await redis.get('canary:start_time') || '0');
    if (canaryStart && (Date.now() - canaryStart) > CANARY_CONFIG.canaryDurationMs) {
      return res.json({ action: 'skip', reason: 'canary period expired' });
    }

    // 2. Check minute rate limit (sliding window)
    const minuteKey = `canary:minute:${Math.floor(Date.now() / 60000)}`;
    const minuteCount = parseInt(await redis.get(minuteKey) || '0');
    if (minuteCount >= CANARY_CONFIG.maxFansPerMinute) {
      return res.json({ action: 'skip', reason: `minute limit (${minuteCount}/${CANARY_CONFIG.maxFansPerMinute})` });
    }

    // 3. Read pending queue (DB/Redis only — NO OF API calls)
    const raw = await redis.zrange(`webhook:pending:${accountId}`, 0, -1, { withScores: true });
    if (!raw || raw.length === 0) {
      return res.json({ action: 'skip', reason: 'queue empty' });
    }

    // Parse pending into [{fanId, timestamp}]
    let pending = [];
    if (raw.length > 0 && typeof raw[0] === 'object' && 'member' in raw[0]) {
      pending = raw.map(r => ({ fanId: r.member, timestamp: r.score }));
    } else {
      for (let i = 0; i < raw.length; i += 2) {
        pending.push({ fanId: raw[i], timestamp: raw[i + 1] });
      }
    }

    // 4. Filter: skip locked fans, skip already-seen events
    const eligible = [];
    for (const fan of pending) {
      // Event idempotency (48h TTL keys)
      const eventKey = `${fan.fanId}:${fan.timestamp}`;
      const seen = await redis.get(`canary:seen:${eventKey}`);
      if (seen) continue;

      // Fan lock check
      const lockKey = `fan_lock:${accountId}:${fan.fanId}`;
      const locked = await redis.get(lockKey);
      if (locked) continue;

      eligible.push(fan);
      if (eligible.length >= 1) break; // canary: max 1 fan per tick
    }

    if (eligible.length === 0) {
      return res.json({ action: 'skip', reason: 'no eligible fans (all locked or seen)' });
    }

    // 5. Acquire locks + mark events seen
    for (const fan of eligible) {
      const lockKey = `fan_lock:${accountId}:${fan.fanId}`;
      await redis.set(lockKey, Date.now().toString(), { ex: CANARY_CONFIG.fanLockTTL });
      // Use per-event keys with 48h TTL instead of unbounded set
      await redis.set(`canary:seen:${fan.fanId}:${fan.timestamp}`, '1', { ex: 172800 });
      await redis.sadd('canary:events_seen', `${fan.fanId}:${fan.timestamp}`);
    }

    // 6. Increment counters
    await redis.incr('canary:opus_total');
    await redis.incr(minuteKey);
    await redis.expire(minuteKey, 120); // TTL 2 min
    if (!canaryStart) {
      await redis.set('canary:start_time', Date.now().toString());
    }

    // 7. Get message context for each fan
    const fansWithContext = [];
    for (const fan of eligible) {
      const msg = await redis.get(`webhook:msg:${accountId}:${fan.fanId}`);
      fansWithContext.push({ fanId: fan.fanId, timestamp: fan.timestamp, lastMessage: msg || null });
    }

    return res.json({ action: 'spawn', fans: fansWithContext });
  } catch (e) {
    console.error('❌ Dispatch tick error:', e.message);
    res.status(500).json({ action: 'error', reason: e.message });
  }
});

// POST /dispatch/track — log OF API calls + send/skip/ppv stats from Opus workers
app.post('/dispatch/track', async (req, res) => {
  try {
    const { ofApiCalls, sent, skipped, ppvsSent } = req.body;
    if (ofApiCalls) {
      await redis.incrby('canary:of_api_calls', ofApiCalls);
      const minKey = `canary:of_api_min:${Math.floor(Date.now() / 60000)}`;
      await redis.incrby(minKey, ofApiCalls);
      await redis.expire(minKey, 120);
    }
    if (sent) await redis.incrby('canary:sends_total', sent);
    if (skipped) await redis.incrby('canary:skips_total', skipped);
    if (ppvsSent) await redis.incrby('canary:ppvs_sent', ppvsSent);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /dispatch/complete — called after Opus worker finishes, releases lock + clears pending
app.post('/dispatch/complete', async (req, res) => {
  try {
    const { fanIds, accountId } = req.body;
    if (!fanIds || !accountId) return res.status(400).json({ error: 'fanIds and accountId required' });

    for (const fanId of fanIds) {
      // Release lock
      await redis.del(`fan_lock:${accountId}:${fanId}`);
      // Clear from pending queue
      await redis.zrem(`webhook:pending:${accountId}`, String(fanId));
    }

    res.json({ ok: true, cleared: fanIds.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /dispatch/abort — called on Opus error (429 etc), releases locks but keeps in queue
app.post('/dispatch/abort', async (req, res) => {
  try {
    const { fanIds, accountId, reason } = req.body;
    if (!fanIds || !accountId) return res.status(400).json({ error: 'fanIds and accountId required' });

    for (const fanId of fanIds) {
      await redis.del(`fan_lock:${accountId}:${fanId}`);
      // Remove from events_seen so they can be retried
      const members = await redis.smembers('canary:events_seen');
      for (const m of members) {
        if (m.startsWith(`${fanId}:`)) {
          await redis.srem('canary:events_seen', m);
        }
      }
    }

    // If it's a rate limit, disable canary
    if (reason === '429') {
      CANARY_CONFIG.enabled = false;
      console.log('🛑 Canary DISABLED due to 429');
    }

    res.json({ ok: true, released: fanIds.length, canaryEnabled: CANARY_CONFIG.enabled });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /dispatch/reset — reset canary counters for new run
app.post('/dispatch/reset', async (req, res) => {
  try {
    await redis.set('canary:opus_total', '0');
    await redis.set('canary:start_time', '0');
    await redis.set('canary:minute_count', '0');
    await redis.set('canary:sends_total', '0');
    await redis.set('canary:skips_total', '0');
    await redis.set('canary:ppvs_sent', '0');
    await redis.set('canary:of_api_calls', '0');
    CANARY_CONFIG.enabled = true;
    res.json({ ok: true, message: 'Canary counters reset, system enabled' });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// === OPENCLAW TUNNEL CONFIG ===
app.post('/openclaw/tunnel', async (req, res) => {
  const { url, token } = req.body;
  if (url) await redis.set('openclaw:tunnel_url', url);
  if (token) await redis.set('openclaw:hook_token', token);
  console.log(`🔗 OpenClaw tunnel updated: ${url}`);
  res.json({ ok: true, url, tokenSet: !!token });
});

app.get('/openclaw/tunnel', async (req, res) => {
  const url = await redis.get('openclaw:tunnel_url');
  const hasToken = !!(await redis.get('openclaw:hook_token'));
  res.json({ url, hasToken });
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
  
  // Run startup recovery (non-blocking so health check passes)
  startupRecovery().catch(e => console.error('❌ Startup recovery error:', e.message));

  // Auto-start biancawoods chatbot if enabled
  if (process.env.CHATBOT_BIANCA_ENABLED === 'true') {
    biancaChatbot.startChatbot(redis).then(result => {
      console.log('🤖 Bianca chatbot auto-start:', result.message);
    }).catch(e => {
      console.error('❌ Bianca chatbot auto-start failed:', e.message);
    });
  } else {
    console.log('🤖 Bianca chatbot: DISABLED (set CHATBOT_BIANCA_ENABLED=true to enable)');
  }

  // ── Bianca Bump Loop (standalone, no AI) ──────────────────────────────────
  // Sends a free mass message every hour with a random bump photo + caption.
  // Deletes the previous bump before sending a new one. Pure automation.
  const BIANCA_BUMP_PHOTOS = [
    "4295115634", "4295115608", "4271207724", "4128847737", "4118094254",
    "4118094218", "4084333700", "4084332834", "4084332833", "4084332827",
    "4084332825", "4084332375", "4084332371", "4084332368", "4084332364",
    "4084331945", "4084331943", "4084331942", "4083927398", "4083927388",
    "4083927385", "4083927380", "4083927378", "4083927375"
  ];
  const BIANCA_BUMP_CAPTIONS = [
    'heyyy u 💕 been thinking about u',
    'bored and looking cute rn 😏 wanna see?',
    'miss talking to u 🥺',
    'just took this for u 📸',
    'are u ignoring me 😤💕',
    'pssst 😘',
    'hiiii remember me? 🙈',
    'heyy how are u 😊',
    'hey babe what are u up to rn',
    'hiii 💕',
    'heyyy whatcha doing 😊',
    'hey handsome 😏',
    'bored rn... entertain me? 😊',
    'heyy stranger 💕',
    'thinking about u rn 😊',
    'hey cutie wyd 💕',
  ];
  const BIANCA_BUMP_ACCOUNT = 'acct_54e3119e77da4429b6537f7dd2883a05';
  const BIANCA_BUMP_EXCLUDE_LISTS = [1231455148, 1232110158, 1258116798, 1232588865, 1254929574];

  async function runBiancaBump() {
    try {
      console.log('📢 [bianca-bump] Running hourly bump...');
      const bumpState = await redis.get('bianca:bump_state') || {
        lastMessageId: null, recentCaptions: [], totalSent: 0
      };

      // Delete previous bump message
      if (bumpState.lastMessageId) {
        try {
          const delRes = await fetch(`${OF_API_BASE}/${BIANCA_BUMP_ACCOUNT}/mass-messages/${bumpState.lastMessageId}`, {
            method: 'DELETE',
            headers: { 'Authorization': `Bearer ${OF_API_KEY}` }
          });
          console.log(`🗑️ [bianca-bump] Deleted previous bump ${bumpState.lastMessageId}: ${delRes.status}`);
        } catch (e) {
          console.log(`⚠️ [bianca-bump] Could not delete previous bump: ${e.message}`);
        }
      }

      // Pick random photo + caption (avoid recent captions)
      const photo = BIANCA_BUMP_PHOTOS[Math.floor(Math.random() * BIANCA_BUMP_PHOTOS.length)];
      const recentSet = new Set(bumpState.recentCaptions || []);
      const availCaptions = BIANCA_BUMP_CAPTIONS.filter(c => !recentSet.has(c));
      const pool = availCaptions.length > 0 ? availCaptions : BIANCA_BUMP_CAPTIONS;
      const caption = pool[Math.floor(Math.random() * pool.length)];

      // Get active chat fan IDs to exclude (fans chatting with bot in last 2 hours)
      const activeMembers = await redis.zrangebyscore(`webhook:active:${BIANCA_BUMP_ACCOUNT}`, Date.now() - 2 * 3600000, '+inf');
      const excludeUserIds = (activeMembers || []).map(Number).filter(n => !isNaN(n));

      // Send free mass message
      const sendRes = await fetch(`${OF_API_BASE}/${BIANCA_BUMP_ACCOUNT}/mass-messages`, {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${OF_API_KEY}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({
          text: caption,
          mediaFiles: [photo],
          excludeListIds: BIANCA_BUMP_EXCLUDE_LISTS,
          excludeUserIds: [...new Set(excludeUserIds)]
        })
      });
      const sendData = await sendRes.json();
      const messageId = sendData?.data?.id || sendData?.id || sendData?.data?.[0]?.id || null;

      // Save state
      await redis.set('bianca:bump_state', {
        lastMessageId: messageId,
        lastBumpAt: new Date().toISOString(),
        lastCaption: caption,
        recentCaptions: [caption, ...(bumpState.recentCaptions || [])].slice(0, 4),
        totalSent: (bumpState.totalSent || 0) + 1
      });

      console.log(`📢 [bianca-bump] Sent: "${caption}" (msg ${messageId}, excluded ${excludeUserIds.length} active fans)`);
    } catch (e) {
      console.error('❌ [bianca-bump] Error:', e.message);
    }
  }

  // Run on the hour
  cron.schedule('0 * * * *', runBiancaBump);
  console.log('📢 [bianca-bump] Hourly bump loop started (standalone, no AI)');
});
