# Biancawoods Chatbot — Integration with index.js

## Add to index.js

### 1. After the existing requires (top of file), add:

```js
const biancaChatbot = require('./chatbot-engine');
```

### 2. After `app.use(express.json(...))`, add:

```js
// Initialize bianca chatbot with shared Redis client
biancaChatbot.init(redis);
```

### 3. After all existing endpoint definitions (before the webhook handler), add:

```js
// === BIANCAWOODS CHATBOT ENDPOINTS ===
app.get('/chatbot/bianca/status', (req, res) => biancaChatbot.statusHandler(req, res));
app.post('/chatbot/bianca/start', (req, res) => biancaChatbot.startHandler(req, res));
app.post('/chatbot/bianca/stop', (req, res) => biancaChatbot.stopHandler(req, res));
app.get('/chatbot/bianca/fans', (req, res) => biancaChatbot.fansHandler(req, res));
app.get('/chatbot/bianca/logs', (req, res) => biancaChatbot.logsHandler(req, res));
app.post('/chatbot/bianca/exclude/:fanId', (req, res) => biancaChatbot.excludeHandler(req, res));
app.delete('/chatbot/bianca/exclude/:fanId', (req, res) => biancaChatbot.unexcludeHandler(req, res));
```

### 4. In the webhook handler (`app.post('/webhooks/onlyfans', ...)`), add this inside the try block, after the millie chatbot handling:

```js
    // Biancawoods chatbot
    if (account_id === biancaChatbot.BIANCA_ACCOUNT_ID) {
      biancaChatbot.handleWebhookEvent(event, payload);
    }
```

### 5. In the startup section (inside `app.listen` callback), add at the end:

```js
  // Auto-start biancawoods chatbot if enabled
  if (process.env.CHATBOT_BIANCA_ENABLED === 'true') {
    biancaChatbot.startChatbot(redis).then(result => {
      console.log('🤖 Bianca chatbot auto-start:', result.message);
    }).catch(e => {
      console.error('❌ Bianca chatbot auto-start failed:', e.message);
    });
  }
```

## Environment Variables (add to Railway)

```env
CHATBOT_BIANCA_ENABLED=false    # Set to 'true' to enable
ANTHROPIC_API_KEY=<already set>  # Shared with millie chatbot
```

## Files to deploy

1. `chatbot-engine.js` — the new module (this file)
2. `research/chatbot-brain-v3.md` — must be accessible at `../research/chatbot-brain-v3.md` relative to index.js
3. `research/biancawoods-content-map.json` — must be accessible at `../research/biancawoods-content-map.json`

**Alternative:** Copy both files into the s4s-rotation-service directory if the research folder isn't deployed. The module has a fallback that tries the local directory too.

## Testing

1. Deploy with `CHATBOT_BIANCA_ENABLED=false`
2. Hit `POST /chatbot/bianca/start` to start manually
3. Check `GET /chatbot/bianca/status` for loop status
4. Hit `POST /chatbot/bianca/stop` to stop

## Redis Key Namespace

All keys use `chatbot:bianca:` prefix. See tech spec for full list.
