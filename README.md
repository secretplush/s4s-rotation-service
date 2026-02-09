# S4S Rotation Service

24/7 ghost tag rotation for OnlyFans S4S campaigns.

## Features

- **Automated posting**: Creates ghost tags with @mentions
- **Auto-delete**: Removes posts after 5 minutes
- **Daily randomization**: Fresh schedule each day
- **56 tags/day per model**: ~26 min intervals
- **Cloud-ready**: Runs on Railway, Render, or any VPS

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Service status |
| POST | `/start` | Start rotation |
| POST | `/stop` | Stop rotation |
| GET | `/schedule` | View upcoming tags |
| GET | `/stats` | View statistics |
| GET | `/health` | Health check |

## Environment Variables

```
OF_API_KEY=ofapi_xxxxx
KV_REST_API_URL=https://xxxxx.upstash.io
KV_REST_API_TOKEN=xxxxx
PORT=3000
```

## Deploy to Railway

1. Push to GitHub
2. Connect repo to Railway
3. Add environment variables
4. Deploy!

## Usage

```bash
# Start rotation
curl -X POST https://your-app.railway.app/start

# Check status
curl https://your-app.railway.app/stats

# View upcoming tags
curl https://your-app.railway.app/schedule

# Stop rotation
curl -X POST https://your-app.railway.app/stop
```
