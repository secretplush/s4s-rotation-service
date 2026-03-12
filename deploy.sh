#!/bin/bash
# deploy.sh — Push + Railway redeploy + verify
# Usage: ./deploy.sh [commit message]

set -e

cd "$(dirname "$0")"

MSG="${*:-auto deploy}"
RAILWAY_API="https://backboard.railway.app/graphql/v2"
TOKEN="edfbb3f6-b95c-48fc-bd11-514c0890bcbe"
SERVICE_ID="9c046639-4376-4f79-a94c-b288c9740b7a"
ENV_ID="26dab490-d89f-4cff-8105-c3bdf5c92222"
WORKER_URL="https://s4s-worker-production.up.railway.app"

echo "📦 Committing..."
git add -A
git commit -m "$MSG" || echo "Nothing to commit"

echo "⬆️  Pushing to GitHub..."
git push origin main

echo "🚀 Deploying to Railway (railway up)..."
railway up --detach
# railway up uploads local code and triggers a new build
# serviceInstanceRedeploy only RESTARTS the old build — DO NOT USE

echo "⏳ Waiting for build + deploy (90s)..."
sleep 90

echo "🔍 Verifying deployment..."
for i in 1 2 3 4 5; do
  STATS=$(curl -s --max-time 10 "$WORKER_URL/stats" 2>&1)
  if echo "$STATS" | grep -q '"isRunning"'; then
    STARTED=$(echo "$STATS" | grep -o '"startedAt":"[^"]*"')
    echo "✅ Live! $STARTED"
    echo "$STATS" | python3 -m json.tool 2>/dev/null | head -20
    exit 0
  fi
  echo "   Retry $i/5..."
  sleep 10
done

echo "⚠️  Worker not responding after 90s — check Railway dashboard"
exit 1
