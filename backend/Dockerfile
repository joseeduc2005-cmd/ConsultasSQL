# ─── Stage 1: dependencies ───────────────────────────────────────────────────
FROM node:20-alpine AS deps
WORKDIR /app

# oracledb v6+ runs in Thin mode (pure JS) — no Oracle Instant Client needed
COPY package*.json ./
RUN npm ci --omit=dev

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
FROM node:20-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Remove local .env so only docker env vars are used
RUN rm -f .env

EXPOSE 3002
CMD ["node", "src/app.js"]
