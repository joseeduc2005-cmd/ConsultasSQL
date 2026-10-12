# Version fijada a proposito. El modulo de soporte corre TypeScript sin compilar y
# eso necesita Node >= 22.18; «node:22-alpine» es flotante y el dia que resolviera
# a 22.17 el contenedor no arrancaria. Al subir de version, probar el arranque.
# ─── Stage 1: dependencies ───────────────────────────────────────────────────
FROM node:22.18-alpine AS deps
WORKDIR /app

# pnpm via corepack (project migrated from npm → pnpm)
RUN corepack enable

# --ignore-scripts: sharp and @next/swc ship prebuilt musl binaries as separate
# packages (no build script needed) — verified present after install. Avoids
# pnpm 11's ERR_PNPM_IGNORED_BUILDS gate.
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile --ignore-scripts

# ─── Stage 2: builder ────────────────────────────────────────────────────────
FROM node:22.18-alpine AS builder
WORKDIR /app

RUN corepack enable

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Remove local env files so only Dockerfile ENV values are used
RUN rm -f .env .env.local .env.production .env.production.local

ENV BACKEND_URL=http://backend:3002
ENV NEXT_TELEMETRY_DISABLED=1

RUN pnpm build

# ─── Stage 3: runner ─────────────────────────────────────────────────────────
FROM node:22.18-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV BACKEND_URL=http://backend:3002

RUN corepack enable

# Copy everything needed to run `next start`
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/next.config.mjs ./next.config.mjs
COPY --from=builder /app/public ./public

EXPOSE 3000
CMD ["pnpm", "start"]
