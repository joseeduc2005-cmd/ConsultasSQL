# Node 22 (>= 22.18) es obligatorio: el modulo src/soporte esta en TypeScript y se
# ejecuta sin compilar gracias al type-stripping nativo. Ver SOPORTE.md.

# ─── Stage 1: dependencies ───────────────────────────────────────────────────
FROM node:22-alpine AS deps
WORKDIR /app

# pnpm via corepack (project migrated from npm → pnpm)
RUN corepack enable

# Copy only the manifest + lockfile + npmrc for a cacheable install layer.
# --ignore-scripts: all backend prod deps are pure JS — oracledb v6 runs in Thin
# mode (verified: loads without its native build script), bcryptjs/mysql2/pg/
# mongodb are pure JS. Avoids pnpm 11's ERR_PNPM_IGNORED_BUILDS gate.
COPY package.json pnpm-lock.yaml .npmrc ./
RUN pnpm install --frozen-lockfile --prod --ignore-scripts

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
FROM node:22-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production

COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Remove local env so only docker env vars are used
RUN rm -f .env

EXPOSE 3002
CMD ["node", "src/app.js"]
