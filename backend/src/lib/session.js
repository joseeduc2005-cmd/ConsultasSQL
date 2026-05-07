// ─── Session store & request helpers ─────────────────────────────────────────
// Shared between auth, content, and any route that needs the current user.

export const sessions = new Map();

export function getUserFromRequest(req) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.replace('Bearer ', '').trim();
  return sessions.get(token) || null;
}
