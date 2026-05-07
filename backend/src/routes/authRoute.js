// ─── Auth routes: /api/auth/* ─────────────────────────────────────────────────
import { Router } from 'express';
import bcrypt from 'bcryptjs';
import { sessions } from '../lib/session.js';

/**
 * @param {{ pool: import('pg').Pool }} deps
 */
export function createAuthRouter({ pool }) {
  const router = Router();

  // POST /api/auth/login
  router.post('/login', async (req, res) => {
    try {
      const { username, password } = req.body;

      if (!username || !password) {
        return res.status(400).json({ error: 'Usuario y contraseña requeridos' });
      }

      const result = await pool.query(
        'SELECT id, username, password, role FROM users WHERE username = $1',
        [username]
      );

      if (result.rowCount === 0) {
        return res.status(401).json({ success: false, error: 'Usuario o contraseña incorrecta' });
      }

      const user = result.rows[0];
      const passwordValid = await bcrypt.compare(password, user.password);

      if (!passwordValid) {
        return res.status(401).json({ success: false, error: 'Usuario o contraseña incorrecta' });
      }

      const token = 'mock-jwt-token-' + Date.now();
      const sessionUser = {
        id: user.id,
        username: user.username,
        name: user.username,
        role: user.role,
      };

      sessions.set(token, sessionUser);

      return res.status(200).json({ success: true, token, user: sessionUser });
    } catch (error) {
      console.error('[AUTH] ❌ Error login:', error);
      return res.status(500).json({ error: 'Error en login' });
    }
  });

  // POST /api/auth/register
  router.post('/register', (req, res) => {
    try {
      const { username, email, password } = req.body;

      if (!username || !email || !password) {
        return res.status(400).json({ error: 'Usuario, email y contraseña requeridos' });
      }

      const token = 'mock-jwt-token-' + Date.now();

      return res.status(201).json({
        success: true,
        token,
        user: {
          id: Date.now(),
          username,
          email,
          name: username,
          role: 'user',
        },
      });
    } catch {
      return res.status(500).json({ error: 'Error en registro' });
    }
  });

  // GET /api/auth/me
  router.get('/me', (req, res) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader) {
        return res.status(401).json({ error: 'No autorizado' });
      }

      const token = authHeader.replace('Bearer ', '').trim();
      const user = sessions.get(token);

      if (!user) {
        return res.status(401).json({ error: 'Sesión no válida o expirada' });
      }

      return res.status(200).json({ success: true, user });
    } catch {
      return res.status(500).json({ error: 'Error obteniendo usuario' });
    }
  });

  return router;
}
