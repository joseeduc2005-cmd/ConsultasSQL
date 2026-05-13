// ─── Auth routes: /api/auth/* ─────────────────────────────────────────────────
import { Router } from 'express';
import bcrypt from 'bcryptjs';
import { sessions } from '../lib/session.js';

/**
 * @param {{ pool: import('pg').Pool, logLoginEvent: Function }} deps
 */
export function createAuthRouter({ pool, logLoginEvent }) {
  const router = Router();

  const getIp = (req) =>
    req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket?.remoteAddress || null;
  const getUA = (req) =>
    req.headers['user-agent'] ? String(req.headers['user-agent']).substring(0, 300) : null;

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
        // Auditoría: login fallido — usuario no existe
        if (typeof logLoginEvent === 'function') {
          logLoginEvent({ username, rol: null, ip: getIp(req), userAgent: getUA(req), estado: 'fail', detalle: 'Usuario no encontrado' }).catch(() => {});
        }
        return res.status(401).json({ success: false, error: 'Usuario o contraseña incorrecta' });
      }

      const user = result.rows[0];
      const passwordValid = await bcrypt.compare(password, user.password);

      if (!passwordValid) {
        // Auditoría: login fallido — contraseña incorrecta
        if (typeof logLoginEvent === 'function') {
          logLoginEvent({ username, rol: user.role, ip: getIp(req), userAgent: getUA(req), estado: 'fail', detalle: 'Contraseña incorrecta' }).catch(() => {});
        }
        return res.status(401).json({ success: false, error: 'Usuario o contraseña incorrecta' });
      }

      const token = 'mock-jwt-token-' + Date.now();
      const sessionUser = {
        id: user.id,
        username: user.username,
        name: user.username,
        role: user.role,
      };

      // Guardar en memoria (cache rápido)
      sessions.set(token, sessionUser);

      // Guardar en BD (persiste reinicios del backend)
      try {
        await pool.query(
          'INSERT INTO sessions (user_id, token, activo) VALUES ($1, $2, true)',
          [user.id, token]
        );
      } catch (dbErr) {
        console.warn('[AUTH] ⚠️ No se pudo guardar sesión en BD:', dbErr?.message);
      }

      // Auditoría: login exitoso
      if (typeof logLoginEvent === 'function') {
        logLoginEvent({ username: user.username, rol: user.role, ip: getIp(req), userAgent: getUA(req), estado: 'ok', detalle: null }).catch(() => {});
      }

      return res.status(200).json({ success: true, token, user: sessionUser });
    } catch (error) {
      console.error('[AUTH] ❌ Error login:', error);
      return res.status(500).json({ error: 'Error en login' });
    }
  });

  // POST /api/auth/register
  router.post('/register', async (req, res) => {
    try {
      const { username, email, password, role } = req.body;

      if (!username || !password) {
        return res.status(400).json({ error: 'Usuario y contraseña son requeridos' });
      }

      // Verificar si el usuario ya existe
      const existing = await pool.query(
        'SELECT id FROM users WHERE username = $1',
        [username]
      );
      if (existing.rowCount > 0) {
        return res.status(409).json({ success: false, error: 'El usuario ya existe' });
      }

      // Hash de la contraseña con bcrypt (10 rounds)
      const hashedPassword = await bcrypt.hash(password, 10);

      // Insertar en la BD — el rol solo puede ser 'user' salvo que se pase explícitamente
      const allowedRole = role === 'admin' ? 'admin' : 'user';
      const insertResult = await pool.query(
        `INSERT INTO users (username, password, role)
         VALUES ($1, $2, $3)
         RETURNING id, username, role`,
        [username, hashedPassword, allowedRole]
      );

      const newUser = insertResult.rows[0];
      const token = 'mock-jwt-token-' + Date.now();
      const sessionUser = {
        id: newUser.id,
        username: newUser.username,
        name: newUser.username,
        role: newUser.role,
      };

      // Guardar sesión en memoria y en BD
      sessions.set(token, sessionUser);
      try {
        await pool.query(
          'INSERT INTO sessions (user_id, token, activo) VALUES ($1, $2, true)',
          [newUser.id, token]
        );
      } catch (dbErr) {
        console.warn('[AUTH] ⚠️ No se pudo guardar sesión en BD:', dbErr?.message);
      }

      return res.status(201).json({ success: true, token, user: sessionUser });
    } catch (error) {
      console.error('[AUTH] ❌ Error registro:', error);
      return res.status(500).json({ error: 'Error en registro' });
    }
  });

  // GET /api/auth/me
  router.get('/me', async (req, res) => {
    try {
      const authHeader = req.headers.authorization;
      if (!authHeader) {
        return res.status(401).json({ error: 'No autorizado' });
      }

      const token = authHeader.replace('Bearer ', '').trim();

      // 1. Buscar en cache en memoria
      let user = sessions.get(token);

      // 2. Si no está en memoria, buscar en BD (por si el backend se reinició)
      if (!user) {
        const dbResult = await pool.query(
          `SELECT u.id, u.username, u.role
           FROM sessions s
           JOIN users u ON s.user_id = u.id
           WHERE s.token = $1 AND s.activo = true
           LIMIT 1`,
          [token]
        );

        if (dbResult.rowCount === 0) {
          return res.status(401).json({ error: 'Sesión no válida o expirada' });
        }

        const row = dbResult.rows[0];
        user = { id: row.id, username: row.username, name: row.username, role: row.role };
        sessions.set(token, user); // Restaurar en cache
      }

      return res.status(200).json({ success: true, user });
    } catch (error) {
      console.error('[AUTH] ❌ Error /me:', error);
      return res.status(500).json({ error: 'Error obteniendo usuario' });
    }
  });

  return router;
}
