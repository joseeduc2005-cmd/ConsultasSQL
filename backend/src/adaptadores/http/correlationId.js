/**
 * Middleware: correlationId
 *
 * Asigna un ID único a cada request HTTP.
 * Si el cliente ya envió X-Request-Id (p.ej. desde el proxy Next.js),
 * se reutiliza ese ID para poder correlacionar frontend ↔ backend.
 *
 * El ID queda disponible en:
 *   - req.requestId          → para usarlo en handlers / servicios
 *   - res.setHeader(...)     → se devuelve al cliente para trazabilidad
 *   - req.logger             → child logger con requestId pre-cargado
 *
 * Uso en app.js:
 *   import { correlationIdMiddleware } from './middleware/correlationId.js';
 *   app.use(correlationIdMiddleware);
 */

import { randomUUID } from 'crypto';
import logger, { childLogger } from '../../infrastructure/logger/logger.js';

export function correlationIdMiddleware(req, res, next) {
  // Reutilizar ID del cliente o generar uno nuevo
  const requestId = req.headers['x-request-id'] || randomUUID();

  // Adjuntar al request para que los handlers lo usen
  req.requestId = requestId;

  // Devolver al cliente para que pueda correlacionar errores
  res.setHeader('X-Request-Id', requestId);

  // Child logger con el requestId ya cargado — los handlers solo hacen:
  //   req.logger.info('mensaje', { campo: valor })
  req.logger = childLogger({
    requestId,
    method: req.method,
    path: req.path,
  });

  // Log de entrada de la request (nivel http, muy verboso)
  req.logger.http('→ request', {
    ip: req.ip,
    userAgent: req.headers['user-agent'],
  });

  // Registrar duración al cerrar la respuesta
  const startMs = Date.now();
  res.on('finish', () => {
    const ms = Date.now() - startMs;
    const level = res.statusCode >= 500 ? 'error'
      : res.statusCode >= 400 ? 'warn'
      : 'http';
    req.logger[level]('← response', {
      status: res.statusCode,
      ms,
    });
  });

  next();
}
