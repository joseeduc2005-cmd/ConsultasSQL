/**
 * Logger centralizado — Winston con rotación diaria
 *
 * Niveles: error > warn > info > http > debug
 *
 * En producción (NODE_ENV=production):
 *   - Salida JSON estructurada a stdout (para Docker logs / CloudWatch)
 *   - Archivos rotativos en /app/logs/ (retención 14 días, máx 20 MB por archivo)
 *
 * En desarrollo:
 *   - Salida colorida a consola
 *   - Sin archivos (para no ensuciar el equipo local)
 *
 * Uso:
 *   import logger from '../infrastructure/logger/logger.js';
 *   logger.info('Mensaje', { requestId, userId, tabla });
 *   logger.error('Fallo', { requestId, error: err.message, stack: err.stack });
 */

import { createRequire } from 'module';
const require = createRequire(import.meta.url);

const winston = require('winston');
require('winston-daily-rotate-file');

const { combine, timestamp, json, colorize, printf, errors } = winston.format;

const IS_PROD = process.env.NODE_ENV === 'production';
const LOG_LEVEL = process.env.LOG_LEVEL || (IS_PROD ? 'info' : 'debug');
const LOG_DIR = process.env.LOG_DIR || '/app/logs';

/* ─── Formato de consola (dev) ─────────────────────────────────────── */
const devFormat = combine(
  colorize({ all: true }),
  timestamp({ format: 'HH:mm:ss' }),
  errors({ stack: true }),
  printf(({ level, message, timestamp, requestId, ...meta }) => {
    const rid = requestId ? ` [${requestId}]` : '';
    const extras = Object.keys(meta).length
      ? ' ' + JSON.stringify(meta, null, 0)
      : '';
    return `${timestamp}${rid} ${level}: ${message}${extras}`;
  }),
);

/* ─── Formato JSON (producción / archivos) ─────────────────────────── */
const prodFormat = combine(
  timestamp(),
  errors({ stack: true }),
  json(),
);

/* ─── Transportes ──────────────────────────────────────────────────── */
const transports = [];

// Siempre loguear a stdout (Docker lo recoge con `docker logs`)
transports.push(
  new winston.transports.Console({
    format: IS_PROD ? prodFormat : devFormat,
  }),
);

// En producción también escribir archivos con rotación
if (IS_PROD) {
  // Todos los logs (info+) — rota diario, guarda 14 días, máx 20 MB/archivo
  transports.push(
    new winston.transports.DailyRotateFile({
      dirname: LOG_DIR,
      filename: 'app-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,          // comprime los archivos viejos con gzip
      maxSize: '20m',               // rota si el archivo llega a 20 MB
      maxFiles: '14d',              // elimina archivos más viejos de 14 días
      level: 'info',
      format: prodFormat,
    }),
  );

  // Errores por separado para búsqueda rápida
  transports.push(
    new winston.transports.DailyRotateFile({
      dirname: LOG_DIR,
      filename: 'error-%DATE%.log',
      datePattern: 'YYYY-MM-DD',
      zippedArchive: true,
      maxSize: '10m',
      maxFiles: '30d',              // errores se conservan 30 días
      level: 'error',
      format: prodFormat,
    }),
  );
}

/* ─── Instancia del logger ─────────────────────────────────────────── */
const logger = winston.createLogger({
  level: LOG_LEVEL,
  transports,
  // No salir del proceso en excepciones de logging
  exitOnError: false,
});

/* ─── Helper: logger con contexto fijo (request-scoped) ────────────── */
/**
 * Crea un child logger con requestId y campos extra fijos.
 * Útil dentro de middlewares y handlers de rutas.
 *
 * @param {object} ctx  — e.g. { requestId, userId, route }
 * @returns {winston.Logger}
 */
export function childLogger(ctx = {}) {
  return logger.child(ctx);
}

export default logger;
