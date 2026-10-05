/**
 * El script estructurado de un artículo: qué acciones admite y cómo se leen sus
 * parámetros.
 *
 * Un artículo puede traer su solución escrita como datos en vez de como SQL: una
 * acción con nombre y unos parámetros. La lista de acciones es cerrada a propósito —
 * añadir una obliga a escribirla aquí— y eso es lo que impide que algo que llegue en
 * una petición acabe siendo el nombre de lo que se ejecuta.
 *
 * Es código puro: valida y normaliza, no ejecuta nada.
 */
import { sanitizeStructuredParamValue, validateStructuredParamByName } from '../sql/construccion.js';

export const MANUAL_SQL_TIMEOUT_MS = Math.max(1000, Number(process.env.SQL_QUERY_TIMEOUT_MS) || 120000);

export const EXECUTABLE_SOLUTION_TYPES = new Set(['ejecutable', 'database', 'script']);

/**
 * Los nombres con los que se puede pedir cada parametro de una accion.
 *
 * Son datos del negocio: que «usuario» y «username» signifiquen lo mismo es una
 * decision de como se escriben los articulos, no un detalle tecnico. Lo que *hace*
 * cada accion no esta aqui — eso lo une app.js, que es quien tiene acceso a la base.
 */
export const ALIAS_DE_ACCION = {
  verificar_conexion: {
    usuario: 'username',
    username: 'username',
  },
  desbloquear_cuenta: {
    usuario: 'username',
    username: 'username',
    motivo: 'motivo',
  },
  limpiar_sesiones: {
    usuario: 'username',
    username: 'username',
    horas: 'horas',
  },
  verificar_estado_cuenta: {
    usuario: 'username',
    username: 'username',
    incluir_logs: 'incluir_logs',
  },
};

export function parseStructuredScript(scriptJson) {
  if (!scriptJson) {
    return null;
  }

  if (typeof scriptJson === 'object') {
    return scriptJson;
  }

  if (typeof scriptJson === 'string') {
    try {
      return JSON.parse(scriptJson);
    } catch {
      return null;
    }
  }

  return null;
}

export function normalizeAndValidateStructuredParams(script, rawParams = {}) {
  const normalizedParams = Object.entries(rawParams || {}).reduce((acc, [key, value]) => {
    acc[key] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});

  Object.entries(normalizedParams).forEach(([paramName, value]) => {
    validateStructuredParamByName(paramName, value);
  });

  return normalizedParams;
}

export const MANUAL_SQL_NAMED_PLACEHOLDER_REGEX = /(^|[^:]):([a-zA-Z_][a-zA-Z0-9_]*)/g;

export function normalizeStructuredParams(actionName, params) {
  // Solo los alias: lo que la accion *hace* no hace falta para normalizar como se
  // llaman sus parametros, y pedirlo ataria este modulo a algo que abre conexiones.
  const aliases = ALIAS_DE_ACCION[actionName];
  if (!aliases) {
    return {};
  }

  return Object.entries(params || {}).reduce((acc, [key, value]) => {
    const mappedKey = aliases[key] || key;
    acc[mappedKey] = sanitizeStructuredParamValue(value);
    return acc;
  }, {});
}
