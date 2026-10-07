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

/**
 * Los tipos de artículo que ejecutan algo.
 *
 * «dinamico» es el nombre de ahora: un artículo que pide los datos que le falten —una
 * cédula, un número de transacción— y consulta. Los tres nombres viejos siguen
 * aceptándose para que no quede sin abrir contenido ya creado, y significan lo mismo:
 * solo lectura. Lo que cambió no es el nombre, es que ninguno escribe.
 */
export const SOLUTION_TYPE_DINAMICO = 'dinamico';

export const EXECUTABLE_SOLUTION_TYPES = new Set([
  SOLUTION_TYPE_DINAMICO,
  'ejecutable', 'database', 'script',
]);

/** Lo que se ofrece al crear un artículo: los viejos se aceptan, no se ofrecen. */
export const SOLUTION_TYPES_OFRECIDOS = ['lectura', SOLUTION_TYPE_DINAMICO];

/** Todo lo que se admite al guardar, nombres viejos incluidos. */
export const SOLUTION_TYPES_ACEPTADOS = ['lectura', ...EXECUTABLE_SOLUTION_TYPES];

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
