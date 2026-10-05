/**
 * El caché de consultas, en memoria.
 *
 * Guarda el resultado de una consulta para no repetirla, con dos formas de caducar:
 * por tiempo y por versión de los datos. La segunda es la que importa — cuando algo
 * escribe en la base, se sube la versión y todo lo guardado antes deja de valer de
 * golpe. Sin eso, alguien podría ver un saldo viejo justo después de que otro lo
 * cambiara, y eso en una cooperativa no es un detalle.
 *
 * Es un adaptador y no dominio porque lo que guarda vive en memoria de este proceso:
 * si mañana el caché pasa a Redis, cambia este fichero y nada más.
 */
// La version de los datos. Vive aqui dentro y solo sale por accesor: exportar el
// `let` dejaria que quien lo importe se quede con el numero viejo justo despues de
// una escritura, que es precisamente lo que este contador existe para evitar.
let QUERY_CACHE_DATA_VERSION = 1;

export const getQueryCacheDataVersion = () => QUERY_CACHE_DATA_VERSION;

export const INTELLIGENT_QUERY_CACHE = new Map();

export const QUERY_CACHE_TTL_MS = 60 * 1000;

export function bumpQueryCacheDataVersion() {
  QUERY_CACHE_DATA_VERSION += 1;
  INTELLIGENT_QUERY_CACHE.clear();
}

export function getQueryCacheEntry(cacheKey) {
  const found = INTELLIGENT_QUERY_CACHE.get(cacheKey);
  if (!found) return null;
  if (found.expiresAt < Date.now()) {
    INTELLIGENT_QUERY_CACHE.delete(cacheKey);
    return null;
  }
  if (found.dataVersion !== QUERY_CACHE_DATA_VERSION) {
    INTELLIGENT_QUERY_CACHE.delete(cacheKey);
    return null;
  }
  return found.value;
}

export function setQueryCacheEntry(cacheKey, value, ttlMs = QUERY_CACHE_TTL_MS) {
  INTELLIGENT_QUERY_CACHE.set(cacheKey, {
    dataVersion: QUERY_CACHE_DATA_VERSION,
    expiresAt: Date.now() + ttlMs,
    value,
  });
}
